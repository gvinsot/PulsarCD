"""Versioned recovery material. Only authenticated ciphertext reaches PostgreSQL.

No secret-bearing exception from a driver, filesystem or crypto library may escape
this module. A recovery copy of ENCRYPTION_KEY must exist outside these backups.
"""

import base64
import hashlib
import json
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


class BackupError(Exception):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class Vault:
    def __init__(self, connection_string, encryption_key, scope="production"):
        self.connection_string = connection_string
        self.scope = scope
        try:
            raw = encryption_key.strip()
            # Accept the existing deployment convention: 32 random bytes, hex
            # or base64. Derive a separate application key, never reuse the
            # master directly for another ciphertext format.
            try:
                decoded = bytes.fromhex(raw) if len(raw) == 64 else b""
            except ValueError:
                decoded = b""
            if not decoded:
                decoded = base64.b64decode(raw, validate=True)
            if len(decoded) < 32 or not connection_string or not scope:
                raise ValueError()
            key = HKDF(algorithm=hashes.SHA256(), length=32, salt=None,
                       info=b"pulsarcd/recovery/v1").derive(decoded)
            self.active = hashlib.sha256(key).hexdigest()[:24]
            self.keys = {self.active: key}
        except Exception:
            raise BackupError("Backup configuration/ENCRYPTION_KEY is invalid") from None

    @contextmanager
    def connect(self):
        try:
            import psycopg
            from psycopg.rows import dict_row
            with psycopg.connect(self.connection_string, connect_timeout=10,
                                 row_factory=dict_row,
                                 options="-c statement_timeout=15000 -c lock_timeout=10000") as conn:
                yield conn
        except BackupError:
            raise
        except Exception:
            raise BackupError("Backup database operation failed") from None

    def initialize(self):
        with self.connect() as conn:
            # Serialize first boot if several processes initialize together.
            conn.execute("SELECT pg_advisory_xact_lock(721046190)")
            conn.execute("""CREATE TABLE IF NOT EXISTS recovery_versions (
                sequence BIGSERIAL PRIMARY KEY,
                id TEXT UNIQUE NOT NULL,
                scope TEXT NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('env', 'ssh')),
                resource TEXT NOT NULL,
                metadata JSONB NOT NULL,
                nonce BYTEA NOT NULL,
                ciphertext BYTEA NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('pending', 'applied', 'observed'))
            )""")
            conn.execute("""CREATE INDEX IF NOT EXISTS recovery_versions_resource
                ON recovery_versions (scope, kind, resource, sequence DESC)""")

    def decrypt(self, row):
        try:
            meta = row["metadata"]
            # Bind both the row identity and the intended destination to the tag.
            for name in ("id", "scope", "kind", "resource"):
                if row[name] != meta[name]:
                    raise ValueError()
            if meta["scope"] != self.scope or meta["format"] != 1:
                raise ValueError()
            return AESGCM(self.keys[meta["key_id"]]).decrypt(
                bytes(row["nonce"]), bytes(row["ciphertext"]), canonical(meta))
        except Exception:
            raise BackupError("Backup cannot be authenticated with this ENCRYPTION_KEY") from None

    def save(self, kind, resource, content, *, source, actor="system", state="observed"):
        if kind not in ("env", "ssh") or state not in ("pending", "observed"):
            raise BackupError("Invalid backup kind or state")
        if not isinstance(content, bytes) or len(content) > 4 * 1024 * 1024:
            raise BackupError("Backup file exceeds the 4 MiB limit")
        meta = dict(format=1, id=str(uuid.uuid4()), scope=self.scope, kind=kind,
                    resource=resource, created_at=datetime.now(timezone.utc).isoformat(),
                    key_id=self.active, source=source, actor=actor)
        nonce = os.urandom(12)
        ciphertext = AESGCM(self.keys[self.active]).encrypt(nonce, content, canonical(meta))
        with self.connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                         (canonical([self.scope, kind, resource]).decode(),))
            if state == "observed":
                previous = conn.execute("""SELECT * FROM recovery_versions
                    WHERE scope=%s AND kind=%s AND resource=%s AND state <> 'pending'
                    ORDER BY sequence DESC LIMIT 1""", (self.scope, kind, resource)).fetchone()
                if previous and self.decrypt(previous) == content:
                    return previous["id"]
            conn.execute("""INSERT INTO recovery_versions
                (id, scope, kind, resource, metadata, nonce, ciphertext, state)
                VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s)""",
                (meta["id"], self.scope, kind, resource, canonical(meta).decode(),
                 nonce, ciphertext, state))
        return meta["id"]

    def mark_applied(self, revision):
        with self.connect() as conn:
            result = conn.execute("""UPDATE recovery_versions SET state='applied'
                WHERE id=%s AND scope=%s AND state='pending'""", (revision, self.scope))
            if result.rowcount != 1:
                raise BackupError("Backup revision could not be marked applied")

    def read(self, kind, resource, revision=None):
        with self.connect() as conn:
            if revision:
                row = conn.execute("""SELECT * FROM recovery_versions
                    WHERE scope=%s AND kind=%s AND resource=%s AND id=%s""",
                    (self.scope, kind, resource, revision)).fetchone()
            else:
                row = conn.execute("""SELECT * FROM recovery_versions
                    WHERE scope=%s AND kind=%s AND resource=%s AND state <> 'pending'
                    ORDER BY sequence DESC LIMIT 1""", (self.scope, kind, resource)).fetchone()
        if not row:
            raise BackupError("Backup revision not found")
        return self.decrypt(row)

    def history(self, kind, resource):
        with self.connect() as conn:
            rows = conn.execute("""SELECT metadata, state FROM recovery_versions
                WHERE scope=%s AND kind=%s AND resource=%s
                ORDER BY sequence DESC LIMIT 100""", (self.scope, kind, resource)).fetchall()
        return [dict(row["metadata"], state=row["state"]) for row in rows]

    def resources(self):
        with self.connect() as conn:
            return conn.execute("""SELECT DISTINCT kind, resource FROM recovery_versions
                WHERE scope=%s AND state <> 'pending' ORDER BY kind, resource""",
                (self.scope,)).fetchall()


@lru_cache(maxsize=4)
def _configured_vault(connection_string, encryption_key, scope):
    vault = Vault(connection_string, encryption_key, scope)
    vault.initialize()
    return vault


def get_vault():
    if os.environ.get("PULSARCD_BACKUP__ENABLED", "false").lower() != "true":
        return None
    try:
        key_file = os.environ.get("ENCRYPTION_KEY_FILE")
        key = Path(key_file).read_text().strip() if key_file else os.environ.get("ENCRYPTION_KEY", "")
        if not key and Path("/run/secrets/ENCRYPTION_KEY").is_file():
            key = Path("/run/secrets/ENCRYPTION_KEY").read_text().strip()
        return _configured_vault(
            os.environ.get("PULSARCD_BACKUP__CONNECTION_STRING")
            or os.environ.get("DATABASE_CONNECTION_STRING", ""), key,
            os.environ.get("PULSARCD_BACKUP__SCOPE", "production"))
    except BackupError:
        raise
    except Exception:
        raise BackupError("Unable to load backup credentials") from None
