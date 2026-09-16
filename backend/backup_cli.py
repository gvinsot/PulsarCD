"""Recovery without the PulsarCD API: python -m backend.backup_cli --help."""

import argparse
import json
import os
from pathlib import Path

from shared.secrets import load_docker_secrets_into_env
from .backup_vault import BackupError, get_vault
from .recovery_files import atomic_write, safe_path


def restore(vault, output_dir, resources, revision=None):
    root = Path(output_dir)
    # A recovery drill must never overwrite a live deployment or SSH identity.
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    restored = 0
    for item in resources:
        kind, resource = item["kind"], item["resource"]
        if kind not in ("env", "ssh"):
            raise BackupError("Invalid recovery kind")
        content = vault.read(kind, resource, revision)
        target = safe_path(root, f"{kind}/{resource}")
        atomic_write(target, content)
        restored += 1
    return restored


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List recoverable files, never secret values")
    commands.add_parser("verify", help="Authenticate every latest confirmed backup")
    history = commands.add_parser("history", help="List up to 100 recent versions of a file")
    history.add_argument("kind", choices=("env", "ssh"))
    history.add_argument("resource", help="Relative path, e.g. PulsarCD/devops/.env or id_ed25519")
    single = commands.add_parser("restore", help="Restore one file into a new directory")
    single.add_argument("kind", choices=("env", "ssh"))
    single.add_argument("resource")
    single.add_argument("--revision", help="Explicit revision; may include a pending write")
    single.add_argument("--output-dir", required=True)
    all_files = commands.add_parser("restore-all", help="Restore latest confirmed files into a new directory")
    all_files.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    load_docker_secrets_into_env()
    os.environ["PULSARCD_BACKUP__ENABLED"] = "true"
    try:
        vault = get_vault()
        if args.command == "history":
            print(json.dumps(vault.history(args.kind, args.resource), indent=2))
        elif args.command == "list":
            print(json.dumps(vault.resources(), indent=2))
        elif args.command == "verify":
            resources = vault.resources()
            for item in resources:
                vault.read(item["kind"], item["resource"])
            print(f"Verified {len(resources)} files")
        else:
            resources = (vault.resources() if args.command == "restore-all"
                         else [{"kind": args.kind, "resource": args.resource}])
            if not resources:
                raise BackupError("No confirmed backup files available")
            count = restore(vault, args.output_dir, resources, getattr(args, "revision", None))
            print(f"Restored {count} files into the new recovery directory")
    except BackupError as exc:
        parser.exit(1, str(exc) + "\n")
    except Exception:
        parser.exit(1, "Recovery failed; check output directory and permissions. No existing file was overwritten.\n")


if __name__ == "__main__":
    main()
