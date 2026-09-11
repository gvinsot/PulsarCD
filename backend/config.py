"""Configuration management for PulsarCD.

Configuration is loaded from environment variables + /data/config.yml file.
Environment variables override config file values.

Environment variables:
- PULSARCD_HOSTS: JSON array of host configs
- PULSARCD_OPENSEARCH__HOSTS: JSON array of OpenSearch URLs
- PULSARCD_OPENSEARCH__INDEX_PREFIX: Index prefix string
- PULSARCD_COLLECTOR__LOG_INTERVAL_SECONDS: Log collection interval
- PULSARCD_COLLECTOR__METRICS_INTERVAL_SECONDS: Metrics collection interval
- PULSARCD_AI__MODEL: AI model name
- PULSARCD_DATA_DIR: Data directory for config and users files (default: /data)
- PULSARCD_AUTH__AGENT_KEY: Shared fallback key for agent-to-backend calls.
  Used for every agent when PULSARCD_AUTH__AGENT_KEYS is not set, which means a
  single compromised agent container exposes the whole fleet.
- PULSARCD_AUTH__AGENT_KEYS: JSON object mapping an agent id to its own key,
  e.g. {"host-a": "<secret-a>", "host-b": "<secret-b>"}. When set, an agent must
  present the key registered for the agent_id it acts on and the shared
  PULSARCD_AUTH__AGENT_KEY is no longer accepted on its own. A malformed value
  is reported as a warning and ignored.
- PULSARCD_AUTH__GOOGLE_CLIENT_ID: OAuth 2.0 Web client id from the Google Cloud
  console. Required for Google sign-in, which is how everyone signs in; it is a
  public value, not a secret. Unset disables Google sign-in.
- PULSARCD_AUTH__GOOGLE_ADMINS / PULSARCD_AUTH__GOOGLE_VIEWERS: addresses allowed
  to sign in, as a comma-separated list or a JSON array. Reapplied on every boot,
  so they override anything edited in Settings > Users (which persists the rest
  to allowed_emails.json in the data directory).
- PULSARCD_AUTH__USERNAME / PULSARCD_AUTH__PASSWORD: the optional break-glass
  administrator. No password means no local account and password login answers
  403; see backend/user_manager.py.
- PULSARCD_AUTH__JWT_SECRET: JWT signing secret. Auto-generated when unset (all
  sessions are invalidated on restart); when set it must be at least
  MIN_SECRET_LENGTH characters or startup fails.
- PULSARCD_SSH_KNOWN_HOSTS: known_hosts file used when a host does not set
  ssh_known_hosts_path (default ~/.ssh/known_hosts). Point it at a writable
  location: the container bind-mounts ~/.ssh read-only.
- PULSARCD_SSH_ACCEPT_NEW_HOSTKEYS: "true" enables trust-on-first-use for hosts
  that have no explicit ssh_known_hosts_path. Off by default: an unknown host
  aborts the connection instead of trusting the key it is offered.
- PULSARCD_GITHUB__SSH_KNOWN_HOSTS_PATH: host key policy for the build/deploy SSH
  connection only (see GitHubConfig.ssh_known_hosts_path). Defaults to
  "accept-new"; set a file path to verify that host strictly, or "none" to skip
  verification entirely (not recommended).
- PULSARCD_TRUST_PROXY_HEADERS: "true" makes the login rate limiter read
  X-Forwarded-For / X-Real-IP. Only set it behind a proxy that overwrites those
  headers; otherwise a client picks its own rate-limit bucket.
"""

import json
import os
import uuid
from typing import Dict, List, Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings

from shared.secrets import load_docker_secrets_into_env

# Load Docker secrets (mounted at /run/secrets/<NAME>) into the environment so
# the existing pydantic / os.environ based loading picks them up transparently.
load_docker_secrets_into_env()


class HostConfig(BaseModel):
    """Configuration for a single host."""
    name: str
    hostname: str = "localhost"
    port: int = 22
    username: str = "root"
    ssh_key_path: Optional[str] = None
    # Path to known_hosts file for SSH host key verification.
    # Verification is STRICT: a host with no matching entry aborts the
    # connection instead of trusting the key the network presents.
    #   empty        -> PULSARCD_SSH_KNOWN_HOSTS, else ~/.ssh/known_hosts. Only
    #                   these hosts follow the global
    #                   PULSARCD_SSH_ACCEPT_NEW_HOSTKEYS opt-in.
    #   <path>       -> that file, always strict (the global opt-in does not
    #                   apply, so onboarding one host cannot downgrade this one).
    #   "accept-new" -> trust-on-first-use for this host only, saving the key to
    #                   the default file.
    #   "none"       -> no verification at all (NOT recommended).
    ssh_known_hosts_path: Optional[str] = None
    
    # Connection mode (choose one):
    # - "ssh": Connect via SSH (default for remote hosts)
    # - "docker": Connect via Docker API socket or TCP
    # - "local": Run commands locally (for development without Docker)
    mode: str = "ssh"
    
    # Docker API URL (only used when mode="docker")
    # Examples:
    # - "unix:///var/run/docker.sock" (local socket, default)
    # - "tcp://192.168.1.10:2375" (remote TCP)
    # - "tcp://host.docker.internal:2375" (host from container)
    docker_url: Optional[str] = None
    
    # Swarm manager flag: set to true if this host is a Docker Swarm manager
    # Used for stack operations and grouping
    swarm_manager: bool = False

    # Swarm routing: when True, commands for containers on other Swarm nodes
    # will be routed through this manager instead of direct SSH connections.
    # This eliminates the need for SSH access to worker nodes.
    # Only applicable when swarm_manager=True and mode="docker" or "ssh"
    swarm_routing: bool = False

    # Swarm auto-discovery: when True, automatically discovers all nodes in the
    # Swarm cluster and monitors their containers. No need to configure worker
    # nodes manually - they are discovered from the manager.
    # Requires swarm_manager=True and mode="docker"
    swarm_autodiscover: bool = False
    
    
class OpenSearchConfig(BaseModel):
    """OpenSearch configuration."""
    hosts: List[str] = ["http://localhost:9200"]
    index_prefix: str = "pulsarcd"
    username: Optional[str] = None
    password: Optional[str] = None


class CollectorConfig(BaseModel):
    """Collector configuration."""
    log_interval_seconds: int = 30
    metrics_interval_seconds: int = 15
    log_lines_per_fetch: int = 500
    retention_days: int = 7
    # When True, backend collection is completely disabled (agents handle everything)
    # The collector will only maintain container lists for the UI, not collect logs/metrics
    agents_only: bool = False


class AIConfig(BaseModel):
    """AI/vLLM configuration."""
    model: str = "txn545/Qwen3.5-122B-A10B-NVFP4"


class GitHubConfig(BaseModel):
    """GitHub integration configuration."""
    token: Optional[str] = None
    username: Optional[str] = None
    useremail: Optional[str] = None
    # Path where repos are cloned on the host
    repos_path: str = "~/repos"
    # SSH configuration for executing commands on the host
    # Required when PulsarCD runs in a container and needs to run git/build on the host
    ssh_host: Optional[str] = None
    ssh_user: str = "root"
    ssh_port: int = 22
    ssh_key_path: Optional[str] = None
    # Host key policy for that build/deploy connection, using the same values as
    # HostConfig.ssh_known_hosts_path.  It defaults to "accept-new" rather than
    # to the strict global default because this target is normally the container's
    # own Docker host, reached through the `dockerhost` host-gateway alias: that
    # name cannot appear in any pre-existing known_hosts, so a strict default
    # leaves build, deploy and the .env editor failing on a fresh volume with no
    # key an operator could have installed beforehand.  The key is pinned on the
    # first connection and every later one is verified against it.  Point this at
    # a managed known_hosts file to verify strictly from the very first connection.
    ssh_known_hosts_path: Optional[str] = "accept-new"
    # Docker registry configuration for push operations
    registry_url: Optional[str] = None
    registry_username: Optional[str] = None
    registry_password: Optional[str] = None
    # Which repos to show in Stacks: "all" (default) or "starred"
    repos_mode: str = "all"


class AuthConfig(BaseModel):
    """Authentication configuration.

    Users sign in with Google (google_client_id + the two allowlists below).
    The username/password pair provisions the optional break-glass
    administrator only -- see backend/user_manager.py.
    """
    # OAuth 2.0 Web client id from the Google Cloud console. Public by design:
    # the browser needs it to ask Google for an ID token. Empty disables Google
    # sign-in entirely, which leaves only the break-glass account.
    google_client_id: str = ""
    # Addresses allowed to sign in, by role. These are reapplied on every boot
    # and take precedence over anything edited in the UI (see allowlist.py).
    google_admins: List[str] = []
    google_viewers: List[str] = []

    # Break-glass local administrator. Empty password = no local account at all.
    username: str = "admin"
    # Empty on purpose: a "changeme" default is a weak credential waiting for a
    # future caller to wire it into a login check.  The password comes from
    # PULSARCD_AUTH__PASSWORD and is validated by user_manager.
    password: str = ""
    jwt_secret: str = ""
    jwt_expiry_hours: int = 24
    # Shared key for agent-to-backend API authentication.
    # Only used as a fallback when agent_keys is empty: every agent then holds
    # the same secret, so compromising one agent compromises the whole fleet.
    agent_key: str = ""
    # Per-agent keys: {"agent_id": "key"}, loaded from PULSARCD_AUTH__AGENT_KEYS.
    # When non-empty, an agent must present the key registered for the agent_id
    # it acts on and the shared agent_key alone is no longer sufficient.
    agent_keys: Dict[str, str] = {}


class MCPConfig(BaseModel):
    """MCP (Model Context Protocol) server configuration."""
    enabled: bool = True
    api_key: str = ""  # Dedicated MCP API key (auto-generated if empty)


class SwarmConfig(BaseModel):
    """Swarm agent API configuration."""
    secret_key: str = ""  # API key for swarm.methodinfo.fr (Bearer token)


class Settings(BaseSettings):
    """Application settings."""
    app_name: str = "PulsarCD"
    debug: bool = False

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # Data directory for config file and users file
    data_dir: str = "/data"

    # OS user for running terminal and git commands locally (via su)
    run_user: Optional[str] = None

    # OpenSearch
    opensearch: OpenSearchConfig = OpenSearchConfig()

    # Collector
    collector: CollectorConfig = CollectorConfig()

    # AI
    ai: AIConfig = AIConfig()

    # GitHub
    github: GitHubConfig = GitHubConfig()

    # Auth
    auth: AuthConfig = AuthConfig()

    # MCP
    mcp: MCPConfig = MCPConfig()

    # Swarm agent API (legacy, replaced by LLM agent)
    swarm: SwarmConfig = SwarmConfig()

    # Hosts (configured via PULSARCD_HOSTS env var)
    hosts: List[HostConfig] = []

    # Config file (loaded from /data/config.yml)
    pulsar_config: Optional[object] = None

    class Config:
        env_prefix = "PULSARCD_"
        env_nested_delimiter = "__"


# Minimum length for a secret an operator sets explicitly.  32 characters is what
# `openssl rand -base64 32` produces; anything shorter is brute-forceable offline
# from a single captured JWT.
MIN_SECRET_LENGTH = 32

# Values shipped in the sample compose/.env files, refused whatever their length.
_PLACEHOLDER_SECRETS = frozenset({
    "changeme", "change-me", "changemenow", "password", "secret", "pulsarcd",
    "admin", "test", "dev", "development", "your-secret-here", "replace-me",
})


def _validate_configured_secrets(settings: "Settings") -> None:
    """Refuse to start on a weak, explicitly configured auth secret.

    Only non-empty values are checked: an unset secret is auto-generated a few
    lines below, which is a different (documented) trade-off.  The compose guard
    ``${VAR:?}`` only rejects an empty value, so without this a literal
    ``PULSARCD_AUTH__JWT_SECRET=x`` satisfied the deployment contract while
    leaving the strongest credential of the stack guessable.
    """
    for name, value in (
        ("PULSARCD_AUTH__JWT_SECRET", settings.auth.jwt_secret),
        ("PULSARCD_AUTH__AGENT_KEY", settings.auth.agent_key),
    ):
        if not value:
            continue
        if value.strip().lower() in _PLACEHOLDER_SECRETS:
            raise RuntimeError(
                f"{name} is a well-known placeholder value. Generate a real "
                f"secret with `openssl rand -base64 32`."
            )
        if len(value) < MIN_SECRET_LENGTH:
            raise RuntimeError(
                f"{name} is too weak: it must be at least {MIN_SECRET_LENGTH} "
                f"characters (got {len(value)}). Generate one with "
                f"`openssl rand -base64 32`."
            )


def load_config() -> Settings:
    """Load configuration from environment variables.

    All configuration is done via environment variables prefixed with PULSARCD_.
    Pydantic-settings handles most env vars automatically via env_nested_delimiter.

    Required:
    - PULSARCD_HOSTS: JSON array of host configs

    Optional (auto-loaded by pydantic-settings):
    - PULSARCD_OPENSEARCH__HOSTS: JSON array of OpenSearch URLs
    - PULSARCD_OPENSEARCH__INDEX_PREFIX: Index prefix
    - PULSARCD_OPENSEARCH__USERNAME: OpenSearch username
    - PULSARCD_OPENSEARCH__PASSWORD: OpenSearch password
    - PULSARCD_COLLECTOR__LOG_INTERVAL_SECONDS: integer
    - PULSARCD_COLLECTOR__METRICS_INTERVAL_SECONDS: integer
    - PULSARCD_COLLECTOR__LOG_LINES_PER_FETCH: integer
    - PULSARCD_COLLECTOR__RETENTION_DAYS: integer
    - PULSARCD_AI__MODEL: string
    - PULSARCD_GITHUB__*: GitHub configuration

    Optional (parsed explicitly, see the module docstring):
    - PULSARCD_AUTH__AGENT_KEY: shared fallback agent key
    - PULSARCD_AUTH__AGENT_KEYS: JSON object {"agent_id": "key"}

    Example PULSARCD_HOSTS:
    [{"name": "local", "mode": "docker", "docker_url": "unix:///var/run/docker.sock"}]
    """
    # These three carry structured values (a JSON object, and two address
    # lists an operator writes comma-separated). Hide them from
    # pydantic-settings and parse them below so a malformed value degrades to a
    # warning instead of aborting startup with a SettingsError.
    hidden_env = {
        name: os.environ.pop(name, None)
        for name in ("PULSARCD_AUTH__AGENT_KEYS",
                     "PULSARCD_AUTH__GOOGLE_ADMINS",
                     "PULSARCD_AUTH__GOOGLE_VIEWERS")
    }
    agent_keys_env = hidden_env["PULSARCD_AUTH__AGENT_KEYS"]

    settings = Settings()

    for name, value in hidden_env.items():
        if value is not None:
            os.environ[name] = value

    # Load hosts from environment variable (JSON array)
    # This needs special handling because it's a complex nested structure
    hosts_env = os.environ.get("PULSARCD_HOSTS")
    if hosts_env:
        try:
            hosts_list = json.loads(hosts_env)
            if isinstance(hosts_list, list):
                settings.hosts = [HostConfig(**h) for h in hosts_list]
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse PULSARCD_HOSTS: {e}")
        except Exception as e:
            print(f"Warning: Invalid host configuration: {e}")

    # OpenSearch hosts need special handling (JSON array or single string)
    opensearch_hosts_env = os.environ.get("PULSARCD_OPENSEARCH__HOSTS")
    if opensearch_hosts_env:
        try:
            hosts_list = json.loads(opensearch_hosts_env)
            if isinstance(hosts_list, list):
                settings.opensearch.hosts = hosts_list
        except json.JSONDecodeError:
            # Single host string
            settings.opensearch.hosts = [opensearch_hosts_env]

    # Helper function to load env vars with type conversion
    def load_env(obj, attr: str, env_var: str, converter=str):
        value = os.environ.get(env_var)
        if value:
            try:
                setattr(obj, attr, converter(value))
            except (ValueError, TypeError) as e:
                print(f"Warning: Failed to parse {env_var}: {e}")

    # OpenSearch settings
    load_env(settings.opensearch, "index_prefix", "PULSARCD_OPENSEARCH__INDEX_PREFIX")
    load_env(settings.opensearch, "username", "PULSARCD_OPENSEARCH__USERNAME")
    load_env(settings.opensearch, "password", "PULSARCD_OPENSEARCH__PASSWORD")

    # Collector settings
    load_env(settings.collector, "log_interval_seconds", "PULSARCD_COLLECTOR__LOG_INTERVAL_SECONDS", int)
    load_env(settings.collector, "metrics_interval_seconds", "PULSARCD_COLLECTOR__METRICS_INTERVAL_SECONDS", int)
    load_env(settings.collector, "log_lines_per_fetch", "PULSARCD_COLLECTOR__LOG_LINES_PER_FETCH", int)
    load_env(settings.collector, "retention_days", "PULSARCD_COLLECTOR__RETENTION_DAYS", int)
    # Load agents_only as bool (accepts "true", "1", "yes")
    agents_only_env = os.environ.get("PULSARCD_COLLECTOR__AGENTS_ONLY", "").lower()
    if agents_only_env in ("true", "1", "yes"):
        settings.collector.agents_only = True

    # AI settings
    load_env(settings.ai, "model", "PULSARCD_AI__MODEL")

    # Auth settings
    load_env(settings.auth, "google_client_id", "PULSARCD_AUTH__GOOGLE_CLIENT_ID")
    # Address lists: JSON array or a comma/semicolon/whitespace separated string.
    from backend.allowlist import parse_email_list
    settings.auth.google_admins = parse_email_list(
        hidden_env["PULSARCD_AUTH__GOOGLE_ADMINS"] or "")
    settings.auth.google_viewers = parse_email_list(
        hidden_env["PULSARCD_AUTH__GOOGLE_VIEWERS"] or "")
    load_env(settings.auth, "username", "PULSARCD_AUTH__USERNAME")
    load_env(settings.auth, "password", "PULSARCD_AUTH__PASSWORD")
    load_env(settings.auth, "jwt_secret", "PULSARCD_AUTH__JWT_SECRET")
    load_env(settings.auth, "jwt_expiry_hours", "PULSARCD_AUTH__JWT_EXPIRY_HOURS", int)
    load_env(settings.auth, "agent_key", "PULSARCD_AUTH__AGENT_KEY")
    # Per-agent keys (JSON object) - same defensive parsing as PULSARCD_HOSTS
    if agent_keys_env:
        try:
            parsed_keys = json.loads(agent_keys_env)
            if isinstance(parsed_keys, dict):
                settings.auth.agent_keys = {
                    str(k): str(v) for k, v in parsed_keys.items() if k and v
                }
            else:
                print("Warning: PULSARCD_AUTH__AGENT_KEYS must be a JSON object")
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse PULSARCD_AUTH__AGENT_KEYS: {e}")
        except Exception as e:
            print(f"Warning: Invalid agent keys configuration: {e}")
    # A configured secret must actually be a secret.  Checked BEFORE the
    # auto-generation below, so the "not set at all" case keeps its existing
    # behaviour.  The JWT secret is the strongest credential of the stack:
    # forging a token bypasses the password, the login rate limit and the role
    # policy in one step, so a short value is offline-crackable from a single
    # captured token.
    _validate_configured_secrets(settings)

    # Auto-generate JWT secret if not provided
    if not settings.auth.jwt_secret:
        settings.auth.jwt_secret = uuid.uuid4().hex
        print("Warning: PULSARCD_AUTH__JWT_SECRET is not set; a random secret was "
              "generated. Every restart invalidates all sessions and a multi-replica "
              "deployment cannot validate its own tokens.")
    # Auto-generate agent key if not provided
    if not settings.auth.agent_key:
        settings.auth.agent_key = uuid.uuid4().hex
        print("Warning: PULSARCD_AUTH__AGENT_KEY is not set; a random key was "
              "generated. Agents configured with a fixed key will fail to "
              "authenticate after a restart.")

    # MCP settings
    load_env(settings.mcp, "api_key", "PULSARCD_MCP__API_KEY")
    mcp_enabled_env = os.environ.get("PULSARCD_MCP__ENABLED", "").lower()
    if mcp_enabled_env in ("false", "0", "no"):
        settings.mcp.enabled = False
    if not settings.mcp.api_key:
        settings.mcp.api_key = uuid.uuid4().hex

    # Swarm agent API settings (legacy)
    load_env(settings.swarm, "secret_key", "PULSARCD_SWARM__SECRET_KEY")

    # Data directory
    load_env(settings, "data_dir", "PULSARCD_DATA_DIR")

    # Run user
    load_env(settings, "run_user", "PULSARCD_RUN_USER")

    # GitHub settings
    load_env(settings.github, "token", "PULSARCD_GITHUB__TOKEN")
    load_env(settings.github, "username", "PULSARCD_GITHUB__USERNAME")
    load_env(settings.github, "useremail", "PULSARCD_GITHUB__USEREMAIL")
    load_env(settings.github, "repos_path", "PULSARCD_GITHUB__REPOS_PATH")
    load_env(settings.github, "ssh_host", "PULSARCD_GITHUB__SSH_HOST")
    load_env(settings.github, "ssh_user", "PULSARCD_GITHUB__SSH_USER")
    load_env(settings.github, "ssh_port", "PULSARCD_GITHUB__SSH_PORT", int)
    load_env(settings.github, "ssh_key_path", "PULSARCD_GITHUB__SSH_KEY_PATH")
    load_env(settings.github, "ssh_known_hosts_path",
             "PULSARCD_GITHUB__SSH_KNOWN_HOSTS_PATH")
    load_env(settings.github, "registry_url", "PULSARCD_GITHUB__REGISTRY_URL")
    load_env(settings.github, "registry_username", "PULSARCD_GITHUB__REGISTRY_USERNAME")
    load_env(settings.github, "registry_password", "PULSARCD_GITHUB__REGISTRY_PASSWORD")

    # Load config file from data directory
    try:
        from backend.config_file import load_config_file
        settings.pulsar_config = load_config_file(settings.data_dir)
    except Exception as e:
        print(f"Warning: Failed to load config file: {e}")

    return settings


# Global settings instance
settings = load_config()


def wrap_command_for_user(command: str) -> str:
    """Wrap a shell command with su if PULSARCD_RUN_USER is set."""
    if settings.run_user:
        escaped = command.replace("'", "'\"'\"'")
        return f"su - {settings.run_user} -c '{escaped}'"
    return command
