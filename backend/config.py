"""Configuration management for LogsCrawler.

All configuration is done via environment variables. No config file required!

Environment variables:
- LOGSCRAWLER_HOSTS: JSON array of host configs
- LOGSCRAWLER_OPENSEARCH__HOSTS: JSON array of OpenSearch URLs
- LOGSCRAWLER_OPENSEARCH__INDEX_PREFIX: Index prefix string
- LOGSCRAWLER_COLLECTOR__LOG_INTERVAL_SECONDS: Log collection interval
- LOGSCRAWLER_COLLECTOR__METRICS_INTERVAL_SECONDS: Metrics collection interval
- LOGSCRAWLER_AI__MODEL: AI model name
"""

import json
import os
import uuid
from typing import List, Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class HostConfig(BaseModel):
    """Configuration for a single host."""
    name: str
    hostname: str = "localhost"
    port: int = 22
    username: str = "root"
    ssh_key_path: Optional[str] = None
    
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
    index_prefix: str = "logscrawler"
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
    """AI/Ollama configuration."""
    model: str = "qwen2.5:1.5b"


class GitHubConfig(BaseModel):
    """GitHub integration configuration."""
    token: Optional[str] = None
    username: Optional[str] = None
    useremail: Optional[str] = None
    # Path where repos are cloned on the host
    repos_path: str = "~/repos"
    # Path to deployment scripts (LogsCrawler/scripts folder)
    scripts_path: str = "~/repos/LogsCrawler/scripts"
    # SSH configuration for executing commands on the host
    # Required when LogsCrawler runs in a container and needs to run git/build on the host
    ssh_host: Optional[str] = None
    ssh_user: str = "root"
    ssh_port: int = 22
    ssh_key_path: Optional[str] = None
    # Docker registry configuration for push operations
    registry_url: Optional[str] = None
    registry_username: Optional[str] = None
    registry_password: Optional[str] = None


class AuthConfig(BaseModel):
    """Authentication configuration."""
    username: str = "admin"
    password: str = "changeme"
    jwt_secret: str = ""
    jwt_expiry_hours: int = 24
    # Shared key for agent-to-backend API authentication
    agent_key: str = ""


class MCPConfig(BaseModel):
    """MCP (Model Context Protocol) server configuration."""
    enabled: bool = True
    api_key: str = ""  # Dedicated MCP API key (auto-generated if empty)


class Settings(BaseSettings):
    """Application settings."""
    app_name: str = "LogsCrawler"
    debug: bool = False

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

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

    # Hosts (configured via LOGSCRAWLER_HOSTS env var)
    hosts: List[HostConfig] = []

    class Config:
        env_prefix = "LOGSCRAWLER_"
        env_nested_delimiter = "__"


def load_config() -> Settings:
    """Load configuration from environment variables.

    All configuration is done via environment variables prefixed with LOGSCRAWLER_.
    Pydantic-settings handles most env vars automatically via env_nested_delimiter.

    Required:
    - LOGSCRAWLER_HOSTS: JSON array of host configs

    Optional (auto-loaded by pydantic-settings):
    - LOGSCRAWLER_OPENSEARCH__HOSTS: JSON array of OpenSearch URLs
    - LOGSCRAWLER_OPENSEARCH__INDEX_PREFIX: Index prefix
    - LOGSCRAWLER_OPENSEARCH__USERNAME: OpenSearch username
    - LOGSCRAWLER_OPENSEARCH__PASSWORD: OpenSearch password
    - LOGSCRAWLER_COLLECTOR__LOG_INTERVAL_SECONDS: integer
    - LOGSCRAWLER_COLLECTOR__METRICS_INTERVAL_SECONDS: integer
    - LOGSCRAWLER_COLLECTOR__LOG_LINES_PER_FETCH: integer
    - LOGSCRAWLER_COLLECTOR__RETENTION_DAYS: integer
    - LOGSCRAWLER_AI__MODEL: string
    - LOGSCRAWLER_GITHUB__*: GitHub configuration

    Example LOGSCRAWLER_HOSTS:
    [{"name": "local", "mode": "docker", "docker_url": "unix:///var/run/docker.sock"}]
    """
    settings = Settings()

    # Load hosts from environment variable (JSON array)
    # This needs special handling because it's a complex nested structure
    hosts_env = os.environ.get("LOGSCRAWLER_HOSTS")
    if hosts_env:
        try:
            hosts_list = json.loads(hosts_env)
            if isinstance(hosts_list, list):
                settings.hosts = [HostConfig(**h) for h in hosts_list]
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse LOGSCRAWLER_HOSTS: {e}")
        except Exception as e:
            print(f"Warning: Invalid host configuration: {e}")

    # OpenSearch hosts need special handling (JSON array or single string)
    opensearch_hosts_env = os.environ.get("LOGSCRAWLER_OPENSEARCH__HOSTS")
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
    load_env(settings.opensearch, "index_prefix", "LOGSCRAWLER_OPENSEARCH__INDEX_PREFIX")
    load_env(settings.opensearch, "username", "LOGSCRAWLER_OPENSEARCH__USERNAME")
    load_env(settings.opensearch, "password", "LOGSCRAWLER_OPENSEARCH__PASSWORD")

    # Collector settings
    load_env(settings.collector, "log_interval_seconds", "LOGSCRAWLER_COLLECTOR__LOG_INTERVAL_SECONDS", int)
    load_env(settings.collector, "metrics_interval_seconds", "LOGSCRAWLER_COLLECTOR__METRICS_INTERVAL_SECONDS", int)
    load_env(settings.collector, "log_lines_per_fetch", "LOGSCRAWLER_COLLECTOR__LOG_LINES_PER_FETCH", int)
    load_env(settings.collector, "retention_days", "LOGSCRAWLER_COLLECTOR__RETENTION_DAYS", int)
    # Load agents_only as bool (accepts "true", "1", "yes")
    agents_only_env = os.environ.get("LOGSCRAWLER_COLLECTOR__AGENTS_ONLY", "").lower()
    if agents_only_env in ("true", "1", "yes"):
        settings.collector.agents_only = True

    # AI settings
    load_env(settings.ai, "model", "LOGSCRAWLER_AI__MODEL")

    # Auth settings
    load_env(settings.auth, "username", "LOGSCRAWLER_AUTH__USERNAME")
    load_env(settings.auth, "password", "LOGSCRAWLER_AUTH__PASSWORD")
    load_env(settings.auth, "jwt_secret", "LOGSCRAWLER_AUTH__JWT_SECRET")
    load_env(settings.auth, "jwt_expiry_hours", "LOGSCRAWLER_AUTH__JWT_EXPIRY_HOURS", int)
    load_env(settings.auth, "agent_key", "LOGSCRAWLER_AUTH__AGENT_KEY")
    # Auto-generate JWT secret if not provided
    if not settings.auth.jwt_secret:
        settings.auth.jwt_secret = uuid.uuid4().hex
    # Auto-generate agent key if not provided
    if not settings.auth.agent_key:
        settings.auth.agent_key = uuid.uuid4().hex

    # MCP settings
    load_env(settings.mcp, "api_key", "LOGSCRAWLER_MCP__API_KEY")
    mcp_enabled_env = os.environ.get("LOGSCRAWLER_MCP__ENABLED", "").lower()
    if mcp_enabled_env in ("false", "0", "no"):
        settings.mcp.enabled = False
    if not settings.mcp.api_key:
        settings.mcp.api_key = uuid.uuid4().hex

    # Run user
    load_env(settings, "run_user", "LOGSCRAWLER_RUN_USER")

    # GitHub settings
    load_env(settings.github, "token", "LOGSCRAWLER_GITHUB__TOKEN")
    load_env(settings.github, "username", "LOGSCRAWLER_GITHUB__USERNAME")
    load_env(settings.github, "useremail", "LOGSCRAWLER_GITHUB__USEREMAIL")
    load_env(settings.github, "repos_path", "LOGSCRAWLER_GITHUB__REPOS_PATH")
    load_env(settings.github, "scripts_path", "LOGSCRAWLER_GITHUB__SCRIPTS_PATH")
    load_env(settings.github, "ssh_host", "LOGSCRAWLER_GITHUB__SSH_HOST")
    load_env(settings.github, "ssh_user", "LOGSCRAWLER_GITHUB__SSH_USER")
    load_env(settings.github, "ssh_port", "LOGSCRAWLER_GITHUB__SSH_PORT", int)
    load_env(settings.github, "ssh_key_path", "LOGSCRAWLER_GITHUB__SSH_KEY_PATH")
    load_env(settings.github, "registry_url", "LOGSCRAWLER_GITHUB__REGISTRY_URL")
    load_env(settings.github, "registry_username", "LOGSCRAWLER_GITHUB__REGISTRY_USERNAME")
    load_env(settings.github, "registry_password", "LOGSCRAWLER_GITHUB__REGISTRY_PASSWORD")

    return settings


# Global settings instance
settings = load_config()


def wrap_command_for_user(command: str) -> str:
    """Wrap a shell command with su if LOGSCRAWLER_RUN_USER is set."""
    if settings.run_user:
        escaped = command.replace("'", "'\"'\"'")
        return f"su - {settings.run_user} -c '{escaped}'"
    return command
