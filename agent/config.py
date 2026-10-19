"""Agent configuration."""

import os
from typing import Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings

from shared.secrets import load_docker_secrets_into_env

# Load Docker secrets mounted at /run/secrets/<NAME> into the environment so
# pydantic and os.environ based code keeps working transparently.
load_docker_secrets_into_env()


class VictoriaLogsConfig(BaseModel):
    """VictoriaLogs, where the agent writes logs and metrics."""
    url: str = "http://localhost:9428"
    username: Optional[str] = None
    password: Optional[str] = None


class AgentSettings(BaseSettings):
    """Agent settings loaded from environment variables."""

    # Agent identification
    agent_id: str = "agent-default"

    # Backend URL for polling actions
    backend_url: str = "http://localhost:8000"

    # VictoriaLogs for direct data writes
    victorialogs: VictoriaLogsConfig = VictoriaLogsConfig()

    # Docker connection
    docker_url: str = "unix:///var/run/docker.sock"

    # Collection intervals (seconds)
    log_interval: int = 30
    metrics_interval: int = 15
    action_poll_interval: int = 2

    # Collection settings
    log_lines_per_fetch: int = 500

    # Authentication key for backend API
    auth_key: str = ""

    class Config:
        env_prefix = "AGENT_"
        env_nested_delimiter = "__"


def load_agent_config() -> AgentSettings:
    """Load agent configuration from environment variables.

    Environment variables:
    - AGENT_AGENT_ID: Unique agent identifier (hostname recommended)
    - AGENT_BACKEND_URL: URL of the PulsarCD backend
    - AGENT_VICTORIALOGS__URL: VictoriaLogs base URL
    - AGENT_VICTORIALOGS__USERNAME / __PASSWORD: basic auth (optional)
    - AGENT_DOCKER_URL: Docker socket or TCP URL
    - AGENT_LOG_INTERVAL: Log collection interval in seconds
    - AGENT_METRICS_INTERVAL: Metrics collection interval in seconds
    - AGENT_ACTION_POLL_INTERVAL: Action polling interval in seconds
    """
    settings = AgentSettings()

    # Helper function to load env vars with type conversion and error handling
    def load_env(obj, attr: str, env_var: str, converter=str):
        value = os.environ.get(env_var)
        if value:
            try:
                setattr(obj, attr, converter(value))
            except (ValueError, TypeError) as e:
                print(f"Warning: Failed to parse {env_var}: {e}")

    # Agent settings
    load_env(settings, "agent_id", "AGENT_AGENT_ID")
    load_env(settings, "backend_url", "AGENT_BACKEND_URL")
    load_env(settings, "docker_url", "AGENT_DOCKER_URL")

    # VictoriaLogs
    load_env(settings.victorialogs, "url", "AGENT_VICTORIALOGS__URL")
    load_env(settings.victorialogs, "username", "AGENT_VICTORIALOGS__USERNAME")
    load_env(settings.victorialogs, "password", "AGENT_VICTORIALOGS__PASSWORD")

    # Intervals
    load_env(settings, "log_interval", "AGENT_LOG_INTERVAL", int)
    load_env(settings, "metrics_interval", "AGENT_METRICS_INTERVAL", int)
    load_env(settings, "action_poll_interval", "AGENT_ACTION_POLL_INTERVAL", int)
    load_env(settings, "log_lines_per_fetch", "AGENT_LOG_LINES_PER_FETCH", int)

    # Auth
    load_env(settings, "auth_key", "AGENT_AUTH_KEY")

    return settings
