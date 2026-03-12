"""Agent configuration."""

import json
import os
from typing import List, Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings


class OpenSearchConfig(BaseModel):
    """OpenSearch configuration for direct writes."""
    hosts: List[str] = ["http://localhost:9200"]
    index_prefix: str = "pulsarcd"
    username: Optional[str] = None
    password: Optional[str] = None


class AgentSettings(BaseSettings):
    """Agent settings loaded from environment variables."""

    # Agent identification
    agent_id: str = "agent-default"

    # Backend URL for polling actions
    backend_url: str = "http://localhost:8000"

    # OpenSearch for direct data writes
    opensearch: OpenSearchConfig = OpenSearchConfig()

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
    - AGENT_OPENSEARCH__HOSTS: JSON array of OpenSearch URLs
    - AGENT_OPENSEARCH__USERNAME: OpenSearch username
    - AGENT_OPENSEARCH__PASSWORD: OpenSearch password
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

    # OpenSearch - hosts needs special handling (JSON array or single string)
    opensearch_hosts = os.environ.get("AGENT_OPENSEARCH__HOSTS")
    if opensearch_hosts:
        try:
            hosts_list = json.loads(opensearch_hosts)
            if isinstance(hosts_list, list):
                settings.opensearch.hosts = hosts_list
        except json.JSONDecodeError:
            settings.opensearch.hosts = [opensearch_hosts]

    load_env(settings.opensearch, "index_prefix", "AGENT_OPENSEARCH__INDEX_PREFIX")
    load_env(settings.opensearch, "username", "AGENT_OPENSEARCH__USERNAME")
    load_env(settings.opensearch, "password", "AGENT_OPENSEARCH__PASSWORD")

    # Intervals
    load_env(settings, "log_interval", "AGENT_LOG_INTERVAL", int)
    load_env(settings, "metrics_interval", "AGENT_METRICS_INTERVAL", int)
    load_env(settings, "action_poll_interval", "AGENT_ACTION_POLL_INTERVAL", int)
    load_env(settings, "log_lines_per_fetch", "AGENT_LOG_LINES_PER_FETCH", int)

    # Auth
    load_env(settings, "auth_key", "AGENT_AUTH_KEY")

    return settings
