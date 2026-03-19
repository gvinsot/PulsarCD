"""YAML configuration file loader for PulsarCD.

Loads /data/config.yml with auto-creation of defaults on first boot.
Environment variables override file values when set.
"""

import os
from pathlib import Path
from typing import List, Optional

import structlog
import yaml
from pydantic import BaseModel

logger = structlog.get_logger()


class LLMConfig(BaseModel):
    """LLM provider settings."""
    url: str = "http://vllm-dev-service:8000"
    model: str = "txn545/Qwen3.5-122B-A10B-NVFP4"
    api_key: str = ""
    context_tokens: int = 256000
    max_output_tokens: int = 128000


class MCPServerConfig(BaseModel):
    """MCP server endpoint configuration."""
    name: str = "TicketManager"
    url: str = "http://team-api:3001/api/swarm/mcp"
    api_key: str = ""


class ErrorHandlingConfig(BaseModel):
    """LLM error handling instructions."""
    enabled: bool = True
    instructions: str = (
        "You are a DevOps agent for PulsarCD. You MUST use the available MCP "
        "tools to investigate errors.\n"
        "Investigation tools (PulsarCD):\n"
        "- search_logs: search for errors in recent logs\n"
        "- list_containers: check container status\n"
        "- list_computers: check node health\n"
        "- get_action_status: check the status of a previous action\n\n"
        "IMPORTANT: NEVER call build_stack or deploy_stack directly. "
    )
    on_build_failure: str = (
        "A Docker build has failed. Steps:\n"
        "1. Call search_logs with the project name to find the build logs\n"
        "2. Identify the root cause (code error, Dockerfile, dependency, infra)\n"
        "3. Check the error-handling history below (if present) for prior reports "
        "of the same or similar issue\n"
        "4. If this error was NOT already reported, create a PulsarTeam task "
        "with the diagnosis and corrective actions via the create_task tool\n"
        "5. If a task already exists for this error, do NOT create a duplicate — "
        "instead summarize the current status and whether the issue persists"
    )
    on_test_failure: str = (
        "Tests have failed. Steps:\n"
        "1. Call search_logs to find the specific test errors\n"
        "2. Identify which tests failed and why\n"
        "3. Determine if it is a regression, a flaky test, or an environment issue\n"
        "4. Check the error-handling history below for prior reports of the same issue\n"
        "5. If this error was NOT already reported, create a PulsarTeam task "
        "with the diagnosis and affected tests via the create_task tool\n"
        "6. If a task already exists for this error, do NOT create a duplicate — "
        "summarize the current status instead"
    )
    on_deploy_failure: str = (
        "A deployment has failed. Steps:\n"
        "1. Call list_containers to check the service container status\n"
        "2. Call search_logs to find recent service errors\n"
        "3. Verify the Docker image exists and is accessible\n"
        "4. Check the error-handling history below for prior reports of the same issue\n"
        "5. If this error was NOT already reported, create a PulsarTeam task "
        "with the diagnosis and recommended actions via the create_task tool\n"
        "6. If a task already exists for this error, do NOT create a duplicate — "
        "summarize the current status instead"
    )
    on_recurring_error: str = (
        "A recurring error has been detected. Steps:\n"
        "1. Call search_logs with the error message to determine the scope\n"
        "2. Call list_containers to check the affected services\n"
        "3. Determine if the issue is systemic or isolated to one service\n"
        "4. Check the error-handling history below for prior reports of the same "
        "or similar error pattern\n"
        "5. If this error was NOT already reported, create a PulsarTeam task "
        "with the root cause and severity via the create_task tool\n"
        "6. If a task already exists for this error, do NOT create a duplicate. "
        "If the error appears to no longer be recurring, note that the issue "
        "seems resolved and skip task creation"
    )


class PipelineGatesConfig(BaseModel):
    """LLM gate configuration between pipeline stages."""
    build_to_test: bool = False
    test_to_deploy: bool = False
    instructions: str = (
        "You are a DevOps agent that validates CI/CD pipeline transitions. "
        "Analyze the logs from the previous stage and the version to determine "
        "whether it is safe to proceed to the next stage. "
        "You can use MCP tools (including git) to review the code changes "
        "that are about to be deployed. "
        "Respond ONLY with JSON: {\"approve\": true/false, \"reason\": \"...\"}"
    )
    on_build_to_test: str = (
        "The build succeeded. Check the build logs for critical warnings, "
        "review recent code changes via git, and determine whether tests "
        "can be launched safely."
    )
    on_test_to_deploy: str = (
        "Tests have passed. Review the test results, code changes via git, "
        "and determine whether the deployment can proceed safely. "
        "Be especially vigilant about database migrations and API changes."
    )


class TagCleanupConfig(BaseModel):
    """Automatic cleanup of old git tags and Docker registry images."""
    enabled: bool = False
    max_age_days: int = 30
    interval_hours: int = 24
    dry_run: bool = True
    keep_latest_n: int = 5


class PulsarConfig(BaseModel):
    """Top-level configuration loaded from config.yml."""
    llm: LLMConfig = LLMConfig()
    mcp_servers: List[MCPServerConfig] = [
        MCPServerConfig(),
        MCPServerConfig(name="pulsarteam", url="http://team-api:3001/api/swarm/mcp"),
    ]
    error_handling: ErrorHandlingConfig = ErrorHandlingConfig()
    pipeline_gates: PipelineGatesConfig = PipelineGatesConfig()
    tag_cleanup: TagCleanupConfig = TagCleanupConfig()


def _apply_env_overrides(config: PulsarConfig) -> PulsarConfig:
    """Override config file values with environment variables when set."""
    vllm_url = os.environ.get("PULSARCD_VLLM_URL")
    if vllm_url:
        config.llm.url = vllm_url

    ai_model = os.environ.get("PULSARCD_AI__MODEL")
    if ai_model:
        config.llm.model = ai_model

    return config


def load_config_file(data_dir: str = "/data") -> PulsarConfig:
    """Load configuration from YAML file, creating defaults if absent.

    Args:
        data_dir: Directory containing config.yml

    Returns:
        Parsed and env-overridden PulsarConfig
    """
    config_path = Path(data_dir) / "config.yml"

    if config_path.exists():
        try:
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            config = PulsarConfig(**raw)
            logger.info("Config file loaded", path=str(config_path))
        except Exception as e:
            logger.error("Failed to parse config file, using defaults",
                         path=str(config_path), error=str(e))
            config = PulsarConfig()
    else:
        config = PulsarConfig()
        try:
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                yaml.dump(config.model_dump(), default_flow_style=False, allow_unicode=True, sort_keys=False),
                encoding="utf-8",
            )
            logger.info("Default config file created", path=str(config_path))
        except Exception as e:
            logger.warning("Could not write default config file",
                           path=str(config_path), error=str(e))

    return _apply_env_overrides(config)


def save_config_file(config: PulsarConfig, data_dir: str = "/data") -> None:
    """Save configuration to YAML file.

    Args:
        config: Configuration to save
        data_dir: Directory containing config.yml
    """
    config_path = Path(data_dir) / "config.yml"
    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            yaml.dump(config.model_dump(), default_flow_style=False, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        logger.info("Config file saved", path=str(config_path))
    except Exception as e:
        logger.error("Failed to save config file", path=str(config_path), error=str(e))
        raise
