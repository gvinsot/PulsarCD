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
    url: str = "http://vllm:8000"
    model: str = "txn545/Qwen3.5-122B-A10B-NVFP4"
    api_key: str = ""


class MCPServerConfig(BaseModel):
    """MCP server endpoint configuration."""
    name: str = "pulsarcd"
    url: str = "http://localhost:8000/ai/mcp"
    api_key: str = ""


class ErrorHandlingConfig(BaseModel):
    """LLM error handling instructions."""
    enabled: bool = True
    instructions: str = (
        "Tu es un agent DevOps pour PulsarCD. Quand des erreurs surviennent, "
        "utilise les outils disponibles pour investiguer et determiner l'action "
        "appropriee. Sois conservateur - investigue avant d'agir."
    )
    on_build_failure: str = (
        "Un build Docker a echoue. Cherche dans les logs de build, regarde les "
        "commits recents, determine si c'est un probleme de code ou d'infra."
    )
    on_test_failure: str = (
        "Les tests ont echoue. Cherche les erreurs specifiques dans la sortie, "
        "verifie si les memes tests passaient recemment."
    )
    on_deploy_failure: str = (
        "Un deploiement a echoue. Verifie les logs du service, que l'image "
        "existe, et que les autres services sur le meme host sont sains."
    )
    on_recurring_error: str = (
        "Une erreur recurrente a ete detectee. Investigue avec la recherche "
        "de logs pour comprendre la portee et la cause racine."
    )


class PulsarConfig(BaseModel):
    """Top-level configuration loaded from config.yml."""
    llm: LLMConfig = LLMConfig()
    mcp_servers: List[MCPServerConfig] = [MCPServerConfig()]
    error_handling: ErrorHandlingConfig = ErrorHandlingConfig()


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
