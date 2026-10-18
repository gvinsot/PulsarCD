"""Docker registry settings shared by the backend and the build/deploy scripts.

The registry PulsarCD pushes to and deploys from is configured once, through
``PULSARCD_GITHUB__REGISTRY_URL`` (``settings.github.registry_url``). The shell
scripts in ``scripts/`` read it from the ``REGISTRY`` environment variable and
fall back to their historical default when it is unset, so a standalone run of
a script behaves exactly as before.
"""

import shlex
from typing import Optional

# Fallback used by scripts/build-push.sh, deploy-service.sh and test.sh when
# REGISTRY is not provided. Kept here for documentation and tests only: the
# backend never injects it, so leaving the setting empty preserves the
# scripts' own default.
DEFAULT_SCRIPT_REGISTRY = "registry.methodinfo.fr"

# Environment variable the scripts read the registry host from.
REGISTRY_ENV_VAR = "REGISTRY"


def normalize_registry_host(registry_url: Optional[str]) -> str:
    """Return the registry as an image-reference prefix (``host[:port][/path]``).

    Image references never carry a scheme, so an operator-supplied
    ``https://registry.example.com/`` is reduced to ``registry.example.com``.
    Returns an empty string when nothing is configured.
    """
    host = (registry_url or "").strip()
    for scheme in ("https://", "http://"):
        if host.lower().startswith(scheme):
            host = host[len(scheme):]
            break
    return host.rstrip("/")


def registry_env_prefix(registry_url: Optional[str]) -> str:
    """Shell prefix that exports the configured registry to a script invocation.

    The assignment is placed directly before ``bash <script>`` inside the
    command string, so it reaches the script identically whether the command
    runs locally or on the Docker host over SSH. An empty string is returned
    when no registry is configured, letting the script apply its default.
    """
    host = normalize_registry_host(registry_url)
    if not host:
        return ""
    return f"{REGISTRY_ENV_VAR}={shlex.quote(host)} "
