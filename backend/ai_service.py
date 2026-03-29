"""AI Service for natural language to OpenSearch query conversion using vLLM (OpenAI-compatible API)."""

import json
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import aiohttp
import structlog

logger = structlog.get_logger()


def build_system_prompt(metadata: Optional[Dict[str, Any]] = None) -> str:
    """Build system prompt with dynamic context from available metadata."""

    # Base prompt structure
    base_prompt = """You are an AI assistant that converts natural language questions about logs into OpenSearch query parameters.

You must respond with a valid JSON object containing these fields:
- query: string or null (full-text search query, use OpenSearch query syntax)
- levels: array of strings (log levels to filter)
- http_status_min: number or null (minimum HTTP status code)
- http_status_max: number or null (maximum HTTP status code)
- hosts: array of strings (host names to filter - use EXACT names from the available list)
- containers: array of strings (container names to filter - use EXACT names from the available list)
- compose_projects: array of strings (compose project names to filter - use EXACT names)
- time_range: string (relative time: "5m", "10m", "1h", "6h", "24h", "7d") or null
- sort_order: "desc" or "asc"

These parameters are converted into an OpenSearch query with the following structure:
{
  "query": {
    "bool": {
      "must": [
        {"query_string": {"query": "search terms", "default_field": "message"}}
      ],
      "filter": [
        {"terms": {"host": ["host1", "host2"]}},
        {"terms": {"container_name": ["container1", "container2"]}},
        {"terms": {"compose_project": ["project1"]}},
        {"terms": {"level": ["ERROR", "WARN"]}},
        {"range": {"http_status": {"gte": 400, "lte": 499}}},
        {"range": {"timestamp": {"gte": "2024-01-01T00:00:00", "lte": "2024-01-02T00:00:00"}}}
      ]
    }
  },
  "sort": [{"timestamp": {"order": "desc"}}],
  "from": 0,
  "size": 100
}

Available log fields in OpenSearch:
- timestamp: ISO datetime string
- host: string (keyword field - exact match only)
- container_name: string (keyword field - exact match only)
- container_id: string (keyword field)
- compose_project: string (keyword field - Docker Compose project name)
- compose_service: string (keyword field - Docker Compose service name)
- message: string (text field, full-text searchable with wildcards)
- level: string (keyword - exact match)
- http_status: integer (HTTP status code, e.g., 200, 404, 500)
- stream: string (stdout or stderr)

IMPORTANT RULES:
1. For hosts and containers filters, use EXACT names from the available values below
2. If user mentions a partial name, find the best matching full name from available values
3. The "query" field searches in the message text - use it for keywords like "timeout", "connection", "error message content"
4. The "levels" field filters by log level - use it for ERROR, WARN, INFO, DEBUG
5. If searching for a service/container by name, put it in "containers" array, NOT in "query"
6. Use "compose_projects" to filter by Docker Compose stack/project name"""

    # Add dynamic context if metadata is provided
    if metadata:
        context_parts = []

        if metadata.get("hosts"):
            hosts_list = metadata["hosts"][:30]  # Limit for prompt size
            context_parts.append(f"Available hosts: {json.dumps(hosts_list)}")

        if metadata.get("containers"):
            containers_list = metadata["containers"][:50]  # Limit for prompt size
            context_parts.append(f"Available containers: {json.dumps(containers_list)}")

        if metadata.get("compose_projects"):
            projects_list = metadata["compose_projects"][:20]
            context_parts.append(f"Available compose projects: {json.dumps(projects_list)}")

        if metadata.get("compose_services"):
            services_list = metadata["compose_services"][:50]
            context_parts.append(f"Available compose services: {json.dumps(services_list)}")

        if metadata.get("levels"):
            context_parts.append(f"Available log levels: {json.dumps(metadata['levels'])}")

        if context_parts:
            base_prompt += "\n\n=== AVAILABLE VALUES IN THIS ENVIRONMENT ===\n"
            base_prompt += "\n".join(context_parts)
            base_prompt += "\n\nUse these EXACT values when filtering by host, container, or project names."

    # Add examples with dynamic context hints
    base_prompt += """

Examples:
User: "Find errors from the last 10 minutes"
Response: {"query": null, "levels": ["ERROR"], "http_status_min": null, "http_status_max": null, "hosts": [], "containers": [], "compose_projects": [], "time_range": "10m", "sort_order": "desc"}

User: "Show me all 500 errors in nginx"
Response: {"query": null, "levels": [], "http_status_min": 500, "http_status_max": 599, "hosts": [], "containers": ["nginx"], "compose_projects": [], "time_range": null, "sort_order": "desc"}

User: "What warnings occurred in the api container in the last hour?"
Response: {"query": null, "levels": ["WARN"], "http_status_min": null, "http_status_max": null, "hosts": [], "containers": ["api"], "compose_projects": [], "time_range": "1h", "sort_order": "desc"}

User: "Find timeout errors from server-1"
Response: {"query": "timeout", "levels": ["ERROR"], "http_status_min": null, "http_status_max": null, "hosts": ["server-1"], "containers": [], "compose_projects": [], "time_range": null, "sort_order": "desc"}

User: "Show recent 404 not found errors"
Response: {"query": null, "levels": [], "http_status_min": 404, "http_status_max": 404, "hosts": [], "containers": [], "compose_projects": [], "time_range": "1h", "sort_order": "desc"}

User: "Logs from the monitoring stack"
Response: {"query": null, "levels": [], "http_status_min": null, "http_status_max": null, "hosts": [], "containers": [], "compose_projects": ["monitoring"], "time_range": null, "sort_order": "desc"}

User: "Show connection refused errors in the backend"
Response: {"query": "connection refused", "levels": ["ERROR"], "http_status_min": null, "http_status_max": null, "hosts": [], "containers": ["backend"], "compose_projects": [], "time_range": null, "sort_order": "desc"}

IMPORTANT: Only respond with the JSON object, no explanations or markdown."""

    return base_prompt


# Default system prompt (fallback without context)
SYSTEM_PROMPT = build_system_prompt()


class AIService:
    """Service for AI-powered query conversion using an OpenAI-compatible API."""

    def __init__(self, vllm_url: str = "http://localhost:8000", model: str = "Qwen/Qwen2.5-1.5B-Instruct", api_key: str = ""):
        self.vllm_url = vllm_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        # Resolve the chat endpoint: use as-is if it already contains
        # chat/completions (e.g. Gemini), otherwise append the standard path.
        stripped = self.vllm_url
        if "chat/completions" in stripped:
            self.chat_url = stripped
        else:
            self.chat_url = f"{stripped}/v1/chat/completions"
        self._session: Optional[aiohttp.ClientSession] = None
        self._available = False

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def check_availability(self) -> bool:
        """Check if the LLM provider is available and model is loaded."""
        try:
            session = await self._get_session()
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            async with session.get(f"{self.vllm_url}/v1/models", headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    models = [m.get("id", "") for m in data.get("data", [])]
                    # Check if our model or a variant is available
                    model_base = self.model.split("/")[-1].lower()
                    self._available = any(model_base in m.lower() for m in models) or len(models) > 0
                    if not self._available:
                        logger.warning("AI model not found on vLLM", model=self.model, available=models)
                    return self._available
        except Exception as e:
            logger.debug("vLLM not available", error=str(e))
            self._available = False
        return False

    async def _chat_completion(self, messages: list, max_tokens: int = 512, temperature: float = 0.1, timeout: float = 30) -> Optional[str]:
        """Send a chat completion request and return the response text."""
        session = await self._get_session()

        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        async with session.post(
            self.chat_url,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                choices = data.get("choices", [])
                if choices:
                    return choices[0].get("message", {}).get("content") or ""
            else:
                error_text = await resp.text()
                logger.error("vLLM request failed", status=resp.status, error=error_text[:200])
        return None

    async def convert_to_query(self, natural_query: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Convert natural language question to OpenSearch query parameters.

        Args:
            natural_query: The user's natural language question
            metadata: Optional dict containing available hosts, containers, projects, etc.
        """
        if not self._available:
            await self.check_availability()

        if not self._available:
            return self._fallback_parse(natural_query, metadata)

        try:
            system_prompt = build_system_prompt(metadata) if metadata else SYSTEM_PROMPT

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": natural_query},
            ]

            response_text = await self._chat_completion(messages, max_tokens=512, temperature=0.1, timeout=30)
            if response_text:
                return self._parse_ai_response(response_text, natural_query, metadata)
            else:
                return self._fallback_parse(natural_query, metadata)

        except Exception as e:
            logger.error("AI conversion failed", error=str(e))
            return self._fallback_parse(natural_query, metadata)

    def _parse_ai_response(self, response: str, original_query: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Parse AI response JSON."""
        try:
            # Try to extract JSON from response
            response = response.strip()

            # Handle markdown code blocks
            if "```json" in response:
                response = response.split("```json")[1].split("```")[0]
            elif "```" in response:
                response = response.split("```")[1].split("```")[0]

            # Parse JSON
            result = json.loads(response.strip())

            # Validate and normalize
            return {
                "query": result.get("query"),
                "levels": result.get("levels", []),
                "http_status_min": result.get("http_status_min"),
                "http_status_max": result.get("http_status_max"),
                "hosts": result.get("hosts", []),
                "containers": result.get("containers", []),
                "compose_projects": result.get("compose_projects", []),
                "time_range": result.get("time_range"),
                "sort_order": result.get("sort_order", "desc"),
            }

        except json.JSONDecodeError as e:
            logger.warning("Failed to parse AI response", response=response[:200], error=str(e))
            return self._fallback_parse(original_query, metadata)

    def _fallback_parse(self, query: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Simple fallback parser for when AI is unavailable.

        Uses metadata to match container/host names if available.
        """
        query_lower = query.lower()

        result = {
            "query": None,
            "levels": [],
            "http_status_min": None,
            "http_status_max": None,
            "hosts": [],
            "containers": [],
            "compose_projects": [],
            "time_range": None,
            "sort_order": "desc",
        }

        # Detect error levels
        if any(w in query_lower for w in ["error", "erreur", "fail", "fatal", "critical"]):
            result["levels"].append("ERROR")
        if any(w in query_lower for w in ["warning", "warn", "avertissement"]):
            result["levels"].append("WARN")
        if any(w in query_lower for w in ["debug"]):
            result["levels"].append("DEBUG")

        # Try to match container names from metadata
        if metadata and metadata.get("containers"):
            for container in metadata["containers"]:
                container_lower = container.lower()
                # Check if container name appears in query
                if container_lower in query_lower or any(
                    part in query_lower for part in container_lower.split("-") if len(part) > 2
                ):
                    result["containers"].append(container)
                    break  # Only match first container

        # Try to match host names from metadata
        if metadata and metadata.get("hosts"):
            for host in metadata["hosts"]:
                host_lower = host.lower()
                if host_lower in query_lower or any(
                    part in query_lower for part in host_lower.split("-") if len(part) > 2
                ):
                    result["hosts"].append(host)
                    break

        # Try to match compose projects from metadata
        if metadata and metadata.get("compose_projects"):
            for project in metadata["compose_projects"]:
                project_lower = project.lower()
                if project_lower in query_lower:
                    result["compose_projects"].append(project)
                    break

        # Detect time ranges
        time_patterns = [
            (r"(\d+)\s*minutes?", lambda m: f"{m.group(1)}m"),
            (r"(\d+)\s*hours?", lambda m: f"{m.group(1)}h"),
            (r"(\d+)\s*heures?", lambda m: f"{m.group(1)}h"),
            (r"last\s*hour", lambda m: "1h"),
            (r"dernière\s*heure", lambda m: "1h"),
            (r"today|aujourd", lambda m: "24h"),
            (r"yesterday|hier", lambda m: "24h"),
        ]

        for pattern, converter in time_patterns:
            match = re.search(pattern, query_lower)
            if match:
                result["time_range"] = converter(match)
                break

        # Detect HTTP status codes
        status_match = re.search(r"\b([45]\d{2})\b", query)
        if status_match:
            status = int(status_match.group(1))
            result["http_status_min"] = status
            result["http_status_max"] = status
        elif "5xx" in query_lower or "500" in query_lower:
            result["http_status_min"] = 500
            result["http_status_max"] = 599
        elif "4xx" in query_lower or "400" in query_lower:
            result["http_status_min"] = 400
            result["http_status_max"] = 499

        # Extract search terms (simple approach)
        # Remove common words and use remaining as query
        stop_words = {"find", "show", "get", "list", "search", "logs", "log", "from", "in", "the",
                     "last", "recent", "all", "me", "trouve", "affiche", "cherche", "les", "des",
                     "dernières", "derniers", "minutes", "heures", "hours", "errors", "warnings"}
        words = re.findall(r'\b\w+\b', query_lower)
        search_words = [w for w in words if w not in stop_words and len(w) > 2 and not w.isdigit()]

        if search_words and not result["levels"] and result["http_status_min"] is None:
            result["query"] = " ".join(search_words[:3])  # Limit to 3 words

        return result

    async def analyze_log(self, message: str, level: str = "", container_name: str = "") -> Dict[str, Any]:
        """Analyze a log message to determine if it needs attention."""

        # Quick heuristic checks first (avoid AI call for obvious cases)
        message_lower = message.lower()

        # Clear error indicators
        critical_patterns = [
            "exception", "fatal", "critical", "panic", "crash", "out of memory",
            "connection refused", "permission denied", "access denied", "segmentation fault",
            "stack trace", "traceback", "killed", "oom", "deadlock"
        ]

        error_patterns = [
            "error", "failed", "failure", "unable to", "cannot", "could not",
            "timeout", "timed out", "refused", "rejected", "invalid", "corrupt"
        ]

        warning_patterns = [
            "warning", "warn", "deprecated", "slow", "retry", "retrying",
            "high", "low memory", "disk space", "rate limit"
        ]

        # Check for obvious critical issues first (fast heuristic)
        if level in ["FATAL", "CRITICAL"] or any(p in message_lower for p in critical_patterns):
            # Still try AI for better description
            if not self._available:
                await self.check_availability()
            if self._available:
                try:
                    return await self._ai_analyze_log(message, level, container_name, hint_severity="critical")
                except Exception as e:
                    logger.warning("AI analysis failed for critical log", error=str(e))
            return {
                "severity": "critical",
                "assessment": "Critical issue detected - requires immediate attention."
            }

        # Check for HTTP logs with status codes (fast heuristic)
        is_http_log = "http" in message_lower and ('" 2' in message or '" 3' in message or '" 4' in message or '" 5' in message)
        if is_http_log:
            http_status = re.search(r'" (\d{3})', message)
            if http_status:
                status = int(http_status.group(1))
                if 200 <= status < 400:
                    return {
                        "severity": "normal",
                        "assessment": f"HTTP {status} - Successful request."
                    }
                elif 400 <= status < 500:
                    return {
                        "severity": "attention",
                        "assessment": f"HTTP {status} client error - Check request parameters or URL."
                    }
                elif status >= 500:
                    return {
                        "severity": "critical",
                        "assessment": f"HTTP {status} server error - Backend issue needs investigation."
                    }

        # For all other logs, try AI analysis
        if not self._available:
            await self.check_availability()

        if self._available:
            try:
                # Hint the AI about probable severity based on level
                hint = None
                if level == "ERROR" or any(p in message_lower for p in error_patterns):
                    hint = "attention"
                elif level in ["WARN", "WARNING"] or any(p in message_lower for p in warning_patterns):
                    hint = "attention"
                elif level == "DEBUG":
                    hint = "normal"

                return await self._ai_analyze_log(message, level, container_name, hint_severity=hint)
            except Exception as e:
                logger.debug("AI analysis failed, using heuristics", error=str(e))

        # Fallback to heuristics if AI not available
        has_error_in_path = "/error" in message_lower or "/errors" in message_lower

        if level == "ERROR" or (any(p in message_lower for p in error_patterns) and not has_error_in_path):
            return {
                "severity": "attention",
                "assessment": "Error indicator detected - review recommended."
            }

        if level in ["WARN", "WARNING"] or any(p in message_lower for p in warning_patterns):
            return {
                "severity": "attention",
                "assessment": "Warning indicator detected - may need monitoring."
            }

        # Default: appears normal
        return {
            "severity": "normal",
            "assessment": "Standard operational message."
        }

    async def _ai_analyze_log(self, message: str, level: str, container_name: str, hint_severity: str = None) -> Dict[str, Any]:
        """Use AI to analyze a log message."""
        # Build context hint if provided
        context_hint = ""
        if hint_severity:
            context_hint = f"\nNote: Based on log level '{level}', this is likely a '{hint_severity}' severity, but analyze the actual content."

        user_prompt = f"""Analyze this log message and provide a specific assessment.

Log message: {message[:500]}
Log level: {level or 'UNKNOWN'}
Container: {container_name or 'UNKNOWN'}{context_hint}

Respond with a JSON object containing:
- severity: "normal", "attention", or "critical"
- assessment: A brief, SPECIFIC explanation about THIS log (max 100 chars). Be precise about what the log shows.

Examples:
{{"severity": "normal", "assessment": "Startup message - service initialized successfully."}}
{{"severity": "attention", "assessment": "Connection to Redis timed out after 30s."}}
{{"severity": "critical", "assessment": "Database connection pool exhausted (0/50 available)."}}
{{"severity": "normal", "assessment": "Debug trace: processing user request ID 12345."}}
{{"severity": "attention", "assessment": "Deprecated API called - migrate to v2 endpoint."}}

DO NOT use generic messages. Describe what THIS specific log is about.
Respond only with valid JSON, no markdown or extra text."""

        messages = [
            {"role": "user", "content": user_prompt},
        ]

        response_text = await self._chat_completion(messages, max_tokens=150, temperature=0.3, timeout=15)

        if response_text:
            try:
                text = response_text.strip()
                if "```" in text:
                    text = text.split("```")[1] if "```json" in text else text.split("```")[0]
                    text = text.replace("json", "").strip()

                result = json.loads(text)
                severity = result.get("severity", "normal")
                if severity not in ["normal", "attention", "critical"]:
                    severity = "normal"

                return {
                    "severity": severity,
                    "assessment": result.get("assessment", "Analysis complete.")[:150]
                }
            except Exception as e:
                logger.warning("Failed to parse AI log analysis response",
                               error=str(e), response_preview=response_text[:200])

        # Fallback
        return {
            "severity": "normal",
            "assessment": "Unable to analyze. Log appears standard."
        }


# Global instance
ai_service: Optional[AIService] = None


def get_ai_service() -> AIService:
    """Get or create AI service instance using PulsarConfig.llm as source of truth."""
    from .config import settings

    global ai_service

    pulsar_config = getattr(settings, "pulsar_config", None)
    llm = getattr(pulsar_config, "llm", None) if pulsar_config else None

    if llm:
        url = llm.url
        model = llm.model
        api_key = llm.api_key
    else:
        import os
        url = os.environ.get("PULSARCD_VLLM_URL", "http://vllm:8000")
        model = settings.ai.model
        api_key = ""

    # Recreate if config has changed since last call
    if ai_service is not None:
        if ai_service.vllm_url != url.rstrip("/") or ai_service.model != model or ai_service.api_key != api_key:
            logger.info("LLM config changed, recreating AIService",
                        old_url=ai_service.vllm_url, new_url=url,
                        old_model=ai_service.model, new_model=model)
            ai_service = None

    if ai_service is None:
        ai_service = AIService(url, model, api_key)
    return ai_service
