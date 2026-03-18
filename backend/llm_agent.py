"""LLM Agent with MCP tool-calling for PulsarCD error handling.

Replaces direct Swarm API notifications with an agentic loop that:
1. Receives error context + instructions from config
2. Discovers MCP tools from configured servers
3. Runs an iterative tool-calling loop via vLLM (OpenAI-compatible API)
4. Uses MCP tools to investigate and take action

Context compaction:
- Estimates token usage per message (~4 chars/token)
- Progressively compresses oldest tool results when approaching the context budget
- Preserves the system prompt, latest user message, and recent tool exchanges
- Supports 256k context windows with 128k output
"""

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import structlog

from .config_file import PulsarConfig

logger = structlog.get_logger()

_NOTIFICATION_COOLDOWN = timedelta(hours=1)
_MAX_ITERATIONS = 25  # More iterations with context compaction
_MAX_OUTPUT_BYTES = 64 * 1024  # 64 KB for error output in prompts
_MAX_HISTORY = 100  # Max entries in agent history

# Token estimation: ~4 chars per token for mixed content (code, logs, natural language).
# This is a conservative estimate; actual ratio varies by language/content.
_CHARS_PER_TOKEN = 4

# When compacting, keep at least this many of the most recent message pairs intact
_MIN_RECENT_MESSAGES = 4

# Compaction triggers when estimated tokens exceed this fraction of the context budget
_COMPACTION_TRIGGER_RATIO = 0.65  # Trigger at 65% of context budget (leaves room for output)

_ERROR_LINE_RE = re.compile(
    r'\b(error|ERROR|Error|FAIL|failed|FAILED|exception|Exception|'
    r'Traceback|traceback|fatal|FATAL|critical|CRITICAL)\b'
)


# ---------------------------------------------------------------------------
# Token estimation & context compaction
# ---------------------------------------------------------------------------

def _estimate_tokens(text: str) -> int:
    """Estimate token count from text length. Conservative: ~4 chars/token."""
    if not text:
        return 0
    return max(1, len(text) // _CHARS_PER_TOKEN)


def _estimate_message_tokens(msg: dict) -> int:
    """Estimate tokens for a single chat message (content + tool_calls metadata)."""
    tokens = 4  # overhead per message (role, separators)
    content = msg.get("content") or ""
    tokens += _estimate_tokens(content)
    # Tool calls in assistant messages add JSON overhead
    tool_calls = msg.get("tool_calls")
    if tool_calls:
        tokens += _estimate_tokens(json.dumps(tool_calls))
    return tokens


def _estimate_messages_tokens(messages: List[dict]) -> int:
    """Estimate total tokens across all messages."""
    return sum(_estimate_message_tokens(m) for m in messages)


def _truncate_tool_result(content: str, max_chars: int) -> str:
    """Truncate a tool result to max_chars, keeping head + tail for context."""
    if len(content) <= max_chars:
        return content
    if max_chars < 200:
        return content[:max_chars] + "\n... [truncated]"
    # Keep 60% head, 40% tail to preserve both start context and final output
    head_size = int(max_chars * 0.6)
    tail_size = max_chars - head_size - 50  # 50 chars for truncation marker
    head = content[:head_size]
    tail = content[-tail_size:] if tail_size > 0 else ""
    # Cut at line boundaries when possible
    head_nl = head.rfind('\n')
    if head_nl > head_size // 2:
        head = head[:head_nl]
    tail_nl = tail.find('\n')
    if tail_nl > 0 and tail_nl < len(tail) // 2:
        tail = tail[tail_nl + 1:]
    original_lines = content.count('\n') + 1
    return f"{head}\n\n... [{original_lines} lines, truncated to fit context budget] ...\n\n{tail}"


def _summarize_tool_result(content: str, max_chars: int = 300) -> str:
    """Aggressively summarize a tool result — keep only error lines + stats."""
    lines = content.strip().splitlines()
    error_lines = [l for l in lines if _ERROR_LINE_RE.search(l)]

    if error_lines:
        summary_parts = error_lines[:5]  # Keep up to 5 error lines
        summary = '\n'.join(summary_parts)
        if len(error_lines) > 5:
            summary += f"\n... (+{len(error_lines) - 5} more error lines)"
    else:
        # No error lines — keep first 2 and last 2 lines
        if len(lines) <= 4:
            summary = '\n'.join(lines)
        else:
            summary = '\n'.join(lines[:2]) + f"\n... [{len(lines)} lines] ...\n" + '\n'.join(lines[-2:])

    if len(summary) > max_chars:
        summary = summary[:max_chars] + "..."
    return f"[COMPACTED] {summary}"


def compact_messages(
    messages: List[dict],
    context_budget_tokens: int,
    output_budget_tokens: int,
) -> List[dict]:
    """Compact conversation messages to fit within the context token budget.

    Strategy (applied in order until we fit):
    1. Truncate old tool results progressively (oldest first, 50% reduction each pass)
    2. Summarize old tool results to just error lines + stats
    3. Drop oldest tool result/response pairs entirely (preserving conversation flow)

    Never touches:
    - The system prompt (messages[0])
    - The initial user message (messages[1])
    - The most recent _MIN_RECENT_MESSAGES messages
    """
    # Available budget for input messages = context - output reservation
    input_budget = context_budget_tokens - output_budget_tokens
    if input_budget < 4000:
        input_budget = 4000  # Absolute minimum

    current_tokens = _estimate_messages_tokens(messages)
    if current_tokens <= input_budget:
        return messages  # No compaction needed

    logger.debug("Context compaction triggered",
                 current_tokens=current_tokens,
                 input_budget=input_budget,
                 message_count=len(messages))

    # Work on a copy
    messages = [dict(m) for m in messages]

    # Identify compactable tool-result messages (skip system, first user, and recent)
    # Tool results are messages with role="tool"
    protected_count = max(_MIN_RECENT_MESSAGES, 2)  # At least system + user
    compactable_indices = []
    for i in range(2, max(2, len(messages) - protected_count)):
        if messages[i].get("role") == "tool":
            compactable_indices.append(i)

    # Phase 1: Progressive truncation (oldest first, reduce by 50% each pass)
    for pass_num in range(3):  # Up to 3 passes
        if _estimate_messages_tokens(messages) <= input_budget:
            break
        for idx in compactable_indices:
            content = messages[idx].get("content", "")
            current_len = len(content)
            # Each pass halves the allowed length, minimum 500 chars
            target_len = max(500, current_len // 2)
            if current_len > target_len:
                messages[idx] = dict(messages[idx])
                messages[idx]["content"] = _truncate_tool_result(content, target_len)
        if _estimate_messages_tokens(messages) <= input_budget:
            break

    # Phase 2: Aggressive summarization of oldest tool results
    if _estimate_messages_tokens(messages) > input_budget:
        for idx in compactable_indices:
            content = messages[idx].get("content", "")
            if len(content) > 400 and not content.startswith("[COMPACTED]"):
                messages[idx] = dict(messages[idx])
                messages[idx]["content"] = _summarize_tool_result(content, 300)
            if _estimate_messages_tokens(messages) <= input_budget:
                break

    # Phase 3: Drop oldest tool exchange pairs (tool_call assistant + tool result)
    if _estimate_messages_tokens(messages) > input_budget:
        # Find droppable pairs: (assistant with tool_calls, following tool results)
        drop_indices = set()
        for idx in compactable_indices:
            if _estimate_messages_tokens(messages) <= input_budget:
                break
            # Mark this tool result for dropping
            drop_indices.add(idx)
            # Also check if the preceding assistant message only had tool_calls
            # (no useful text content) — if so, drop it too
            if idx > 0 and messages[idx - 1].get("role") == "assistant":
                assistant_msg = messages[idx - 1]
                if assistant_msg.get("tool_calls") and not assistant_msg.get("content", "").strip():
                    # Check no other tool results reference this assistant's tool_calls
                    drop_indices.add(idx - 1)

        if drop_indices:
            messages = [m for i, m in enumerate(messages) if i not in drop_indices]

    final_tokens = _estimate_messages_tokens(messages)
    logger.info("Context compaction complete",
                original_tokens=current_tokens,
                compacted_tokens=final_tokens,
                reduction_pct=round((1 - final_tokens / max(1, current_tokens)) * 100, 1),
                message_count=len(messages))

    return messages


# ---------------------------------------------------------------------------
# Error output builder
# ---------------------------------------------------------------------------

def _build_error_output(output: str, max_bytes: int = _MAX_OUTPUT_BYTES) -> str:
    """Build compact error output prioritizing error lines + tail context."""
    if not output or not output.strip():
        return "(no output)"

    lines = output.strip().splitlines()
    error_lines = [line for line in lines if _ERROR_LINE_RE.search(line)]
    context_lines = lines[-50:]

    seen: set = set()
    merged: list = []
    for line in error_lines + context_lines:
        if line not in seen:
            seen.add(line)
            merged.append(line)

    result = '\n'.join(merged)
    encoded = result.encode('utf-8')
    if len(encoded) > max_bytes:
        truncated = encoded[:max_bytes - 60].decode('utf-8', errors='ignore')
        last_nl = truncated.rfind('\n')
        if last_nl > max_bytes // 2:
            truncated = truncated[:last_nl]
        result = truncated + f'\n... [truncated - {len(lines)} lines total]'

    return result


class LLMAgent:
    """Async LLM agent that uses MCP tools to investigate and handle errors."""

    def __init__(self, config: PulsarConfig, mcp_api_key: str = "", data_dir: str = "/data"):
        self._llm_url = config.llm.url.rstrip("/")
        self._llm_model = config.llm.model
        self._llm_api_key = config.llm.api_key
        self._context_tokens = config.llm.context_tokens
        self._max_output_tokens = config.llm.max_output_tokens
        self._mcp_servers = config.mcp_servers
        self._error_handling = config.error_handling
        self._pipeline_gates = config.pipeline_gates
        self._mcp_api_key = mcp_api_key
        self._tools_cache: Optional[List[dict]] = None
        self._tool_server_map: Dict[str, Tuple[str, str]] = {}
        self._cooldown_map: Dict[str, datetime] = {}
        self._history_path = Path(data_dir) / "agent_history.json"
        self._history: List[Dict[str, Any]] = self._load_history()

    def _load_history(self) -> List[Dict[str, Any]]:
        """Load agent history from file."""
        if self._history_path.exists():
            try:
                raw = json.loads(self._history_path.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    return raw[-_MAX_HISTORY:]
            except Exception as e:
                logger.warning("Failed to load agent history", error=str(e))
        return []

    def _save_history(self) -> None:
        """Persist agent history to file."""
        try:
            self._history_path.parent.mkdir(parents=True, exist_ok=True)
            self._history_path.write_text(
                json.dumps(self._history, ensure_ascii=False, indent=1),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning("Failed to save agent history", error=str(e))

    def _record(self, action_type: str, **kwargs) -> None:
        """Record an agent action in history and persist to file."""
        entry = {
            "type": action_type,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            **kwargs,
        }
        self._history.append(entry)
        if len(self._history) > _MAX_HISTORY:
            self._history = self._history[-_MAX_HISTORY:]
        self._save_history()

    def get_history(self) -> List[Dict[str, Any]]:
        """Return agent action history (newest first)."""
        return list(reversed(self._history))

    def _is_cooled_down(self, dedup_key: str) -> bool:
        """Check if a dedup key is within cooldown window."""
        now = datetime.utcnow()
        last_sent = self._cooldown_map.get(dedup_key)
        if last_sent and (now - last_sent) < _NOTIFICATION_COOLDOWN:
            return True
        # Purge stale entries
        stale = [k for k, ts in self._cooldown_map.items()
                 if (now - ts) >= _NOTIFICATION_COOLDOWN]
        for k in stale:
            del self._cooldown_map[k]
        return False

    async def _discover_tools(self) -> List[dict]:
        """Discover tools from all configured MCP servers via JSON-RPC tools/list."""
        if self._tools_cache is not None:
            return self._tools_cache

        tools = []
        self._tool_server_map = {}

        for server in self._mcp_servers:
            server_url = server.url.rstrip("/")
            api_key = server.api_key or self._mcp_api_key
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"

            try:
                payload = {"jsonrpc": "2.0", "method": "tools/list", "id": 1}
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        server_url,
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        data = await resp.json()
                        mcp_tools = data.get("result", {}).get("tools", [])
                        for tool in mcp_tools:
                            tool_name = tool["name"]
                            tools.append({
                                "type": "function",
                                "function": {
                                    "name": tool_name,
                                    "description": tool.get("description", ""),
                                    "parameters": tool.get("inputSchema", {
                                        "type": "object",
                                        "properties": {},
                                    }),
                                },
                            })
                            self._tool_server_map[tool_name] = (server_url, api_key)

                        logger.info("MCP tools discovered",
                                    server=server.name,
                                    tool_count=len(mcp_tools),
                                    tools=[t["name"] for t in mcp_tools])

            except Exception as e:
                logger.warning("Failed to discover MCP tools",
                               server=server.name, url=server_url,
                               error_type=type(e).__name__, error=str(e))

        self._tools_cache = tools
        return tools

    async def _call_tool(self, name: str, arguments: dict) -> str:
        """Execute a tool call via MCP JSON-RPC tools/call."""
        server_info = self._tool_server_map.get(name)
        if not server_info:
            return f"Error: unknown tool '{name}'"

        server_url, api_key = server_info
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        payload = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
            "id": 1,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    server_url,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    data = await resp.json()

                    if "error" in data:
                        error = data["error"]
                        return f"Tool error: {error.get('message', str(error))}"

                    result = data.get("result", {})
                    content_parts = result.get("content", [])
                    text_parts = []
                    for part in content_parts:
                        if isinstance(part, dict):
                            text_parts.append(part.get("text", json.dumps(part)))
                        else:
                            text_parts.append(str(part))
                    return "\n".join(text_parts) if text_parts else json.dumps(result)

        except Exception as e:
            logger.warning("MCP tool call failed",
                           tool=name, error_type=type(e).__name__, error=str(e))
            return f"Error calling tool '{name}': {e}"

    def _compute_max_output_tokens(self, messages: List[dict]) -> int:
        """Compute max_tokens for the LLM response based on remaining context budget.

        Ensures we don't request more output tokens than the context can hold,
        while still being generous with the output budget.
        """
        input_tokens = _estimate_messages_tokens(messages)
        # Reserve the full output budget, but cap so input + output <= context
        available = self._context_tokens - input_tokens
        # Use the configured max output but don't exceed what's available
        max_out = min(self._max_output_tokens, max(2048, available))
        return max_out

    async def _run_agent(self, system_prompt: str, user_message: str) -> str:
        """Run the agentic tool-calling loop with context compaction.

        Sends messages to the LLM with available MCP tools, processes tool calls,
        and iterates until the LLM gives a final text response.

        Context management:
        - Before each LLM call, estimates total token usage
        - When approaching the context budget, compacts older messages
        - Preserves the system prompt, initial user message, and recent exchanges
        """
        tools = await self._discover_tools()
        openai_tools = tools if tools else None

        # Estimate tokens consumed by tools schema (sent with every request)
        tools_tokens = _estimate_tokens(json.dumps(openai_tools)) if openai_tools else 0

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]

        for iteration in range(_MAX_ITERATIONS):
            # --- Context compaction before each LLM call ---
            # Budget = context_tokens - tools overhead
            effective_budget = self._context_tokens - tools_tokens
            messages = compact_messages(
                messages,
                context_budget_tokens=effective_budget,
                output_budget_tokens=self._max_output_tokens,
            )

            # Compute dynamic max_tokens for this call
            max_output = self._compute_max_output_tokens(messages)

            # Build LLM request
            payload: Dict[str, Any] = {
                "model": self._llm_model,
                "messages": messages,
                "max_tokens": max_output,
                "temperature": 0.2,
            }
            if openai_tools:
                payload["tools"] = openai_tools

            headers = {"Content-Type": "application/json"}
            if self._llm_api_key:
                headers["Authorization"] = f"Bearer {self._llm_api_key}"

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self._llm_url}/v1/chat/completions",
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=120),
                    ) as resp:
                        if resp.status != 200:
                            body = await resp.text()
                            logger.error("LLM API error",
                                         status=resp.status, body=body[:500])
                            return f"LLM API error (status {resp.status})"

                        data = await resp.json()

            except Exception as e:
                logger.error("LLM API request failed",
                             error_type=type(e).__name__, error=str(e))
                return f"LLM request failed: {e}"

            choice = data.get("choices", [{}])[0]
            assistant_msg = choice.get("message", {})
            finish_reason = choice.get("finish_reason", "stop")

            # Append assistant message to conversation
            messages.append(assistant_msg)

            # If no tool calls, the agent is done
            tool_calls = assistant_msg.get("tool_calls")
            if finish_reason != "tool_calls" and not tool_calls:
                final_content = assistant_msg.get("content", "")
                if iteration == 0 and openai_tools:
                    logger.warning("LLM agent responded without calling any tools",
                                   response_preview=final_content[:200])
                logger.info("LLM agent completed",
                            iterations=iteration + 1,
                            tool_calls_made=iteration > 0,
                            response_len=len(final_content),
                            context_tokens=_estimate_messages_tokens(messages))
                return final_content

            # Execute each tool call
            if tool_calls:
                for tool_call in tool_calls:
                    fn = tool_call.get("function", {})
                    fn_name = fn.get("name", "")
                    try:
                        fn_args = json.loads(fn.get("arguments", "{}"))
                    except json.JSONDecodeError:
                        fn_args = {}

                    logger.info("LLM agent calling tool",
                                tool=fn_name, iteration=iteration + 1)

                    result = await self._call_tool(fn_name, fn_args)

                    # Initial truncation of very large tool results (256KB hard cap)
                    if len(result) > 256 * 1024:
                        result = _truncate_tool_result(result, 256 * 1024)

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.get("id", ""),
                        "content": result,
                    })

        logger.warning("LLM agent reached max iterations", max=_MAX_ITERATIONS)
        return "Agent reached maximum iterations without a final response."

    async def handle_failure(self, stage: str, repo_name: str, version: str, error_output: str):
        """Handle a build/test/deploy failure via the LLM agent.

        Called from api.py when a build, test, or deploy fails.
        """
        if not self._error_handling.enabled:
            logger.debug("Error handling disabled in config")
            return

        dedup_key = f"failure:{stage}:{repo_name}"
        if self._is_cooled_down(dedup_key):
            logger.info("LLM agent skipped (cooldown)", stage=stage, repo=repo_name)
            return

        instruction_map = {
            "build": self._error_handling.on_build_failure,
            "test": self._error_handling.on_test_failure,
            "deploy": self._error_handling.on_deploy_failure,
        }
        specific_instructions = instruction_map.get(stage, "")

        system_prompt = (
            f"{self._error_handling.instructions}\n\n"
            f"--- Specific instructions for this error ---\n"
            f"{specific_instructions}\n\n"
            f"IMPORTANT: You MUST call at least one MCP tool to investigate "
            f"before responding. A response without any tool call is insufficient.\n"
            f"Respond with: 1) Tools called and their results "
            f"2) Diagnosis 3) Actions taken or recommended."
        )

        compact_output = _build_error_output(error_output)
        user_message = (
            f"{stage.upper()} FAILED for project '{repo_name}' (version: {version}).\n"
            f"Error output:\n```\n{compact_output}\n```"
        )

        logger.info("LLM agent handling failure",
                     stage=stage, repo=repo_name, version=version)

        try:
            result = await self._run_agent(system_prompt, user_message)
            self._cooldown_map[dedup_key] = datetime.utcnow()
            self._record("failure_handled", stage=stage, repo=repo_name,
                         version=version, response=result[:2000] if result else "")
            logger.info("LLM agent handled failure",
                        stage=stage, repo=repo_name,
                        result_preview=result[:200] if result else "(empty)")
        except Exception as e:
            self._record("failure_error", stage=stage, repo=repo_name,
                         version=version, error=str(e))
            logger.error("LLM agent error during failure handling",
                         stage=stage, repo=repo_name,
                         error_type=type(e).__name__, error=str(e))

    async def handle_recurring_error(self, pattern, resolved_stacks: list = None) -> Optional[str]:
        """Handle a recurring error pattern via the LLM agent.

        Called from error_detector.py when a recurring error pattern
        exceeds the notification threshold.

        Args:
            pattern: ErrorPattern with count, services, sample_message, etc.
            resolved_stacks: Stack names resolved from pipeline state (proper case).

        Returns:
            LLM agent's final response text, or None on failure.
        """
        # Build label info for history recording (needed regardless of enabled/cooldown)
        services_list = ', '.join(sorted(pattern.services)[:10])
        stacks = resolved_stacks if resolved_stacks is not None else (
            sorted(pattern.compose_projects)[:10] if pattern.compose_projects else []
        )
        projects_list = ', '.join(stacks) if stacks else ''
        stack_service_label = ''
        if projects_list and services_list:
            stack_service_label = f"{projects_list} / {services_list}"
        elif services_list:
            stack_service_label = services_list
        elif projects_list:
            stack_service_label = projects_list

        if not self._error_handling.enabled:
            logger.info("LLM agent: error handling disabled, recording detection only",
                        fingerprint=pattern.fingerprint, count=pattern.count)
            self._record("recurring_detected", services=services_list,
                         projects=projects_list, label=stack_service_label,
                         count=pattern.count,
                         response="Error handling disabled — detection only (no LLM investigation)")
            return None

        dedup_key = f"recurring:{pattern.fingerprint}"
        if self._is_cooled_down(dedup_key):
            logger.info("LLM agent skipped recurring error (cooldown)",
                        fingerprint=pattern.fingerprint, count=pattern.count)
            self._record("recurring_cooldown", services=services_list,
                         projects=projects_list, label=stack_service_label,
                         count=pattern.count,
                         response="Skipped — cooldown active (max 1 investigation per hour per pattern)")
            return None

        system_prompt = (
            f"{self._error_handling.instructions}\n\n"
            f"--- Specific instructions for this error ---\n"
            f"{self._error_handling.on_recurring_error}\n\n"
            f"IMPORTANT: You MUST call at least one MCP tool to investigate "
            f"before responding. A response without any tool call is insufficient.\n"
            f"Respond with: 1) Tools called and their results "
            f"2) Diagnosis 3) Actions taken or recommended."
        )

        duration = pattern.last_seen - pattern.first_seen
        if duration.total_seconds() < 3600:
            duration_str = f"{int(duration.total_seconds())}s"
        else:
            duration_str = f"{duration.total_seconds() / 3600:.1f}h"

        user_message = (
            f"RECURRING ERROR DETECTED\n"
            f"========================\n"
            f"Occurrences: {pattern.count} in {duration_str}\n"
            f"Affected services: {services_list}\n"
            f"Error sample:\n```\n{pattern.sample_message}\n```"
        )

        logger.info("LLM agent handling recurring error",
                     fingerprint=pattern.fingerprint,
                     count=pattern.count, services=services_list)

        try:
            result = await self._run_agent(system_prompt, user_message)
            self._cooldown_map[dedup_key] = datetime.utcnow()
            self._record("recurring_handled", services=services_list,
                         projects=projects_list, label=stack_service_label,
                         count=pattern.count,
                         response=result[:2000] if result else "")
            logger.info("LLM agent handled recurring error",
                        fingerprint=pattern.fingerprint,
                        result_preview=result[:200] if result else "(empty)")
            return result
        except Exception as e:
            self._record("recurring_error", services=services_list,
                         projects=projects_list, label=stack_service_label,
                         count=pattern.count, error=str(e))
            logger.error("LLM agent error during recurring error handling",
                         fingerprint=pattern.fingerprint,
                         error_type=type(e).__name__, error=str(e))
            return None

    async def evaluate_gate(
        self, transition: str, repo_name: str, version: str, stage_output: str
    ) -> Tuple[bool, str]:
        """Evaluate whether the pipeline should proceed to the next stage.

        Args:
            transition: "build_to_test" or "test_to_deploy"
            repo_name: Repository name
            version: Current version/tag
            stage_output: Output logs from the completed stage

        Returns:
            (approved, reason) tuple. approved=True means proceed.
        """
        gates = self._pipeline_gates

        # Check if this gate is enabled
        if transition == "build_to_test" and not gates.build_to_test:
            return True, "Gate disabled, auto-approved"
        if transition == "test_to_deploy" and not gates.test_to_deploy:
            return True, "Gate disabled, auto-approved"

        instruction_map = {
            "build_to_test": gates.on_build_to_test,
            "test_to_deploy": gates.on_test_to_deploy,
        }
        specific = instruction_map.get(transition, "")

        system_prompt = (
            f"{gates.instructions}\n\n"
            f"{specific}\n\n"
            f"You have access to MCP tools. Use them to check git history, "
            f"code changes, and any other relevant information.\n"
            f"After your analysis, you MUST respond with a JSON object:\n"
            f'{{"approve": true, "reason": "..."}}\n'
            f"or\n"
            f'{{"approve": false, "reason": "..."}}\n'
            f"The JSON must be the LAST line of your response."
        )

        compact_output = _build_error_output(stage_output)
        from_stage = transition.split("_to_")[0]
        to_stage = transition.split("_to_")[1]
        user_message = (
            f"PIPELINE GATE: {from_stage.upper()} → {to_stage.upper()}\n"
            f"Project: {repo_name}\n"
            f"Version: {version}\n"
            f"{from_stage.capitalize()} output:\n```\n{compact_output}\n```"
        )

        logger.info("LLM gate evaluation starting",
                     transition=transition, repo=repo_name, version=version)

        try:
            result = await self._run_agent(system_prompt, user_message)

            # Parse JSON decision from response (last line or embedded)
            approved, reason = self._parse_gate_decision(result)
            self._record("gate_decision", transition=transition, repo=repo_name,
                         version=version, approved=approved, reason=reason[:500])
            logger.info("LLM gate decision",
                        transition=transition, repo=repo_name,
                        approved=approved, reason=reason[:200])
            return approved, reason

        except Exception as e:
            self._record("gate_error", transition=transition, repo=repo_name,
                         version=version, error=str(e))
            logger.error("LLM gate evaluation failed, auto-approving",
                         transition=transition, repo=repo_name,
                         error_type=type(e).__name__, error=str(e))
            return True, f"Gate error (auto-approved): {e}"

    @staticmethod
    def _parse_gate_decision(response: str) -> Tuple[bool, str]:
        """Extract approve/reject decision from LLM response."""
        if not response:
            return True, "Empty response, auto-approved"

        # Try to find JSON in the response (last occurrence)
        import re
        json_pattern = re.compile(r'\{[^{}]*"approve"\s*:\s*(true|false)[^{}]*\}', re.IGNORECASE)
        matches = list(json_pattern.finditer(response))

        if matches:
            try:
                decision = json.loads(matches[-1].group())
                approved = bool(decision.get("approve", True))
                reason = decision.get("reason", "No reason provided")
                return approved, reason
            except (json.JSONDecodeError, KeyError):
                pass

        # Fallback: look for keywords
        lower = response.lower()
        if '"approve": false' in lower or '"approve":false' in lower:
            return False, response[-500:]
        return True, response[-500:] if response else "Could not parse, auto-approved"

    def invalidate_tools_cache(self):
        """Force re-discovery of MCP tools on next call."""
        self._tools_cache = None
        self._tool_server_map = {}
