"""Unit tests for context compaction in backend/llm_agent.py."""

import pytest

from backend.llm_agent import (
    _estimate_tokens,
    _estimate_message_tokens,
    _estimate_messages_tokens,
    _truncate_tool_result,
    _summarize_tool_result,
    compact_messages,
)


class TestTokenEstimation:
    def test_empty(self):
        assert _estimate_tokens("") == 0

    def test_short_text(self):
        assert _estimate_tokens("hello") >= 1

    def test_proportional(self):
        short = _estimate_tokens("abc")
        long = _estimate_tokens("a" * 400)
        assert long > short

    def test_message_tokens_includes_overhead(self):
        msg = {"role": "user", "content": "hello"}
        tokens = _estimate_message_tokens(msg)
        assert tokens > _estimate_tokens("hello")

    def test_messages_tokens_additive(self):
        msgs = [
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hi there"},
        ]
        total = _estimate_messages_tokens(msgs)
        assert total == sum(_estimate_message_tokens(m) for m in msgs)


class TestTruncateToolResult:
    def test_short_content_unchanged(self):
        content = "short result"
        assert _truncate_tool_result(content, 1000) == content

    def test_long_content_truncated(self):
        content = "line\n" * 500
        result = _truncate_tool_result(content, 200)
        assert len(result) < len(content)
        assert "truncated" in result.lower()

    def test_preserves_head_and_tail(self):
        lines = [f"line_{i}" for i in range(100)]
        content = "\n".join(lines)
        result = _truncate_tool_result(content, 300)
        # Should contain content from the start
        assert "line_0" in result
        # Should contain content from the end
        assert "line_99" in result


class TestSummarizeToolResult:
    def test_with_error_lines(self):
        content = "INFO: starting\nERROR: disk full\nINFO: processing\nERROR: timeout\nINFO: done"
        result = _summarize_tool_result(content, 300)
        assert "[COMPACTED]" in result
        assert "disk full" in result
        assert "timeout" in result

    def test_without_error_lines(self):
        content = "\n".join([f"normal line {i}" for i in range(20)])
        result = _summarize_tool_result(content, 300)
        assert "[COMPACTED]" in result
        # Should keep first and last lines
        assert "normal line 0" in result
        assert "normal line 19" in result


class TestCompactMessages:
    def _make_messages(self, tool_result_size=1000, num_tool_exchanges=5):
        """Build a conversation with system + user + N tool exchanges."""
        messages = [
            {"role": "system", "content": "You are a DevOps agent."},
            {"role": "user", "content": "Investigate the error."},
        ]
        for i in range(num_tool_exchanges):
            # Assistant with tool call
            messages.append({
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": f"call_{i}", "function": {"name": f"tool_{i}", "arguments": "{}"}}],
            })
            # Tool result
            messages.append({
                "role": "tool",
                "tool_call_id": f"call_{i}",
                "content": f"result line {i}\n" * (tool_result_size // 15),
            })
        # Final assistant response
        messages.append({"role": "assistant", "content": "Analysis complete."})
        return messages

    def test_no_compaction_when_under_budget(self):
        """Messages under budget should be returned unchanged."""
        msgs = self._make_messages(tool_result_size=100, num_tool_exchanges=2)
        result = compact_messages(msgs, context_budget_tokens=100000, output_budget_tokens=50000)
        assert len(result) == len(msgs)

    def test_compaction_reduces_tokens(self):
        """Large messages should be compacted below budget."""
        msgs = self._make_messages(tool_result_size=50000, num_tool_exchanges=10)
        original_tokens = _estimate_messages_tokens(msgs)
        # Set a budget much smaller than the original
        budget = original_tokens // 4
        result = compact_messages(msgs, context_budget_tokens=budget, output_budget_tokens=budget // 2)
        compacted_tokens = _estimate_messages_tokens(result)
        assert compacted_tokens < original_tokens

    def test_system_and_user_preserved(self):
        """System prompt and initial user message must never be removed."""
        msgs = self._make_messages(tool_result_size=50000, num_tool_exchanges=10)
        result = compact_messages(msgs, context_budget_tokens=5000, output_budget_tokens=2000)
        assert result[0]["role"] == "system"
        assert result[0]["content"] == "You are a DevOps agent."
        assert result[1]["role"] == "user"
        assert result[1]["content"] == "Investigate the error."

    def test_recent_messages_preserved(self):
        """Recent messages (last few) should be kept intact."""
        msgs = self._make_messages(tool_result_size=10000, num_tool_exchanges=8)
        last_msg = msgs[-1]
        result = compact_messages(msgs, context_budget_tokens=10000, output_budget_tokens=4000)
        # The final assistant message should still be present
        assert any(m.get("content") == last_msg["content"] for m in result)

    def test_compaction_with_256k_context(self):
        """Simulate a 256k context with 128k output budget."""
        # Build large conversation
        msgs = self._make_messages(tool_result_size=100000, num_tool_exchanges=15)
        result = compact_messages(
            msgs,
            context_budget_tokens=256000,
            output_budget_tokens=128000,
        )
        input_tokens = _estimate_messages_tokens(result)
        # Input should fit within context - output budget
        assert input_tokens <= 256000 - 128000 + 1000  # small tolerance
