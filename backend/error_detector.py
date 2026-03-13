import asyncio
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
import structlog
from pydantic import BaseModel

from .config import Settings
from .opensearch_client import OpenSearchClient
from .github_service import GitHubService

logger = structlog.get_logger()

class ErrorNotification(BaseModel):
    stage: str
    repo_name: str
    error_message: str
    error_context: str
    timestamp: datetime
    log_url: Optional[str] = None

class RecurringErrorDetector:
    def __init__(
        self,
        opensearch_client: OpenSearchClient,
        swarm_api_base: str,
        swarm_agent_name: str,
        swarm_secret_key: str,
        github_service: GitHubService,
    ):
        self.opensearch = opensearch_client
        self.swarm_api_base = swarm_api_base
        self.swarm_agent_name = swarm_agent_name
        self.swarm_secret_key = swarm_secret_key
        self.github_service = github_service
        self._running = False
        self._task = None
        self._notified_failures: Dict[str, datetime] = {}
        self._notification_cooldown = timedelta(hours=1)
        # Stages that should include full logs on failure
        self._build_stages = {"build", "test", "deploy"}

    async def start(self):
        \"\"\"Start the recurring error detection task.\"\"\"
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        \"\"\"Stop the recurring error detection task.\"\"\"
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

    async def _run(self):
        \"\"\"Main loop for error detection.\"\"\"
        while self._running:
            try:
                await self._check_for_errors()
            except Exception as e:
                logger.error("Error in error detector loop", error=str(e))
            await asyncio.sleep(60)  # Check every minute

    async def _check_for_errors(self):
        \"\"\"Check for recurring errors in logs and notify Swarm.\"\"\"
        # Search for recent errors in OpenSearch
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": "now-5m"}}},
                        {"regexp": {"message": ".*(error|ERROR|Error|FAIL|failed|FAILED|exception|Exception|Traceback|traceback|fatal|FATAL|critical|CRITICAL).*}}
                    ]
                }
            },
            "size": 100,
            "sort": [{"@timestamp": {"order": "desc"}}]
        }

        try:
            results = await self.opensearch.search(index="logs-*", body=query)
            hits = results.get("hits", {}).get("hits", [])

            if not hits:
                return

            # Group errors by stage and repo
            error_groups = {}
            for hit in hits:
                source = hit.get("_source", {})
                stage = source.get("stage", "unknown")
                repo_name = source.get("repo_name", "unknown")
                message = source.get("message", "")
                key = f"{stage}:{repo_name}"

                if key not in error_groups:
                    error_groups[key] = {
                        "stage": stage,
                        "repo_name": repo_name,
                        "messages": set(),
                        "contexts": set(),
                        "timestamp": source.get("@timestamp"),
                        "log_url": source.get("log_url")
                    }

                error_groups[key]["messages"].add(message)
                if "context" in source:
                    error_groups[key]["contexts"].add(source["context"])

            # Check each error group for notification
            for key, group in error_groups.items():
                if key in self._notified_failures:
                    last_notified = self._notified_failures[key]
                    if datetime.now() - last_notified < self._notification_cooldown:
                        continue

                # Get the last 256KB of logs for this stage/repo
                log_content = await self._get_failure_logs(group["stage"], group["repo_name"])

                # Create notification
                notification = ErrorNotification(
                    stage=group["stage"],
                    repo_name=group["repo_name"],
                    error_message=" | ".join(group["messages"]),
                    error_context=" | ".join(group["contexts"]),
                    timestamp=group["timestamp"],
                    log_url=group["log_url"]
                )

                # Send notification
                await self._notify_swarm(notification, log_content)
                self._notified_failures[key] = datetime.now()

        except Exception as e:
            logger.error("Error checking for recurring errors", error=str(e))

    async def _get_failure_logs(self, stage: str, repo_name: str) -> str:
        \"\"\"Get the last 256KB of logs for a specific stage and repo.
        For build/test/deploy stages, we want to ensure we get the complete log
        of the failed operation, not just the last few lines.
        \"\"\"
        # For build/test/deploy stages, we want to get the complete log of the failed operation
        is_build_stage = stage in self._build_stages

        # First get the timestamp of the first error in this stage/repo
        error_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"stage": stage}},
                        {"term": {"repo_name": repo_name}},
                        {"regexp": {"message": ".*(error|ERROR|Error|FAIL|failed|FAILED|exception|Exception|Traceback|traceback|fatal|FATAL|critical|CRITICAL).*}},
                        {"range": {"@timestamp": {"gte": "now-1h"}}}
                    ]
                }
            },
            "size": 1,
            "sort": [{"@timestamp": {"order": "asc"}}]  # Get the first error
        }

        try:
            error_results = await self.opensearch.search(index="logs-*", body=error_query)
            error_hits = error_results.get("hits", {}).get("hits", [])

            # If no errors found, just get the last 256KB of logs
            if not error_hits:
                return await self._get_last_logs(stage, repo_name, 262144)  # 256KB

            first_error_time = error_hits[0].get("_source", {}).get("@timestamp")

            # For build/test/deploy stages, get all logs from the start of the operation
            if is_build_stage:
                # Get the start of the operation (looking for "Starting" messages)
                start_query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"stage": stage}},
                                {"term": {"repo_name": repo_name}},
                                {"regexp": {"message": ".*(Starting|start|BEGIN|begin).*$"}},
                                {"range": {"@timestamp": {"lte": first_error_time, "gte": "now-1h"}}}
                            ]
                        }
                    },
                    "size": 1,
                    "sort": [{"@timestamp": {"order": "desc"}}]  # Get the most recent start before the error
                }

                start_results = await self.opensearch.search(index="logs-*", body=start_query)
                start_hits = start_results.get("hits", {}).get("hits", [])

                if start_hits:
                    start_time = start_hits[0].get("_source", {}).get("@timestamp")
                    return await self._get_logs_between(stage, repo_name, start_time, first_error_time)
                else:
                    # If no start found, get logs from 5 minutes before the error
                    return await self._get_logs_between(stage, repo_name,
                                                      f"now-5m/{first_error_time}",
                                                      first_error_time)
            else:
                # For non-build stages, just get the last 256KB
                return await self._get_last_logs(stage, repo_name, 262144)  # 256KB

        except Exception as e:
            logger.error("Error getting failure logs", error=str(e))
            # Fallback to getting last 256KB if anything fails
            return await self._get_last_logs(stage, repo_name, 262144)

    async def _get_logs_between(self, stage: str, repo_name: str, start_time: str, end_time: str) -> str:
        \"\"\"Get all logs between two timestamps for a specific stage and repo.\"\"\"
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"stage": stage}},
                        {"term": {"repo_name": repo_name}},
                        {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}
                    ]
                }
            },
            "size": 10000,  # Get up to 10,000 log entries
            "sort": [{"@timestamp": {"order": "asc"}}]  # Chronological order
        }

        try:
            results = await self.opensearch.search(index="logs-*", body=query)
            hits = results.get("hits", {}).get("hits", [])

            if not hits:
                return "No logs found for this time period"

            # Combine all log messages in chronological order
            log_messages = []
            for hit in hits:
                source = hit.get("_source", {})
                message = source.get("message", "")
                log_messages.append(message)

            full_log = "\\n".join(log_messages)

            # If the log is too large, truncate to last 256KB
            if len(full_log) > 262144:  # 256KB
                return full_log[-262144:]
            return full_log

        except Exception as e:
            logger.error("Error getting logs between timestamps", error=str(e))
            return f"Error retrieving logs: {str(e)}"

    async def _get_last_logs(self, stage: str, repo_name: str, max_bytes: int) -> str:
        \"\"\"Get the last N bytes of logs for a specific stage and repo.\"\"\"
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"stage": stage}},
                        {"term": {"repo_name": repo_name}},
                        {"range": {"@timestamp": {"gte": "now-1h"}}}
                    ]
                }
            },
            "size": 10000,  # Get up to 10,000 log entries
            "sort": [{"@timestamp": {"order": "desc"}}]  # Most recent first
        }

        try:
            results = await self.opensearch.search(index="logs-*", body=query)
            hits = results.get("hits", {}).get("hits", [])

            if not hits:
                return "No logs found for this stage/repo"

            # Combine all log messages
            log_messages = []
            for hit in hits:
                source = hit.get("_source", {})
                message = source.get("message", "")
                log_messages.append(message)

            # Join all messages and return last max_bytes
            full_log = "\\n".join(reversed(log_messages))  # Reverse to get chronological order
            return full_log[-max_bytes:] if len(full_log) > max_bytes else full_log

        except Exception as e:
            logger.error("Error getting last logs", error=str(e))
            return f"Error retrieving logs: {str(e)}"

    async def _notify_swarm(self, notification: ErrorNotification, log_content: str):
        \"\"\"Notify Swarm about a recurring error.\"\"\"
        # Truncate log content if it's too large (Swarm API might have limits)
        if len(log_content) > 262144:  # 256KB
            log_content = log_content[-262144:]
            log_content = "[LOG TRUNCATED TO LAST 256KB]\\n" + log_content

        payload = {
            "agent": self.swarm_agent_name,
            "stage": notification.stage,
            "repo_name": notification.repo_name,
            "error_message": notification.error_message,
            "error_context": notification.error_context,
            "timestamp": notification.timestamp.isoformat(),
            "log_url": notification.log_url,
            "log_content": log_content
        }

        headers = {
            "Content-Type": "application/json",
            "X-Swarm-Secret": self.swarm_secret_key
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.swarm_api_base}/notify-error",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(
                            "Failed to notify Swarm",
                            status=response.status,
                            error=error_text
                        )
                    else:
                        logger.info(
                            "Successfully notified Swarm about error",
                            stage=notification.stage,
                            repo_name=notification.repo_name
                        )
        except Exception as e:
            logger.error("Error notifying Swarm", error=str(e))