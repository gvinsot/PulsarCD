"""PulsarCD Agent - Main entry point.

This agent runs on each host and:
1. Collects Docker container logs and metrics locally
2. Writes data directly to VictoriaLogs
3. Polls the backend for actions to execute (start, stop, exec, etc.)
"""

import asyncio
import logging
import os
import signal
import sys

import structlog

from .config import load_agent_config
from .docker_collector import DockerCollector
from .victorialogs_writer import VictoriaLogsWriter
from .action_poller import ActionPoller

# Configure logging level from environment (default: WARNING for less noise)
log_level = os.environ.get("AGENT_LOG_LEVEL", "WARNING").upper()
logging.basicConfig(
    format="%(message)s",
    level=getattr(logging, log_level, logging.WARNING),
)

# Silence noisy HTTP client logs
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("aiohttp.client").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("httpcore").setLevel(logging.ERROR)

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class Agent:
    """Main agent class that orchestrates collection and action polling."""

    def __init__(self):
        self.config = load_agent_config()
        self._running = False
        self._tasks = []

        # Initialize components
        self.docker = DockerCollector(
            docker_url=self.config.docker_url,
            host_name=self.config.agent_id,
        )
        self.writer = VictoriaLogsWriter(self.config.victorialogs, host_name=self.config.agent_id)
        self.action_poller = ActionPoller(
            backend_url=self.config.backend_url,
            agent_id=self.config.agent_id,
            docker_collector=self.docker,
            poll_interval=self.config.action_poll_interval,
            auth_key=self.config.auth_key,
        )

    async def start(self):
        """Start the agent."""
        logger.warning(
            "Starting PulsarCD Agent",
            agent_id=self.config.agent_id,
            backend_url=self.config.backend_url,
            victorialogs_url=self.config.victorialogs.url,
        )

        # Wait for VictoriaLogs (DNS / network / service start)
        max_retries = 30
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                await self.writer.initialize()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        "VictoriaLogs not ready, retrying...",
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        error=str(e),
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to VictoriaLogs after retries", error=str(e))
                    raise

        # Run self-test to verify the full write/read pipeline
        test_ok = await self.writer.self_test()
        if not test_ok:
            logger.critical(
                "VictoriaLogs self-test FAILED — logs will NOT be stored! "
                "Check VictoriaLogs connectivity and disk space."
            )
            await self.action_poller.report_system_error(
                "logstore", "SelfTestFailed",
                "VictoriaLogs self-test failed — logs will not be stored")

        # Resume after the newest line already stored for each container,
        # instead of sending the tail of every log again.
        try:
            positions = await self.writer.latest_log_times()
            self.docker.resume_from(positions)
            logger.warning("Log positions restored", containers=len(positions))
        except Exception as e:
            logger.warning("Could not restore log positions; tails will be re-read", error=str(e))

        self._running = True

        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._log_collection_loop()),
            asyncio.create_task(self._metrics_collection_loop()),
            asyncio.create_task(self.action_poller.run()),
        ]

        logger.debug("Agent started successfully")

        # Wait for all tasks
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.debug("Agent tasks cancelled")

    async def stop(self):
        """Stop the agent gracefully."""
        logger.debug("Stopping agent...")
        self._running = False
        self.action_poller.stop()

        # Cancel all tasks if they exist
        if self._tasks:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close connections (always, even if init failed)
        try:
            await self.docker.close()
        except Exception:
            pass
        try:
            await self.writer.close()
        except Exception:
            pass
        try:
            await self.action_poller.close()
        except Exception:
            pass

        logger.debug("Agent stopped")

    async def _log_collection_loop(self):
        """Periodically collect logs from all containers."""
        logger.warning("Log collection loop started",
                       interval=self.config.log_interval,
                       tail=self.config.log_lines_per_fetch)
        cycle = 0

        while self._running:
            cycle += 1
            try:
                logs = await self.docker.collect_all_logs(
                    tail=self.config.log_lines_per_fetch
                )

                indexed = 0
                if logs:
                    indexed = await self.writer.index_logs(logs)

                # Always log at WARNING — silent failures are the worst kind
                logger.warning(
                    "Log cycle",
                    cycle=cycle,
                    collected=len(logs),
                    indexed=indexed,
                )

                # Periodic stored-lines check (every 10 cycles ≈ 5min)
                if cycle % 10 == 0:
                    stored = await self.writer.count_recent_logs(minutes=5)
                    logger.warning("Lines stored over the last 5 minutes",
                                   count=stored, cycle=cycle)

            except Exception as e:
                logger.error("Log collection error", error=str(e),
                             error_type=type(e).__name__, cycle=cycle)
                await self.action_poller.report_system_error(
                    "log_collection", type(e).__name__, str(e))

            await asyncio.sleep(self.config.log_interval)

    async def _metrics_collection_loop(self):
        """Periodically collect metrics from host and containers."""
        logger.debug("Metrics collection loop started", interval=self.config.metrics_interval)

        while self._running:
            try:
                host_metrics, container_stats = await self.docker.collect_all_stats()

                await self.writer.index_metrics(host_metrics, container_stats)

                # Log GPU status for visibility (DEBUG level)
                gpu_percent = host_metrics.get("gpu_percent")
                gpu_mem_used = host_metrics.get("gpu_memory_used_mb")
                gpu_mem_total = host_metrics.get("gpu_memory_total_mb")
                
                if gpu_percent is not None or gpu_mem_used is not None:
                    logger.debug(
                        "Collected metrics with GPU",
                        host_cpu=host_metrics.get("cpu_percent"),
                        gpu_percent=gpu_percent,
                        gpu_mem_used_mb=round(gpu_mem_used, 2) if gpu_mem_used else None,
                        gpu_mem_total_mb=round(gpu_mem_total, 2) if gpu_mem_total else None,
                        containers=len(container_stats),
                    )
                else:
                    logger.debug(
                        "Collected metrics (no GPU data)",
                        host_cpu=host_metrics.get("cpu_percent"),
                        containers=len(container_stats),
                    )

            except Exception as e:
                logger.error("Metrics collection error", error=str(e))
                await self.action_poller.report_system_error(
                    "metrics_collection", type(e).__name__, str(e))

            await asyncio.sleep(self.config.metrics_interval)


async def main():
    """Main entry point."""
    agent = Agent()

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()

    def signal_handler():
        logger.debug("Received shutdown signal")
        asyncio.create_task(agent.stop())

    # Handle SIGINT and SIGTERM
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    try:
        await agent.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        await agent.stop()
    except Exception as e:
        logger.error("Agent failed", error=str(e))
        await agent.stop()
        sys.exit(1)


def run():
    """Entry point for console script."""
    asyncio.run(main())


if __name__ == "__main__":
    run()
