"""Main entry point for PulsarCD."""

import logging
import uvicorn
import structlog

from .config import load_config


class EndpointFilter(logging.Filter):
    """Filter out noisy endpoints from access logs."""

    # Endpoints to exclude from logging (high-frequency polling)
    EXCLUDED_PATHS = [
        "/api/agent/actions",
        "/api/health",
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        # Check if the log message contains any excluded path
        message = record.getMessage()
        return not any(path in message for path in self.EXCLUDED_PATHS)


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


def main():
    """Run the PulsarCD server."""
    settings = load_config()

    # Add filter to uvicorn access logger to exclude noisy endpoints
    logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

    uvicorn.run(
        "backend.api:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="info",
    )


if __name__ == "__main__":
    main()
