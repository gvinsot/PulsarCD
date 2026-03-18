# ============================================================================
# Stage: base - Common dependencies
# ============================================================================
FROM python:3.11-slim AS base

WORKDIR /app

# Install system dependencies including Docker CLI for build/deploy scripts
RUN apt-get update && apt-get install -y \
    curl \
    git \
    openssh-client \
    ca-certificates \
    gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY shared/ ./shared/
COPY backend/ ./backend/
COPY frontend/ ./frontend/

# ============================================================================
# Stage: test - Run tests then exit
# ============================================================================
FROM base AS test

# Install test dependencies
RUN pip install --no-cache-dir pytest pytest-asyncio "httpx>=0.23.0,<0.28.0"

# Copy test files
COPY tests/ ./tests/

# Run tests on container start
CMD ["python", "-m", "pytest", "tests/", "-v", "--tb=short", "-p", "no:warnings"]

# ============================================================================
# Stage: production (default) - Run the application
# ============================================================================
FROM base AS production

# Create non-root user
RUN useradd -m -s /bin/bash appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')"

# Run the application
CMD ["python", "-m", "backend.main"]
