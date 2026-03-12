"""Data models for PulsarCD."""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class ContainerStatus(str, Enum):
    """Container status enum."""
    RUNNING = "running"
    PAUSED = "paused"
    EXITED = "exited"
    RESTARTING = "restarting"
    DEAD = "dead"
    CREATED = "created"
    REMOVING = "removing"


class ContainerInfo(BaseModel):
    """Container information."""
    id: str
    name: str
    image: str
    status: ContainerStatus
    created: datetime
    host: str
    compose_project: Optional[str] = None
    compose_service: Optional[str] = None
    ports: Dict[str, Any] = {}
    labels: Dict[str, str] = {}
    # Latest stats (optional, populated from cached metrics)
    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    memory_usage_mb: Optional[float] = None


class ContainerStats(BaseModel):
    """Container resource statistics."""
    container_id: str
    container_name: str
    host: str
    timestamp: datetime
    cpu_percent: float
    memory_usage_mb: float
    memory_limit_mb: float
    memory_percent: float
    network_rx_bytes: int = 0
    network_tx_bytes: int = 0
    block_read_bytes: int = 0
    block_write_bytes: int = 0


class HostMetrics(BaseModel):
    """Host-level metrics."""
    host: str
    timestamp: datetime
    cpu_percent: float
    memory_total_mb: float
    memory_used_mb: float
    memory_percent: float
    disk_total_gb: float = 0
    disk_used_gb: float = 0
    disk_percent: float = 0
    gpu_percent: Optional[float] = None
    gpu_memory_used_mb: Optional[float] = None
    gpu_memory_total_mb: Optional[float] = None


class LogEntry(BaseModel):
    """Log entry model."""
    id: Optional[str] = None
    timestamp: datetime
    host: str
    container_id: str
    container_name: str
    compose_project: Optional[str] = None
    compose_service: Optional[str] = None
    stream: str = "stdout"  # stdout or stderr
    message: str
    level: Optional[str] = None
    http_status: Optional[int] = None
    parsed_fields: Dict[str, Any] = {}


class LogSearchQuery(BaseModel):
    """Log search query parameters."""
    query: Optional[str] = None
    hosts: List[str] = []
    containers: List[str] = []
    compose_projects: List[str] = []
    levels: List[str] = []
    http_status_min: Optional[int] = None
    http_status_max: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    size: int = Field(default=100, le=10000)
    from_: int = Field(default=0, alias="from")
    sort_order: str = "desc"
    
    class Config:
        populate_by_name = True


class LogSearchResult(BaseModel):
    """Log search result."""
    total: int
    hits: List[LogEntry]
    aggregations: Dict[str, Any] = {}


class DashboardStats(BaseModel):
    """Dashboard statistics."""
    total_containers: int
    running_containers: int
    total_hosts: int
    healthy_hosts: int
    errors_24h: int
    warnings_24h: int
    http_4xx_24h: int
    http_5xx_24h: int
    avg_cpu_percent: float
    avg_memory_percent: float
    avg_gpu_percent: Optional[float] = None
    avg_vram_used_mb: Optional[float] = None
    avg_vram_total_mb: Optional[float] = None


class TimeSeriesPoint(BaseModel):
    """Time series data point."""
    timestamp: datetime
    value: float


class TimeSeriesByHost(BaseModel):
    """Time series data grouped by host."""
    host: str
    data: List[TimeSeriesPoint]


class ContainerAction(str, Enum):
    """Container action enum."""
    START = "start"
    STOP = "stop"
    RESTART = "restart"
    PAUSE = "pause"
    UNPAUSE = "unpause"
    REMOVE = "remove"


class ActionRequest(BaseModel):
    """Container action request."""
    host: str
    container_id: str
    action: ContainerAction


class ActionResult(BaseModel):
    """Container action result."""
    success: bool
    message: str
    container_id: str
    action: ContainerAction
