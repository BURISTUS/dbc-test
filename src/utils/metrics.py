from __future__ import annotations

import structlog
from config import MetricsConfig
from prometheus_client import Counter, Histogram, start_http_server

logger = structlog.get_logger(__name__)

PROCESSED_FRAMES = Counter("dbc_frames_processed_total", "Total processed frames", ["status"])
PROCESSING_TIME = Histogram("dbc_processing_duration_seconds", "Frame processing time")
PUBLISHED_MESSAGES = Counter("dbc_messages_published_total", "Published messages", ["output_type"])


class MetricsServer:
    def __init__(self, config: MetricsConfig) -> None:
        self.config = config
        self._server = None

    async def start(self) -> None:
        if self.config.enabled:
            start_http_server(self.config.port)
            logger.info("metrics_server_started", port=self.config.port)

    async def stop(self) -> None:
        logger.info("metrics_server_stopped")
