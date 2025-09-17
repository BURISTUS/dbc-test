from __future__ import annotations

import asyncio

import structlog
from config import Settings
from core.parser import FrameParser
from core.processor import DBCProcessor
from interfaces.grpc.server import GRPCServer
from interfaces.redis.pubsub import RedisPubSub
from utils.metrics import MetricsServer

logger = structlog.get_logger(__name__)


class DBCService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.frame_parser = FrameParser(settings.processing.worker_pool_size)
        self.dbc_processor = DBCProcessor(settings.dbc_file, settings.processing.worker_pool_size)

        self.redis_pubsub: RedisPubSub | None = None
        self.grpc_server: GRPCServer | None = None
        self.metrics_server: MetricsServer | None = None

        self.running = False
        self.stats: dict[str, int] = {"total": 0, "valid": 0, "errors": 0, "published": 0}

    async def start(self) -> None:
        logger.info("service_starting", output_type=self.settings.output_type)

        await self.dbc_processor.initialize()

        if self.settings.metrics.enabled:
            self.metrics_server = MetricsServer(self.settings.metrics)
            await self.metrics_server.start()

        if self.settings.output_type == "redis":
            self.redis_pubsub = RedisPubSub(self.settings.redis)
            self.redis_pubsub.set_message_handler(self.handle_message)
            await self.redis_pubsub.start()
            await self.redis_pubsub.listen()

        elif self.settings.output_type == "grpc":
            self.grpc_server = GRPCServer(self.settings.grpc)
            self.grpc_server.set_message_handler(self.handle_message)
            await self.grpc_server.start()
            await self.grpc_server.serve()

        logger.info("service_started")
        self.running = True

    async def handle_message(self, topic: str, payload: bytes) -> None:
        asyncio.create_task(self._process_message(topic, payload))

    async def _process_message(self, topic: str, payload: bytes) -> None:
        try:
            self.stats["total"] += 1

            comm_data = await self.frame_parser.parse(payload)
            if not comm_data:
                self.stats["errors"] += 1
                return

            parsed_message = await self.dbc_processor.process_message(comm_data, topic)
            if not parsed_message:
                self.stats["errors"] += 1
                return

            self.stats["valid"] += 1

            success = False
            if self.redis_pubsub:
                success = await self.redis_pubsub.publish_message(parsed_message)
            elif self.grpc_server:
                success = await self.grpc_server.publish_message(parsed_message)

            if success:
                self.stats["published"] += 1

            if self.stats["total"] % 1000 == 0:
                logger.info("stats", **self.stats)

        except Exception as e:
            logger.error("process_error", error=str(e))
            self.stats["errors"] += 1

    async def shutdown(self) -> None:
        logger.info("service_shutting_down")
        self.running = False

        if self.redis_pubsub:
            await self.redis_pubsub.stop()

        if self.grpc_server:
            await self.grpc_server.stop()

        if self.metrics_server:
            await self.metrics_server.stop()

        await self.frame_parser.close()
        await self.dbc_processor.close()

        logger.info("service_stopped", final_stats=self.stats)
