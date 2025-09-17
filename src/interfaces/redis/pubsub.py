from __future__ import annotations

import asyncio
from collections.abc import Callable

import orjson
import structlog
from config import RedisConfig
from core.models import ParsedMessage

from .client import RedisClient

logger = structlog.get_logger(__name__)


class RedisPubSub:
    def __init__(self, config: RedisConfig) -> None:
        self.config = config
        self._client = RedisClient(config)
        self._pubsub = None
        self._message_handler: Callable[[str, bytes], None] | None = None
        self._running = False

    async def start(self) -> None:
        await self._client.connect()
        self._pubsub = self._client.redis.pubsub()
        await self._pubsub.subscribe(self.config.input_channel)
        logger.info("redis_pubsub_started", channel=self.config.input_channel)

    def set_message_handler(self, handler: Callable[[str, bytes], None]) -> None:
        self._message_handler = handler

    async def listen(self) -> None:
        self._running = True

        if not self._pubsub:
            raise RuntimeError("PubSub not started")

        async for message in self._pubsub.listen():
            if not self._running:
                break

            if message["type"] == "message" and self._message_handler:
                asyncio.create_task(
                    self._handle_message(message["channel"].decode(), message["data"])
                )

    async def _handle_message(self, channel: str, data: bytes) -> None:
        try:
            if self._message_handler:
                self._message_handler(channel, data)
        except Exception as e:
            logger.error("redis_message_error", channel=channel, error=str(e))

    async def publish_message(self, message: ParsedMessage) -> bool:
        try:
            payload = orjson.dumps(message.model_dump())
            await self._client.redis.publish(self.config.output_channel, payload)
            logger.debug("message_published", channel=self.config.output_channel)
            return True
        except Exception as e:
            logger.error("redis_publish_error", error=str(e))
            return False

    async def stop(self) -> None:
        self._running = False
        if self._pubsub:
            await self._pubsub.close()
        await self._client.disconnect()
        logger.info("redis_pubsub_stopped")
