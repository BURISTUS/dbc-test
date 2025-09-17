from __future__ import annotations

import redis.asyncio as redis
import structlog
from config import RedisConfig

logger = structlog.get_logger(__name__)


class RedisClient:
    def __init__(self, config: RedisConfig) -> None:
        self.config = config
        self._redis: redis.Redis | None = None
        self._pool: redis.ConnectionPool | None = None

    async def connect(self) -> None:
        self._pool = redis.ConnectionPool(
            host=self.config.host,
            port=self.config.port,
            db=self.config.db,
            username=self.config.username,
            password=self.config.password,
            max_connections=self.config.pool_size,
            decode_responses=False,
        )

        self._redis = redis.Redis(connection_pool=self._pool)

        await self._redis.ping()
        logger.info("redis_connected", host=self.config.host, port=self.config.port)

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.close()
        if self._pool:
            await self._pool.disconnect()
        logger.info("redis_disconnected")

    @property
    def redis(self) -> redis.Redis:
        if not self._redis:
            raise RuntimeError("Redis client not connected")
        return self._redis
