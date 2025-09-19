from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import orjson
import structlog
from grpc import aio

from config import GRPCConfig  # Абсолютный импорт
from core.models import ParsedMessage  # Абсолютный импорт

logger = structlog.get_logger(__name__)


class DBCServicer:
    def __init__(self) -> None:
        self._message_handler: Callable[[str, bytes], None] | None = None
        self._output_queue: asyncio.Queue[ParsedMessage] = asyncio.Queue()
    
    def set_message_handler(self, handler: Callable[[str, bytes], None]) -> None:
        self._message_handler = handler
    
    async def ProcessFrames(self, request_iterator: AsyncIterator[Any], context: Any) -> AsyncIterator[Any]:
        async for request in request_iterator:
            if self._message_handler:
                asyncio.create_task(
                    self._handle_frame(request.topic, request.payload)
                )
            
            try:
                response = await asyncio.wait_for(self._output_queue.get(), timeout=1.0)
                yield self._create_response(response)
            except asyncio.TimeoutError:
                continue
    
    async def _handle_frame(self, topic: str, payload: bytes) -> None:
        try:
            if self._message_handler:
                self._message_handler(topic, payload)
        except Exception as e:
            logger.error("grpc_frame_error", topic=topic, error=str(e))
    
    def _create_response(self, message: ParsedMessage) -> Any:
        return type('Response', (), {
            'success': True,
            'data': orjson.dumps(message.model_dump()).decode(),
            'device_address': message.device_address,
            'can_message_id': message.can_message_id
        })()
    
    async def queue_response(self, message: ParsedMessage) -> None:
        try:
            await self._output_queue.put(message)
        except asyncio.QueueFull:
            logger.warning("grpc_queue_full")


class GRPCServer:
    def __init__(self, config: GRPCConfig) -> None:
        self.config = config
        self._server: aio.Server | None = None
        self._servicer = DBCServicer()
    
    def set_message_handler(self, handler: Callable[[str, bytes], None]) -> None:
        self._servicer.set_message_handler(handler)
    
    async def start(self) -> None:
        self._server = aio.server()
        
        listen_addr = f"{self.config.host}:{self.config.port}"
        self._server.add_insecure_port(listen_addr)
        
        await self._server.start()
        logger.info("grpc_server_started", address=listen_addr)
    
    async def serve(self) -> None:
        if self._server:
            await self._server.wait_for_termination()
    
    async def publish_message(self, message: ParsedMessage) -> bool:
        try:
            await self._servicer.queue_response(message)
            return True
        except Exception as e:
            logger.error("grpc_publish_error", error=str(e))
            return False
    
    async def stop(self) -> None:
        if self._server:
            await self._server.stop(grace=5)
            logger.info("grpc_server_stopped")
