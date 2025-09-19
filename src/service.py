from __future__ import annotations

import structlog

from config import Settings  # Абсолютный импорт
from core.parser import FrameParser
from core.processor import DBCProcessor
from interfaces.grpc.server import GRPCServer
from utils.metrics import MetricsServer

logger = structlog.get_logger(__name__)


class DBCService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.frame_parser = FrameParser()
        self.dbc_processor = DBCProcessor(settings.dbc_file)
        
        self.grpc_server = GRPCServer(settings.grpc)
        self.metrics_server: MetricsServer | None = None
        
        self.running = False
        self.stats: dict[str, int] = {"total": 0, "valid": 0, "errors": 0, "published": 0}
    
    async def start(self) -> None:
        logger.info("service_starting")
        
        await self.dbc_processor.initialize()
        
        if self.settings.metrics.enabled:
            self.metrics_server = MetricsServer(self.settings.metrics)
            await self.metrics_server.start()
        
        self.grpc_server.set_message_handler(self.handle_message)
        await self.grpc_server.start()
        
        logger.info("service_started")
        self.running = True
        
        await self.grpc_server.serve()
    
    async def handle_message(self, topic: str, payload: bytes) -> None:
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
        
        await self.grpc_server.stop()
        
        if self.metrics_server:
            await self.metrics_server.stop()
        
        await self.frame_parser.close()
        await self.dbc_processor.close()
        
        logger.info("service_stopped", final_stats=self.stats)