from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import cantools
import structlog

from .models import CommData, ParsedMessage

logger = structlog.get_logger(__name__)


class DBCProcessor:
    def __init__(self, dbc_file: Path, max_workers: int = 4) -> None:
        self.dbc_file = dbc_file
        self.db: cantools.database.Database | None = None
        self._message_cache: dict[int, cantools.database.Message] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="dbc_processor"
        )

    async def initialize(self) -> None:
        try:
            self.db = await asyncio.get_event_loop().run_in_executor(
                self._executor, cantools.database.load_file, str(self.dbc_file)
            )
            logger.info("dbc_loaded", file=str(self.dbc_file), messages=len(self.db.messages))
        except Exception as e:
            logger.error("dbc_load_failed", file=str(self.dbc_file), error=str(e))
            raise

    async def process_message(
        self, comm_data: CommData, source_topic: str = ""
    ) -> ParsedMessage | None:
        if not self.db:
            raise RuntimeError("DBC processor not initialized")

        return await asyncio.get_event_loop().run_in_executor(
            self._executor, self._process_sync, comm_data, source_topic
        )

    def _process_sync(self, comm_data: CommData, source_topic: str) -> ParsedMessage:
        can_id = comm_data.frame_id.msg_id
        dev_addr = comm_data.frame_id.dev_addr
        packet_type = "broadcast" if comm_data.frame_id.is_broadcast else "unicast"

        try:
            message = self._message_cache.get(can_id)
            if message is None:
                message = self.db.get_message_by_frame_id(can_id)
                self._message_cache[can_id] = message

            decoded_signals = message.decode(comm_data.data)

            return ParsedMessage(
                device_address=dev_addr,
                packet_type=packet_type,
                can_message_id=can_id,
                message_name=message.name,
                signals=decoded_signals,
                raw_payload=comm_data.data.hex().upper(),
                crc16=f"0x{comm_data.crc16:04X}",
                crc_valid=True,
                timestamp=comm_data.timestamp.isoformat(),
                parsed=True,
                source_topic=source_topic,
            )
        except KeyError:
            logger.warning("unknown_can_id", can_id=can_id, device_addr=dev_addr)

            return ParsedMessage(
                device_address=dev_addr,
                packet_type=packet_type,
                can_message_id=can_id,
                raw_payload=comm_data.data.hex().upper(),
                crc16=f"0x{comm_data.crc16:04X}",
                crc_valid=True,
                timestamp=comm_data.timestamp.isoformat(),
                parsed=False,
                source_topic=source_topic,
                error=f"Unknown CAN ID: {can_id}",
            )

    async def close(self) -> None:
        self._executor.shutdown(wait=True)
