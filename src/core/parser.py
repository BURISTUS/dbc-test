from __future__ import annotations

import asyncio
import struct
from concurrent.futures import ThreadPoolExecutor

import structlog
from utils.crc import CRC16ARC

from .models import CommAddr, CommData

logger = structlog.get_logger(__name__)


class FrameParser:
    def __init__(self, max_workers: int = 4) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="frame_parser"
        )

    async def parse(self, raw_data: bytes) -> CommData | None:
        if len(raw_data) != 12:
            logger.warning("invalid_frame_size", size=len(raw_data))
            return None

        try:
            return await asyncio.get_event_loop().run_in_executor(
                self._executor, self._parse_sync, raw_data
            )
        except Exception as e:
            logger.error("frame_parse_error", error=str(e))
            return None

    def _parse_sync(self, data: bytes) -> CommData | None:
        try:
            unpacked = struct.unpack("<H8BH", data)
            comm_addr_raw = unpacked[0]
            payload = bytes(unpacked[1:9])
            received_crc = unpacked[9]

            dev_addr = comm_addr_raw & 0x1F
            msg_id = (comm_addr_raw >> 5) & 0x1FF
            reserved = (comm_addr_raw >> 15) & 0x1

            comm_addr = CommAddr(dev_addr=dev_addr, msg_id=msg_id, reserved=reserved)

            if not CRC16ARC.verify(data[:-2], received_crc):
                raise ValueError("CRC mismatch")

            return CommData(frame_id=comm_addr, data=payload, crc16=received_crc)
        except Exception as e:
            logger.error("sync_parse_error", error=str(e))
            return None

    async def close(self) -> None:
        self._executor.shutdown(wait=True)
