# src/core/parser.py - убрать thread pool
from __future__ import annotations

import struct
import structlog

from utils.crc import CRC16ARC
from core.models import CommAddr, CommData

logger = structlog.get_logger(__name__)


class FrameParser:
    def __init__(self) -> None:
        pass
    
    async def parse(self, raw_data: bytes) -> CommData | None:
        if len(raw_data) != 12:
            logger.warning("invalid_frame_size", size=len(raw_data))
            return None
        
        try:
            unpacked = struct.unpack('<H8BH', raw_data)
            comm_addr_raw = unpacked[0]
            payload = bytes(unpacked[1:9])
            received_crc = unpacked[9]
            
            dev_addr = comm_addr_raw & 0x1F
            msg_id = (comm_addr_raw >> 5) & 0x3FF
            reserved = (comm_addr_raw >> 15) & 0x1
            
            comm_addr = CommAddr(dev_addr=dev_addr, msg_id=msg_id, reserved=reserved)
            
            if not CRC16ARC.verify(raw_data[:-2], received_crc):
                logger.warning("crc_mismatch", expected=received_crc)
                return None
            
            return CommData(
                frame_id=comm_addr,
                data=payload,
                crc16=received_crc
            )
        except Exception as e:
            logger.error("frame_parse_error", error=str(e))
            return None
    
    async def close(self) -> None:
        pass  # Ничего не нужно закрывать