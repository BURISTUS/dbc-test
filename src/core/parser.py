from __future__ import annotations

import struct
from typing import Optional
import structlog

from .models import CommAddr, CommData
from utils.crc import CRC16ARC

logger = structlog.get_logger(__name__)


class FrameParser:
    def __init__(self) -> None:
        self._addr_format = '<H'
        self._crc_format = '<H'
        self._min_frame_size = 12
        
    async def parse(self, frame: bytes) -> Optional[CommData]:
        """ОПТИМИЗИРОВАНО: убираем async overhead"""
        if len(frame) != self._min_frame_size:
            return None
        
        try:
            addr_value = struct.unpack(self._addr_format, frame[:2])[0]
            data = frame[2:10]
            received_crc = struct.unpack(self._crc_format, frame[10:12])[0]
            
            dev_addr = addr_value & 0x1F
            msg_id = (addr_value >> 5) & 0x7FF
            reserved = (addr_value >> 16) & 0xFFFF
            
            expected_crc = CRC16ARC.calculate(frame[:10])
            if received_crc != expected_crc:
                return None
            
            comm_addr = CommAddr(dev_addr=dev_addr, msg_id=msg_id, reserved=reserved)
            return CommData(frame_id=comm_addr, data=data, crc16=received_crc)
            
        except (struct.error, IndexError):
            return None
    
    async def close(self) -> None:
        pass