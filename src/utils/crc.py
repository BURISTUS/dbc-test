from __future__ import annotations

CRC16_TABLE = tuple(
    sum(((i >> j) & 1) * (0xA001 << j) for j in range(8) if ((i >> j) & 1)) ^ (i >> 8)
    for i in range(256)
)


class CRC16ARC:
    @staticmethod
    def calculate(data: bytes) -> int:
        crc = 0x0000
        for byte in data:
            crc = ((crc >> 8) ^ CRC16_TABLE[(crc ^ byte) & 0xFF]) & 0xFFFF
        return crc

    @staticmethod
    def verify(data: bytes, expected_crc: int) -> bool:
        return CRC16ARC.calculate(data) == expected_crc
