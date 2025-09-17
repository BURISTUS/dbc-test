import pytest

from src.core.parser import FrameParser
from src.utils.crc import CRC16ARC


class TestFrameParser:
    @pytest.fixture
    async def parser(self):
        parser = FrameParser(max_workers=2)
        yield parser
        await parser.close()

    async def test_parse_valid_frame(self, parser):
        # Create valid frame with correct CRC
        comm_addr = 1 | (100 << 5)  # DEV_ADDR=1, MSG_ID=100
        payload = bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
        crc_data = comm_addr.to_bytes(2, "little") + payload
        crc = CRC16ARC.calculate(crc_data)

        frame = comm_addr.to_bytes(2, "little") + payload + crc.to_bytes(2, "little")

        result = await parser.parse(frame)

        assert result is not None
        assert result.frame_id.dev_addr == 1
        assert result.frame_id.msg_id == 100
        assert result.data == payload

    async def test_parse_invalid_size(self, parser):
        result = await parser.parse(b"short")
        assert result is None

    async def test_parse_invalid_crc(self, parser):
        frame = bytes(12)  # All zeros, invalid CRC
        result = await parser.parse(frame)
        assert result is None
