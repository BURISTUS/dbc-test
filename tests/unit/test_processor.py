from pathlib import Path

import pytest

from src.core.models import CommAddr, CommData
from src.core.processor import DBCProcessor


class TestDBCProcessor:
    @pytest.fixture
    async def processor(self):
        dbc_path = Path("tests/fixtures/test.dbc")
        processor = DBCProcessor(dbc_path, max_workers=2)
        await processor.initialize()
        yield processor
        await processor.close()

    async def test_process_known_message(self, processor):
        comm_data = CommData(
            frame_id=CommAddr(dev_addr=1, msg_id=100, reserved=0),
            data=bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
            crc16=0x1234,
        )

        result = await processor.process_message(comm_data, "test_topic")

        assert result is not None
        assert result.device_address == 1
        assert result.can_message_id == 100
        assert result.parsed is True

    async def test_process_unknown_message(self, processor):
        comm_data = CommData(
            frame_id=CommAddr(dev_addr=1, msg_id=999, reserved=0),  # Unknown ID
            data=bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
            crc16=0x1234,
        )

        result = await processor.process_message(comm_data, "test_topic")

        assert result is not None
        assert result.parsed is False
        assert "Unknown CAN ID" in result.error
