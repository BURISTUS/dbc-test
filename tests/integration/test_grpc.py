import pytest

from src.core.models import ParsedMessage
from src.interfaces.grpc.server import GRPCServer


class TestGRPCServer:
    @pytest.fixture
    async def grpc_server(self, test_settings):
        server = GRPCServer(test_settings.grpc)
        await server.start()
        yield server
        await server.stop()

    async def test_publish_message(self, grpc_server):
        message = ParsedMessage(
            device_address=1,
            packet_type="unicast",
            can_message_id=100,
            message_name="TestMessage",
            signals={"signal1": 42},
            raw_payload="0102030405060708",
            crc16="0x1234",
            crc_valid=True,
            timestamp="2024-01-01T00:00:00",
            parsed=True,
        )

        result = await grpc_server.publish_message(message)
        assert result is True
