import pytest

from src.core.models import ParsedMessage
from src.interfaces.redis.pubsub import RedisPubSub


class TestRedisPubSub:
    @pytest.fixture
    async def pubsub(self, test_settings):
        pubsub = RedisPubSub(test_settings.redis)
        await pubsub.start()
        yield pubsub
        await pubsub.stop()

    async def test_publish_message(self, pubsub):
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

        result = await pubsub.publish_message(message)
        assert result is True
