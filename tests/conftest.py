from pathlib import Path

import pytest
import redis.asyncio as redis

from src.config import ProcessingConfig, RedisConfig, Settings


@pytest.fixture
def test_settings():
    return Settings(
        dbc_file=Path("tests/fixtures/test.dbc"),
        output_type="redis",
        redis=RedisConfig(
            host="localhost",
            port=6379,
            db=1,  # Use test DB
            input_channel="test:input",
            output_channel="test:output",
        ),
        processing=ProcessingConfig(worker_pool_size=2),
    )


@pytest.fixture
async def redis_client():
    client = redis.Redis(host="localhost", port=6379, db=1)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.close()


@pytest.fixture
def sample_can_frame():
    # DEV_ADDR=1, MSG_ID=100, valid CRC
    return bytes(
        [
            0x61,
            0x0C,  # COMM_ADDR: 1 + (100 << 5) = 3201 = 0x0C61
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
            0x06,
            0x07,
            0x08,  # CAN payload
            0x45,
            0x67,  # CRC16 (example)
        ]
    )
