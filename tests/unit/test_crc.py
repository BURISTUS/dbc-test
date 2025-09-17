from src.utils.crc import CRC16ARC


class TestCRC16ARC:
    def test_calculate_empty(self):
        result = CRC16ARC.calculate(b"")
        assert result == 0x0000

    def test_calculate_known_value(self):
        # Test with known CRC-16/ARC test vector
        data = b"123456789"
        result = CRC16ARC.calculate(data)
        assert result == 0xBB3D  # Known test vector

    def test_verify_correct(self):
        data = b"test"
        crc = CRC16ARC.calculate(data)
        assert CRC16ARC.verify(data, crc) is True

    def test_verify_incorrect(self):
        data = b"test"
        assert CRC16ARC.verify(data, 0x0000) is False
