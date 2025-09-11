def crc16_arc(data: bytes) -> int:
    crc = 0x0000
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc

def validate_crc16(frame_bytes: bytes) -> bool:
    if len(frame_bytes) != 12:
        return False
    
    payload = frame_bytes[:10]
    received_crc = int.from_bytes(frame_bytes[10:12], byteorder='little')
    calculated_crc = crc16_arc(payload)
    
    return received_crc == calculated_crc
