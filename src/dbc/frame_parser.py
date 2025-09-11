import struct
from typing import Tuple
from .crc import validate_crc16

def parse_comm_addr(comm_addr_bytes: bytes) -> Tuple[int, int]:
    """
    Парсинг COMM_ADDR по C структуре comm_addr_s:
    
    struct comm_addr_s {
        uint16_t DEV_ADDR : 5;   // Биты 4-0
        uint16_t MSG_ID : 10;    // Биты 14-5
        uint16_t RSVD : 1;       // Бит 15
    };
    """
    comm_addr_raw = struct.unpack('<H', comm_addr_bytes)[0]  # Little-endian
    
    dev_addr = comm_addr_raw & 0x1F          # Биты 4-0: DEV_ADDR (5 бит)
    msg_id = (comm_addr_raw >> 5) & 0x3FF    # Биты 14-5: MSG_ID (10 бит)
    
    return dev_addr, msg_id

def parse_frame(frame_hex: str) -> Tuple[int, int, bytes, bool, str]:
    """
    Парсинг по C структуре comm_data_s
    
    Returns: device_address, message_id, can_data, crc_valid, error
    """
    if len(frame_hex) != 24:
        return 0, 0, b'', False, "invalid_frame_size"
    
    try:
        frame_bytes = bytes.fromhex(frame_hex)
    except ValueError:
        return 0, 0, b'', False, "invalid_hex"
    
    # Парсинг по C структуре
    dev_addr, msg_id = parse_comm_addr(frame_bytes[0:2])  # comm_addr_t fid
    can_data = frame_bytes[2:10]                          # uint8_t data[8]
    crc_valid = validate_crc16(frame_bytes)               # crc16 валидация
    
    return dev_addr, msg_id, can_data, crc_valid, ""
