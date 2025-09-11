import cantools
import os
import json
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timezone

class DBCProcessor:
    def __init__(self, dbc_file_path: str):
        self.dbc_file_path = dbc_file_path
        self.db: Optional[cantools.database.Database] = None
        self.loaded_at: Optional[datetime] = None
        
        # Проверяем существование DBC файла
        if not os.path.exists(dbc_file_path):
            raise FileNotFoundError(f"DBC file not found: {dbc_file_path}")
            
        self.load_dbc()
    
    def load_dbc(self):
        """Загрузка DBC файла"""
        try:
            self.db = cantools.database.load_file(self.dbc_file_path)
            self.loaded_at = datetime.now(timezone.utc)
            print(f"DBC loaded: {len(self.db.messages)} messages from {self.dbc_file_path}")
            
            # Выводим информацию о загруженных сообщениях для отладки
            for msg in self.db.messages:
                print(f"  - {msg.name} (ID: {msg.frame_id})")
                
        except Exception as e:
            print(f"DBC load error: {e}")
            self.db = None
            raise
    
    def get_dbc_info(self) -> Dict[str, Any]:
        """Информация о DBC файле"""
        if not self.db:
            return {
                "dbc_file": "Not loaded",
                "total_messages": 0,
                "messages": {},
                "loaded_at": "Never"
            }
        
        messages = {}
        for msg in self.db.messages:
            messages[str(msg.frame_id)] = msg.name
        
        return {
            "dbc_file": os.path.basename(self.dbc_file_path),
            "total_messages": len(self.db.messages),
            "messages": messages,
            "loaded_at": self.loaded_at.isoformat()
        }
    
    def get_message_info(self, msg_id: int) -> Dict[str, Any]:
        """Детальная информация о конкретном сообщении"""
        if not self.db:
            return {"error": "DBC not loaded"}
            
        try:
            message = self.db.get_message_by_frame_id(msg_id)
            signals_info = {}
            
            for signal in message.signals:
                signals_info[signal.name] = {
                    "start_bit": signal.start,
                    "length": signal.length,
                    "byte_order": "little_endian" if signal.byte_order == "little_endian" else "big_endian",
                    "is_signed": signal.is_signed,
                    "scale": signal.scale,
                    "offset": signal.offset,
                    "minimum": signal.minimum,
                    "maximum": signal.maximum,
                    "unit": signal.unit or "",
                    "comment": signal.comment or ""
                }
            
            return {
                "name": message.name,
                "frame_id": message.frame_id,
                "length": message.length,
                "signals": signals_info,
                "comment": message.comment or ""
            }
            
        except KeyError:
            return {"error": f"Message ID {msg_id} not found"}
    
    def decode_message(self, msg_id: int, can_data: bytes) -> Tuple[Optional[str], Dict[str, Any], Optional[str]]:
        """Декодирование CAN сообщения"""
        if not self.db:
            return None, {}, "DBC not loaded"
        
        try:
            message = self.db.get_message_by_frame_id(msg_id)
            signals = message.decode(can_data)
            
            # Преобразуем все значения в JSON-сериализуемые типы
            json_signals = {}
            for key, value in signals.items():
                if isinstance(value, (int, float, str, bool)):
                    json_signals[key] = value
                else:
                    json_signals[key] = str(value)
            
            return message.name, json_signals, None
            
        except KeyError:
            return None, {}, f"unknown_can_id: {msg_id}"
        except cantools.database.DecodeError as e:
            return None, {}, f"decode_error: {str(e)}"
        except Exception as e:
            return None, {}, f"unexpected_error: {str(e)}"
    
    def encode_message(self, msg_name: str, signals: Dict[str, Any]) -> Tuple[Optional[bytes], Optional[str]]:
        """Кодирование сообщения в CAN данные"""
        if not self.db:
            return None, "DBC not loaded"
        
        try:
            message = self.db.get_message_by_name(msg_name)
            can_data = message.encode(signals)
            return can_data, None
            
        except KeyError:
            return None, f"unknown_message_name: {msg_name}"
        except cantools.database.EncodeError as e:
            return None, f"encode_error: {str(e)}"
        except Exception as e:
            return None, f"unexpected_error: {str(e)}"
    
    def get_available_messages(self) -> Dict[str, int]:
        if not self.db:
            return {}
            
        return {msg.name: msg.frame_id for msg in self.db.messages}
    
    def validate_signals(self, msg_name: str, signals: Dict[str, Any]) -> Tuple[bool, List[str]]:
        if not self.db:
            return False, ["DBC not loaded"]
        
        try:
            message = self.db.get_message_by_name(msg_name)
            errors = []
            
            message_signals = {signal.name for signal in message.signals}
            for signal_name in signals.keys():
                if signal_name not in message_signals:
                    errors.append(f"Unknown signal: {signal_name}")
            
            # Проверяем диапазоны значений
            for signal in message.signals:
                if signal.name in signals:
                    value = signals[signal.name]
                    if signal.minimum is not None and value < signal.minimum:
                        errors.append(f"Signal {signal.name}: value {value} below minimum {signal.minimum}")
                    if signal.maximum is not None and value > signal.maximum:
                        errors.append(f"Signal {signal.name}: value {value} above maximum {signal.maximum}")
            
            return len(errors) == 0, errors
            
        except KeyError:
            return False, [f"Unknown message: {msg_name}"]
