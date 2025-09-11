import grpc
import json
import time
import sys
import os
from concurrent import futures
from datetime import datetime, timezone
from typing import Iterator

# Добавляем пути для поиска модулей
current_dir = os.path.dirname(__file__)
src_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(src_dir)

# Добавляем в sys.path если их там нет
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

import dbc_pb2
import dbc_pb2_grpc

# Импорты из других модулей
from dbc.frame_parser import parse_frame
from dbc.processor import DBCProcessor

class DBCParserServicer(dbc_pb2_grpc.DBCParserServiceServicer):
    def __init__(self):
        self.dbc_processor = DBCProcessor("../dbc/evse_data.dbc")
        self.start_time = time.time()
    
    def ParseFrame(self, request, context):
        """Парсинг одного фрейма"""
        try:
            # Парсинг фрейма
            dev_addr, msg_id, can_data, crc_valid, error = parse_frame(request.frame_data)
            
            # Базовые данные ответа
            response = dbc_pb2.ParseFrameResponse(
                device_address=dev_addr,
                message_id=msg_id,
                message_name="",
                signals_json="{}",
                raw_payload=can_data.hex().upper(),
                crc_valid=crc_valid,
                parsed=False,
                error=error if error else ("crc_mismatch" if not crc_valid else ""),
                timestamp=request.timestamp or datetime.now(timezone.utc).isoformat()
            )
            
            # Если нет ошибок и CRC валидный - декодируем
            if not error and crc_valid:
                message_name, signals, decode_error = self.dbc_processor.decode_message(msg_id, can_data)
                response.message_name = message_name or ""
                response.signals_json = json.dumps(signals)
                response.parsed = decode_error is None
                response.error = decode_error or ""
            
            return response
            
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return dbc_pb2.ParseFrameResponse()
    
    def ParseBatch(self, request, context) -> Iterator[dbc_pb2.ParseFrameResponse]:
        """Batch обработка с streaming ответом"""
        try:
            for frame_request in request.frames:
                response = self.ParseFrame(frame_request, context)
                yield response
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Batch processing error: {str(e)}")
    
    def GetDBCInfo(self, request, context):
        """Информация о DBC файле"""
        try:
            info = self.dbc_processor.get_dbc_info()
            return dbc_pb2.DBCInfoResponse(
                dbc_file=info["dbc_file"],
                total_messages=info["total_messages"],
                messages_json=json.dumps(info["messages"]),
                loaded_at=info["loaded_at"]
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error getting DBC info: {str(e)}")
            return dbc_pb2.DBCInfoResponse()
    
    def HealthCheck(self, request, context):
        """Health check"""
        uptime = int(time.time() - self.start_time)
        dbc_loaded = self.dbc_processor.db is not None
        
        return dbc_pb2.HealthResponse(
            status="healthy" if dbc_loaded else "unhealthy",
            dbc_loaded=dbc_loaded,
            uptime_seconds=uptime
        )

def serve():
    """Запуск gRPC сервера"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dbc_pb2_grpc.add_DBCParserServiceServicer_to_server(DBCParserServicer(), server)
    
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    
    print(f"🚀 gRPC DBC Server starting on {listen_addr}")
    server.start()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("🛑 Server stopping...")
        server.stop(0)

if __name__ == '__main__':
    serve()
