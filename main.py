import os
import sys

# Получаем абсолютные пути
project_root = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(project_root, 'src')
grpc_dir = os.path.join(project_root, 'src', 'grpc')

# Добавляем все нужные пути
sys.path.insert(0, src_dir)
sys.path.insert(0, grpc_dir)

# Меняем рабочую директорию на src/grpc (как при прямом запуске)
os.chdir(grpc_dir)

from server import serve

if __name__ == "__main__":
    print("Starting Python DBC Service with gRPC...")
    serve()