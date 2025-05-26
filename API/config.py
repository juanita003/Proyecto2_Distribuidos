# config.py
import os

# Configuración del NameNode
NAMENODE_HOST = os.getenv('NAMENODE_HOST', 'localhost')
NAMENODE_PORT = int(os.getenv('NAMENODE_PORT', 5000))

# Configuración de DataNodes
DATANODE_PORTS = [5001, 5002, 5003]  # Puertos por defecto para DataNodes
DATANODE_HOST = os.getenv('DATANODE_HOST', 'localhost')

# Configuración de bloques
BLOCK_SIZE = int(os.getenv('BLOCK_SIZE', 64 * 1024 * 1024))  # 64MB por defecto
REPLICATION_FACTOR = int(os.getenv('REPLICATION_FACTOR', 2))  # Factor de replicación mínimo

# Configuración de directorios
NAMENODE_METADATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'namenode_data')
DATANODE_STORAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'datanode_data')

# Configuración de autenticación (opcional)
ENABLE_AUTH = os.getenv('ENABLE_AUTH', 'false').lower() == 'true'
SECRET_KEY = os.getenv('SECRET_KEY', 'dfs-secret-key-2025')

# Configuración de red
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 30))  # segundos
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))

# Configuración de logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Crear directorios si no existen
os.makedirs(NAMENODE_METADATA_DIR, exist_ok=True)
os.makedirs(DATANODE_STORAGE_DIR, exist_ok=True)