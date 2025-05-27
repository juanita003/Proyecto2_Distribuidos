# Puerto por defecto para el NameNode REST
default_namenode_port = 8080
# Dirección por defecto del NameNode (host:puerto)
default_namenode_host = 'localhost'
default_namenode_address = f"{default_namenode_host}:{default_namenode_port}"

# Puerto base para DataNodes gRPC (se suma el número de nodo)
grpc_base_port = 50051

# Tamaño de bloque por defecto en bytes (por ejemplo, 64 MiB)
default_block_size = 64 * 1024 * 1024  # 64 MiB

# Ruta de almacenamiento de bloques en cada DataNode
blocks_storage_dir = 'storage/blocks'