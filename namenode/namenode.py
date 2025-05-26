import grpc
from concurrent import futures
import yaml
import os

# Importar los archivos generados directamente desde este directorio
import namenode_pb2
import namenode_pb2_grpc
from metadata import MetadataStore
import logging

class NameNode(namenode_pb2_grpc.NameNodeServiceServicer):
    def __init__(self, config):
        self.config = config
        self.metadata = MetadataStore()
        self.datanodes = []
        
    def RegisterDataNode(self, request, context):
        datanode = {'host': request.host, 'port': request.port}
        if datanode not in self.datanodes:
            self.datanodes.append(datanode)
            logging.info(f"DataNode registered: {request.host}:{request.port}")
            return namenode_pb2.RegistrationResponse(success=True, message="Registration successful")
        return namenode_pb2.RegistrationResponse(success=False, message="Already registered")

    def GetBlockLocations(self, request, context):
        blocks = self.metadata.get_file_blocks(request.filename)
        block_infos = []
        for block_id, datanodes in blocks.items():
            dn_infos = [namenode_pb2.DataNodeInfo(host=dn['host'], port=dn['port']) for dn in datanodes]
            block_infos.append(namenode_pb2.BlockInfo(block_id=block_id, datanodes=dn_infos))
        return namenode_pb2.BlockLocations(blocks=block_infos)

    def CreateFile(self, request, context):
        block_size = 64 * 1024 * 1024  # 64MB blocks
        block_count = (request.size + block_size - 1) // block_size
        blocks = {}
        
        for i in range(block_count):
            block_id = f"{request.filename}_block_{i}"
            # Select datanodes for this block (simple round-robin)
            primary_dn = self.datanodes[i % len(self.datanodes)]
            replica_dn = self.datanodes[(i + 1) % len(self.datanodes)]
            blocks[block_id] = [primary_dn, replica_dn]
        
        self.metadata.add_file(request.filename, blocks)
        return namenode_pb2.FileResponse(success=True, blocks=[
            namenode_pb2.BlockInfo(block_id=bid, datanodes=[
                namenode_pb2.DataNodeInfo(host=dn['host'], port=dn['port']) for dn in dns
            ]) for bid, dns in blocks.items()
        ])

    def ListFiles(self, request, context):
        files = self.metadata.list_files(request.path)
        return namenode_pb2.ListResponse(files=files)

def serve():
    # Buscar config.yaml en el directorio padre
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.yaml')
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    namenode = NameNode(config)
    namenode_pb2_grpc.add_NameNodeServiceServicer_to_server(namenode, server)
    
    nn_config = config['namenode']
    # Limpiar espacios en blanco del host
    host = nn_config['host'].strip()
    port = nn_config['port']
    
    server.add_insecure_port(f"[::]:{port}")  # Escuchar en todas las interfaces
    server.start()
    print(f"NameNode running on {host}:{port}")
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve()

# Métodos que debes implementar en tu NameNode para que funcione el cliente:

def CreateFile(self, request, context):
    """
    Recibe: FileMetadata con filename, size, replication_factor
    Retorna: CreateFileResponse con success, blocks[]
    
    Cada block debe contener:
    - block_id: identificador único del bloque
    - datanodes[]: lista de DataNodeInfo donde almacenar réplicas
    """
    try:
        filename = request.filename
        file_size = request.size
        replication_factor = request.replication_factor
        
        # Calcular número de bloques
        block_size = 64 * 1024 * 1024  # 64MB
        num_blocks = (file_size + block_size - 1) // block_size
        
        blocks = []
        for i in range(num_blocks):
            block_id = f"{filename}_block_{i}_{int(time.time())}"
            
            # Seleccionar DataNodes para réplicas (algoritmo simple)
            selected_datanodes = self.select_datanodes_for_block(replication_factor)
            
            block_assignment = namenode_pb2.BlockAssignment(
                block_id=block_id,
                datanodes=selected_datanodes
            )
            blocks.append(block_assignment)
        
        # Guardar metadata del archivo
        self.file_metadata[filename] = {
            'size': file_size,
            'blocks': blocks,
            'created_at': time.time()
        }
        
        return namenode_pb2.CreateFileResponse(
            success=True,
            blocks=blocks
        )
        
    except Exception as e:
        return namenode_pb2.CreateFileResponse(
            success=False,
            blocks=[]
        )

def GetBlockLocations(self, request, context):
    """
    Recibe: FileRequest con filename
    Retorna: BlockLocationsResponse con blocks[]
    """
    try:
        filename = request.filename
        
        if filename not in self.file_metadata:
            return namenode_pb2.BlockLocationsResponse(blocks=[])
        
        file_info = self.file_metadata[filename]
        return namenode_pb2.BlockLocationsResponse(blocks=file_info['blocks'])
        
    except Exception as e:
        return namenode_pb2.BlockLocationsResponse(blocks=[])

def ListFiles(self, request, context):
    """
    Recibe: ListRequest con path
    Retorna: ListResponse con files[]
    """
    try:
        # Por ahora, listar todos los archivos (sin directorios)
        files = list(self.file_metadata.keys())
        return namenode_pb2.ListResponse(files=files)
        
    except Exception as e:
        return namenode_pb2.ListResponse(files=[])

def select_datanodes_for_block(self, replication_factor):
    """Seleccionar DataNodes para almacenar réplicas de un bloque"""
    available_datanodes = list(self.datanodes.values())
    
    # Algoritmo simple: seleccionar los primeros N disponibles
    selected = available_datanodes[:replication_factor]
    
    datanode_infos = []
    for dn in selected:
        datanode_info = namenode_pb2.DataNodeInfo(
            host=dn['host'],
            port=dn['port']
        )
        datanode_infos.append(datanode_info)
    
    return datanode_infos