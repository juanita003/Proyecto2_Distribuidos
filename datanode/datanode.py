import grpc
from concurrent import futures
import yaml
import os
import sys
import time
import logging
import threading

# Agregar el directorio actual al path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

try:
    import datanode_pb2
    import datanode_pb2_grpc
    # Importar también los archivos del namenode para registro
    sys.path.insert(0, os.path.join(os.path.dirname(current_dir), 'namenode'))
    import namenode_pb2
    import namenode_pb2_grpc
except ImportError as e:
    print(f"Error importando archivos proto: {e}")
    print("Ejecuta primero: python -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. ../proto/datanode.proto")
    sys.exit(1)

class DataNode(datanode_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, config, node_id=0):
        self.config = config
        self.node_id = node_id
        self.blocks_dir = f"blocks_node_{node_id}"
        self.ensure_blocks_directory()
        self.namenode_stub = None
        self.register_with_namenode()
        
    def ensure_blocks_directory(self):
        """Crear directorio para almacenar bloques si no existe"""
        if not os.path.exists(self.blocks_dir):
            os.makedirs(self.blocks_dir)
            print(f"✅ Directorio de bloques creado: {self.blocks_dir}")
    
    def register_with_namenode(self):
        """Registrarse con el NameNode"""
        try:
            nn_config = self.config['namenode']
            nn_host = nn_config['host'].strip()
            nn_port = nn_config['port']
            
            channel = grpc.insecure_channel(f"{nn_host}:{nn_port}")
            self.namenode_stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
            
            # Obtener configuración de este datanode
            dn_config = self.config['datanodes'][self.node_id]
            
            request = namenode_pb2.DataNodeInfo(
                host=dn_config['host'],
                port=dn_config['port']
            )
            
            response = self.namenode_stub.RegisterDataNode(request)
            if response.success:
                print(f"✅ Registrado exitosamente con NameNode: {response.message}")
            else:
                print(f"⚠️  Error en registro: {response.message}")
                
        except Exception as e:
            print(f"❌ Error conectando con NameNode: {e}")
    
    def StoreBlock(self, request, context):
        """Almacenar un bloque de datos"""
        try:
            block_path = os.path.join(self.blocks_dir, request.block_id)
            
            with open(block_path, 'wb') as f:
                f.write(request.data)
            
            logging.info(f"Bloque almacenado: {request.block_id} ({len(request.data)} bytes)")
            return datanode_pb2.StoreBlockResponse(
                success=True, 
                message=f"Block {request.block_id} stored successfully"
            )
            
        except Exception as e:
            logging.error(f"Error almacenando bloque {request.block_id}: {e}")
            return datanode_pb2.StoreBlockResponse(
                success=False, 
                message=f"Error storing block: {str(e)}"
            )
    
    def RetrieveBlock(self, request, context):
        """Recuperar un bloque de datos"""
        try:
            block_path = os.path.join(self.blocks_dir, request.block_id)
            
            if not os.path.exists(block_path):
                return datanode_pb2.RetrieveBlockResponse(
                    success=False,
                    data=b"",
                    message=f"Block {request.block_id} not found"
                )
            
            with open(block_path, 'rb') as f:
                data = f.read()
            
            logging.info(f"Bloque recuperado: {request.block_id} ({len(data)} bytes)")
            return datanode_pb2.RetrieveBlockResponse(
                success=True,
                data=data,
                message="Block retrieved successfully"
            )
            
        except Exception as e:
            logging.error(f"Error recuperando bloque {request.block_id}: {e}")
            return datanode_pb2.RetrieveBlockResponse(
                success=False,
                data=b"",
                message=f"Error retrieving block: {str(e)}"
            )
    
    def DeleteBlock(self, request, context):
        """Eliminar un bloque"""
        try:
            block_path = os.path.join(self.blocks_dir, request.block_id)
            
            if os.path.exists(block_path):
                os.remove(block_path)
                logging.info(f"Bloque eliminado: {request.block_id}")
                return datanode_pb2.DeleteBlockResponse(
                    success=True,
                    message=f"Block {request.block_id} deleted successfully"
                )
            else:
                return datanode_pb2.DeleteBlockResponse(
                    success=False,
                    message=f"Block {request.block_id} not found"
                )
                
        except Exception as e:
            logging.error(f"Error eliminando bloque {request.block_id}: {e}")
            return datanode_pb2.DeleteBlockResponse(
                success=False,
                message=f"Error deleting block: {str(e)}"
            )
    
    def ListBlocks(self, request, context):
        """Listar todos los bloques almacenados"""
        try:
            if os.path.exists(self.blocks_dir):
                blocks = [f for f in os.listdir(self.blocks_dir) if os.path.isfile(os.path.join(self.blocks_dir, f))]
            else:
                blocks = []
            
            return datanode_pb2.ListBlocksResponse(block_ids=blocks)
            
        except Exception as e:
            logging.error(f"Error listando bloques: {e}")
            return datanode_pb2.ListBlocksResponse(block_ids=[])

def serve(node_id=0):
    # Cargar configuración
    config_path = os.path.join(os.path.dirname(current_dir), 'config.yaml')
    
    if not os.path.exists(config_path):
        print(f"Error: No se encontró config.yaml en {config_path}")
        return
    
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error leyendo config.yaml: {e}")
        return
    
    if node_id >= len(config['datanodes']):
        print(f"Error: node_id {node_id} no existe en la configuración")
        return
    
    # Crear servidor
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datanode = DataNode(config, node_id)
    datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(datanode, server)
    
    # Configurar puerto
    dn_config = config['datanodes'][node_id]
    port = dn_config['port']
    listen_addr = f"[::]:{port}"
    
    server.add_insecure_port(listen_addr)
    server.start()
    
    print(f"✅ DataNode {node_id} running on port {port}")
    print(f"   Listening on {listen_addr}")
    print(f"   Blocks directory: {datanode.blocks_dir}")
    print("Press Ctrl+C to stop...")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n🛑 Shutting down DataNode {node_id}...")
        server.stop(0)

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='DataNode para el sistema de archivos distribuido')
    parser.add_argument('--node-id', type=int, default=0, help='ID del DataNode (0 o 1)')
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    serve(args.node_id)