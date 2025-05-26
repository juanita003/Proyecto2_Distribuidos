import grpc
from concurrent import futures
import yaml
import os
import sys
import time
import logging
import threading
import requests

# Agregar el directorio actual al path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

try:
    import datanode_pb2
    import datanode_pb2_grpc
    import namenode_pb2
    import namenode_pb2_grpc
except ImportError as e:
    print(f"Error importando archivos proto: {e}")
    sys.exit(1)

class DataNode(datanode_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, config):
        self.config = config
        # Obtener mi IP pública para identificarme
        try:
            # Cambiar el endpoint para obtener la IP pública
            self.my_ip = requests.get('http://checkip.amazonaws.com', timeout=5).text.strip()
            print(f"📍 Mi IP pública: {self.my_ip}")
        except Exception as e:
            print(f"⚠️  Error obteniendo IP pública: {e}")
            # Fallback: intentar con el servicio de metadatos de AWS
            try:
                self.my_ip = requests.get('http://169.254.169.254/latest/meta-data/public-ipv4', timeout=5).text.strip()
                print(f"📍 Mi IP pública (fallback): {self.my_ip}")
            except:
                self.my_ip = "localhost"
                print("⚠️  No se pudo obtener IP pública, usando localhost")
        
        # Buscar mi configuración en la lista de datanodes
        self.my_config = None
        self.node_id = None
        
        for i, dn_config in enumerate(config['datanodes']):
            if dn_config['host'] == self.my_ip or dn_config['host'] == "localhost":
                self.my_config = dn_config
                self.node_id = i
                break
        
        if self.my_config is None:
            print(f"❌ Error: No se encontró configuración para IP {self.my_ip}")
            print("   Asegúrate de que tu IP esté en config.yaml")
            sys.exit(1)
        
        print(f"✅ DataNode configurado - ID: {self.node_id}, Puerto: {self.my_config['port']}")
        
        self.blocks_dir = f"blocks_storage_{self.node_id}"
        self.ensure_blocks_directory()
        self.namenode_stub = None
        
        # Esperar un poco antes de registrarse
        time.sleep(2)
        self.register_with_namenode()
        
    def ensure_blocks_directory(self):
        """Crear directorio para almacenar bloques si no existe"""
        if not os.path.exists(self.blocks_dir):
            os.makedirs(self.blocks_dir)
            print(f"✅ Directorio de bloques creado: {self.blocks_dir}")
    
    def register_with_namenode(self):
        """Registrarse con el NameNode"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                nn_config = self.config['namenode']
                nn_host = nn_config['host'].strip()
                nn_port = nn_config['port']
                
                print(f"🔗 Intentando conectar con NameNode: {nn_host}:{nn_port}")
                
                channel = grpc.insecure_channel(f"{nn_host}:{nn_port}")
                self.namenode_stub = namenode_pb2_grpc.NameNodeServiceStub(channel)
                
                request = namenode_pb2.DataNodeInfo(
                    host=self.my_ip,
                    port=self.my_config['port']
                )
                
                response = self.namenode_stub.RegisterDataNode(request)
                if response.success:
                    print(f"✅ Registrado exitosamente con NameNode: {response.message}")
                    return
                else:
                    print(f"⚠️  Error en registro: {response.message}")
                    
            except Exception as e:
                retry_count += 1
                print(f"❌ Error conectando con NameNode (intento {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    print(f"🔄 Reintentando en 5 segundos...")
                    time.sleep(5)
                else:
                    print("❌ No se pudo conectar con el NameNode después de varios intentos")
    
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

def serve():
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
    
    # Crear servidor
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datanode = DataNode(config)
    datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(datanode, server)
    
    # Usar el puerto de la configuración encontrada
    port = datanode.my_config['port']
    listen_addr = f"[::]:{port}"
    
    server.add_insecure_port(listen_addr)
    server.start()
    
    print(f"✅ DataNode {datanode.node_id} running on port {port}")
    print(f"   Listening on {listen_addr}")
    print(f"   Blocks directory: {datanode.blocks_dir}")
    print("Press Ctrl+C to stop...")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n🛑 Shutting down DataNode {datanode.node_id}...")
        server.stop(0)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    serve()