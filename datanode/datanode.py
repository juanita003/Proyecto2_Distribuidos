import grpc
from concurrent import futures
import yaml
import os
import sys
import time
import logging
import threading
import requests
from flask import Flask, request, jsonify, send_file
from werkzeug.serving import make_server
import io

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
        
        print(f"✅ DataNode configurado - ID: {self.node_id}, Puerto gRPC: {self.my_config['port']}")
        
        self.blocks_dir = f"blocks_storage_{self.node_id}"
        self.ensure_blocks_directory()
        self.namenode_stub = None
        
        # Puerto REST (puerto gRPC + 1000)
        self.rest_port = self.my_config['port'] + 1000
        
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
    
    # ========== Métodos gRPC ==========
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
    
    # ========== Métodos auxiliares para REST ==========
    def store_block_data(self, block_id, data):
        """Método auxiliar para almacenar bloque (usado por REST y gRPC)"""
        try:
            block_path = os.path.join(self.blocks_dir, block_id)
            
            with open(block_path, 'wb') as f:
                f.write(data)
            
            logging.info(f"Bloque almacenado: {block_id} ({len(data)} bytes)")
            return True, f"Block {block_id} stored successfully"
            
        except Exception as e:
            logging.error(f"Error almacenando bloque {block_id}: {e}")
            return False, f"Error storing block: {str(e)}"
    
    def retrieve_block_data(self, block_id):
        """Método auxiliar para recuperar bloque (usado por REST y gRPC)"""
        try:
            block_path = os.path.join(self.blocks_dir, block_id)
            
            if not os.path.exists(block_path):
                return False, None, f"Block {block_id} not found"
            
            with open(block_path, 'rb') as f:
                data = f.read()
            
            logging.info(f"Bloque recuperado: {block_id} ({len(data)} bytes)")
            return True, data, "Block retrieved successfully"
            
        except Exception as e:
            logging.error(f"Error recuperando bloque {block_id}: {e}")
            return False, None, f"Error retrieving block: {str(e)}"
    
    def delete_block_data(self, block_id):
        """Método auxiliar para eliminar bloque (usado por REST y gRPC)"""
        try:
            block_path = os.path.join(self.blocks_dir, block_id)
            
            if os.path.exists(block_path):
                os.remove(block_path)
                logging.info(f"Bloque eliminado: {block_id}")
                return True, f"Block {block_id} deleted successfully"
            else:
                return False, f"Block {block_id} not found"
                
        except Exception as e:
            logging.error(f"Error eliminando bloque {block_id}: {e}")
            return False, f"Error deleting block: {str(e)}"
    
    def list_blocks_data(self):
        """Método auxiliar para listar bloques (usado por REST y gRPC)"""
        try:
            if os.path.exists(self.blocks_dir):
                blocks = [f for f in os.listdir(self.blocks_dir) if os.path.isfile(os.path.join(self.blocks_dir, f))]
            else:
                blocks = []
            
            return blocks
            
        except Exception as e:
            logging.error(f"Error listando bloques: {e}")
            return []

def create_rest_api(datanode):
    """Crear la aplicación Flask para la API REST"""
    app = Flask(__name__)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """Endpoint de salud"""
        return jsonify({
            "status": "healthy",
            "node_id": datanode.node_id,
            "grpc_port": datanode.my_config['port'],
            "rest_port": datanode.rest_port,
            "blocks_directory": datanode.blocks_dir
        })
    
    @app.route('/blocks', methods=['POST'])
    def store_block():
        """Almacenar un bloque via REST"""
        try:
            # Obtener el ID del bloque desde los parámetros de consulta o formulario
            block_id = request.args.get('block_id') or request.form.get('block_id')
            
            if not block_id:
                return jsonify({
                    "success": False,
                    "message": "block_id parameter is required"
                }), 400
            
            # Obtener los datos del bloque
            if 'file' in request.files:
                # Archivo subido
                file = request.files['file']
                data = file.read()
            elif 'data' in request.form:
                # Datos en el formulario
                data = request.form['data'].encode()
            elif request.data:
                # Datos en el cuerpo de la petición
                data = request.data
            else:
                return jsonify({
                    "success": False,
                    "message": "No data provided"
                }), 400
            
            success, message = datanode.store_block_data(block_id, data)
            
            return jsonify({
                "success": success,
                "message": message,
                "block_id": block_id,
                "size": len(data)
            }), 200 if success else 500
            
        except Exception as e:
            logging.error(f"Error en REST store_block: {e}")
            return jsonify({
                "success": False,
                "message": f"Internal server error: {str(e)}"
            }), 500
    
    @app.route('/blocks/<block_id>', methods=['GET'])
    def retrieve_block(block_id):
        """Recuperar un bloque via REST"""
        try:
            success, data, message = datanode.retrieve_block_data(block_id)
            
            if success:
                # Devolver el archivo como respuesta binaria
                return send_file(
                    io.BytesIO(data),
                    as_attachment=True,
                    download_name=f"{block_id}.block",
                    mimetype='application/octet-stream'
                )
            else:
                return jsonify({
                    "success": False,
                    "message": message
                }), 404
                
        except Exception as e:
            logging.error(f"Error en REST retrieve_block: {e}")
            return jsonify({
                "success": False,
                "message": f"Internal server error: {str(e)}"
            }), 500
    
    @app.route('/blocks/<block_id>/info', methods=['GET'])
    def get_block_info(block_id):
        """Obtener información de un bloque sin descargarlo"""
        try:
            success, data, message = datanode.retrieve_block_data(block_id)
            
            if success:
                return jsonify({
                    "success": True,
                    "block_id": block_id,
                    "size": len(data),
                    "message": message
                })
            else:
                return jsonify({
                    "success": False,
                    "message": message
                }), 404
                
        except Exception as e:
            logging.error(f"Error en REST get_block_info: {e}")
            return jsonify({
                "success": False,
                "message": f"Internal server error: {str(e)}"
            }), 500
    
    @app.route('/blocks/<block_id>', methods=['DELETE'])
    def delete_block(block_id):
        """Eliminar un bloque via REST"""
        try:
            success, message = datanode.delete_block_data(block_id)
            
            return jsonify({
                "success": success,
                "message": message,
                "block_id": block_id
            }), 200 if success else 404
            
        except Exception as e:
            logging.error(f"Error en REST delete_block: {e}")
            return jsonify({
                "success": False,
                "message": f"Internal server error: {str(e)}"
            }), 500
    
    @app.route('/blocks', methods=['GET'])
    def list_blocks():
        """Listar todos los bloques via REST"""
        try:
            blocks = datanode.list_blocks_data()
            
            # Obtener información adicional de cada bloque
            blocks_info = []
            for block_id in blocks:
                block_path = os.path.join(datanode.blocks_dir, block_id)
                if os.path.exists(block_path):
                    size = os.path.getsize(block_path)
                    modified_time = os.path.getmtime(block_path)
                    blocks_info.append({
                        "block_id": block_id,
                        "size": size,
                        "modified_time": modified_time
                    })
            
            return jsonify({
                "success": True,
                "total_blocks": len(blocks_info),
                "blocks": blocks_info
            })
            
        except Exception as e:
            logging.error(f"Error en REST list_blocks: {e}")
            return jsonify({
                "success": False,
                "message": f"Internal server error: {str(e)}"
            }), 500
    
    @app.route('/stats', methods=['GET'])
    def get_stats():
        """Obtener estadísticas del DataNode"""
        try:
            blocks = datanode.list_blocks_data()
            total_size = 0
            
            for block_id in blocks:
                block_path = os.path.join(datanode.blocks_dir, block_id)
                if os.path.exists(block_path):
                    total_size += os.path.getsize(block_path)
            
            return jsonify({
                "node_id": datanode.node_id,
                "host": datanode.my_ip,
                "grpc_port": datanode.my_config['port'],
                "rest_port": datanode.rest_port,
                "total_blocks": len(blocks),
                "total_size_bytes": total_size,
                "blocks_directory": datanode.blocks_dir
            })
            
        except Exception as e:
            logging.error(f"Error en REST get_stats: {e}")
            return jsonify({
                "success": False,
                "message": f"Internal server error: {str(e)}"
            }), 500
    
    return app

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
    
    # Crear DataNode
    datanode = DataNode(config)
    
    # ========== Servidor gRPC ==========
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(datanode, grpc_server)
    
    grpc_port = datanode.my_config['port']
    grpc_listen_addr = f"[::]:{grpc_port}"
    
    grpc_server.add_insecure_port(grpc_listen_addr)
    grpc_server.start()
    
    print(f"✅ DataNode {datanode.node_id} gRPC server running on port {grpc_port}")
    
    # ========== Servidor REST ==========
    rest_app = create_rest_api(datanode)
    rest_server = make_server('0.0.0.0', datanode.rest_port, rest_app, threaded=True)
    
    def run_rest_server():
        print(f"✅ DataNode {datanode.node_id} REST API running on port {datanode.rest_port}")
        rest_server.serve_forever()
    
    # Ejecutar servidor REST en un hilo separado
    rest_thread = threading.Thread(target=run_rest_server, daemon=True)
    rest_thread.start()
    
    print(f"📁 Blocks directory: {datanode.blocks_dir}")
    print(f"🌐 REST API endpoints:")
    print(f"   GET  http://{datanode.my_ip}:{datanode.rest_port}/health")
    print(f"   GET  http://{datanode.my_ip}:{datanode.rest_port}/blocks")
    print(f"   POST http://{datanode.my_ip}:{datanode.rest_port}/blocks?block_id=<id>")
    print(f"   GET  http://{datanode.my_ip}:{datanode.rest_port}/blocks/<block_id>")
    print(f"   GET  http://{datanode.my_ip}:{datanode.rest_port}/blocks/<block_id>/info")
    print(f"   DEL  http://{datanode.my_ip}:{datanode.rest_port}/blocks/<block_id>")
    print(f"   GET  http://{datanode.my_ip}:{datanode.rest_port}/stats")
    print("Press Ctrl+C to stop...")
    
    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n🛑 Shutting down DataNode {datanode.node_id}...")
        grpc_server.stop(0)
        rest_server.shutdown()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    serve()