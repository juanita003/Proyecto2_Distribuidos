import grpc
from concurrent import futures
import yaml
import os
import time
import logging
import threading
from flask import Flask, request, jsonify
from werkzeug.serving import make_server

# Importar los archivos generados directamente desde este directorio
import namenode_pb2
import namenode_pb2_grpc
from metadata import MetadataStore

class NameNode(namenode_pb2_grpc.NameNodeServiceServicer):
    def __init__(self, config):
        self.config = config
        self.metadata = MetadataStore()
        self.datanodes = {}  # Cambiado a dict para mejor gestión
        self.file_metadata = {}  # Para almacenar metadata de archivos
        self.block_size = config.get('block_size', 64 * 1024 * 1024)  # 64MB por defecto
        
        # Puerto REST (puerto gRPC + 2000)
        nn_config = config['namenode']
        self.grpc_port = nn_config['port']
        self.rest_port = self.grpc_port + 2000
        self.host = nn_config['host'].strip()
        
    def RegisterDataNode(self, request, context):
        """Registrar un DataNode"""
        datanode_key = f"{request.host}:{request.port}"
        datanode_info = {
            'host': request.host, 
            'port': request.port,
            'registered_at': time.time(),
            'last_heartbeat': time.time()
        }
        
        if datanode_key not in self.datanodes:
            self.datanodes[datanode_key] = datanode_info
            logging.info(f"DataNode registered: {request.host}:{request.port}")
            return namenode_pb2.RegistrationResponse(
                success=True, 
                message="Registration successful"
            )
        else:
            # Actualizar heartbeat
            self.datanodes[datanode_key]['last_heartbeat'] = time.time()
            return namenode_pb2.RegistrationResponse(
                success=True, 
                message="Already registered - heartbeat updated"
            )

    def GetBlockLocations(self, request, context):
        """Obtener ubicaciones de bloques para un archivo"""
        try:
            filename = request.filename
            
            if filename not in self.file_metadata:
                return namenode_pb2.BlockLocationsResponse(blocks=[])
            
            file_info = self.file_metadata[filename]
            return namenode_pb2.BlockLocationsResponse(blocks=file_info['blocks'])
            
        except Exception as e:
            logging.error(f"Error getting block locations for {request.filename}: {e}")
            return namenode_pb2.BlockLocationsResponse(blocks=[])

    def CreateFile(self, request, context):
        """Crear un archivo en el sistema distribuido"""
        try:
            filename = request.filename
            file_size = request.size
            replication_factor = getattr(request, 'replication_factor', 2)  # Default 2 réplicas
            
            if filename in self.file_metadata:
                return namenode_pb2.CreateFileResponse(
                    success=False,
                    message="File already exists",
                    blocks=[]
                )
            
            # Verificar que tengamos suficientes DataNodes
            if len(self.datanodes) < replication_factor:
                return namenode_pb2.CreateFileResponse(
                    success=False,
                    message=f"Not enough DataNodes. Need {replication_factor}, have {len(self.datanodes)}",
                    blocks=[]
                )
            
            # Calcular número de bloques
            num_blocks = (file_size + self.block_size - 1) // self.block_size
            
            blocks = []
            for i in range(num_blocks):
                block_id = f"{filename}_block_{i}_{int(time.time())}"
                
                # Seleccionar DataNodes para réplicas
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
                'replication_factor': replication_factor,
                'created_at': time.time(),
                'num_blocks': num_blocks
            }
            
            logging.info(f"File created: {filename} ({file_size} bytes, {num_blocks} blocks)")
            
            return namenode_pb2.CreateFileResponse(
                success=True,
                message=f"File {filename} created successfully",
                blocks=blocks
            )
            
        except Exception as e:
            logging.error(f"Error creating file {request.filename}: {e}")
            return namenode_pb2.CreateFileResponse(
                success=False,
                message=f"Error creating file: {str(e)}",
                blocks=[]
            )

    def ListFiles(self, request, context):
        """Listar archivos en el sistema"""
        try:
            # Por ahora, listar todos los archivos (sin implementar directorios)
            files = list(self.file_metadata.keys())
            logging.info(f"Listed {len(files)} files")
            return namenode_pb2.ListResponse(files=files)
            
        except Exception as e:
            logging.error(f"Error listing files: {e}")
            return namenode_pb2.ListResponse(files=[])
    
    def DeleteFile(self, request, context):
        """Eliminar un archivo del sistema"""
        try:
            filename = request.filename
            
            if filename not in self.file_metadata:
                return namenode_pb2.FileResponse(
                    success=False,
                    message="File not found"
                )
            
            # Eliminar metadata del archivo
            del self.file_metadata[filename]
            logging.info(f"File deleted: {filename}")
            
            return namenode_pb2.FileResponse(
                success=True,
                message=f"File {filename} deleted successfully"
            )
            
        except Exception as e:
            logging.error(f"Error deleting file {request.filename}: {e}")
            return namenode_pb2.FileResponse(
                success=False,
                message=f"Error deleting file: {str(e)}"
            )

    def select_datanodes_for_block(self, replication_factor):
        """Seleccionar DataNodes para almacenar réplicas de un bloque"""
        available_datanodes = list(self.datanodes.values())
        
        # Algoritmo simple: round-robin con los DataNodes disponibles
        if len(available_datanodes) < replication_factor:
            selected = available_datanodes
        else:
            # Seleccionar de forma cíclica
            selected = available_datanodes[:replication_factor]
        
        datanode_infos = []
        for dn in selected:
            datanode_info = namenode_pb2.DataNodeInfo(
                host=dn['host'],
                port=dn['port']
            )
            datanode_infos.append(datanode_info)
        
        return datanode_infos
    
    def get_datanode_stats(self):
        """Obtener estadísticas de los DataNodes"""
        active_datanodes = 0
        current_time = time.time()
        
        for dn_key, dn_info in self.datanodes.items():
            # Considerar activo si el último heartbeat fue hace menos de 60 segundos
            if current_time - dn_info['last_heartbeat'] < 60:
                active_datanodes += 1
        
        return {
            'total_datanodes': len(self.datanodes),
            'active_datanodes': active_datanodes,
            'datanodes': self.datanodes
        }

def create_rest_api(namenode):
    """Crear la aplicación Flask para la API REST"""
    app = Flask(__name__)
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """Endpoint de salud del NameNode"""
        stats = namenode.get_datanode_stats()
        return jsonify({
            "status": "healthy",
            "namenode": {
                "host": namenode.host,
                "grpc_port": namenode.grpc_port,
                "rest_port": namenode.rest_port,
                "block_size": namenode.block_size
            },
            "datanodes": stats,
            "total_files": len(namenode.file_metadata)
        })
    
    @app.route('/files', methods=['GET'])
    def list_files():
        """Listar todos los archivos"""
        try:
            files_info = []
            for filename, metadata in namenode.file_metadata.items():
                files_info.append({
                    "filename": filename,
                    "size": metadata['size'],
                    "num_blocks": metadata['num_blocks'],
                    "replication_factor": metadata['replication_factor'],
                    "created_at": metadata['created_at']
                })
            
            return jsonify({
                "success": True,
                "total_files": len(files_info),
                "files": files_info
            })
            
        except Exception as e:
            logging.error(f"Error in REST list_files: {e}")
            return jsonify({
                "success": False,
                "message": f"Error listing files: {str(e)}"
            }), 500
    
    @app.route('/files/<filename>', methods=['POST'])
    def create_file(filename):
        """Crear un nuevo archivo"""
        try:
            data = request.get_json()
            if not data or 'size' not in data:
                return jsonify({
                    "success": False,
                    "message": "File size is required"
                }), 400
            
            file_size = data['size']
            replication_factor = data.get('replication_factor', 2)
            
            # Verificar que el archivo no exista
            if filename in namenode.file_metadata:
                return jsonify({
                    "success": False,
                    "message": "File already exists"
                }), 409
            
            # Verificar DataNodes disponibles
            if len(namenode.datanodes) < replication_factor:
                return jsonify({
                    "success": False,
                    "message": f"Not enough DataNodes. Need {replication_factor}, have {len(namenode.datanodes)}"
                }), 503
            
            # Calcular bloques
            num_blocks = (file_size + namenode.block_size - 1) // namenode.block_size
            blocks = []
            
            for i in range(num_blocks):
                block_id = f"{filename}_block_{i}_{int(time.time())}"
                selected_datanodes = namenode.select_datanodes_for_block(replication_factor)
                
                block_info = {
                    "block_id": block_id,
                    "datanodes": [
                        {"host": dn.host, "port": dn.port} 
                        for dn in selected_datanodes
                    ]
                }
                blocks.append(block_info)
            
            # Guardar metadata
            namenode.file_metadata[filename] = {
                'size': file_size,
                'blocks': blocks,
                'replication_factor': replication_factor,
                'created_at': time.time(),
                'num_blocks': num_blocks
            }
            
            logging.info(f"File created via REST: {filename}")
            
            return jsonify({
                "success": True,
                "message": f"File {filename} created successfully",
                "filename": filename,
                "size": file_size,
                "num_blocks": num_blocks,
                "blocks": blocks
            })
            
        except Exception as e:
            logging.error(f"Error in REST create_file: {e}")
            return jsonify({
                "success": False,
                "message": f"Error creating file: {str(e)}"
            }), 500
    
    @app.route('/files/<filename>', methods=['GET'])
    def get_file_info(filename):
        """Obtener información de un archivo"""
        try:
            if filename not in namenode.file_metadata:
                return jsonify({
                    "success": False,
                    "message": "File not found"
                }), 404
            
            file_info = namenode.file_metadata[filename]
            
            return jsonify({
                "success": True,
                "filename": filename,
                "size": file_info['size'],
                "num_blocks": file_info['num_blocks'],
                "replication_factor": file_info['replication_factor'],
                "created_at": file_info['created_at'],
                "blocks": file_info['blocks']
            })
            
        except Exception as e:
            logging.error(f"Error in REST get_file_info: {e}")
            return jsonify({
                "success": False,
                "message": f"Error getting file info: {str(e)}"
            }), 500
    
    @app.route('/files/<filename>', methods=['DELETE'])
    def delete_file(filename):
        """Eliminar un archivo"""
        try:
            if filename not in namenode.file_metadata:
                return jsonify({
                    "success": False,
                    "message": "File not found"
                }), 404
            
            # Eliminar metadata
            del namenode.file_metadata[filename]
            logging.info(f"File deleted via REST: {filename}")
            
            return jsonify({
                "success": True,
                "message": f"File {filename} deleted successfully"
            })
            
        except Exception as e:
            logging.error(f"Error in REST delete_file: {e}")
            return jsonify({
                "success": False,
                "message": f"Error deleting file: {str(e)}"
            }), 500
    
    @app.route('/datanodes', methods=['GET'])
    def list_datanodes():
        """Listar todos los DataNodes"""
        try:
            stats = namenode.get_datanode_stats()
            datanodes_info = []
            current_time = time.time()
            
            for dn_key, dn_info in namenode.datanodes.items():
                last_heartbeat_ago = current_time - dn_info['last_heartbeat']
                is_active = last_heartbeat_ago < 60
                
                datanodes_info.append({
                    "key": dn_key,
                    "host": dn_info['host'],
                    "port": dn_info['port'],
                    "registered_at": dn_info['registered_at'],
                    "last_heartbeat": dn_info['last_heartbeat'],
                    "last_heartbeat_ago_seconds": last_heartbeat_ago,
                    "is_active": is_active
                })
            
            return jsonify({
                "success": True,
                "total_datanodes": stats['total_datanodes'],
                "active_datanodes": stats['active_datanodes'],
                "datanodes": datanodes_info
            })
            
        except Exception as e:
            logging.error(f"Error in REST list_datanodes: {e}")
            return jsonify({
                "success": False,
                "message": f"Error listing datanodes: {str(e)}"
            }), 500
    
    @app.route('/stats', methods=['GET'])
    def get_stats():
        """Obtener estadísticas generales del sistema"""
        try:
            stats = namenode.get_datanode_stats()
            
            # Calcular estadísticas de archivos
            total_size = sum(f['size'] for f in namenode.file_metadata.values())
            total_blocks = sum(f['num_blocks'] for f in namenode.file_metadata.values())
            
            return jsonify({
                "namenode": {
                    "host": namenode.host,
                    "grpc_port": namenode.grpc_port,
                    "rest_port": namenode.rest_port,
                    "block_size": namenode.block_size
                },
                "datanodes": {
                    "total": stats['total_datanodes'],
                    "active": stats['active_datanodes']
                },
                "files": {
                    "total_files": len(namenode.file_metadata),
                    "total_size_bytes": total_size,
                    "total_blocks": total_blocks
                }
            })
            
        except Exception as e:
            logging.error(f"Error in REST get_stats: {e}")
            return jsonify({
                "success": False,
                "message": f"Error getting stats: {str(e)}"
            }), 500
    
    return app

def serve():
    # Buscar config.yaml en el directorio padre
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.yaml')
    
    if not os.path.exists(config_path):
        print(f"Error: No se encontró config.yaml en {config_path}")
        return
    
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error leyendo config.yaml: {e}")
        return
    
    # Crear NameNode
    namenode = NameNode(config)
    
    # ========== Servidor gRPC ==========
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    namenode_pb2_grpc.add_NameNodeServiceServicer_to_server(namenode, grpc_server)
    
    grpc_server.add_insecure_port(f"[::]:{namenode.grpc_port}")
    grpc_server.start()
    
    print(f"✅ NameNode gRPC server running on {namenode.host}:{namenode.grpc_port}")
    
    # ========== Servidor REST ==========
    rest_app = create_rest_api(namenode)
    rest_server = make_server('0.0.0.0', namenode.rest_port, rest_app, threaded=True)
    
    def run_rest_server():
        print(f"✅ NameNode REST API running on port {namenode.rest_port}")
        rest_server.serve_forever()
    
    # Ejecutar servidor REST en un hilo separado
    rest_thread = threading.Thread(target=run_rest_server, daemon=True)
    rest_thread.start()
    
    print(f"🌐 REST API endpoints:")
    print(f"   GET  http://{namenode.host}:{namenode.rest_port}/health")
    print(f"   GET  http://{namenode.host}:{namenode.rest_port}/files")
    print(f"   POST http://{namenode.host}:{namenode.rest_port}/files/<filename>")
    print(f"   GET  http://{namenode.host}:{namenode.rest_port}/files/<filename>")
    print(f"   DEL  http://{namenode.host}:{namenode.rest_port}/files/<filename>")
    print(f"   GET  http://{namenode.host}:{namenode.rest_port}/datanodes")
    print(f"   GET  http://{namenode.host}:{namenode.rest_port}/stats")
    print("Press Ctrl+C to stop...")
    
    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n🛑 Shutting down NameNode...")
        grpc_server.stop(0)
        rest_server.shutdown()

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    serve()