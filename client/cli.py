#!/usr/bin/env python3

import grpc
import os
import sys
import argparse
import requests
import yaml
from pathlib import Path
import time
import json
from concurrent.futures import ThreadPoolExecutor

# Importar los archivos protobuf generados
import namenode_pb2
import namenode_pb2_grpc

class DFSClient:
    def __init__(self, namenode_host, namenode_port=9000):
        """Inicializar cliente DFS para conectar a instancias AWS"""
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.namenode_channel = None
        self.namenode_stub = None
        self.current_path = "/"
        self._connect_to_namenode()
    
    def _connect_to_namenode(self):
        """Establecer conexión gRPC con el NameNode en AWS"""
        try:
            address = f"{self.namenode_host}:{self.namenode_port}"
            self.namenode_channel = grpc.insecure_channel(address)
            self.namenode_stub = namenode_pb2_grpc.NameNodeServiceStub(self.namenode_channel)
            
            # Probar conexión
            print(f"🔄 Conectando al NameNode AWS en {address}...")
            # Hacer una prueba simple
            test_request = namenode_pb2.ListRequest(path="/")
            self.namenode_stub.ListFiles(test_request)
            print(f"✅ Conectado exitosamente al NameNode en {address}")
            
        except Exception as e:
            print(f"❌ Error conectando al NameNode AWS: {e}")
            print("💡 Verifica que:")
            print("   - La instancia EC2 del NameNode esté ejecutándose")
            print("   - El puerto 9000 esté abierto en el Security Group")
            print("   - La IP pública sea correcta")
            sys.exit(1)
    
    def put_file(self, local_path, remote_path=None):
        """Subir archivo desde PC local al DFS en AWS"""
        if not os.path.exists(local_path):
            print(f"❌ Archivo local no encontrado: {local_path}")
            return False
        
        if remote_path is None:
            remote_path = os.path.basename(local_path)
        
        if not remote_path.startswith('/'):
            remote_path = os.path.join(self.current_path, remote_path).replace('\\', '/')
        
        file_size = os.path.getsize(local_path)
        print(f"📤 Subiendo {local_path} -> {remote_path} ({self._format_size(file_size)})")
        
        try:
            # 1. Solicitar al NameNode la asignación de bloques
            request = namenode_pb2.FileMetadata(
                filename=remote_path,
                size=file_size,
                replication_factor=2
            )
            
            print("🔄 Solicitando asignación de bloques al NameNode...")
            response = self.namenode_stub.CreateFile(request)
            
            if not response.success:
                print(f"❌ NameNode rechazó la creación: {getattr(response, 'message', 'Error desconocido')}")
                return False
            
            print(f"✅ NameNode asignó {len(response.blocks)} bloques")
            
            # 2. Dividir archivo en bloques
            block_size = 64 * 1024 * 1024  # 64MB
            blocks_data = []
            
            print("🔄 Dividiendo archivo en bloques...")
            with open(local_path, 'rb') as f:
                block_index = 0
                while True:
                    chunk = f.read(block_size)
                    if not chunk:
                        break
                    blocks_data.append((block_index, chunk))
                    print(f"   Bloque {block_index}: {self._format_size(len(chunk))}")
                    block_index += 1
            
            # 3. Subir cada bloque a los DataNodes AWS
            successful_uploads = 0
            total_blocks = len(response.blocks)
            
            for i, block_info in enumerate(response.blocks):
                if i < len(blocks_data):
                    block_index, block_data = blocks_data[i]
                    print(f"📤 Subiendo bloque {i+1}/{total_blocks}: {block_info.block_id}")
                    
                    if self._upload_block_to_aws(block_info, block_data):
                        successful_uploads += 1
                        print(f"   ✅ Bloque {block_info.block_id} subido")
                    else:
                        print(f"   ❌ Error subiendo bloque {block_info.block_id}")
            
            if successful_uploads == total_blocks:
                print(f"🎉 Archivo subido completamente: {remote_path}")
                print(f"   📊 {total_blocks} bloques, {self._format_size(file_size)} total")
                return True
            else:
                print(f"⚠️  Upload parcial: {successful_uploads}/{total_blocks} bloques")
                return False
                
        except grpc.RpcError as e:
            print(f"❌ Error gRPC con NameNode: {e.code()} - {e.details()}")
            return False
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            return False
    
    def _upload_block_to_aws(self, block_info, block_data):
        """Subir bloque a DataNode en AWS usando REST API"""
        if not block_info.datanodes:
            return False
        
        # Usar el primer DataNode como primario
        primary_dn = block_info.datanodes[0]
        
        try:
            # Preparar información de réplicas para el DataNode primario
            replica_targets = []
            if len(block_info.datanodes) > 1:
                for dn in block_info.datanodes[1:]:
                    replica_targets.append({
                        'host': dn.host,
                        'port': dn.port
                    })
            
            # URL del DataNode en AWS (asumiendo puerto 8080 para REST API)
            datanode_url = f"http://{primary_dn.host}:8080/upload_block"
            
            # Preparar request
            files = {
                'block_data': (block_info.block_id, block_data, 'application/octet-stream')
            }
            
            data = {
                'block_id': block_info.block_id,
                'replicas': json.dumps(replica_targets)
            }
            
            print(f"   🔄 Enviando a DataNode AWS {primary_dn.host}:8080...")
            
            # Enviar al DataNode primario (que manejará la replicación)
            response = requests.post(
                datanode_url,
                files=files,
                data=data,
                timeout=60  # Timeout generoso para AWS
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    print(f"   ✅ DataNode confirmó: {result.get('message', 'OK')}")
                    return True
                else:
                    print(f"   ❌ DataNode reportó error: {result.get('message', 'Unknown')}")
                    return False
            else:
                print(f"   ❌ HTTP Error {response.status_code}: {response.text}")
                return False
                
        except requests.Timeout:
            print(f"   ⏰ Timeout conectando a DataNode {primary_dn.host}")
            return False
        except requests.ConnectionError:
            print(f"   🔌 No se pudo conectar a DataNode {primary_dn.host}:8080")
            print("      💡 Verifica que el DataNode esté ejecutándose y el puerto 8080 abierto")
            return False
        except Exception as e:
            print(f"   ❌ Error subiendo a DataNode: {e}")
            return False
    
    def get_file(self, remote_path, local_path=None):
        """Descargar archivo del DFS AWS al PC local"""
        if local_path is None:
            local_path = os.path.basename(remote_path)
        
        if not remote_path.startswith('/'):
            remote_path = os.path.join(self.current_path, remote_path).replace('\\', '/')
        
        print(f"📥 Descargando {remote_path} -> {local_path}")
        
        try:
            # 1. Obtener ubicaciones de bloques del NameNode
            request = namenode_pb2.FileRequest(filename=remote_path)
            response = self.namenode_stub.GetBlockLocations(request)
            
            if not response.blocks:
                print(f"❌ Archivo no encontrado: {remote_path}")
                return False
            
            print(f"📍 Archivo tiene {len(response.blocks)} bloques")
            
            # 2. Descargar cada bloque desde los DataNodes AWS
            all_blocks_data = {}
            
            for i, block_info in enumerate(response.blocks):
                print(f"📥 Descargando bloque {i+1}/{len(response.blocks)}: {block_info.block_id}")
                block_data = self._download_block_from_aws(block_info)
                
                if block_data is not None:
                    all_blocks_data[i] = block_data
                    print(f"   ✅ Bloque {i+1} descargado ({self._format_size(len(block_data))})")
                else:
                    print(f"   ❌ Error descargando bloque {i+1}")
                    return False
            
            # 3. Ensamblar archivo completo
            print("🔄 Ensamblando archivo...")
            with open(local_path, 'wb') as f:
                for i in range(len(all_blocks_data)):
                    f.write(all_blocks_data[i])
            
            file_size = os.path.getsize(local_path)
            print(f"🎉 Archivo descargado: {local_path} ({self._format_size(file_size)})")
            return True
            
        except grpc.RpcError as e:
            print(f"❌ Error gRPC: {e.code()} - {e.details()}")
            return False
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            return False
    
    def _download_block_from_aws(self, block_info):
        """Descargar bloque desde DataNode en AWS"""
        # Intentar con cada DataNode hasta que uno funcione
        for datanode in block_info.datanodes:
            try:
                url = f"http://{datanode.host}:8080/download_block/{block_info.block_id}"
                print(f"   🔄 Intentando desde {datanode.host}:8080...")
                
                response = requests.get(url, timeout=60)
                
                if response.status_code == 200:
                    return response.content
                else:
                    print(f"   ⚠️  {datanode.host} respondió {response.status_code}")
                    continue
                    
            except requests.RequestException as e:
                print(f"   ⚠️  Error con {datanode.host}: {e}")
                continue
        
        return None
    
    def ls(self, path=None):
        """Listar archivos en el DFS"""
        if path is None:
            path = self.current_path
        elif not path.startswith('/'):
            path = os.path.join(self.current_path, path).replace('\\', '/')
        
        try:
            request = namenode_pb2.ListRequest(path=path)
            response = self.namenode_stub.ListFiles(request)
            
            if not response.files:
                print(f"📁 Directorio vacío: {path}")
                return
            
            print(f"📁 Contenido de {path}:")
            for file in response.files:
                print(f"   📄 {file}")
            
        except grpc.RpcError as e:
            print(f"❌ Error listando archivos: {e.details()}")
    
    def _format_size(self, size_bytes):
        """Formatear tamaño en bytes a formato legible"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

def main():
    parser = argparse.ArgumentParser(description='Cliente CLI para DFS distribuido en AWS')
    parser.add_argument('--namenode', required=True, help='IP pública del NameNode en AWS')
    parser.add_argument('--port', default=9000, type=int, help='Puerto del NameNode (default: 9000)')
    
    subparsers = parser.add_subparsers(dest='command', help='Comandos disponibles')
    
    # Comando put
    put_parser = subparsers.add_parser('put', help='Subir archivo al DFS')
    put_parser.add_argument('local_file', help='Archivo local a subir')
    put_parser.add_argument('remote_file', nargs='?', help='Nombre en el DFS (opcional)')
    
    # Comando get
    get_parser = subparsers.add_parser('get', help='Descargar archivo del DFS')
    get_parser.add_argument('remote_file', help='Archivo en el DFS')
    get_parser.add_argument('local_file', nargs='?', help='Nombre local (opcional)')
    
    # Comando ls
    ls_parser = subparsers.add_parser('ls', help='Listar archivos')
    ls_parser.add_argument('path', nargs='?', help='Ruta a listar (opcional)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Crear cliente y conectar al NameNode AWS
    client = DFSClient(args.namenode, args.port)
    
    # Ejecutar comando
    if args.command == 'put':
        client.put_file(args.local_file, args.remote_file)
    elif args.command == 'get':
        client.get_file(args.remote_file, args.local_file)
    elif args.command == 'ls':
        client.ls(args.path)

if __name__ == '__main__':
    main()

# Ejemplo de uso:
# python dfs_client.py --namenode 3.15.45.123 put mi_archivo.txt
# python dfs_client.py --namenode 3.15.45.123 get archivo_remoto.txt mi_descarga.txt
# python dfs_client.py --namenode 3.15.45.123 ls /