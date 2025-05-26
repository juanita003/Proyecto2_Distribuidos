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
import hashlib
from urllib.parse import urljoin

# Importar los archivos protobuf generados
import namenode_pb2
import namenode_pb2_grpc

class DFSClient:
    def __init__(self, namenode_host, namenode_port=9000, rest_port=8080):
        """Inicializar cliente DFS para conectar a instancias AWS"""
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.rest_port = rest_port
        self.namenode_channel = None
        self.namenode_stub = None
        self.current_path = "/"
        self.session = requests.Session()
        self.session.timeout = 30
        self._connect_to_namenode()
    
    def _connect_to_namenode(self):
        """Establecer conexión gRPC con el NameNode en AWS"""
        try:
            address = f"{self.namenode_host}:{self.namenode_port}"
            self.namenode_channel = grpc.insecure_channel(address)
            self.namenode_stub = namenode_pb2_grpc.NameNodeServiceStub(self.namenode_channel)
            
            # Probar conexión con timeout
            print(f"🔄 Conectando al NameNode AWS en {address}...")
            test_request = namenode_pb2.ListRequest(path="/")
            response = self.namenode_stub.ListFiles(test_request, timeout=10)
            print(f"✅ Conectado exitosamente al NameNode en {address}")
            
        except grpc.RpcError as e:
            print(f"❌ Error gRPC conectando al NameNode: {e.code()} - {e.details()}")
            self._print_namenode_troubleshooting()
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error conectando al NameNode AWS: {e}")
            self._print_namenode_troubleshooting()
            sys.exit(1)
    
    def _print_namenode_troubleshooting(self):
        """Imprimir consejos de troubleshooting"""
        print("💡 Verifica que:")
        print("   - La instancia EC2 del NameNode esté ejecutándose")
        print("   - El puerto 9000 esté abierto en el Security Group")
        print("   - La IP pública sea correcta")
        print("   - El servicio NameNode esté activo: python namenode.py")
    
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
            
            # 3. Subir cada bloque a los DataNodes AWS usando REST API
            successful_uploads = 0
            total_blocks = len(response.blocks)
            
            for i, block_info in enumerate(response.blocks):
                if i < len(blocks_data):
                    block_index, block_data = blocks_data[i]
                    print(f"📤 Subiendo bloque {i+1}/{total_blocks}: {block_info.block_id}")
                    
                    if self._upload_block_via_rest(block_info, block_data):
                        successful_uploads += 1
                        print(f"   ✅ Bloque {block_info.block_id} subido exitosamente")
                    else:
                        print(f"   ❌ Error subiendo bloque {block_info.block_id}")
            
            # 4. Confirmar al NameNode que la escritura fue exitosa
            if successful_uploads == total_blocks:
                self._confirm_file_creation(remote_path, True)
                print(f"🎉 Archivo subido completamente: {remote_path}")
                print(f"   📊 {total_blocks} bloques, {self._format_size(file_size)} total")
                return True
            else:
                self._confirm_file_creation(remote_path, False)
                print(f"⚠️  Upload parcial: {successful_uploads}/{total_blocks} bloques")
                return False
                
        except grpc.RpcError as e:
            print(f"❌ Error gRPC con NameNode: {e.code()} - {e.details()}")
            return False
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            return False
    
    def _upload_block_via_rest(self, block_info, block_data):
        """Subir bloque a DataNode usando REST API"""
        if not block_info.datanodes:
            print("   ❌ No hay DataNodes asignados para este bloque")
            return False
        
        # Usar el primer DataNode como primario (leader)
        primary_datanode = block_info.datanodes[0]
        
        try:
            # Preparar datos para replicación
            replica_info = []
            for replica_dn in block_info.datanodes[1:]:
                replica_info.append({
                    'host': replica_dn.host,
                    'port': self.rest_port,
                    'node_id': getattr(replica_dn, 'node_id', f"{replica_dn.host}:{replica_dn.port}")
                })
            
            # URL del DataNode primario
            primary_url = f"http://{primary_datanode.host}:{self.rest_port}"
            upload_endpoint = f"{primary_url}/api/v1/blocks/{block_info.block_id}"
            
            print(f"   🔄 Enviando a DataNode primario {primary_datanode.host}:{self.rest_port}...")
            
            # Preparar el payload
            files = {
                'block_data': (block_info.block_id, block_data, 'application/octet-stream')
            }
            
            data = {
                'block_id': block_info.block_id,
                'is_primary': 'true',
                'replication_factor': str(len(block_info.datanodes)),
                'replicas': json.dumps(replica_info)
            }
            
            # Calcular checksum para integridad
            checksum = hashlib.md5(block_data).hexdigest()
            data['checksum'] = checksum
            
            # Realizar upload con retry
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.post(
                        upload_endpoint,
                        files=files,
                        data=data,
                        timeout=120  # Timeout generoso para uploads grandes
                    )
                    
                    if response.status_code == 200 or response.status_code == 201:
                        result = response.json()
                        if result.get('success', False):
                            replicas_created = result.get('replicas_created', 0)
                            print(f"   ✅ Bloque almacenado con {replicas_created + 1} réplicas")
                            return True
                        else:
                            error_msg = result.get('error', 'Error desconocido')
                            print(f"   ❌ DataNode reportó error: {error_msg}")
                            if attempt < max_retries - 1:
                                print(f"   🔄 Reintentando... ({attempt + 2}/{max_retries})")
                                time.sleep(2)
                                continue
                            return False
                    else:
                        print(f"   ❌ HTTP {response.status_code}: {response.text}")
                        if attempt < max_retries - 1:
                            print(f"   🔄 Reintentando... ({attempt + 2}/{max_retries})")
                            time.sleep(2)
                            continue
                        return False
                        
                except requests.Timeout:
                    print(f"   ⏰ Timeout en intento {attempt + 1}")
                    if attempt < max_retries - 1:
                        print(f"   🔄 Reintentando... ({attempt + 2}/{max_retries})")
                        time.sleep(5)
                        continue
                    return False
                except requests.ConnectionError as e:
                    print(f"   🔌 Error de conexión: {e}")
                    if attempt < max_retries - 1:
                        print(f"   🔄 Reintentando... ({attempt + 2}/{max_retries})")
                        time.sleep(3)
                        continue
                    return False
            
            return False
                
        except Exception as e:
            print(f"   ❌ Error inesperado subiendo bloque: {e}")
            return False
    
    def get_file(self, remote_path, local_path=None):
        """Descargar archivo del DFS AWS al PC local"""
        if local_path is None:
            local_path = os.path.basename(remote_path)
        
        if not remote_path.startswith('/'):
            remote_path = os.path.join(self.current_path, remote_path).replace('\\', '/')
        
        print(f"📥 Descargando {remote_path} -> {local_path}")
        
        try:
            # 1. Obtener metadatos y ubicaciones de bloques del NameNode
            request = namenode_pb2.FileRequest(filename=remote_path)
            response = self.namenode_stub.GetBlockLocations(request)
            
            if not response.blocks:
                print(f"❌ Archivo no encontrado: {remote_path}")
                return False
            
            print(f"📍 Archivo tiene {len(response.blocks)} bloques")
            
            # 2. Descargar cada bloque desde los DataNodes usando REST API
            all_blocks_data = {}
            
            # Usar ThreadPoolExecutor para downloads paralelos
            with ThreadPoolExecutor(max_workers=min(4, len(response.blocks))) as executor:
                future_to_block = {
                    executor.submit(self._download_block_via_rest, i, block_info): i
                    for i, block_info in enumerate(response.blocks)
                }
                
                for future in future_to_block:
                    block_index = future_to_block[future]
                    try:
                        block_data = future.result(timeout=60)
                        if block_data is not None:
                            all_blocks_data[block_index] = block_data
                            print(f"   ✅ Bloque {block_index + 1} descargado ({self._format_size(len(block_data))})")
                        else:
                            print(f"   ❌ Error descargando bloque {block_index + 1}")
                            return False
                    except Exception as e:
                        print(f"   ❌ Error en bloque {block_index + 1}: {e}")
                        return False
            
            # 3. Ensamblar archivo completo
            print("🔄 Ensamblando archivo...")
            with open(local_path, 'wb') as f:
                for i in range(len(all_blocks_data)):
                    if i in all_blocks_data:
                        f.write(all_blocks_data[i])
                    else:
                        print(f"❌ Falta el bloque {i}")
                        return False
            
            file_size = os.path.getsize(local_path)
            print(f"🎉 Archivo descargado: {local_path} ({self._format_size(file_size)})")
            
            # Verificar integridad si es posible
            self._verify_file_integrity(local_path, remote_path)
            return True
            
        except grpc.RpcError as e:
            print(f"❌ Error gRPC: {e.code()} - {e.details()}")
            return False
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            return False
    
    def _download_block_via_rest(self, block_index, block_info):
        """Descargar bloque específico usando REST API"""
        print(f"📥 Descargando bloque {block_index + 1}: {block_info.block_id}")
        
        # Intentar con cada DataNode hasta que uno funcione
        for datanode_index, datanode in enumerate(block_info.datanodes):
            try:
                datanode_url = f"http://{datanode.host}:{self.rest_port}"
                download_endpoint = f"{datanode_url}/api/v1/blocks/{block_info.block_id}"
                
                print(f"   🔄 Intentando desde DataNode {datanode.host}:{self.rest_port}...")
                
                response = self.session.get(download_endpoint, timeout=60)
                
                if response.status_code == 200:
                    block_data = response.content
                    
                    # Verificar checksum si está disponible
                    checksum_header = response.headers.get('X-Block-Checksum')
                    if checksum_header:
                        calculated_checksum = hashlib.md5(block_data).hexdigest()
                        if calculated_checksum != checksum_header:
                            print(f"   ⚠️  Checksum no coincide en {datanode.host}, intentando otro DataNode...")
                            continue
                    
                    print(f"   ✅ Descargado desde {datanode.host}")
                    return block_data
                    
                elif response.status_code == 404:
                    print(f"   ⚠️  Bloque no encontrado en {datanode.host}")
                    continue
                else:
                    print(f"   ⚠️  {datanode.host} respondió {response.status_code}: {response.text}")
                    continue
                    
            except requests.Timeout:
                print(f"   ⏰ Timeout con {datanode.host}")
                continue
            except requests.ConnectionError:
                print(f"   🔌 No se pudo conectar a {datanode.host}:{self.rest_port}")
                continue
            except Exception as e:
                print(f"   ❌ Error con {datanode.host}: {e}")
                continue
        
        # Si llegamos aquí, no pudimos descargar de ningún DataNode
        print(f"   ❌ No se pudo descargar el bloque desde ningún DataNode")
        return None
    
    def ls(self, path=None):
        """Listar archivos en el DFS usando gRPC"""
        if path is None:
            path = self.current_path
        elif not path.startswith('/'):
            path = os.path.join(self.current_path, path).replace('\\', '/')
        
        try:
            request = namenode_pb2.ListRequest(path=path)
            response = self.namenode_stub.ListFiles(request)
            
            if not response.files:
                print(f"📁 Directorio vacío: {path}")
                return True
            
            print(f"📁 Contenido de {path}:")
            for file_info in response.files:
                # Asumir que file_info tiene campos como name, size, is_directory
                if hasattr(file_info, 'is_directory') and file_info.is_directory:
                    print(f"   📁 {file_info.name}/")
                else:
                    size_str = self._format_size(getattr(file_info, 'size', 0))
                    print(f"   📄 {file_info.name} ({size_str})")
            return True
            
        except grpc.RpcError as e:
            print(f"❌ Error listando archivos: {e.details()}")
            return False
    
    def rm(self, remote_path):
        """Eliminar archivo del DFS"""
        if not remote_path.startswith('/'):
            remote_path = os.path.join(self.current_path, remote_path).replace('\\', '/')
        
        try:
            request = namenode_pb2.FileRequest(filename=remote_path)
            response = self.namenode_stub.DeleteFile(request)
            
            if response.success:
                print(f"🗑️  Archivo eliminado: {remote_path}")
                return True
            else:
                print(f"❌ Error eliminando archivo: {getattr(response, 'message', 'Error desconocido')}")
                return False
                
        except grpc.RpcError as e:
            print(f"❌ Error gRPC eliminando archivo: {e.details()}")
            return False
    
    def cd(self, path):
        """Cambiar directorio actual"""
        if path.startswith('/'):
            new_path = path
        else:
            new_path = os.path.join(self.current_path, path).replace('\\', '/')
        
        # Normalizar ruta
        new_path = os.path.normpath(new_path).replace('\\', '/')
        if not new_path.startswith('/'):
            new_path = '/' + new_path
        
        # Verificar que el directorio existe
        try:
            request = namenode_pb2.ListRequest(path=new_path)
            self.namenode_stub.ListFiles(request)
            self.current_path = new_path
            print(f"📁 Directorio actual: {self.current_path}")
            return True
        except grpc.RpcError as e:
            print(f"❌ Directorio no encontrado: {new_path}")
            return False
    
    def mkdir(self, path):
        """Crear directorio"""
        if not path.startswith('/'):
            path = os.path.join(self.current_path, path).replace('\\', '/')
        
        try:
            request = namenode_pb2.DirectoryRequest(path=path)
            response = self.namenode_stub.CreateDirectory(request)
            
            if response.success:
                print(f"📁 Directorio creado: {path}")
                return True
            else:
                print(f"❌ Error creando directorio: {getattr(response, 'message', 'Error desconocido')}")
                return False
                
        except grpc.RpcError as e:
            print(f"❌ Error gRPC creando directorio: {e.details()}")
            return False
    
    def status(self):
        """Mostrar estado del sistema DFS"""
        try:
            request = namenode_pb2.StatusRequest()
            response = self.namenode_stub.GetStatus(request)
            
            print("🔍 Estado del Sistema DFS:")
            print(f"   NameNode: {self.namenode_host}:{self.namenode_port}")
            print(f"   DataNodes activos: {getattr(response, 'active_datanodes', 'N/A')}")
            print(f"   Archivos totales: {getattr(response, 'total_files', 'N/A')}")
            print(f"   Bloques totales: {getattr(response, 'total_blocks', 'N/A')}")
            print(f"   Espacio utilizado: {self._format_size(getattr(response, 'used_space', 0))}")
            print(f"   Espacio disponible: {self._format_size(getattr(response, 'available_space', 0))}")
            return True
            
        except grpc.RpcError as e:
            print(f"❌ Error obteniendo estado: {e.details()}")
            return False
    
    def _confirm_file_creation(self, filename, success):
        """Confirmar al NameNode el resultado de la creación de archivo"""
        try:
            request = namenode_pb2.FileConfirmation(
                filename=filename,
                success=success
            )
            self.namenode_stub.ConfirmFileCreation(request)
        except:
            pass  # No crítico si falla
    
    def _verify_file_integrity(self, local_path, remote_path):
        """Verificar integridad del archivo descargado"""
        try:
            # Calcular hash del archivo local
            local_hash = self._calculate_file_hash(local_path)
            print(f"🔍 Hash local: {local_hash[:16]}...")
            
            # Aquí podrías comparar con un hash almacenado en el NameNode
            # Por simplicidad, solo mostramos el hash calculado
            
        except Exception as e:
            print(f"⚠️  No se pudo verificar integridad: {e}")
    
    def _calculate_file_hash(self, filepath):
        """Calcular hash MD5 de un archivo"""
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def _format_size(self, size_bytes):
        """Formatear tamaño en bytes a formato legible"""
        if size_bytes == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} PB"
    
    def close(self):
        """Cerrar conexiones"""
        if self.namenode_channel:
            self.namenode_channel.close()
        self.session.close()

def main():
    parser = argparse.ArgumentParser(description='Cliente CLI para DFS distribuido en AWS')
    parser.add_argument('--namenode', required=True, help='IP pública del NameNode en AWS')
    parser.add_argument('--port', default=9000, type=int, help='Puerto gRPC del NameNode (default: 9000)')
    parser.add_argument('--rest-port', default=8080, type=int, help='Puerto REST de DataNodes (default: 8080)')
    
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
    
    # Comando rm
    rm_parser = subparsers.add_parser('rm', help='Eliminar archivo')
    rm_parser.add_argument('remote_file', help='Archivo a eliminar')
    
    # Comando cd
    cd_parser = subparsers.add_parser('cd', help='Cambiar directorio')
    cd_parser.add_argument('path', help='Directorio destino')
    
    # Comando mkdir
    mkdir_parser = subparsers.add_parser('mkdir', help='Crear directorio')
    mkdir_parser.add_argument('path', help='Directorio a crear')
    
    # Comando status
    status_parser = subparsers.add_parser('status', help='Estado del sistema')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Crear cliente y conectar al NameNode AWS
    client = DFSClient(args.namenode, args.port, args.rest_port)
    
    try:
        # Ejecutar comando
        success = False
        if args.command == 'put':
            success = client.put_file(args.local_file, args.remote_file)
        elif args.command == 'get':
            success = client.get_file(args.remote_file, args.local_file)
        elif args.command == 'ls':
            success = client.ls(args.path)
        elif args.command == 'rm':
            success = client.rm(args.remote_file)
        elif args.command == 'cd':
            success = client.cd(args.path)
        elif args.command == 'mkdir':
            success = client.mkdir(args.path)
        elif args.command == 'status':
            success = client.status()
        
        sys.exit(0 if success else 1)
        
    finally:
        client.close()

if __name__ == '__main__':
    main()

# Ejemplos de uso:
# python dfs_client.py --namenode 3.15.45.123 put mi_archivo.txt
# python dfs_client.py --namenode 3.15.45.123 get archivo_remoto.txt mi_descarga.txt
# python dfs_client.py --namenode 3.15.45.123 ls /
# python dfs_client.py --namenode 3.15.45.123 rm archivo_remoto.txt
# python dfs_client.py --namenode 3.15.45.123 mkdir /nuevo_directorio
# python dfs_client.py --namenode 3.15.45.123 status