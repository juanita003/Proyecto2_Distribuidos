import grpc
import yaml
import os
import sys

# Importar archivos proto desde namenode
sys.path.insert(0, 'namenode')

try:
    import namenode_pb2
    import namenode_pb2_grpc
except ImportError as e:
    print(f"Error importando archivos proto: {e}")
    print("Asegúrate de estar en el directorio Proyecto2_Distribuidos")
    sys.exit(1)

# Para conectar con datanodes, necesitamos sus archivos proto también
try:
    # Generar archivos proto de datanode si no existen
    if not os.path.exists('datanode_pb2.py'):
        os.system('python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/datanode.proto')
    
    import datanode_pb2
    import datanode_pb2_grpc
except ImportError as e:
    print(f"Error importando archivos proto del datanode: {e}")
    sys.exit(1)

class DFSClient:
    def __init__(self, config_path='config.yaml'):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        # Conectar con NameNode
        nn_config = self.config['namenode']
        nn_host = nn_config['host'].strip()
        nn_port = nn_config['port']
        
        self.namenode_channel = grpc.insecure_channel(f"{nn_host}:{nn_port}")
        self.namenode_stub = namenode_pb2_grpc.NameNodeServiceStub(self.namenode_channel)
        
        print(f"✅ Conectado al NameNode: {nn_host}:{nn_port}")
    
    def upload_file(self, local_path, remote_filename):
        """Subir un archivo al sistema distribuido"""
        try:
            if not os.path.exists(local_path):
                print(f"❌ Archivo no encontrado: {local_path}")
                return False
            
            # Obtener información del archivo
            file_size = os.path.getsize(local_path)
            print(f"📤 Subiendo archivo: {local_path} -> {remote_filename} ({file_size} bytes)")
            
            # Crear archivo en el NameNode
            create_request = namenode_pb2.FileMetadata(
                filename=remote_filename,
                size=file_size,
                replication_factor=2
            )
            
            create_response = self.namenode_stub.CreateFile(create_request)
            
            if not create_response.success:
                print("❌ Error creando archivo en NameNode")
                return False
            
            print(f"✅ Archivo creado. Bloques asignados: {len(create_response.blocks)}")
            
            # Leer archivo y dividir en bloques
            block_size = 64 * 1024 * 1024  # 64MB
            
            with open(local_path, 'rb') as f:
                for i, block_info in enumerate(create_response.blocks):
                    data = f.read(block_size)
                    if not data:
                        break
                    
                    print(f"📦 Almacenando bloque {i+1}/{len(create_response.blocks)}: {block_info.block_id}")
                    
                    # Enviar bloque a cada DataNode asignado
                    for j, datanode_info in enumerate(block_info.datanodes):
                        success = self._store_block_in_datanode(
                            datanode_info, 
                            block_info.block_id, 
                            data
                        )
                        if success:
                            print(f"  ✅ Réplica {j+1} almacenada en {datanode_info.host}:{datanode_info.port}")
                        else:
                            print(f"  ❌ Error almacenando réplica {j+1} en {datanode_info.host}:{datanode_info.port}")
            
            print(f"🎉 Archivo subido exitosamente: {remote_filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error subiendo archivo: {e}")
            return False
    
    def _store_block_in_datanode(self, datanode_info, block_id, data):
        """Almacenar un bloque en un DataNode específico"""
        try:
            print(f"    🔗 Conectando a {datanode_info.host}:{datanode_info.port}...")
            channel = grpc.insecure_channel(f"{datanode_info.host}:{datanode_info.port}")
            stub = datanode_pb2_grpc.DataNodeServiceStub(channel)
            
            request = datanode_pb2.StoreBlockRequest(
                block_id=block_id,
                data=data
            )
            
            response = stub.StoreBlock(request, timeout=30)
            channel.close()
            
            return response.success
            
        except Exception as e:
            print(f"    ❌ Error conectando con DataNode {datanode_info.host}:{datanode_info.port}: {e}")
            return False
    
    def download_file(self, remote_filename, local_path):
        """Descargar un archivo del sistema distribuido"""
        try:
            print(f"📥 Descargando archivo: {remote_filename} -> {local_path}")
            
            # Obtener ubicaciones de bloques del NameNode
            request = namenode_pb2.FileRequest(filename=remote_filename)
            response = self.namenode_stub.GetBlockLocations(request)
            
            if not response.blocks:
                print(f"❌ Archivo no encontrado: {remote_filename}")
                return False
            
            print(f"📦 Descargando {len(response.blocks)} bloques...")
            
            # Descargar cada bloque
            with open(local_path, 'wb') as f:
                for i, block_info in enumerate(response.blocks):
                    print(f"📦 Descargando bloque {i+1}/{len(response.blocks)}: {block_info.block_id}")
                    
                    data = self._retrieve_block_from_datanode(block_info)
                    if data:
                        f.write(data)
                        print(f"  ✅ Bloque descargado ({len(data)} bytes)")
                    else:
                        print(f"  ❌ Error descargando bloque {block_info.block_id}")
                        return False
            
            print(f"🎉 Archivo descargado exitosamente: {local_path}")
            return True
            
        except Exception as e:
            print(f"❌ Error descargando archivo: {e}")
            return False
    
    def _retrieve_block_from_datanode(self, block_info):
        """Recuperar un bloque de cualquiera de los DataNodes que lo tienen"""
        for datanode_info in block_info.datanodes:
            try:
                print(f"    🔗 Intentando obtener de {datanode_info.host}:{datanode_info.port}...")
                channel = grpc.insecure_channel(f"{datanode_info.host}:{datanode_info.port}")
                stub = datanode_pb2_grpc.DataNodeServiceStub(channel)
                
                request = datanode_pb2.RetrieveBlockRequest(block_id=block_info.block_id)
                response = stub.RetrieveBlock(request, timeout=30)
                
                channel.close()
                
                if response.success:
                    print(f"    ✅ Obtenido de {datanode_info.host}:{datanode_info.port}")
                    return response.data
                else:
                    print(f"    ⚠️  {datanode_info.host}:{datanode_info.port}: {response.message}")
                    
            except Exception as e:
                print(f"    ⚠️  Error con {datanode_info.host}:{datanode_info.port}: {e}")
                continue
        
        return None
    
    def list_files(self):
        """Listar archivos en el sistema"""
        try:
            request = namenode_pb2.ListRequest(path="/")
            response = self.namenode_stub.ListFiles(request)
            
            if response.files:
                print("📁 Archivos en el sistema:")
                for filename in response.files:
                    print(f"  - {filename}")
            else:
                print("📁 No hay archivos en el sistema")
                
            return response.files
            
        except Exception as e:
            print(f"❌ Error listando archivos: {e}")
            return []

def main():
    try:
        client = DFSClient()
    except Exception as e:
        print(f"❌ Error conectando con el sistema: {e}")
        return
    
    while True:
        print("\n" + "="*50)
        print("🗂️  CLIENTE SISTEMA DE ARCHIVOS DISTRIBUIDO")
        print("="*50)
        print("1. Subir archivo")
        print("2. Descargar archivo")
        print("3. Listar archivos")
        print("4. Salir")
        
        choice = input("\nSelecciona una opción: ").strip()
        
        if choice == "1":
            local_path = input("Ruta del archivo local: ").strip()
            remote_name = input("Nombre en el sistema remoto: ").strip()
            client.upload_file(local_path, remote_name)
            
        elif choice == "2":
            remote_name = input("Nombre del archivo remoto: ").strip()
            local_path = input("Ruta de descarga local: ").strip()
            client.download_file(remote_name, local_path)
            
        elif choice == "3":
            client.list_files()
            
        elif choice == "4":
            print("👋 ¡Hasta luego!")
            break
            
        else:
            print("❌ Opción inválida")

if __name__ == '__main__':
    main()