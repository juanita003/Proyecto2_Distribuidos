# servicios/archivos_servicio.py
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Ruta base del proyecto
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'namenode_data')  # Carpeta para almacenar metadatos
import json
import hashlib
import shutil
from typing import List, Dict, Optional, Tuple
from modelos.archivo_metadata import ArchivoMetadata, DirectorioMetadata
from modelos.bloque_info import BloqueInfo, DataNodeInfo
from config import NAMENODE_METADATA_DIR, BLOCK_SIZE, REPLICATION_FACTOR
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ArchivosServicio:
    def __init__(self):
        self.metadata_dir = NAMENODE_METADATA_DIR
        self.archivos_metadata = {}  # archivo_path -> ArchivoMetadata
        self.directorios_metadata = {}  # directorio_path -> DirectorioMetadata
        self._cargar_metadata()
        
    def _cargar_metadata(self):
        """Cargar metadatos desde el disco"""
        try:
            archivos_file = os.path.join(self.metadata_dir, 'archivos.json')
            if os.path.exists(archivos_file):
                with open(archivos_file, 'r') as f:
                    data = json.load(f)
                    for path, metadata in data.items():
                        self.archivos_metadata[path] = ArchivoMetadata.from_dict(metadata)
            
            directorios_file = os.path.join(self.metadata_dir, 'directorios.json')
            if os.path.exists(directorios_file):
                with open(directorios_file, 'r') as f:
                    data = json.load(f)
                    for path, metadata in data.items():
                        self.directorios_metadata[path] = DirectorioMetadata.from_dict(metadata)
            
            # Crear directorio raíz si no existe
            if '/' not in self.directorios_metadata:
                self.crear_directorio('/', 'root', 'default')
                
        except Exception as e:
            logger.error(f"Error cargando metadatos: {e}")
    
    def _guardar_metadata(self):
        """Guardar metadatos al disco"""
        try:
            # Guardar archivos
            archivos_file = os.path.join(self.metadata_dir, 'archivos.json')
            with open(archivos_file, 'w') as f:
                data = {path: metadata.to_dict() for path, metadata in self.archivos_metadata.items()}
                json.dump(data, f, indent=2)
            
            # Guardar directorios
            directorios_file = os.path.join(self.metadata_dir, 'directorios.json')
            with open(directorios_file, 'w') as f:
                data = {path: metadata.to_dict() for path, metadata in self.directorios_metadata.items()}
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error guardando metadatos: {e}")
    
    def crear_archivo(self, ruta: str, usuario: str = "default") -> ArchivoMetadata:
        """Crear un nuevo archivo"""
        if ruta in self.archivos_metadata:
            raise ValueError(f"El archivo {ruta} ya existe")
        
        # Verificar que el directorio padre existe
        directorio_padre = os.path.dirname(ruta) or '/'
        if directorio_padre not in self.directorios_metadata:
            raise ValueError(f"El directorio padre {directorio_padre} no existe")
        
        nombre = os.path.basename(ruta)
        archivo = ArchivoMetadata(nombre, ruta, usuario)
        self.archivos_metadata[ruta] = archivo
        
        # Agregar al directorio padre
        self.directorios_metadata[directorio_padre].agregar_archivo(nombre)
        
        self._guardar_metadata()
        logger.info(f"Archivo creado: {ruta}")
        return archivo
    
    def obtener_archivo(self, ruta: str) -> Optional[ArchivoMetadata]:
        """Obtener metadatos de un archivo"""
        return self.archivos_metadata.get(ruta)
    
    def eliminar_archivo(self, ruta: str, usuario: str = "default") -> bool:
        """Eliminar un archivo"""
        if ruta not in self.archivos_metadata:
            return False
        
        archivo = self.archivos_metadata[ruta]
        if archivo.usuario != usuario and usuario != "admin":
            raise PermissionError("No tienes permisos para eliminar este archivo")
        
        # Remover del directorio padre
        directorio_padre = os.path.dirname(ruta) or '/'
        if directorio_padre in self.directorios_metadata:
            self.directorios_metadata[directorio_padre].remover_archivo(archivo.nombre)
        
        del self.archivos_metadata[ruta]
        self._guardar_metadata()
        logger.info(f"Archivo eliminado: {ruta}")
        return True
    
    def crear_directorio(self, ruta: str, nombre: str, usuario: str = "default") -> DirectorioMetadata:
        """Crear un nuevo directorio"""
        ruta_completa = os.path.join(ruta, nombre).replace('\\', '/') if ruta != '/' else f"/{nombre}"
        
        if ruta_completa in self.directorios_metadata:
            raise ValueError(f"El directorio {ruta_completa} ya existe")
        
        # Verificar que el directorio padre existe (excepto para el raíz)
        if ruta != '/' and ruta not in self.directorios_metadata:
            raise ValueError(f"El directorio padre {ruta} no existe")
        
        directorio = DirectorioMetadata(nombre, ruta_completa, usuario)
        self.directorios_metadata[ruta_completa] = directorio
        
        # Agregar al directorio padre (excepto para el raíz)
        if ruta != ruta_completa and ruta in self.directorios_metadata:
            self.directorios_metadata[ruta].agregar_subdirectorio(nombre)
        
        self._guardar_metadata()
        logger.info(f"Directorio creado: {ruta_completa}")
        return directorio
    
    def obtener_directorio(self, ruta: str) -> Optional[DirectorioMetadata]:
        """Obtener metadatos de un directorio"""
        return self.directorios_metadata.get(ruta)
    
    def eliminar_directorio(self, ruta: str, usuario: str = "default", recursivo: bool = False) -> bool:
        """Eliminar un directorio"""
        if ruta not in self.directorios_metadata:
            return False
        
        if ruta == '/':
            raise ValueError("No se puede eliminar el directorio raíz")
        
        directorio = self.directorios_metadata[ruta]
        if directorio.usuario != usuario and usuario != "admin":
            raise PermissionError("No tienes permisos para eliminar este directorio")
        
        # Verificar si está vacío o si se permite eliminación recursiva
        if not recursivo and (directorio.archivos or directorio.subdirectorios):
            raise ValueError("El directorio no está vacío. Use eliminación recursiva.")
        
        if recursivo:
            # Eliminar archivos del directorio
            for archivo_nombre in directorio.archivos.copy():
                archivo_ruta = os.path.join(ruta, archivo_nombre).replace('\\', '/')
                self.eliminar_archivo(archivo_ruta, usuario)
            
            # Eliminar subdirectorios recursivamente
            for subdir_nombre in directorio.subdirectorios.copy():
                subdir_ruta = os.path.join(ruta, subdir_nombre).replace('\\', '/')
                self.eliminar_directorio(subdir_ruta, usuario, True)
        
        # Remover del directorio padre
        directorio_padre = os.path.dirname(ruta) or '/'
        if directorio_padre in self.directorios_metadata:
            self.directorios_metadata[directorio_padre].remover_subdirectorio(directorio.nombre)
        
        del self.directorios_metadata[ruta]
        self._guardar_metadata()
        logger.info(f"Directorio eliminado: {ruta}")
        return True
    
    def listar_directorio(self, ruta: str, usuario: str = "default") -> Dict:
        """Listar contenido de un directorio"""
        if ruta not in self.directorios_metadata:
            raise ValueError(f"El directorio {ruta} no existe")
        
        directorio = self.directorios_metadata[ruta]
        
        archivos = []
        for archivo_nombre in directorio.archivos:
            archivo_ruta = os.path.join(ruta, archivo_nombre).replace('\\', '/')
            archivo_metadata = self.archivos_metadata.get(archivo_ruta)
            if archivo_metadata and (archivo_metadata.usuario == usuario or usuario == "admin"):
                archivos.append({
                    'nombre': archivo_metadata.nombre,
                    'tipo': 'archivo',
                    'tamaño': archivo_metadata.tamaño_total,
                    'fecha_modificacion': archivo_metadata.fecha_modificacion,
                    'permisos': archivo_metadata.permisos,
                    'usuario': archivo_metadata.usuario
                })
        
        subdirectorios = []
        for subdir_nombre in directorio.subdirectorios:
            subdir_ruta = os.path.join(ruta, subdir_nombre).replace('\\', '/')
            subdir_metadata = self.directorios_metadata.get(subdir_ruta)
            if subdir_metadata and (subdir_metadata.usuario == usuario or usuario == "admin"):
                subdirectorios.append({
                    'nombre': subdir_metadata.nombre,
                    'tipo': 'directorio',
                    'fecha_modificacion': subdir_metadata.fecha_modificacion,
                    'permisos': subdir_metadata.permisos,
                    'usuario': subdir_metadata.usuario
                })
        
        return {
            'ruta': ruta,
            'archivos': archivos,
            'directorios': subdirectorios,
            'total_archivos': len(archivos),
            'total_directorios': len(subdirectorios)
        }
    
    def obtener_archivos_usuario(self, usuario: str) -> List[Dict]:
        """Obtener todos los archivos de un usuario"""
        archivos_usuario = []
        for ruta, archivo in self.archivos_metadata.items():
            if archivo.usuario == usuario or usuario == "admin":
                archivos_usuario.append({
                    'nombre': archivo.nombre,
                    'ruta': archivo.ruta,
                    'tamaño': archivo.tamaño_total,
                    'bloques': len(archivo.bloques),
                    'fecha_creacion': archivo.fecha_creacion,
                    'fecha_modificacion': archivo.fecha_modificacion
                })
        return archivos_usuario
    
    def calcular_checksum(self, data: bytes) -> str:
        """Calcular checksum MD5 de los datos"""
        return hashlib.md5(data).hexdigest()
    
    def validar_ruta(self, ruta: str) -> str:
        """Validar y normalizar una ruta"""
        if not ruta.startswith('/'):
            ruta = '/' + ruta
        return ruta.replace('\\', '/').replace('//', '/')
    
    def mover_archivo(self, ruta_origen: str, ruta_destino: str, usuario: str = "default") -> bool:
        """Mover un archivo de una ubicación a otra"""
        if ruta_origen not in self.archivos_metadata:
            raise ValueError(f"El archivo {ruta_origen} no existe")
        
        archivo = self.archivos_metadata[ruta_origen]
        if archivo.usuario != usuario and usuario != "admin":
            raise PermissionError("No tienes permisos para mover este archivo")
        
        # Verificar que el directorio destino existe
        directorio_destino = os.path.dirname(ruta_destino) or '/'
        if directorio_destino not in self.directorios_metadata:
            raise ValueError(f"El directorio destino {directorio_destino} no existe")
        
        # Actualizar metadatos
        nuevo_nombre = os.path.basename(ruta_destino)
        archivo.nombre = nuevo_nombre
        archivo.ruta = ruta_destino
        
        # Actualizar directorios
        directorio_origen = os.path.dirname(ruta_origen) or '/'
        if directorio_origen in self.directorios_metadata:
            self.directorios_metadata[directorio_origen].remover_archivo(os.path.basename(ruta_origen))
        self.directorios_metadata[directorio_destino].agregar_archivo(nuevo_nombre)
        
        self._guardar_metadata()
        logger.info(f"Archivo movido: {ruta_origen} -> {ruta_destino}")
        return True