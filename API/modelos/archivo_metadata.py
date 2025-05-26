# modelos/archivo_metadata.py
from datetime import datetime
from typing import List, Dict, Optional
import json

class ArchivoMetadata:
    def __init__(self, nombre: str, ruta: str, usuario: str = "default"):
        self.nombre = nombre
        self.ruta = ruta
        self.usuario = usuario
        self.tamaño_total = 0
        self.bloques: List[str] = []  # IDs de los bloques
        self.fecha_creacion = datetime.now().isoformat()
        self.fecha_modificacion = datetime.now().isoformat()
        self.permisos = "755"
        self.checksum = ""
        
    def agregar_bloque(self, bloque_id: str):
        """Agregar un bloque al archivo"""
        if bloque_id not in self.bloques:
            self.bloques.append(bloque_id)
            self.fecha_modificacion = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        """Convertir a diccionario para serialización"""
        return {
            'nombre': self.nombre,
            'ruta': self.ruta,
            'usuario': self.usuario,
            'tamaño_total': self.tamaño_total,
            'bloques': self.bloques,
            'fecha_creacion': self.fecha_creacion,
            'fecha_modificacion': self.fecha_modificacion,
            'permisos': self.permisos,
            'checksum': self.checksum
        }
    
    def to_json(self) -> str:
        """Convertir a JSON"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ArchivoMetadata':
        """Crear instancia desde diccionario"""
        archivo = cls(data['nombre'], data['ruta'], data.get('usuario', 'default'))
        archivo.tamaño_total = data.get('tamaño_total', 0)
        archivo.bloques = data.get('bloques', [])
        archivo.fecha_creacion = data.get('fecha_creacion', datetime.now().isoformat())
        archivo.fecha_modificacion = data.get('fecha_modificacion', datetime.now().isoformat())
        archivo.permisos = data.get('permisos', '755')
        archivo.checksum = data.get('checksum', '')
        return archivo
    
    @classmethod
    def from_json(cls, json_str: str) -> 'ArchivoMetadata':
        """Crear instancia desde JSON"""
        return cls.from_dict(json.loads(json_str))

class DirectorioMetadata:
    def __init__(self, nombre: str, ruta: str, usuario: str = "default"):
        self.nombre = nombre
        self.ruta = ruta
        self.usuario = usuario
        self.archivos: List[str] = []  # Nombres de archivos
        self.subdirectorios: List[str] = []  # Nombres de subdirectorios
        self.fecha_creacion = datetime.now().isoformat()
        self.fecha_modificacion = datetime.now().isoformat()
        self.permisos = "755"
    
    def agregar_archivo(self, nombre_archivo: str):
        """Agregar un archivo al directorio"""
        if nombre_archivo not in self.archivos:
            self.archivos.append(nombre_archivo)
            self.fecha_modificacion = datetime.now().isoformat()
    
    def remover_archivo(self, nombre_archivo: str):
        """Remover un archivo del directorio"""
        if nombre_archivo in self.archivos:
            self.archivos.remove(nombre_archivo)
            self.fecha_modificacion = datetime.now().isoformat()
    
    def agregar_subdirectorio(self, nombre_dir: str):
        """Agregar un subdirectorio"""
        if nombre_dir not in self.subdirectorios:
            self.subdirectorios.append(nombre_dir)
            self.fecha_modificacion = datetime.now().isoformat()
    
    def remover_subdirectorio(self, nombre_dir: str):
        """Remover un subdirectorio"""
        if nombre_dir in self.subdirectorios:
            self.subdirectorios.remove(nombre_dir)
            self.fecha_modificacion = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        """Convertir a diccionario para serialización"""
        return {
            'nombre': self.nombre,
            'ruta': self.ruta,
            'usuario': self.usuario,
            'archivos': self.archivos,
            'subdirectorios': self.subdirectorios,
            'fecha_creacion': self.fecha_creacion,
            'fecha_modificacion': self.fecha_modificacion,
            'permisos': self.permisos
        }
    
    def to_json(self) -> str:
        """Convertir a JSON"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DirectorioMetadata':
        """Crear instancia desde diccionario"""
        directorio = cls(data['nombre'], data['ruta'], data.get('usuario', 'default'))
        directorio.archivos = data.get('archivos', [])
        directorio.subdirectorios = data.get('subdirectorios', [])
        directorio.fecha_creacion = data.get('fecha_creacion', datetime.now().isoformat())
        directorio.fecha_modificacion = data.get('fecha_modificacion', datetime.now().isoformat())
        directorio.permisos = data.get('permisos', '755')
        return directorio
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DirectorioMetadata':
        """Crear instancia desde JSON"""
        return cls.from_dict(json.loads(json_str))