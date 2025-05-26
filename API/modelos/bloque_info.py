# modelos/bloque_info.py
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import json
import uuid

class BloqueInfo:
    def __init__(self, bloque_id: str = None, archivo_nombre: str = "", posicion: int = 0):
        self.bloque_id = bloque_id or str(uuid.uuid4())
        self.archivo_nombre = archivo_nombre
        self.posicion = posicion  # Posición del bloque en el archivo
        self.tamaño = 0
        self.checksum = ""
        self.ubicaciones: List[Tuple[str, int]] = []  # (host, puerto) de DataNodes
        self.fecha_creacion = datetime.now().isoformat()
        self.fecha_modificacion = datetime.now().isoformat()
        self.estado = "activo"  # activo, corrupto, eliminado
        
    def agregar_ubicacion(self, host: str, puerto: int):
        """Agregar una ubicación (DataNode) donde está almacenado el bloque"""
        ubicacion = (host, puerto)
        if ubicacion not in self.ubicaciones:
            self.ubicaciones.append(ubicacion)
            self.fecha_modificacion = datetime.now().isoformat()
    
    def remover_ubicacion(self, host: str, puerto: int):
        """Remover una ubicación donde estaba almacenado el bloque"""
        ubicacion = (host, puerto)
        if ubicacion in self.ubicaciones:
            self.ubicaciones.remove(ubicacion)
            self.fecha_modificacion = datetime.now().isoformat()
    
    def get_ubicaciones_uri(self) -> List[str]:
        """Obtener las URIs de las ubicaciones del bloque"""
        return [f"http://{host}:{puerto}" for host, puerto in self.ubicaciones]
    
    def get_leader_uri(self) -> Optional[str]:
        """Obtener la URI del DataNode leader (primera ubicación)"""
        if self.ubicaciones:
            host, puerto = self.ubicaciones[0]
            return f"http://{host}:{puerto}"
        return None
    
    def get_followers_uri(self) -> List[str]:
        """Obtener las URIs de los DataNodes followers"""
        return [f"http://{host}:{puerto}" for host, puerto in self.ubicaciones[1:]]
    
    def is_replicado_suficiente(self, factor_replicacion: int = 2) -> bool:
        """Verificar si el bloque tiene suficiente replicación"""
        return len(self.ubicaciones) >= factor_replicacion
    
    def to_dict(self) -> Dict:
        """Convertir a diccionario para serialización"""
        return {
            'bloque_id': self.bloque_id,
            'archivo_nombre': self.archivo_nombre,
            'posicion': self.posicion,
            'tamaño': self.tamaño,
            'checksum': self.checksum,
            'ubicaciones': self.ubicaciones,
            'fecha_creacion': self.fecha_creacion,
            'fecha_modificacion': self.fecha_modificacion,
            'estado': self.estado
        }
    
    def to_json(self) -> str:
        """Convertir a JSON"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'BloqueInfo':
        """Crear instancia desde diccionario"""
        bloque = cls(
            bloque_id=data.get('bloque_id'),
            archivo_nombre=data.get('archivo_nombre', ''),
            posicion=data.get('posicion', 0)
        )
        bloque.tamaño = data.get('tamaño', 0)
        bloque.checksum = data.get('checksum', '')
        bloque.ubicaciones = [tuple(loc) for loc in data.get('ubicaciones', [])]
        bloque.fecha_creacion = data.get('fecha_creacion', datetime.now().isoformat())
        bloque.fecha_modificacion = data.get('fecha_modificacion', datetime.now().isoformat())
        bloque.estado = data.get('estado', 'activo')
        return bloque
    
    @classmethod
    def from_json(cls, json_str: str) -> 'BloqueInfo':
        """Crear instancia desde JSON"""
        return cls.from_dict(json.loads(json_str))

class DataNodeInfo:
    def __init__(self, host: str, puerto: int, node_id: str = None):
        self.node_id = node_id or str(uuid.uuid4())
        self.host = host
        self.puerto = puerto
        self.estado = "activo"  # activo, inactivo, mantenimiento
        self.espacio_total = 0
        self.espacio_usado = 0
        self.bloques_almacenados: List[str] = []  # IDs de bloques
        self.ultima_conexion = datetime.now().isoformat()
        self.fecha_registro = datetime.now().isoformat()
        
    def get_uri(self) -> str:
        """Obtener la URI del DataNode"""
        return f"http://{self.host}:{self.puerto}"
    
    def agregar_bloque(self, bloque_id: str, tamaño: int = 0):
        """Agregar un bloque al DataNode"""
        if bloque_id not in self.bloques_almacenados:
            self.bloques_almacenados.append(bloque_id)
            self.espacio_usado += tamaño
            self.ultima_conexion = datetime.now().isoformat()
    
    def remover_bloque(self, bloque_id: str, tamaño: int = 0):
        """Remover un bloque del DataNode"""
        if bloque_id in self.bloques_almacenados:
            self.bloques_almacenados.remove(bloque_id)
            self.espacio_usado = max(0, self.espacio_usado - tamaño)
            self.ultima_conexion = datetime.now().isoformat()
    
    def get_espacio_disponible(self) -> int:
        """Obtener el espacio disponible"""
        return max(0, self.espacio_total - self.espacio_usado)
    
    def get_porcentaje_uso(self) -> float:
        """Obtener el porcentaje de uso del espacio"""
        if self.espacio_total == 0:
            return 0.0
        return (self.espacio_usado / self.espacio_total) * 100
    
    def actualizar_heartbeat(self):
        """Actualizar el timestamp de la última conexión"""
        self.ultima_conexion = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        """Convertir a diccionario para serialización"""
        return {
            'node_id': self.node_id,
            'host': self.host,
            'puerto': self.puerto,
            'estado': self.estado,
            'espacio_total': self.espacio_total,
            'espacio_usado': self.espacio_usado,
            'bloques_almacenados': self.bloques_almacenados,
            'ultima_conexion': self.ultima_conexion,
            'fecha_registro': self.fecha_registro
        }
    
    def to_json(self) -> str:
        """Convertir a JSON"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DataNodeInfo':
        """Crear instancia desde diccionario"""
        datanode = cls(
            host=data['host'],
            puerto=data['puerto'],
            node_id=data.get('node_id')
        )
        datanode.estado = data.get('estado', 'activo')
        datanode.espacio_total = data.get('espacio_total', 0)
        datanode.espacio_usado = data.get('espacio_usado', 0)
        datanode.bloques_almacenados = data.get('bloques_almacenados', [])
        datanode.ultima_conexion = data.get('ultima_conexion', datetime.now().isoformat())
        datanode.fecha_registro = data.get('fecha_registro', datetime.now().isoformat())
        return datanode
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DataNodeInfo':
        """Crear instancia desde JSON"""
        return cls.from_dict(json.loads(json_str))