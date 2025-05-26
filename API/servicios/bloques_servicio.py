# servicios/bloques_servicio.py
import os
import json
import hashlib
import random
from typing import List, Dict, Optional, Tuple
from modelos.bloque_info import BloqueInfo, DataNodeInfo
from config import NAMENODE_METADATA_DIR, BLOCK_SIZE, REPLICATION_FACTOR, DATANODE_HOST, DATANODE_PORTS
import logging
import requests
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BloquesServicio:
    def __init__(self):
        self.metadata_dir = NAMENODE_METADATA_DIR
        self.bloques_metadata = {}  # bloque_id -> BloqueInfo
        self.datanodes = {}  # node_id -> DataNodeInfo
        self.block_size = BLOCK_SIZE
        self.replication_factor = REPLICATION_FACTOR
        self._cargar_metadata()
        self._inicializar_datanodes()
        
    def _cargar_metadata(self):
        """Cargar metadatos de bloques desde el disco"""
        try:
            bloques_file = os.path.join(self.metadata_dir, 'bloques.json')
            if os.path.exists(bloques_file):
                with open(bloques_file, 'r') as f:
                    data = json.load(f)
                    for bloque_id, metadata in data.items():
                        self.bloques_metadata[bloque_id] = BloqueInfo.from_dict(metadata)
            
            datanodes_file = os.path.join(self.metadata_dir, 'datanodes.json')
            if os.path.exists(datanodes_file):
                with open(datanodes_file, 'r') as f:
                    data = json.load(f)
                    for node_id, metadata in data.items():
                        self.datanodes[node_id] = DataNodeInfo.from_dict(metadata)
                        
        except Exception as e:
            logger.error(f"Error cargando metadatos de bloques: {e}")
    
    def _guardar_metadata(self):
        """Guardar metadatos de bloques al disco"""
        try:
            bloques_file = os.path.join(self.metadata_dir, 'bloques.json')
            with open(bloques_file, 'w') as f:
                data = {bloque_id: metadata.to_dict() for bloque_id, metadata in self.bloques_metadata.items()}
                json.dump(data, f, indent=2)
            
            datanodes_file = os.path.join(self.metadata_dir, 'datanodes.json')
            with open(datanodes_file, 'w') as f:
                data = {node_id: metadata.to_dict() for node_id, metadata in self.datanodes.items()}
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error guardando metadatos de bloques: {e}")
    
    def _inicializar_datanodes(self):
        """Inicializar DataNodes por defecto si no existen"""
        if not self.datanodes:
            for puerto in DATANODE_PORTS:
                datanode = DataNodeInfo(DATANODE_HOST, puerto)
                datanode.espacio_total = 10 * 1024 * 1024 * 1024  # 10GB por defecto
                self.datanodes[datanode.node_id] = datanode
            self._guardar_metadata()
    
    def registrar_datanode(self, host: str, puerto: int, espacio_total: int = 0) -> DataNodeInfo:
        """Registrar un nuevo DataNode"""
        # Verificar si ya existe
        for datanode in self.datanodes.values():
            if datanode.host == host and datanode.puerto == puerto:
                datanode.actualizar_heartbeat()
                datanode.estado = "activo"
                self._guardar_metadata()
                return datanode
        
        # Crear nuevo DataNode
        datanode = DataNodeInfo(host, puerto)
        datanode.espacio_total = espacio_total or 10 * 1024 * 1024 * 1024  # 10GB por defecto
        self.datanodes[datanode.node_id] = datanode
        self._guardar_metadata()
        logger.info(f"DataNode registrado: {host}:{puerto}")
        return datanode
    
    def obtener_datanodes_activos(self) -> List[DataNodeInfo]:
        """Obtener lista de DataNodes activos"""
        return [dn for dn in self.datanodes.values() if dn.estado == "activo"]
    
    def seleccionar_datanodes_para_escritura(self, cantidad: int = None) -> List[DataNodeInfo]:
        """Seleccionar DataNodes para escritura basado en criterios de optimización"""
        if cantidad is None:
            cantidad = self.replication_factor
        
        datanodes_activos = self.obtener_datanodes_activos()
        if len(datanodes_activos) < cantidad:
            raise ValueError(f"No hay suficientes DataNodes activos. Necesarios: {cantidad}, Disponibles: {len(datanodes_activos)}")
        
        # Criterio de selección: menor uso de espacio y menos bloques
        datanodes_ordenados = sorted(datanodes_activos, 
                                   key=lambda dn: (dn.get_porcentaje_uso(), len(dn.bloques_almacenados)))
        
        return datanodes_ordenados[:cantidad]
    
    def crear_bloques_para_archivo(self, archivo_nombre: str, tamaño_archivo: int) -> List[BloqueInfo]:
        """Crear bloques para un archivo dado su tamaño"""
        num_bloques = (tamaño_archivo + self.block_size - 1) // self.block_size
        bloques = []
        
        for i in range(num_bloques):
            bloque = BloqueInfo(archivo_nombre=archivo_nombre, posicion=i)
            tamaño_bloque = min(self.block_size, tamaño_archivo - i * self.block_size)
            bloque.tamaño = tamaño_bloque
            
            # Seleccionar DataNodes para este bloque
            datanodes_seleccionados = self.seleccionar_datanodes_para_escritura()
            for datanode in datanodes_seleccionados:
                bloque.agregar_ubicacion(datanode.host, datanode.puerto)
                datanode.agregar_bloque(bloque.bloque_id, tamaño_bloque)
            
            self.bloques_metadata[bloque.bloque_id] = bloque
            bloques.append(bloque)
        
        self._guardar_metadata()
        logger.info(f"Creados {len(bloques)} bloques para archivo {archivo_nombre}")
        return bloques
    
    def obtener_bloque(self, bloque_id: str) -> Optional[BloqueInfo]:
        """Obtener información de un bloque"""
        return self.bloques_metadata.get(bloque_id)
    
    def obtener_bloques_archivo(self, archivo_nombre: str) -> List[BloqueInfo]:
        """Obtener todos los bloques de un archivo ordenados por posición"""
        bloques = [bloque for bloque in self.bloques_metadata.values() 
                  if bloque.archivo_nombre == archivo_nombre]
        return sorted(bloques, key=lambda b: b.posicion)
    
    def eliminar_bloques_archivo(self, archivo_nombre: str) -> bool:
        """Eliminar todos los bloques de un archivo"""
        bloques_a_eliminar = [bloque_id for bloque_id, bloque in self.bloques_metadata.items()
                             if bloque.archivo_nombre == archivo_nombre]
        
        for bloque_id in bloques_a_eliminar:
            self.eliminar_bloque(bloque_id)
        
        logger.info(f"Eliminados {len(bloques_a_eliminar)} bloques del archivo {archivo_nombre}")
        return len(bloques_a_eliminar) > 0
    
    def eliminar_bloque(self, bloque_id: str) -> bool:
        """Eliminar un bloque del sistema"""
        if bloque_id not in self.bloques_metadata:
            return False
        
        bloque = self.bloques_metadata[bloque_id]
        
        # Notificar a los DataNodes para eliminar el bloque
        for host, puerto in bloque.ubicaciones:
            try:
                response = requests.delete(f"http://{host}:{puerto}/bloques/{bloque_id}", timeout=5)
                if response.status_code == 200:
                    # Actualizar metadata del DataNode
                    for datanode in self.datanodes.values():
                        if datanode.host == host and datanode.puerto == puerto:
                            datanode.remover_bloque(bloque_id, bloque.tamaño)
                            break
            except Exception as e:
                logger.warning(f"Error eliminando bloque {bloque_id} del DataNode {host}:{puerto}: {e}")
        
        del self.bloques_metadata[bloque_id]
        self._guardar_metadata()
        return True
    
    def verificar_replicacion(self) -> List[str]:
        """Verificar y reportar bloques con replicación insuficiente"""
        bloques_problematicos = []
        
        for bloque_id, bloque in self.bloques_metadata.items():
            if not bloque.is_replicado_suficiente(self.replication_factor):
                bloques_problematicos.append(bloque_id)
                logger.warning(f"Bloque {bloque_id} tiene replicación insuficiente: {len(bloque.ubicaciones)}/{self.replication_factor}")
        
        return bloques_problematicos
    
    def reparar_replicacion(self, bloque_id: str) -> bool:
        """Reparar la replicación de un bloque"""
        if bloque_id not in self.bloques_metadata:
            return False
        
        bloque = self.bloques_metadata[bloque_id]
        ubicaciones_actuales = len(bloque.ubicaciones)
        
        if ubicaciones_actuales >= self.replication_factor:
            return True
        
        # Seleccionar DataNodes adicionales
        datanodes_disponibles = self.obtener_datanodes_activos()
        datanodes_ocupados = {(dn.host, dn.puerto) for dn in datanodes_disponibles 
                             if (dn.host, dn.puerto) in bloque.ubicaciones}
        datanodes_libres = [dn for dn in datanodes_disponibles 
                           if (dn.host, dn.puerto) not in datanodes_ocupados]
        
        replicas_necesarias = self.replication_factor - ubicaciones_actuales
        if len(datanodes_libres) < replicas_necesarias:
            logger.warning(f"No hay suficientes DataNodes disponibles para reparar bloque {bloque_id}")
            return False
        
        # Obtener el bloque desde el primer DataNode disponible
        if not bloque.ubicaciones:
            logger.error(f"Bloque {bloque_id} no tiene ubicaciones disponibles")
            return False
        
        source_host, source_port = bloque.ubicaciones[0]
        
        # Replicar a DataNodes adicionales
        datanodes_seleccionados = datanodes_libres[:replicas_necesarias]
        for datanode in datanodes_seleccionados:
            try:
                # Solicitar replicación del bloque
                replica_data = {
                    'source_host': source_host,
                    'source_port': source_port,
                    'bloque_id': bloque_id
                }
                response = requests.post(
                    f"http://{datanode.host}:{datanode.puerto}/bloques/replicate",
                    json=replica_data,
                    timeout=30
                )
                
                if response.status_code == 200:
                    bloque.agregar_ubicacion(datanode.host, datanode.puerto)
                    datanode.agregar_bloque(bloque_id, bloque.tamaño)
                    logger.info(f"Bloque {bloque_id} replicado a {datanode.host}:{datanode.puerto}")
                
            except Exception as e:
                logger.error(f"Error replicando bloque {bloque_id} a {datanode.host}:{datanode.puerto}: {e}")
        
        self._guardar_metadata()
        return len(bloque.ubicaciones) >= self.replication_factor
    
    def obtener_estadisticas(self) -> Dict:
        """Obtener estadísticas del sistema de bloques"""
        total_bloques = len(self.bloques_metadata)
        total_datanodes = len(self.datanodes)
        datanodes_activos = len(self.obtener_datanodes_activos())
        
        espacio_total = sum(dn.espacio_total for dn in self.datanodes.values())
        espacio_usado = sum(dn.espacio_usado for dn in self.datanodes.values())
        
        bloques_mal_replicados = len(self.verificar_replicacion())
        
        return {
            'total_bloques': total_bloques,
            'total_datanodes': total_datanodes,
            'datanodes_activos': datanodes_activos,
            'espacio_total_gb': espacio_total / (1024**3),
            'espacio_usado_gb': espacio_usado / (1024**3),
            'espacio_disponible_gb': (espacio_total - espacio_usado) / (1024**3),
            'porcentaje_uso': (espacio_usado / espacio_total * 100) if espacio_total > 0 else 0,
            'bloques_mal_replicados': bloques_mal_replicados,
            'factor_replicacion': self.replication_factor,
            'tamaño_bloque_mb': self.block_size / (1024**2)
        }
    
    def heartbeat_datanode(self, host: str, puerto: int, estado_info: Dict = None) -> bool:
        """Procesar heartbeat de un DataNode"""
        datanode = None
        for dn in self.datanodes.values():
            if dn.host == host and dn.puerto == puerto:
                datanode = dn
                break
        
        if not datanode:
            # Registrar nuevo DataNode
            datanode = self.registrar_datanode(host, puerto)
        
        datanode.actualizar_heartbeat()
        datanode.estado = "activo"
        
        if estado_info:
            datanode.espacio_usado = estado_info.get('espacio_usado', datanode.espacio_usado)
            datanode.espacio_total = estado_info.get('espacio_total', datanode.espacio_total)
            datanode.bloques_almacenados = estado_info.get('bloques', datanode.bloques_almacenados)
        
        self._guardar_metadata()
        return True
    
    def verificar_datanodes_inactivos(self, timeout_minutos: int = 5) -> List[str]:
        """Verificar DataNodes que no han enviado heartbeat recientemente"""
        datanodes_inactivos = []
        timeout_threshold = datetime.now() - timedelta(minutes=timeout_minutos)
        
        for node_id, datanode in self.datanodes.items():
            ultima_conexion = datetime.fromisoformat(datanode.ultima_conexion)
            if ultima_conexion < timeout_threshold and datanode.estado == "activo":
                datanode.estado = "inactivo"
                datanodes_inactivos.append(node_id)
                logger.warning(f"DataNode {datanode.host}:{datanode.puerto} marcado como inactivo")
        
        if datanodes_inactivos:
            self._guardar_metadata()
        
        return datanodes_inactivos

def subir_bloque(self, bloque_id: str, data: bytes, leader_uri: str) -> bool:
    """Envía un bloque al DataNode líder para su almacenamiento"""
    try:
        # Implementación usando requests (puedes cambiarlo a gRPC)
        response = requests.post(
            f"{leader_uri}/bloques/{bloque_id}",
            data=data,
            headers={'Content-Type': 'application/octet-stream'}
        )
        
        if response.status_code != 201:
            logger.error(f"DataNode respondió con error: {response.text}")
            return False
            
        # El líder debe replicar a los followers (eso lo maneja el DataNode)
        return True
        
    except Exception as e:
        logger.error(f"Error subiendo bloque {bloque_id} a {leader_uri}: {str(e)}")
        return False