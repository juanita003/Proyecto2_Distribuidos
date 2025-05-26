from flask import Flask, request, jsonify
import hashlib
import uuid
import time
from typing import Dict, List, Optional
import threading
import random

class BloquesControlador:
    def __init__(self):
        # Estructura para almacenar información de archivos y bloques
        self.archivos = {}  # {archivo_id: {nombre, bloques, tamaño, fecha_creacion}}
        self.bloques = {}   # {bloque_id: {archivo_id, indice, tamaño, datanodes, checksum}}
        self.datanodes = {} # {datanode_id: {host, puerto, estado, bloques, espacio_libre}}
        self.directorio = {} # Estructura de directorios {path: {tipo, archivos, subdirectorios}}
        self.usuarios = {}  # {usuario: {password, archivos}} - Autenticación básica opcional
        self.tamaño_bloque = 64 * 1024 * 1024  # 64MB por defecto
        self.factor_replicacion = 2
        self.lock = threading.RLock()
        
        # Inicializar directorio raíz
        self.directorio['/'] = {'tipo': 'directorio', 'archivos': [], 'subdirectorios': []}
    
    def registrar_datanode(self, datanode_info: Dict) -> Dict:
        """Registra un nuevo DataNode en el sistema"""
        with self.lock:
            datanode_id = f"{datanode_info['host']}:{datanode_info['puerto']}"
            self.datanodes[datanode_id] = {
                'host': datanode_info['host'],
                'puerto': datanode_info['puerto'],
                'estado': 'activo',
                'bloques': [],
                'espacio_libre': datanode_info.get('espacio_libre', 1024**3),  # 1GB por defecto
                'ultima_comunicacion': time.time()
            }
            return {'status': 'success', 'datanode_id': datanode_id}
    
    def obtener_datanodes_disponibles(self, excluir: List[str] = None) -> List[str]:
        """Obtiene lista de DataNodes disponibles, excluyendo los especificados"""
        if excluir is None:
            excluir = []
        
        disponibles = []
        for datanode_id, info in self.datanodes.items():
            if (info['estado'] == 'activo' and 
                datanode_id not in excluir and
                info['espacio_libre'] > self.tamaño_bloque):
                disponibles.append(datanode_id)
        
        return disponibles
    
    def seleccionar_datanodes_para_bloque(self, excluir: List[str] = None) -> List[str]:
        """Selecciona DataNodes óptimos para almacenar un bloque"""
        disponibles = self.obtener_datanodes_disponibles(excluir)
        
        if len(disponibles) < self.factor_replicacion:
            raise Exception(f"No hay suficientes DataNodes disponibles. Requeridos: {self.factor_replicacion}, Disponibles: {len(disponibles)}")
        
        # Algoritmo simple: seleccionar los que tienen más espacio libre
        disponibles.sort(key=lambda x: self.datanodes[x]['espacio_libre'], reverse=True)
        return disponibles[:self.factor_replicacion]
    
    def crear_archivo(self, nombre_archivo: str, tamaño: int, usuario: str = None, directorio: str = '/') -> Dict:
        """Crea un nuevo archivo en el sistema y planifica la distribución de bloques"""
        with self.lock:
            # Validar directorio
            if directorio not in self.directorio:
                return {'status': 'error', 'mensaje': 'Directorio no existe'}
            
            archivo_id = str(uuid.uuid4())
            num_bloques = (tamaño + self.tamaño_bloque - 1) // self.tamaño_bloque
            
            bloques_info = []
            
            # Crear información de bloques
            for i in range(num_bloques):
                bloque_id = f"{archivo_id}_bloque_{i}"
                tamaño_bloque_actual = min(self.tamaño_bloque, tamaño - i * self.tamaño_bloque)
                
                try:
                    datanodes_seleccionados = self.seleccionar_datanodes_para_bloque()
                    
                    self.bloques[bloque_id] = {
                        'archivo_id': archivo_id,
                        'indice': i,
                        'tamaño': tamaño_bloque_actual,
                        'datanodes': datanodes_seleccionados,
                        'checksum': None,
                        'estado': 'pendiente'
                    }
                    
                    # Reservar espacio en DataNodes
                    for datanode_id in datanodes_seleccionados:
                        self.datanodes[datanode_id]['bloques'].append(bloque_id)
                        self.datanodes[datanode_id]['espacio_libre'] -= tamaño_bloque_actual
                    
                    bloques_info.append({
                        'bloque_id': bloque_id,
                        'indice': i,
                        'tamaño': tamaño_bloque_actual,
                        'datanodes': datanodes_seleccionados
                    })
                    
                except Exception as e:
                    return {'status': 'error', 'mensaje': str(e)}
            
            # Registrar archivo
            ruta_completa = f"{directorio.rstrip('/')}/{nombre_archivo}"
            self.archivos[archivo_id] = {
                'nombre': nombre_archivo,
                'ruta': ruta_completa,
                'tamaño': tamaño,
                'bloques': [b['bloque_id'] for b in bloques_info],
                'fecha_creacion': time.time(),
                'usuario': usuario,
                'estado': 'creando'
            }
            
            # Agregar al directorio
            self.directorio[directorio]['archivos'].append(archivo_id)
            
            return {
                'status': 'success',
                'archivo_id': archivo_id,
                'bloques': bloques_info,
                'tamaño_bloque': self.tamaño_bloque
            }
    
    def obtener_bloques_archivo(self, archivo_id: str) -> Dict:
        """Obtiene información de bloques de un archivo para lectura"""
        with self.lock:
            if archivo_id not in self.archivos:
                return {'status': 'error', 'mensaje': 'Archivo no encontrado'}
            
            archivo = self.archivos[archivo_id]
            bloques_info = []
            
            for bloque_id in archivo['bloques']:
                if bloque_id in self.bloques:
                    bloque = self.bloques[bloque_id]
                    # Filtrar DataNodes activos
                    datanodes_activos = [dn for dn in bloque['datanodes'] 
                                       if dn in self.datanodes and self.datanodes[dn]['estado'] == 'activo']
                    
                    bloques_info.append({
                        'bloque_id': bloque_id,
                        'indice': bloque['indice'],
                        'tamaño': bloque['tamaño'],
                        'datanodes': datanodes_activos
                    })
            
            return {
                'status': 'success',
                'archivo': {
                    'nombre': archivo['nombre'],
                    'tamaño': archivo['tamaño'],
                    'bloques': bloques_info
                }
            }
    
    def confirmar_bloque_escrito(self, bloque_id: str, datanode_id: str, checksum: str) -> Dict:
        """Confirma que un bloque ha sido escrito correctamente en un DataNode"""
        with self.lock:
            if bloque_id not in self.bloques:
                return {'status': 'error', 'mensaje': 'Bloque no encontrado'}
            
            bloque = self.bloques[bloque_id]
            if datanode_id not in bloque['datanodes']:
                return {'status': 'error', 'mensaje': 'DataNode no autorizado para este bloque'}
            
            # Actualizar información del bloque
            bloque['checksum'] = checksum
            bloque['estado'] = 'confirmado'
            
            # Verificar si todos los bloques del archivo están confirmados
            archivo_id = bloque['archivo_id']
            archivo = self.archivos[archivo_id]
            
            todos_confirmados = all(
                self.bloques[bid]['estado'] == 'confirmado' 
                for bid in archivo['bloques']
            )
            
            if todos_confirmados:
                archivo['estado'] = 'completado'
            
            return {'status': 'success'}
    
    def listar_directorio(self, ruta: str = '/', usuario: str = None) -> Dict:
        """Lista el contenido de un directorio"""
        with self.lock:
            if ruta not in self.directorio:
                return {'status': 'error', 'mensaje': 'Directorio no encontrado'}
            
            directorio_info = self.directorio[ruta]
            
            # Obtener información de archivos
            archivos = []
            for archivo_id in directorio_info['archivos']:
                if archivo_id in self.archivos:
                    archivo = self.archivos[archivo_id]
                    # Filtrar por usuario si se especifica
                    if usuario is None or archivo.get('usuario') == usuario:
                        archivos.append({
                            'nombre': archivo['nombre'],
                            'tamaño': archivo['tamaño'],
                            'fecha_creacion': archivo['fecha_creacion'],
                            'estado': archivo['estado']
                        })
            
            return {
                'status': 'success',
                'directorio': ruta,
                'archivos': archivos,
                'subdirectorios': directorio_info['subdirectorios']
            }
    
    def crear_directorio(self, ruta: str) -> Dict:
        """Crea un nuevo directorio"""
        with self.lock:
            if ruta in self.directorio:
                return {'status': 'error', 'mensaje': 'Directorio ya existe'}
            
            # Verificar que el directorio padre existe
            ruta_padre = '/'.join(ruta.rstrip('/').split('/')[:-1]) or '/'
            if ruta_padre not in self.directorio:
                return {'status': 'error', 'mensaje': 'Directorio padre no existe'}
            
            # Crear directorio
            self.directorio[ruta] = {
                'tipo': 'directorio',
                'archivos': [],
                'subdirectorios': []
            }
            
            # Agregar a directorio padre
            nombre_directorio = ruta.rstrip('/').split('/')[-1]
            self.directorio[ruta_padre]['subdirectorios'].append(nombre_directorio)
            
            return {'status': 'success'}
    
    def eliminar_archivo(self, nombre_archivo: str, directorio: str = '/', usuario: str = None) -> Dict:
        """Elimina un archivo del sistema"""
        with self.lock:
            # Buscar el archivo
            archivo_id = None
            for aid in self.directorio[directorio]['archivos']:
                if (aid in self.archivos and 
                    self.archivos[aid]['nombre'] == nombre_archivo):
                    if usuario is None or self.archivos[aid].get('usuario') == usuario:
                        archivo_id = aid
                        break
            
            if archivo_id is None:
                return {'status': 'error', 'mensaje': 'Archivo no encontrado'}
            
            archivo = self.archivos[archivo_id]
            
            # Marcar bloques para eliminación y liberar espacio
            for bloque_id in archivo['bloques']:
                if bloque_id in self.bloques:
                    bloque = self.bloques[bloque_id]
                    for datanode_id in bloque['datanodes']:
                        if datanode_id in self.datanodes:
                            self.datanodes[datanode_id]['espacio_libre'] += bloque['tamaño']
                            if bloque_id in self.datanodes[datanode_id]['bloques']:
                                self.datanodes[datanode_id]['bloques'].remove(bloque_id)
                    
                    del self.bloques[bloque_id]
            
            # Eliminar archivo del directorio y del sistema
            self.directorio[directorio]['archivos'].remove(archivo_id)
            del self.archivos[archivo_id]
            
            return {'status': 'success'}
    
    def obtener_estado_sistema(self) -> Dict:
        """Obtiene el estado general del sistema"""
        with self.lock:
            datanodes_estado = {}
            for dn_id, dn_info in self.datanodes.items():
                datanodes_estado[dn_id] = {
                    'estado': dn_info['estado'],
                    'bloques': len(dn_info['bloques']),
                    'espacio_libre': dn_info['espacio_libre']
                }
            
            return {
                'status': 'success',
                'sistema': {
                    'archivos_totales': len(self.archivos),
                    'bloques_totales': len(self.bloques),
                    'datanodes': datanodes_estado,
                    'tamaño_bloque': self.tamaño_bloque,
                    'factor_replicacion': self.factor_replicacion
                }
            }