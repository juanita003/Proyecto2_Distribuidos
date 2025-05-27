from flask import Blueprint, request, jsonify, g
from servicios.archivos_servicio import ArchivosServicio
from servicios.bloques_servicio import BloquesServicio
import logging
from functools import wraps
import os

logger = logging.getLogger(__name__)

# Crear Blueprint con prefijo
archivos_bp = Blueprint('archivos', __name__, url_prefix='/api/archivos')

# Configuración de usuarios válidos
USUARIOS_VALIDOS = {
    'admin': 'admin123',
    'user1': 'pass123',
    'user2': 'pass456'
}

def autenticar(f):
    """Decorador para autenticación básica HTTP"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth = request.authorization
        if not auth or USUARIOS_VALIDOS.get(auth.username) != auth.password:
            return jsonify({
                'success': False, 
                'error': 'Autenticación requerida'
            }), 401
        g.usuario = auth.username
        return f(*args, **kwargs)
    return decorated_function

class ArchivosControlador:
    _instance = None
    
    def __new__(cls):
        """Implementación del patrón Singleton"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._inicializar_servicios()
        return cls._instance
    
    def _inicializar_servicios(self):
        """Inicializa los servicios necesarios"""
        self.archivos_servicio = ArchivosServicio()
        self.bloques_servicio = BloquesServicio()

    @staticmethod
    @archivos_bp.route('/upload', methods=['POST'])
    @autenticar
    def upload_file():
        """Endpoint para subir archivos al sistema distribuido"""
        try:
            # Verificar que se haya enviado un archivo
            if 'file' not in request.files:
                return jsonify({
                    'success': False,
                    'error': 'No se encontró el archivo en la solicitud'
                }), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({
                    'success': False,
                    'error': 'Nombre de archivo no válido'
                }), 400

            # Obtener ruta destino y validarla
            ruta = request.form.get('ruta', file.filename)
            ruta = ArchivosControlador().archivos_servicio.validar_ruta(ruta)

            # Determinar tamaño del archivo
            file.seek(0, 2)  # Ir al final del archivo
            tamaño = file.tell()
            file.seek(0)  # Volver al inicio

            # Crear metadatos del archivo
            archivo = ArchivosControlador().archivos_servicio.crear_archivo(ruta, g.usuario)
            
            # Crear bloques para el archivo
            bloques = ArchivosControlador().bloques_servicio.crear_bloques_para_archivo(archivo.nombre, tamaño)
            archivo.bloques = [b.bloque_id for b in bloques]
            archivo.tamaño_total = tamaño

            # Subir cada bloque al sistema distribuido
            block_size = ArchivosControlador().bloques_servicio.block_size
            for bloque in bloques:
                file.seek(bloque.posicion * block_size)
                block_data = file.read(bloque.tamaño)

                success = ArchivosControlador().bloques_servicio.subir_bloque(
                    bloque_id=bloque.bloque_id,
                    data=block_data,
                    leader_uri=bloque.get_leader_uri()
                )

                if not success:
                    raise Exception(f"Error subiendo bloque {bloque.bloque_id}")

            return jsonify({
                'success': True,
                'message': f'Archivo {ruta} subido exitosamente',
                'data': {
                    'archivo': archivo.to_dict(),
                    'bloques_subidos': len(bloques),
                    'tamaño_total': tamaño
                }
            }), 201

        except Exception as e:
            logger.error(f"Error en upload_file: {str(e)}", exc_info=True)
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

# Inicialización del controlador (Singleton)
controlador = ArchivosControlador()