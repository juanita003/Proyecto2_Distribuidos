from flask import Blueprint, request, jsonify, g
from servicios.archivos_servicio import ArchivosServicio
from servicios.bloques_servicio import BloquesServicio
import logging
from functools import wraps
import os

logger = logging.getLogger(__name__)

# Crear Blueprint con prefijo
archivos_bp = Blueprint('archivos', __name__, url_prefix='/api/archivos')

class ArchivosControlador:
    def __init__(self):
        self.archivos_servicio = ArchivosServicio()
        self.bloques_servicio = BloquesServicio()
    
    @staticmethod
    def _autenticar_usuario(username, password):
        usuarios_validos = {
            'admin': 'admin123',
            'user1': 'pass123',
            'user2': 'pass456'
        }
        return usuarios_validos.get(username) == password
    
    @classmethod
    def autenticar(cls, f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # El primer argumento es 'self' cuando se llama desde una instancia
            self = args[0] if args else None
            
            auth = request.authorization
            if not auth or not cls._autenticar_usuario(auth.username, auth.password):
                return jsonify({
                    'success': False, 
                    'error': 'Autenticación requerida'
                }), 401
            g.usuario = auth.username
            
            if self:
                # Si se llama desde una instancia, pasamos 'self' correctamente
                return f(*args, **kwargs)
            else:
                # Si se llama sin instancia (menos común)
                return f(**kwargs)
        return decorated_function

    @archivos_bp.route('/upload', methods=['POST'])
    @autenticar
    def upload_file(self):
        """Endpoint para subir archivos al sistema distribuido"""
        try:
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

            ruta = request.form.get('ruta', file.filename)
            ruta = self.archivos_servicio.validar_ruta(ruta)

            file.seek(0, 2)
            tamaño = file.tell()
            file.seek(0)

            archivo = self.archivos_servicio.crear_archivo(ruta, g.usuario)
            bloques = self.bloques_servicio.crear_bloques_para_archivo(archivo.nombre, tamaño)
            archivo.bloques = [b.bloque_id for b in bloques]
            archivo.tamaño_total = tamaño

            block_size = self.bloques_servicio.block_size
            for bloque in bloques:
                file.seek(bloque.posicion * block_size)
                block_data = file.read(bloque.tamaño)

                success = self.bloques_servicio.subir_bloque(
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

# Crear instancia del controlador y registrar rutas
controlador = ArchivosControlador()

# Registrar manualmente las rutas
archivos_bp.add_url_rule(
    '/upload',
    view_func=controlador.upload_file,
    methods=['POST']
)