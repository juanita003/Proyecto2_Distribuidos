from flask import Blueprint, request, jsonify, g
from servicios.archivos_servicio import ArchivosServicio
from servicios.bloques_servicio import BloquesServicio
import logging
from functools import wraps
import os

logger = logging.getLogger(__name__)

archivos_bp = Blueprint('archivos', __name__, url_prefix='/api/archivos')

class ArchivosControlador:
    def __init__(self):
        self.archivos_servicio = ArchivosServicio()
        self.bloques_servicio = BloquesServicio()

    @staticmethod
# Modificar el decorador @autenticar_usuario para que sea consistente
    def autenticar_usuario(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            auth = request.authorization
            if not auth or not self.autenticar_usuario_basico(auth.username, auth.password):
                return jsonify({'success': False, 'error': 'Autenticación requerida'}), 401
            g.usuario = auth.username
            return f(*args, **kwargs)
        return decorated_function

# Añadir método de autenticación básica
def autenticar_usuario_basico(self, username, password):
    usuarios_validos = {
        'admin': 'admin123',
        'user1': 'pass123',
        'user2': 'pass456'
    }
    return usuarios_validos.get(username) == password
    @archivos_bp.route('/health', methods=['GET'])
    def health_check(self):
        """Verificación de salud del servicio de archivos"""
        return jsonify({
            'status': 'healthy',
            'service': 'archivos',
            'timestamp': '2025-05-26T00:00:00Z'
        })

    @archivos_bp.route('/', methods=['GET'])
    @autenticar_usuario()
    def listar_archivos_usuario(self):
        """Listar todos los archivos del usuario"""
        try:
            archivos = self.archivos_servicio.obtener_archivos_usuario(g.usuario)
            return jsonify({
                'success': True,
                'data': archivos,
                'count': len(archivos)
            })
        except Exception as e:
            logger.error(f"Error listando archivos: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @archivos_bp.route('/<path:ruta>', methods=['GET'])
    @autenticar_usuario()
    def obtener_archivo(self, ruta):
        """Obtener información de un archivo específico"""
        try:
            ruta = self.archivos_servicio.validar_ruta(ruta)
            archivo = self.archivos_servicio.obtener_archivo(ruta)
            
            if not archivo:
                return jsonify({
                    'success': False,
                    'error': 'Archivo no encontrado'
                }), 404
            
            if archivo.usuario != g.usuario and g.usuario != "admin":
                return jsonify({
                    'success': False,
                    'error': 'No tienes permisos para acceder a este archivo'
                }), 403
            
            bloques_info = []
            for bloque_id in archivo.bloques:
                bloque = self.bloques_servicio.obtener_bloque(bloque_id)
                if bloque:
                    bloques_info.append({
                        'bloque_id': bloque.bloque_id,
                        'posicion': bloque.posicion,
                        'tamaño': bloque.tamaño,
                        'ubicaciones': bloque.get_ubicaciones_uri(),
                        'leader': bloque.get_leader_uri(),
                        'followers': bloque.get_followers_uri()
                    })
            
            return jsonify({
                'success': True,
                'data': {
                    'archivo': archivo.to_dict(),
                    'bloques': bloques_info
                }
            })
            
        except Exception as e:
            logger.error(f"Error obteniendo archivo {ruta}: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @archivos_bp.route('/<path:ruta>', methods=['POST'])
    @autenticar_usuario()
    def crear_archivo(self, ruta):
        """Crear un nuevo archivo"""
        try:
            ruta = self.archivos_servicio.validar_ruta(ruta)
            data = request.get_json() or {}
            tamaño = data.get('tamaño', 0)
            
            archivo = self.archivos_servicio.crear_archivo(ruta, g.usuario)
            
            bloques_info = []
            if tamaño > 0:
                bloques = self.bloques_servicio.crear_bloques_para_archivo(archivo.nombre, tamaño)
                archivo.tamaño_total = tamaño
                archivo.bloques = [b.bloque_id for b in bloques]
                
                for bloque in bloques:
                    bloques_info.append({
                        'bloque_id': bloque.bloque_id,
                        'posicion': bloque.posicion,
                        'tamaño': bloque.tamaño,
                        'ubicaciones': bloque.get_ubicaciones_uri(),
                        'leader': bloque.get_leader_uri(),
                        'followers': bloque.get_followers_uri()
                    })
            
            return jsonify({
                'success': True,
                'message': f'Archivo {ruta} creado exitosamente',
                'data': {
                    'archivo': archivo.to_dict(),
                    'bloques': bloques_info
                }
            }), 201
            
        except ValueError as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
        except Exception as e:
            logger.error(f"Error creando archivo {ruta}: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @archivos_bp.route('/<path:ruta>', methods=['DELETE'])
    @autenticar_usuario()
    def eliminar_archivo(self, ruta):
        """Eliminar un archivo"""
        try:
            ruta = self.archivos_servicio.validar_ruta(ruta)
            archivo = self.archivos_servicio.obtener_archivo(ruta)
            
            if not archivo:
                return jsonify({
                    'success': False,
                    'error': 'Archivo no encontrado'
                }), 404
            
            self.bloques_servicio.eliminar_bloques_archivo(archivo.nombre)
            eliminado = self.archivos_servicio.eliminar_archivo(ruta, g.usuario)
            
            if eliminado:
                return jsonify({
                    'success': True,
                    'message': f'Archivo {ruta} eliminado exitosamente'
                })
            return jsonify({
                'success': False,
                'error': 'No se pudo eliminar el archivo'
            }), 500
                
        except PermissionError as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 403
        except Exception as e:
            logger.error(f"Error eliminando archivo {ruta}: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @archivos_bp.route('/<path:ruta>/mover', methods=['POST'])
    @autenticar_usuario()
    def mover_archivo(self, ruta):
        """Mover un archivo a otra ubicación"""
        try:
            ruta = self.archivos_servicio.validar_ruta(ruta)
            data = request.get_json()
            
            if not data or 'destino' not in data:
                return jsonify({
                    'success': False,
                    'error': 'Debe especificar la ruta de destino'
                }), 400
            
            ruta_destino = self.archivos_servicio.validar_ruta(data['destino'])
            movido = self.archivos_servicio.mover_archivo(ruta, ruta_destino, g.usuario)
            
            if movido:
                return jsonify({
                    'success': True,
                    'message': f'Archivo movido de {ruta} a {ruta_destino}'
                })
            return jsonify({
                'success': False,
                'error': 'No se pudo mover el archivo'
            }), 500
                
        except (ValueError, PermissionError) as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
        except Exception as e:
            logger.error(f"Error moviendo archivo {ruta}: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @archivos_bp.route('/<path:ruta>/bloques', methods=['GET'])
    @autenticar_usuario()
    def obtener_bloques_archivo(self, ruta):
        """Obtener información detallada de los bloques de un archivo"""
        try:
            ruta = self.archivos_servicio.validar_ruta(ruta)
            archivo = self.archivos_servicio.obtener_archivo(ruta)
            
            if not archivo:
                return jsonify({
                    'success': False,
                    'error': 'Archivo no encontrado'
                }), 404
            
            if archivo.usuario != g.usuario and g.usuario != "admin":
                return jsonify({
                    'success': False,
                    'error': 'No tienes permisos para acceder a este archivo'
                }), 403
            
            bloques = self.bloques_servicio.obtener_bloques_archivo(archivo.nombre)
            bloques_info = []
            
            for bloque in bloques:
                bloques_info.append({
                    'bloque_id': bloque.bloque_id,
                    'posicion': bloque.posicion,
                    'tamaño': bloque.tamaño,
                    'checksum': bloque.checksum,
                    'ubicaciones': [{'host': host, 'puerto': puerto} for host, puerto in bloque.ubicaciones],
                    'uris': bloque.get_ubicaciones_uri(),
                    'leader_uri': bloque.get_leader_uri(),
                    'followers_uris': bloque.get_followers_uri(),
                    'replicacion_suficiente': bloque.is_replicado_suficiente(),
                    'estado': bloque.estado
                })
            
            return jsonify({
                'success': True,
                'data': {
                    'archivo': ruta,
                    'total_bloques': len(bloques_info),
                    'tamaño_total': archivo.tamaño_total,
                    'bloques': bloques_info
                }
            })
            
        except Exception as e:
            logger.error(f"Error obteniendo bloques del archivo {ruta}: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @archivos_bp.route('/upload/prepare', methods=['POST'])
    @autenticar_usuario()
    def preparar_upload(self):
        """Preparar la subida de un archivo (crear estructura de bloques)"""
        try:
            data = request.get_json()
            
            if not data or 'ruta' not in data or 'tamaño' not in data:
                return jsonify({
                    'success': False,
                    'error': 'Debe especificar ruta y tamaño del archivo'
                }), 400
            
            ruta = self.archivos_servicio.validar_ruta(data['ruta'])
            tamaño = int(data['tamaño'])
            
            if tamaño <= 0:
                return jsonify({
                    'success': False,
                    'error': 'El tamaño del archivo debe ser mayor a 0'
                }), 400
            
            archivo = self.archivos_servicio.crear_archivo(ruta, g.usuario)
            archivo.tamaño_total = tamaño
            
            bloques = self.bloques_servicio.crear_bloques_para_archivo(archivo.nombre, tamaño)
            archivo.bloques = [b.bloque_id for b in bloques]
            
            plan_upload = {
                'archivo': archivo.to_dict(),
                'total_bloques': len(bloques),
                'tamaño_bloque': self.bloques_servicio.block_size,
                'bloques': []
            }
            
            for bloque in bloques:
                plan_upload['bloques'].append({
                    'bloque_id': bloque.bloque_id,
                    'posicion': bloque.posicion,
                    'tamaño': bloque.tamaño,
                    'offset': bloque.posicion * self.bloques_servicio.block_size,
                    'leader_uri': bloque.get_leader_uri(),
                    'replicas_uris': bloque.get_followers_uri()
                })
            
            return jsonify({
                'success': True,
                'message': f'Archivo {ruta} preparado para upload',
                'data': plan_upload
            }), 201
            
        except ValueError as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
        
    @archivos_bp.route('/upload', methods=['POST'])
    @autenticar_usuario()
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

# Crea una instancia del controlador
archivos_controlador = ArchivosControlador()