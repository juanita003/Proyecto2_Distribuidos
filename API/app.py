from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
import os
import threading
import time

from controladores.bloques_controlador import BloquesControlador
from controladores.archivos_controlador import ArchivosControlador
from controladores.archivos_controlador import archivos_bp  # Añade esto con las otras importaciones
from modelos.archivo_metadata import ArchivoMetadata
from modelos.bloque_info import BloqueInfo
from servicios.archivos_servicio import ArchivosServicio
from servicios.bloques_servicio import BloquesServicio

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear aplicación Flask
app = Flask(__name__)
CORS(app)
# Registra el Blueprint de archivos
app.register_blueprint(archivos_bp)
# Inicializar controlador de bloques
controlador = BloquesControlador()

# Configuración por variables de entorno
PUERTO_NAMENODE = int(os.getenv('NAMENODE_PORT', 8080))
HOST_NAMENODE = os.getenv('NAMENODE_HOST', '0.0.0.0')

# ================== Autenticación ==================

def autenticar_usuario(username, password):
    usuarios_validos = {
        'admin': 'admin123',
        'user1': 'pass123',
        'user2': 'pass456'
    }
    return usuarios_validos.get(username) == password

def obtener_usuario_desde_request():
    auth = request.authorization
    if auth and autenticar_usuario(auth.username, auth.password):
        return auth.username
    return None

# ================== Monitor de DataNodes ==================

def monitor_datanodes():
    while True:
        try:
            tiempo_actual = time.time()
            timeout = 60

            with controlador.lock:
                for datanode_id, info in controlador.datanodes.items():
                    if (tiempo_actual - info.get('ultima_comunicacion', 0)) > timeout:
                        if info.get('estado') == 'activo':
                            logger.warning(f"DataNode {datanode_id} marcado como inactivo")
                            info['estado'] = 'inactivo'

            time.sleep(30)

        except Exception as e:
            logger.error(f"Error en monitor de DataNodes: {e}")

monitor_thread = threading.Thread(target=monitor_datanodes, daemon=True)
monitor_thread.start()

# ================== Endpoints del sistema ==================

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'servicio': 'NameNode',
        'timestamp': time.time()
    })

@app.route('/status', methods=['GET'])
def obtener_estado():
    try:
        resultado = controlador.obtener_estado_sistema()
        return jsonify(resultado)
    except Exception as e:
        logger.error(f"Error obteniendo estado: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

# ================== Endpoints de DataNodes ==================

@app.route('/datanodes/register', methods=['POST'])
def registrar_datanode():
    try:
        data = request.get_json()

        if not data or 'host' not in data or 'puerto' not in data:
            return jsonify({'status': 'error', 'mensaje': 'Host y puerto son requeridos'}), 400

        resultado = controlador.registrar_datanode(data)
        logger.info(f"DataNode registrado: {resultado.get('datanode_id')}")
        return jsonify(resultado)

    except Exception as e:
        logger.error(f"Error registrando DataNode: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

@app.route('/datanodes/heartbeat', methods=['POST'])
def heartbeat_datanode():
    try:
        data = request.get_json()
        datanode_id = data.get('datanode_id')

        if not datanode_id:
            return jsonify({'status': 'error', 'mensaje': 'datanode_id requerido'}), 400

        with controlador.lock:
            if datanode_id in controlador.datanodes:
                controlador.datanodes[datanode_id]['ultima_comunicacion'] = time.time()
                controlador.datanodes[datanode_id]['estado'] = 'activo'
                if 'espacio_libre' in data:
                    controlador.datanodes[datanode_id]['espacio_libre'] = data['espacio_libre']
                return jsonify({'status': 'success'})
            else:
                return jsonify({'status': 'error', 'mensaje': 'DataNode no registrado'}), 404

    except Exception as e:
        logger.error(f"Error en heartbeat: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

# ================== Endpoints de Archivos ==================

@app.route('/files/create', methods=['POST'])
def crear_archivo():
    try:
        data = request.get_json()
        usuario = obtener_usuario_desde_request()

        for campo in ['nombre', 'tamaño']:
            if campo not in data:
                return jsonify({'status': 'error', 'mensaje': f'Campo {campo} es requerido'}), 400

        resultado = controlador.crear_archivo(
            nombre_archivo=data['nombre'],
            tamaño=data['tamaño'],
            usuario=usuario,
            directorio=data.get('directorio', '/')
        )

        if resultado['status'] == 'success':
            logger.info(f"Archivo creado: {data['nombre']} por usuario {usuario}")

        return jsonify(resultado)

    except Exception as e:
        logger.error(f"Error creando archivo: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

@app.route('/files/<archivo_id>/blocks', methods=['GET'])
def obtener_bloques_archivo(archivo_id):
    try:
        resultado = controlador.obtener_bloques_archivo(archivo_id)
        return jsonify(resultado)
    except Exception as e:
        logger.error(f"Error obteniendo bloques: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

@app.route('/files/delete', methods=['DELETE'])
def eliminar_archivo():
    try:
        data = request.get_json()
        usuario = obtener_usuario_desde_request()

        if 'nombre' not in data:
            return jsonify({'status': 'error', 'mensaje': 'Nombre del archivo es requerido'}), 400

        resultado = controlador.eliminar_archivo(
            nombre_archivo=data['nombre'],
            directorio=data.get('directorio', '/'),
            usuario=usuario
        )

        if resultado['status'] == 'success':
            logger.info(f"Archivo eliminado: {data['nombre']} por usuario {usuario}")

        return jsonify(resultado)

    except Exception as e:
        logger.error(f"Error eliminando archivo: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

# ================== Endpoints de Bloques ==================

@app.route('/blocks/<bloque_id>/confirm', methods=['POST'])
def confirmar_bloque(bloque_id):
    try:
        data = request.get_json()

        for campo in ['datanode_id', 'checksum']:
            if campo not in data:
                return jsonify({'status': 'error', 'mensaje': f'Campo {campo} es requerido'}), 400

        resultado = controlador.confirmar_bloque_escrito(
            bloque_id=bloque_id,
            datanode_id=data['datanode_id'],
            checksum=data['checksum']
        )

        return jsonify(resultado)

    except Exception as e:
        logger.error(f"Error confirmando bloque: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

# ================== Endpoints de Directorios ==================

@app.route('/directories', methods=['GET'])
def listar_directorio():
    try:
        ruta = request.args.get('path', '/')
        usuario = obtener_usuario_desde_request()
        resultado = controlador.listar_directorio(ruta=ruta, usuario=usuario)
        return jsonify(resultado)
    except Exception as e:
        logger.error(f"Error listando directorio: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

@app.route('/directories/create', methods=['POST'])
def crear_directorio():
    try:
        data = request.get_json()
        if 'ruta' not in data:
            return jsonify({'status': 'error', 'mensaje': 'Ruta del directorio es requerida'}), 400
        resultado = controlador.crear_directorio(data['ruta'])
        if resultado['status'] == 'success':
            logger.info(f"Directorio creado: {data['ruta']}")
        return jsonify(resultado)
    except Exception as e:
        logger.error(f"Error creando directorio: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

# ================== Endpoint de búsqueda ==================

@app.route('/files/search', methods=['GET'])
def buscar_archivos():
    try:
        patron = request.args.get('q', '')
        usuario = obtener_usuario_desde_request()
        archivos_encontrados = []

        with controlador.lock:
            for archivo_id, archivo in controlador.archivos.items():
                if usuario and archivo.get('usuario') != usuario:
                    continue
                if patron.lower() in archivo['nombre'].lower():
                    archivos_encontrados.append({
                        'archivo_id': archivo_id,
                        'nombre': archivo['nombre'],
                        'ruta': archivo['ruta'],
                        'tamaño': archivo['tamaño'],
                        'fecha_creacion': archivo['fecha_creacion'],
                        'estado': archivo['estado']
                    })

        return jsonify({'status': 'success', 'patron': patron, 'archivos': archivos_encontrados})
    except Exception as e:
        logger.error(f"Error buscando archivos: {e}")
        return jsonify({'status': 'error', 'mensaje': str(e)}), 500

# ================== Manejo de errores ==================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'status': 'error', 'mensaje': 'Endpoint no encontrado'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'status': 'error', 'mensaje': 'Error interno del servidor'}), 500

@app.errorhandler(400)
def bad_request(error):
    return jsonify({'status': 'error', 'mensaje': 'Solicitud inválida'}), 400

# ================== Función principal ==================

if __name__ == '__main__':
    logger.info(f"Iniciando NameNode en {HOST_NAMENODE}:{PUERTO_NAMENODE}")
    app.run(host=HOST_NAMENODE, port=PUERTO_NAMENODE, debug=True, threaded=True)
