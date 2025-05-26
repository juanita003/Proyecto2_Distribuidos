from flask import Flask, request, jsonify, g
from flask_cors import CORS
import logging
import os
import threading
import time
from controladores.bloques_controlador import BloquesControlador
from controladores.archivos_controlador import archivos_bp, ArchivosControlador
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

# Inicializar servicios
archivos_servicio = ArchivosServicio()
bloques_servicio = BloquesServicio()

# Configuración por variables de entorno
PUERTO_NAMENODE = int(os.getenv('NAMENODE_PORT', 8080))
HOST_NAMENODE = os.getenv('NAMENODE_HOST', '0.0.0.0')

# ================== Monitor de DataNodes ==================

def monitor_datanodes():
    """Monitorea el estado de los DataNodes periódicamente"""
    while True:
        try:
            # Verificar datanodes inactivos
            bloques_servicio.verificar_datanodes_inactivos()
            
            # Verificar y reparar replicación
            bloques_problematicos = bloques_servicio.verificar_replicacion()
            for bloque_id in bloques_problematicos:
                bloques_servicio.reparar_replicacion(bloque_id)
            
            time.sleep(30)

        except Exception as e:
            logger.error(f"Error en monitor de DataNodes: {e}")
            time.sleep(60)

# Iniciar hilo de monitorización
monitor_thread = threading.Thread(target=monitor_datanodes, daemon=True)
monitor_thread.start()

# ================== Endpoints del sistema ==================

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de salud del servicio"""
    return jsonify({
        'status': 'healthy',
        'servicio': 'NameNode',
        'timestamp': time.time(),
        'estadisticas': bloques_servicio.obtener_estadisticas()
    })

@app.route('/status', methods=['GET'])
def obtener_estado():
    """Obtiene el estado del sistema"""
    try:
        stats = bloques_servicio.obtener_estadisticas()
        return jsonify({
            'status': 'success',
            'data': stats
        })
    except Exception as e:
        logger.error(f"Error obteniendo estado: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ================== Endpoints de DataNodes ==================

@app.route('/datanodes/register', methods=['POST'])
def registrar_datanode():
    """Registra un nuevo DataNode en el sistema"""
    try:
        data = request.get_json()
        if not data or 'host' not in data or 'puerto' not in data:
            return jsonify({'status': 'error', 'message': 'Host y puerto son requeridos'}), 400

        datanode = bloques_servicio.registrar_datanode(
            host=data['host'],
            puerto=data['puerto'],
            espacio_total=data.get('espacio_total', 0)
        )
        logger.info(f"DataNode registrado: {datanode.node_id}")
        return jsonify({
            'status': 'success',
            'datanode_id': datanode.node_id,
            'host': datanode.host,
            'puerto': datanode.puerto
        })

    except Exception as e:
        logger.error(f"Error registrando DataNode: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/datanodes/heartbeat', methods=['POST'])
def heartbeat_datanode():
    """Procesa el heartbeat de un DataNode"""
    try:
        data = request.get_json()
        if not data or 'host' not in data or 'puerto' not in data:
            return jsonify({'status': 'error', 'message': 'Host y puerto son requeridos'}), 400

        success = bloques_servicio.heartbeat_datanode(
            host=data['host'],
            puerto=data['puerto'],
            estado_info=data.get('estado_info'))
        
        if success:
            return jsonify({'status': 'success'})
        else:
            return jsonify({'status': 'error', 'message': 'Error procesando heartbeat'}), 500

    except Exception as e:
        logger.error(f"Error en heartbeat: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ================== Endpoints de Bloques ==================

@app.route('/blocks/<bloque_id>/confirm', methods=['POST'])
def confirmar_bloque(bloque_id):
    """Confirma que un bloque ha sido escrito correctamente"""
    try:
        data = request.get_json()
        if not data or 'datanode_id' not in data or 'checksum' not in data:
            return jsonify({'status': 'error', 'message': 'Datanode ID y checksum son requeridos'}), 400

        bloque = bloques_servicio.obtener_bloque(bloque_id)
        if not bloque:
            return jsonify({'status': 'error', 'message': 'Bloque no encontrado'}), 404

        # Actualizar checksum y estado del bloque
        bloque.checksum = data['checksum']
        bloque.fecha_modificacion = time.time().isoformat()
        bloques_servicio._guardar_metadata()

        return jsonify({'status': 'success'})

    except Exception as e:
        logger.error(f"Error confirmando bloque: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ================== Manejo de errores ==================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'status': 'error', 'message': 'Endpoint no encontrado'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'status': 'error', 'message': 'Error interno del servidor'}), 500

@app.errorhandler(400)
def bad_request(error):
    return jsonify({'status': 'error', 'message': 'Solicitud inválida'}), 400

# ================== Función principal ==================

if __name__ == '__main__':
    logger.info(f"Iniciando NameNode en {HOST_NAMENODE}:{PUERTO_NAMENODE}")
    
    # Asegurar que el directorio de metadata existe
    os.makedirs('namenode_data', exist_ok=True)

    app.run(host=HOST_NAMENODE, port=PUERTO_NAMENODE, debug=True, threaded=True)