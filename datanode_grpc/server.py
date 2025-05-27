import grpc
from concurrent import futures
import os
import sys
import requests
import time

# Permitir importar el paquete common
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.config import grpc_base_port
from services.grpc_service import DataNodeGRPCService
import protos.datanode_pb2_grpc as datanode_pb2_grpc
import protos.datanode_pb2      as datanode_pb2

def registrar_en_namenode(node_id, port):
    namenode_url = namenode_url = "http://34.201.251.107:8080/datanodes/register"
    payload = {
        "host": "ip_privada_o_publica_del_datanode",  # Debe ser donde el NameNode pueda accederlo
        "puerto": port,
        "espacio_total": 100000000  # Ejemplo de espacio en bytes (ajústalo a tu caso)
    }
    try:
        response = requests.post(namenode_url, json=payload)
        if response.status_code == 200:
            print("✅ DataNode registrado en NameNode")
        else:
            print(f"❌ Fallo en el registro del DataNode: {response.text}")
    except Exception as e:
        print(f"❌ Error conectando al NameNode: {e}")


def serve(node_id: int, storage_dir=None):
    port = grpc_base_port + node_id
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(
        DataNodeGRPCService(storage_dir), server
    )
    server.add_insecure_port(f"[::]:{port}")
    print(f"DataNode {node_id} listening on port {port}")

    # 🔴 Registrar el datanode
    registrar_en_namenode(node_id=node_id, port=port)

    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    node_id = int(os.environ.get('NODE_ID', '1'))
    storage_dir = os.environ.get('STORAGE_DIR', None)
    serve(node_id, storage_dir)

def enviar_heartbeat(host, puerto):
    while True:
        try:
            response = requests.post(
                "http://34.201.251.107:8080/datanodes/heartbeat",
                json={"host": host, "puerto": puerto}
            )
            print("Heartbeat:", response.json())
        except Exception as e:
            print("Error en heartbeat:", e)
        time.sleep(15)
