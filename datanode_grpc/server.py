import grpc
from concurrent import futures
import os
from common.config import grpc_base_port
from services.grpc_service import DataNodeService
import protos.datanode_pb2_grpc as datanode_pb2_grpc
import protos.datanode_pb2      as datanode_pb2


def serve(node_id: int, storage_dir=None):
    port = grpc_base_port + node_id
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datanode_pb2_grpc.add_DataNodeServicer_to_server(
        DataNodeService(storage_dir), server
    )
    server.add_insecure_port(f"[::]:{port}")
    print(f"DataNode {node_id} listening on port {port}")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    # el ID de nodo puede venir de variable de entorno o argumento CLI
    node_id = int(os.environ.get('NODE_ID', '1'))
    storage_dir = os.environ.get('STORAGE_DIR', None)
    serve(node_id, storage_dir)