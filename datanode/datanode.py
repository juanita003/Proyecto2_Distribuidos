import grpc
from concurrent import futures
import os
import hashlib
from proto import datanode_pb2, datanode_pb2_grpc
import logging
from storage import BlockStorage

class DataNode(datanode_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, host, port, storage_path="blocks"):
        self.host = host
        self.port = port
        self.storage = BlockStorage(storage_path)
        os.makedirs(storage_path, exist_ok=True)

    def StoreBlock(self, request, context):
        try:
            self.storage.store_block(request.block_id, request.content)
            return datanode_pb2.StoreResponse(success=True, message="Block stored successfully")
        except Exception as e:
            return datanode_pb2.StoreResponse(success=False, message=str(e))

    def RetrieveBlock(self, request, context):
        try:
            content = self.storage.get_block(request.block_id)
            return datanode_pb2.BlockData(block_id=request.block_id, content=content)
        except FileNotFoundError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return datanode_pb2.BlockData()

    def ReplicateBlock(self, request, context):
        # This would actually replicate to another DataNode
        # For simplicity, we'll just store it locally
        try:
            self.storage.store_block(request.block.block_id, request.block.content)
            return datanode_pb2.ReplicationResponse(success=True)
        except Exception as e:
            return datanode_pb2.ReplicationResponse(success=False)

def serve():
    import socket
    host = socket.gethostname()
    port = 50052  # Default port
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datanode = DataNode(host, port)
    datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(datanode, server)
    
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    print(f"DataNode running on {host}:{port}")
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve()