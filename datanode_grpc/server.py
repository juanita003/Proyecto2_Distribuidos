import grpc
from concurrent import futures
from protos import datanode_pb2_grpc
from services.grpc_service import DataNodeServiceServicer

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datanode_pb2_grpc.add_DataNodeServiceServicer_to_server(
        DataNodeServiceServicer(), server
    )
    server.add_insecure_port('[::]:50051')
    print("DataNode gRPC server listening on port 50051")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()