import grpc
import datanode_pb2
import datanode_pb2_grpc

def send_block(data, host, filename, index):
    with grpc.insecure_channel(f"{host}:50051") as channel:
        stub = datanode_pb2_grpc.DataNodeStub(channel)
        request = datanode_pb2.BlockUploadRequest(
            filename=filename,
            index=index,
            data=data
        )
        response = stub.UploadBlock(request)
        return response.message

def get_block(host, filename, index):
    with grpc.insecure_channel(f"{host}:50051") as channel:
        stub = datanode_pb2_grpc.DataNodeStub(channel)
        request = datanode_pb2.BlockRequest(
            filename=filename,
            index=index
        )
        response = stub.DownloadBlock(request)
        return response.data