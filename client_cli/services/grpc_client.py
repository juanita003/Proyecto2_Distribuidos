import grpc
import protos.datanode_pb2_grpc as datanode_pb2_grpc
import protos.datanode_pb2      as datanode_pb2


def send_block(address: str, block_id: str, data: bytes, checksum: str):
    channel = grpc.insecure_channel(address)
    stub = datanode_pb2_grpc.DataNodeServiceStub(channel)

    request = datanode_pb2.WriteBlockRequest(
        block_id=block_id,
        data=data
    )

    try:
        response = stub.WriteBlock(request)
        if not response.success:
            print(f"Error al enviar bloque {block_id}: {response.message}")
    except grpc.RpcError as e:
        print(f"Fallo gRPC con {address} para bloque {block_id}: {e}")

def get_block(host, filename, index):
    with grpc.insecure_channel(f"{host}:50051") as channel:
        stub = datanode_pb2_grpc.DataNodeStub(channel)
        request = datanode_pb2.BlockRequest(
            filename=filename,
            index=index
        )
        response = stub.DownloadBlock(request)
        return response.data