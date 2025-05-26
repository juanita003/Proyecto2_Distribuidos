from protos import datanode_pb2, datanode_pb2_grpc
from services.storage_service import StorageService

class DataNodeServiceServicer(datanode_pb2_grpc.DataNodeServiceServicer):
    def __init__(self):
        # Raíz donde se guardan los bloques en disco
        self.storage = StorageService(base_path='storage/blocks')

    def WriteBlock(self, request, context):
        try:
            self.storage.save_block(request.block_id, request.data)
            return datanode_pb2.WriteBlockResponse(success=True,
                                                   message="Block saved")
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return datanode_pb2.WriteBlockResponse(success=False,
                                                   message=str(e))

    def ReadBlock(self, request, context):
        try:
            data = self.storage.load_block(request.block_id)
            return datanode_pb2.ReadBlockResponse(data=data)
        except FileNotFoundError:
            context.set_details("Block not found")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return datanode_pb2.ReadBlockResponse()
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return datanode_pb2.ReadBlockResponse()