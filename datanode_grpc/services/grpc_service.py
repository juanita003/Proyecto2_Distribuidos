import os
import sys
# Permite importar common
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import protos.datanode_pb2_grpc as datanode_pb2_grpc
import protos.datanode_pb2      as datanode_pb2
from common.models.block import Block as BlockModel
from services.storage_service import StorageService
from common.utils.hashing import verify_checksum


class DataNodeGRPCService(datanode_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, storage_dir=None):
        node_id = int(os.environ.get('NODE_ID', '1'))
        self.storage = StorageService(node_id, storage_dir)

    def WriteBlock(self, request, context):
        # request: WriteBlockRequest { block_id, data }
        block_model = BlockModel(
            block_id=request.block_id,
            data=request.data,
            checksum="",
        )
        # almacenar con cálculo interno de checksum
        self.storage.store_block(block_model)
        return datanode_pb2.WriteBlockResponse(
            success=True,
            message="Block stored successfully"
        )

    def ReadBlock(self, request, context):
        # request: ReadBlockRequest { block_id }
        block_model = self.storage.retrieve_block(request.block_id)
        return datanode_pb2.ReadBlockResponse(
            data=block_model.data
        )