from protos import datanode_pb2, datanode_pb2_grpc
from common.models.block import Block as BlockModel
from common.models.block import BlockInfo
from services.storage_service import StorageService
from common.utils.hashing import verify_checksum
import os
import grpc

class DataNodeService(datanode_pb2_grpc.DataNodeServicer):
    def __init__(self, storage_dir=None):
        # Se asume que NODE_ID está en entorno
        node_id = int(os.environ.get('NODE_ID', '1'))
        self.storage = StorageService(node_id, storage_dir)

    def PutBlock(self, request, context):
        # request contiene block_id, data y metadata
        # Convertir a BlockModel
        block_model = BlockModel(
            block_id=request.block_info.block_id,
            data=request.data,
            checksum=request.block_info.checksum,
        )
        # verificar antes de almacenar
        if not verify_checksum(block_model.data, block_model.checksum):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Checksum mismatch")
        self.storage.store_block(block_model)
        return datanode_pb2.PutBlockResponse(status=True)

    def GetBlock(self, request, context):
        block_id = request.block_id
        block_model = self.storage.retrieve_block(block_id)
        # Crear BlockInfo para respuesta
        info = datanode_pb2.BlockInfo(
            file_name=request.file_name,
            block_id=block_model.block_id,
            sequence=request.sequence,
            size=len(block_model.data),
            checksum=block_model.checksum,
        )
        return datanode_pb2.GetBlockResponse(data=block_model.data, block_info=info)