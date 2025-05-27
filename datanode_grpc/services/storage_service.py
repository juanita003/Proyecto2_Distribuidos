import os
import sys
# Permite importar common
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.config import blocks_storage_dir
from common.models.block import Block
from common.utils.hashing import calculate_checksum


class StorageService:
    def __init__(self, node_id: int, base_dir: str = None):
        self.node_id = node_id
        self.base_dir = base_dir or os.path.join(blocks_storage_dir)
        os.makedirs(self.base_dir, exist_ok=True)

    def _block_path(self, block_id: str) -> str:
        return os.path.join(self.base_dir, block_id)

    def store_block(self, block: Block) -> None:
        path = self._block_path(block.block_id)
        block.checksum = calculate_checksum(block.data)
        with open(path, 'wb') as f:
            f.write(block.data)

    def retrieve_block(self, block_id: str) -> Block:
        path = self._block_path(block_id)
        with open(path, 'rb') as f:
            data = f.read()
        checksum = calculate_checksum(data)
        return Block(block_id=block_id, data=data, checksum=checksum)
