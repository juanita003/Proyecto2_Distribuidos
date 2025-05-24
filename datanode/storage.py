import os
import hashlib

class BlockStorage:
    def __init__(self, base_path):
        self.base_path = base_path

    def _get_block_path(self, block_id):
        return os.path.join(self.base_path, block_id)

    def store_block(self, block_id, content):
        path = self._get_block_path(block_id)
        with open(path, 'wb') as f:
            f.write(content)

    def get_block(self, block_id):
        path = self._get_block_path(block_id)
        with open(path, 'rb') as f:
            return f.read()

    def delete_block(self, block_id):
        path = self._get_block_path(block_id)
        if os.path.exists(path):
            os.remove(path)

    def block_exists(self, block_id):
        return os.path.exists(self._get_block_path(block_id))