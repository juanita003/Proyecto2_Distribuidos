import os

class StorageService:
    def __init__(self, base_path: str):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)

    def save_block(self, block_id: str, data: bytes):
        path = os.path.join(self.base_path, block_id)
        # Modo binario
        with open(path, 'wb') as f:
            f.write(data)

    def load_block(self, block_id: str) -> bytes:
        path = os.path.join(self.base_path, block_id)
        with open(path, 'rb') as f:
            return f.read()