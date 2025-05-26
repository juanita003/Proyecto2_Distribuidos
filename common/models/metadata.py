from dataclasses import dataclass
from typing import List

@dataclass
class FileMetadata:
    file_name: str                 # Nombre del archivo
    file_size: int                 # Tamaño total en bytes
    blocks: List[BlockInfo]        # Lista de bloques que componen el archivo
    created_at: str                # Timestamp ISO 8601
    modified_at: str               # Timestamp ISO 8601