dataclass = None
from dataclasses import dataclass

@dataclass
class Block:
    block_id: str          # UUID o identificador único
    data: bytes            # Contenido del bloque (crudo)
    checksum: str          # Hash calculado del contenido


@dataclass
class BlockInfo:
    file_name: str         # Nombre del archivo al que pertenece
    block_id: str          # Identificador del bloque
    sequence: int          # Número de secuencia (orden) en el archivo
    size: int              # Tamaño real en bytes
    checksum: str          # Hash para validar integridad