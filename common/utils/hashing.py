import hashlib

def calculate_checksum(data: bytes) -> str:
    """
    Calcula un hash SHA-256 de los datos.
    Retorna la cadena hexadecimal.
    """
    sha = hashlib.sha256()
    sha.update(data)
    return sha.hexdigest()


def verify_checksum(data: bytes, checksum: str) -> bool:
    """
    Verifica que el hash SHA-256 de `data` coincida con `checksum`.
    """
    return calculate_checksum(data) == checksum