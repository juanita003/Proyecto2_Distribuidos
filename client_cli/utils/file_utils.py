import os

def split_file(filepath, block_size=1024*1024):
    """Divide un archivo en bloques binarios de tamaño fijo."""
    blocks = []
    with open(filepath, 'rb') as f:
        while chunk := f.read(block_size):
            blocks.append(chunk)
    return blocks

def merge_blocks(blocks, output_path):
    """Une una lista de bloques binarios en un solo archivo."""
    with open(output_path, 'wb') as f:
        for block in blocks:
            f.write(block)

def get_filename(path):
    """Extrae el nombre del archivo desde una ruta."""
    return os.path.basename(path)