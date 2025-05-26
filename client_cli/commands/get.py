from services import rest_client, grpc_client
from utils import file_utils

def run(filename):
    print(f"Ejecutando GET: {filename}")
    block_list = rest_client.get_file_blocks(filename)
    blocks = []
    for block in block_list:
        data = grpc_client.get_block(block['leader'], filename, block['index'])
        blocks.append(data)
    file_utils.merge_blocks(blocks, filename)
    print("Archivo descargado exitosamente.")