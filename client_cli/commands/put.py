from services import rest_client, grpc_client
from utils import file_utils

def run(filename):
    print(f"Ejecutando PUT: {filename}")
    blocks = file_utils.split_file(filename)
    file_name = file_utils.get_filename(filename)
    response = rest_client.register_file(file_name, len(blocks))

    for i, block in enumerate(blocks):
        block_info = response['blocks'][i]
        grpc_client.send_block(block, block_info['leader'], file_name, i)
    print("Archivo subido exitosamente.")
