import os
import math
import uuid
from services.rest_client import register_file
from services.grpc_client import send_block
from common.utils.hashing import calculate_checksum
from common.config import default_block_size


def put_file(filepath: str):
    if not os.path.exists(filepath):
        print(f"Archivo no encontrado: {filepath}")
        return

    file_size = os.path.getsize(filepath)
    file_name = os.path.basename(filepath)

    block_ids = []
    blocks_data = {}

    with open(filepath, 'rb') as f:
        num_blocks = math.ceil(file_size / default_block_size)
        for i in range(num_blocks):
            data = f.read(default_block_size)
            block_id = str(uuid.uuid4())
            checksum = calculate_checksum(data)

            block_ids.append(block_id)
            blocks_data[block_id] = {
                "data": data,
                "checksum": checksum,
                "sequence": i,
                "size": len(data)
            }

    print("Registrando archivo en NameNode...")
    response = register_file(file_name, file_size, block_ids)

    if not response:
        print("Fallo al registrar archivo en NameNode")
        return

    print("Enviando bloques a los DataNodes...")
    for block in response["blocks"]:
        block_id = block["block_id"]
        datanodes = block["datanodes"]
        block_data = blocks_data[block_id]

        for node_address in datanodes:
            send_block(
                node_address,
                block_id,
                block_data["data"],
                block_data["checksum"]
            )
        print(f"Bloque {block_id} enviado a {datanodes}")

    print("Archivo cargado exitosamente.")
