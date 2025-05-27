import requests

BASE_URL = "http:/ 3.93.218.93/:8080"  # Dirección del NameNode Flask API
import requests
from common.config import default_namenode_address


def register_file(file_name: str, file_size: int, block_ids: list[str]):
    url = f"http://{default_namenode_address}/files/"
    payload = {
        "file_name": file_name,
        "file_size": file_size,
        "block_ids": block_ids
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error del NameNode: {response.status_code} {response.text}")
            return None
    except Exception as e:
        print(f"Error al contactar al NameNode: {e}")
        return None

def get_file_blocks(filename):
    url = f"{BASE_URL}/files/{filename}/blocks"
    response = requests.get(url)
    return response.json()

def list_directory():
    url = f"{BASE_URL}/files"
    response = requests.get(url)
    return response.json()

def create_directory(dirname):
    url = f"{BASE_URL}/directories"
    response = requests.post(url, json={"name": dirname})
    return response.json()

def delete_directory(dirname):
    url = f"{BASE_URL}/directories/{dirname}"
    response = requests.delete(url)
    return response.json()

def delete_file(filename):
    url = f"{BASE_URL}/files/{filename}"
    response = requests.delete(url)
    return response.json()
