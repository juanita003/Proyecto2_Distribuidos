import requests

BASE_URL = "http:/ 3.93.218.93/:8080"  # Dirección del NameNode Flask API

def register_file(filename, num_blocks):
    url = f"{BASE_URL}/files/register"
    data = {
        "filename": filename,
        "num_blocks": num_blocks
    }
    response = requests.post(url, json=data)
    return response.json()

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
