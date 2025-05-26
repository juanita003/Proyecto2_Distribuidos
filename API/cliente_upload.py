import requests
import os

def upload_file(server_url, file_path, ruta_destino=None, usuario="default"):
    """Envía un archivo al NameNode para su almacenamiento distribuido"""
    try:
        with open(file_path, 'rb') as f:
            files = {'file': f}
            data = {'ruta': ruta_destino} if ruta_destino else {}
            headers = {'X-Usuario': usuario}
            
            response = requests.post(
                f"{server_url}/api/archivos/upload",
                files=files,
                data=data,
                headers=headers
            )
            
            return response.json()
    
    except Exception as e:
        return {'success': False, 'error': str(e)}

if __name__ == "__main__":
    # Ejemplo de uso
    SERVER = "http://localhost:8080"
    FILE = "mi_archivo.txt"
    
    result = upload_file(SERVER, FILE)
    print(result)