from services import rest_client

def run(dirname):
    print(f"Ejecutando RMDIR: {dirname}")
    rest_client.delete_directory(dirname)
    print("Carpeta eliminada exitosamente.")