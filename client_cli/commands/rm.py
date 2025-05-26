from services import rest_client

def run(filename):
    print(f"Ejecutando RM: {filename}")
    rest_client.delete_file(filename)
    print("Archivo eliminado exitosamente.")