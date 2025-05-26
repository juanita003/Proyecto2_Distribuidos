from services import rest_client

def run(dirname):
    print(f"Ejecutando MKDIR: {dirname}")
    rest_client.create_directory(dirname)
    print("Carpeta creada exitosamente.")
