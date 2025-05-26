from services import rest_client

def run():
    print("Ejecutando LS")
    items = rest_client.list_directory()
    for item in items:
        print(item)