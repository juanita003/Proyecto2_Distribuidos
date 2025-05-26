current_path = "/"

def run(path):
    global current_path
    print(f"Ejecutando CD a: {path}")
    if path.startswith("/"):
        current_path = path
    else:
        current_path = f"{current_path.rstrip('/')}/{path}"
    print(f"Directorio actual: {current_path}")