import sys
from commands import put, get, ls, cd, mkdir, rmdir, rm

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Uso: python main.py put <archivo>")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "put":
        put.put_file(sys.argv[2])
    else:
        print(f"Comando no reconocido: {cmd}")