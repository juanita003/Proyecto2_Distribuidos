import sys
from commands import put, get, ls, cd, mkdir, rmdir, rm

def main():
    if len(sys.argv) < 2:
        print("Usar: python main.py [put|get|ls|cd|mkdir|rmdir|rm] [args]")
        return

    command = sys.argv[1]

    if command == "put" and len(sys.argv) == 3:
        put.run(sys.argv[2])
    elif command == "get" and len(sys.argv) == 3:
        get.run(sys.argv[2])
    elif command == "ls":
        ls.run()
    elif command == "cd" and len(sys.argv) == 3:
        cd.run(sys.argv[2])
    elif command == "mkdir" and len(sys.argv) == 3:
        mkdir.run(sys.argv[2])
    elif command == "rmdir" and len(sys.argv) == 3:
        rmdir.run(sys.argv[2])
    elif command == "rm" and len(sys.argv) == 3:
        rm.run(sys.argv[2])
    else:
        print("Comando no válido o argumentos incorrectos.")

if __name__ == "__main__":
    main()