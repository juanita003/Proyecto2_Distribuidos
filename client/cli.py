from commands import DFSClient
import argparse

def main():
    client = DFSClient()
    parser = argparse.ArgumentParser(description='DFS Client CLI')
    
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    # Put command
    put_parser = subparsers.add_parser('put')
    put_parser.add_argument('local_path', help='Local file path')
    put_parser.add_argument('dfs_path', help='DFS file path')
    
    # Get command
    get_parser = subparsers.add_parser('get')
    get_parser.add_argument('dfs_path', help='DFS file path')
    get_parser.add_argument('local_path', help='Local file path')
    
    # List command
    ls_parser = subparsers.add_parser('ls')
    ls_parser.add_argument('path', nargs='?', default='/', help='DFS path to list')
    
    args = parser.parse_args()
    
    if args.command == 'put':
        client.put(args.local_path, args.dfs_path)
    elif args.command == 'get':
        client.get(args.dfs_path, args.local_path)
    elif args.command == 'ls':
        files = client.ls(args.path)
        for file in files:
            print(file)

if __name__ == '__main__':
    main()