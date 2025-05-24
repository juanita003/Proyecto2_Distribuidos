import grpc
from proto import namenode_pb2, namenode_pb2_grpc, datanode_pb2, datanode_pb2_grpc
import yaml

class DFSClient:
    def __init__(self):
        with open('config.yaml') as f:
            self.config = yaml.safe_load(f)
        
        nn_config = self.config['namenode']
        channel = grpc.insecure_channel(f"{nn_config['host']}:{nn_config['port']}")
        self.namenode = namenode_pb2_grpc.NameNodeServiceStub(channel)

    def put(self, local_path, dfs_path):
        with open(local_path, 'rb') as f:
            content = f.read()
        
        # Create file in DFS
        file_size = len(content)
        response = self.namenode.CreateFile(
            namenode_pb2.FileMetadata(
                filename=dfs_path,
                size=file_size,
                replication_factor=2
            )
        )
        
        if not response.success:
            print("Failed to create file in DFS")
            return False
        
        # Store each block in the appropriate DataNodes
        for block in response.blocks:
            # Get the first datanode for this block
            datanode = block.datanodes[0]
            channel = grpc.insecure_channel(f"{datanode.host}:{datanode.port}")
            datanode_stub = datanode_pb2_grpc.DataNodeServiceStub(channel)
            
            # Calculate block content (simplified)
            start = 0  # In a real implementation, you'd split the file properly
            end = min(start + 64*1024*1024, file_size)
            block_content = content[start:end]
            
            # Store block
            store_response = datanode_stub.StoreBlock(
                datanode_pb2.BlockData(
                    block_id=block.block_id,
                    content=block_content,
                    filename=dfs_path
                )
            )
            
            if not store_response.success:
                print(f"Failed to store block {block.block_id}")
                return False
        
        print(f"File {local_path} uploaded to {dfs_path} successfully")
        return True

    def get(self, dfs_path, local_path):
        # Get block locations from NameNode
        response = self.namenode.GetBlockLocations(
            namenode_pb2.FileRequest(filename=dfs_path)
        )
        
        if not response.blocks:
            print("File not found in DFS")
            return False
        
        # Retrieve each block and reconstruct the file
        content = b''
        for block in response.blocks:
            # Try each datanode until we find one that has the block
            for datanode in block.datanodes:
                try:
                    channel = grpc.insecure_channel(f"{datanode.host}:{datanode.port}")
                    datanode_stub = datanode_pb2_grpc.DataNodeServiceStub(channel)
                    block_data = datanode_stub.RetrieveBlock(
                        datanode_pb2.BlockRequest(block_id=block.block_id)
                    )
                    content += block_data.content
                    break
                except grpc.RpcError:
                    continue
        
        with open(local_path, 'wb') as f:
            f.write(content)
        
        print(f"File {dfs_path} downloaded to {local_path} successfully")
        return True

    def ls(self, path="/"):
        response = self.namenode.ListFiles(
            namenode_pb2.ListRequest(path=path)
        )
        return response.files