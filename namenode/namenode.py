import grpc
from concurrent import futures
import yaml
from proto import namenode_pb2, namenode_pb2_grpc
from metadata import MetadataStore
import logging

class NameNode(namenode_pb2_grpc.NameNodeServiceServicer):
    def __init__(self, config):
        self.config = config
        self.metadata = MetadataStore()
        self.datanodes = []
        
    def RegisterDataNode(self, request, context):
        datanode = {'host': request.host, 'port': request.port}
        if datanode not in self.datanodes:
            self.datanodes.append(datanode)
            logging.info(f"DataNode registered: {request.host}:{request.port}")
            return namenode_pb2.RegistrationResponse(success=True, message="Registration successful")
        return namenode_pb2.RegistrationResponse(success=False, message="Already registered")

    def GetBlockLocations(self, request, context):
        blocks = self.metadata.get_file_blocks(request.filename)
        block_infos = []
        for block_id, datanodes in blocks.items():
            dn_infos = [namenode_pb2.DataNodeInfo(host=dn['host'], port=dn['port']) for dn in datanodes]
            block_infos.append(namenode_pb2.BlockInfo(block_id=block_id, datanodes=dn_infos))
        return namenode_pb2.BlockLocations(blocks=block_infos)

    def CreateFile(self, request, context):
        block_size = 64 * 1024 * 1024  # 64MB blocks
        block_count = (request.size + block_size - 1) // block_size
        blocks = {}
        
        for i in range(block_count):
            block_id = f"{request.filename}_block_{i}"
            # Select datanodes for this block (simple round-robin)
            primary_dn = self.datanodes[i % len(self.datanodes)]
            replica_dn = self.datanodes[(i + 1) % len(self.datanodes)]
            blocks[block_id] = [primary_dn, replica_dn]
        
        self.metadata.add_file(request.filename, blocks)
        return namenode_pb2.FileResponse(success=True, blocks=[
            namenode_pb2.BlockInfo(block_id=bid, datanodes=[
                namenode_pb2.DataNodeInfo(host=dn['host'], port=dn['port']) for dn in dns
            ]) for bid, dns in blocks.items()
        ])

def serve():
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    namenode = NameNode(config)
    namenode_pb2_grpc.add_NameNodeServiceServicer_to_server(namenode, server)
    
    nn_config = config['namenode']
    server.add_insecure_port(f"{nn_config['host']}:{nn_config['port']}")
    server.start()
    print(f"NameNode running on {nn_config['host']}:{nn_config['port']}")
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve()