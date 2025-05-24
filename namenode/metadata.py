import json
import os

class MetadataStore:
    def __init__(self):
        self.metadata_file = "metadata.json"
        self.metadata = {
            "files": {},
            "blocks": {}
        }
        self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file) as f:
                self.metadata = json.load(f)

    def _save_metadata(self):
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f)

    def add_file(self, filename, blocks):
        self.metadata["files"][filename] = {
            "blocks": list(blocks.keys())
        }
        for block_id, datanodes in blocks.items():
            self.metadata["blocks"][block_id] = datanodes
        self._save_metadata()

    def get_file_blocks(self, filename):
        if filename not in self.metadata["files"]:
            return {}
        
        return {
            block_id: self.metadata["blocks"][block_id]
            for block_id in self.metadata["files"][filename]["blocks"]
        }

    def list_files(self, path="/"):
        return [f for f in self.metadata["files"].keys() if f.startswith(path)]