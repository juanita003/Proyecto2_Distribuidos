import json
import os

class MetadataStore:
    def __init__(self):
        # Guardar metadata en el directorio namenode
        self.metadata_file = os.path.join(os.path.dirname(__file__), "metadata.json")
        self.metadata = {
            "files": {},
            "blocks": {}
        }
        self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file) as f:
                    self.metadata = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                # Si hay error al cargar, inicializar con metadata vacía
                self.metadata = {
                    "files": {},
                    "blocks": {}
                }

    def _save_metadata(self):
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f, indent=2)
        except Exception as e:
            print(f"Error saving metadata: {e}")

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
            if block_id in self.metadata["blocks"]
        }

    def list_files(self, path="/"):
        return [f for f in self.metadata["files"].keys() if f.startswith(path)]