import tempfile
from backends.s3 import file_key


files = {}


class Transfer:

    def __init__(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b'Dummy\n')
            tmp.close()
            self.store_file('000test', 'test', tmp.name)

    def store_file(self, prefix, file_path, file):
        files[file_key(prefix, file_path)] = file

    def retrieve_file(self, prefix, file_path):
        return files.get(file_key(prefix, file_path), None)
