import tempfile

from blockserver.backends.s3 import file_key

class Transfer:

    def __init__(self):
        self.files = {}

    def store(self, prefix, file_path, file):
        self.files[file_key(prefix, file_path)] = file

    def retrieve(self, prefix, file_path):
        return self.files.get(file_key(prefix, file_path), None)
