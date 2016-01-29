import tempfile

from blockserver.backends.s3 import file_key

files = {}


class Transfer:

    def __init__(self):
        pass

    def store(self, prefix, file_path, file):
        files[file_key(prefix, file_path)] = file

    def retrieve(self, prefix, file_path):
        return files.get(file_key(prefix, file_path), None)
