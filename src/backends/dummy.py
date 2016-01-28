import tempfile


class Transfer:

    def __init__(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(b'Dummy\n')
            self.dummy = tmp.name

    def store_file(self, prefix, file_path, file):
        pass

    def retrieve_file(self, prefix, file_path):
        return self.dummy
