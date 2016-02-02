from collections import namedtuple


class StorageObject(namedtuple('StorageObject', ['prefix', 'file_path', 'etag', 'local_file'])):
    pass