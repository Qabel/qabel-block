from blockserver.backends.s3 import file_key
from blockserver.backends.util import StorageObject
import random

files = {}


class Transfer:

    def __init__(self):
        pass

    def store(self, storage_object: StorageObject) -> StorageObject:
        new_object = storage_object._replace(etag=str(random.randint(1, 20000)))
        files[file_key(storage_object)] = new_object
        return new_object

    def retrieve(self,  storage_object: StorageObject) -> StorageObject:
        object = files.get(file_key(storage_object), None)
        if object is None:
            return None
        if storage_object.etag == object.etag:
            return storage_object
        else:
            return object

    def delete(self,  storage_object: StorageObject):
        files[file_key(storage_object)] = None
