from blockserver.backends.util import StorageObject, file_key
from typing import Tuple
import random
import os

files = {}


class Transfer:

    def __init__(self):
        pass

    def store(self, storage_object: StorageObject) -> Tuple[StorageObject, int]:
        old_size = 0
        try:
            old_size = os.path.getsize(
                    files[file_key(storage_object)].local_file)
        except KeyError:
            pass
        new_size = os.path.getsize(storage_object.local_file)
        new_object = storage_object._replace(etag=str(random.randint(1, 20000)))
        files[file_key(storage_object)] = new_object
        return new_object, new_size - old_size

    def retrieve(self,  storage_object: StorageObject) -> StorageObject:
        object = files.get(file_key(storage_object), None)
        if object is None:
            return None
        if storage_object.etag == object.etag:
            return storage_object
        else:
            return object

    def delete(self,  storage_object: StorageObject):
        return os.path.getsize(files.pop(file_key(storage_object)).local_file)
