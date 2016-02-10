from blockserver.backends.util import StorageObject, file_key, AbstractTransfer
from typing import Tuple
import random
import os

files = {}


class DummyTransfer(AbstractTransfer):

    def store(self, storage_object: StorageObject) -> Tuple[StorageObject, int]:
        old_size = 0
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            try:
                old_size = os.path.getsize(
                        files[file_key(storage_object)].local_file)
            except KeyError:
                pass
        else:
            old_size = cached.size
        new_size = os.path.getsize(storage_object.local_file)
        new_object = storage_object._replace(
                etag=str(random.randint(1, 20000)), size=new_size)
        self._to_cache(new_object)
        files[file_key(storage_object)] = new_object
        return new_object, new_size - old_size

    def retrieve(self,  storage_object: StorageObject) -> StorageObject:
        cached = self._from_cache(storage_object)
        if cached and cached.etag == storage_object.etag:
            return storage_object._replace(local_file=None)

        object = files.get(file_key(storage_object), None)
        if object is None:
            return None
        if storage_object.etag == object.etag:
            return storage_object._replace(local_file=None)
        else:
            return object

    def delete(self,  storage_object: StorageObject):
        return os.path.getsize(files.pop(file_key(storage_object)).local_file)
