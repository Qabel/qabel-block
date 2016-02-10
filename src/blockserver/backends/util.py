from typing import Union, NamedTuple, Tuple
from abc import ABC, abstractmethod

StorageObject = NamedTuple('StorageObject',
                           [('prefix', str), ('file_path', str),
                            ('etag', str), ('local_file', str),
                            ('size', int)])
StorageObject.__new__.__defaults__ = (None, ) * len(StorageObject._fields)


class AbstractTransfer(ABC):

    def __init__(self, cache):
        self.cache = cache

    def _from_cache(self, storage_object: StorageObject) -> Union[StorageObject, None]:
        return self.cache.get(storage_object)

    def _to_cache(self, storage_object: StorageObject) -> Union[StorageObject, None]:
        self.cache.set(storage_object)

    @abstractmethod
    def store(self, storage_object: StorageObject) -> Tuple[StorageObject, int]:
        pass

    @abstractmethod
    def retrieve(self, storage_object: StorageObject) -> Union[StorageObject, None]:
        pass

    @abstractmethod
    def delete(self, storage_object: StorageObject) -> int:
        pass


def file_key(storage_object: StorageObject):
    return '{}/{}'.format(storage_object.prefix, storage_object.file_path)
