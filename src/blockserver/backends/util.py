from typing import Union, NamedTuple
from abc import ABC, abstractmethod


StorageObject = NamedTuple('StorageObject',
                           [('prefix', str), ('file_path', str),
                            ('etag', str), ('local_file', str)])


class Transfer:

    @abstractmethod
    def store(self, storage_object: StorageObject) -> StorageObject:
        pass

    @abstractmethod
    def retrieve(self, storage_object: StorageObject) -> Union[StorageObject, None]:
        pass

    @abstractmethod
    def delete(self, storage_object: StorageObject) -> int:
        pass

