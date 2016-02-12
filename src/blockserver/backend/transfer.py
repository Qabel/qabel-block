from typing import Tuple, Union, NamedTuple

import boto3
import tempfile
import random
import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from botocore.exceptions import ClientError

StorageObject = NamedTuple('StorageObject',
                           [('prefix', str), ('file_path', str),
                            ('etag', str), ('local_file', str),
                            ('size', int)])

StorageObject.__new__.__defaults__ = (None,) * len(StorageObject._fields)


def file_key(storage_object: StorageObject):
    return '{}/{}'.format(storage_object.prefix, storage_object.file_path)

REGION = 'eu-west-1'
BUCKET = 'qabel'


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


class S3Transfer(AbstractTransfer):
    def __init__(self, cache):
        super().__init__(cache)
        self.s3 = boto3.resource('s3')

    def store(self, storage_object: StorageObject):
        obj = self.s3.Object(BUCKET, file_key(storage_object))
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            size = self.get_size(obj)
        else:
            size = cached.size

        new_size = os.path.getsize(storage_object.local_file)

        with open(storage_object.local_file, 'rb') as f_in:
            response = obj.put(Body=f_in)
            size_diff = new_size - size
            new_object = storage_object._replace(etag=response['ETag'], size=new_size)
            self._to_cache(new_object)
            return new_object, size_diff

    def get_size(self, obj):
        try:
            return obj.content_length
        except ClientError as e:
            status = e.response['ResponseMetadata']['HTTPStatusCode']
            if status == 404:
                return 0
            else:
                raise

    def retrieve(self, storage_object: StorageObject):
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            pass
        else:
            if cached.etag == storage_object.etag:
                return storage_object._replace(local_file=None)
        obj = self.s3.Object(BUCKET, file_key(storage_object))
        try:
            if storage_object.etag:
                response = obj.get(IfNoneMatch=storage_object.etag)
            else:
                response = obj.get()
        except ClientError as e:
            status = e.response['ResponseMetadata']['HTTPStatusCode']
            if status == 304:
                return storage_object._replace(local_file=None)
            else:
                return None

        with tempfile.NamedTemporaryFile('wb', delete=False) as temp:
            streaming_body = response['Body']
            for chunk in iter(lambda: streaming_body.read(8192), b''):
                temp.write(chunk)
            return storage_object._replace(local_file=temp.name, etag=response['ETag'])

    def delete(self, storage_object):
        obj = self.s3.Object(BUCKET, file_key(storage_object))
        size = self.get_size(obj)
        obj.delete()
        return size


files = {}


class DummyTransfer(AbstractTransfer):

    def __init__(self, cache):
        super().__init__(cache)
        self._tempdir = tempfile.mkdtemp()

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
        new_path = os.path.join(self._tempdir, file_key(storage_object))
        dirname = os.path.dirname(new_path)
        os.makedirs(dirname, exist_ok=True)
        if new_size != 0:
            shutil.copyfile(storage_object.local_file, new_path)
        else:
            Path(new_path).touch()
        new_object = storage_object._replace(
                etag=str(random.randint(1, 20000)), size=new_size, local_file=new_path)
        self._to_cache(new_object)
        files[file_key(storage_object)] = new_object
        return new_object, new_size - old_size

    def retrieve(self, storage_object: StorageObject) -> StorageObject:
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            pass
        else:
            if cached.etag == storage_object.etag:
                return storage_object._replace(local_file=None)

        object = files.get(file_key(storage_object), None)
        if object is None:
            return None
        if storage_object.etag == object.etag:
            return storage_object._replace(local_file=None)
        else:
            return object

    def delete(self, storage_object: StorageObject):
        return os.path.getsize(files.pop(file_key(storage_object)).local_file)


