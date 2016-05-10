from typing import Tuple, Union, NamedTuple

import boto3
import tempfile
import random
import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from botocore.exceptions import ClientError

from blockserver import monitoring as mon

StorageObject = NamedTuple('StorageObject',
                           [('prefix', str), ('file_path', str),
                            ('etag', str), ('local_file', str),
                            ('size', int), ('fd', object)])

StorageObject.__new__.__defaults__ = (None,) * len(StorageObject._fields)


def file_key(storage_object: StorageObject):
    return '{}/{}'.format(storage_object.prefix, storage_object.file_path)

REGION = 'eu-west-1'
BUCKET = 'qabel'


class AbstractTransfer(ABC):

    def __init__(self, cache):
        self.cache = cache

    def _from_cache(self, storage_object: StorageObject) -> Union[StorageObject, None]:
        return self.cache.get_storage(storage_object)

    def _to_cache(self, storage_object: StorageObject) -> Union[StorageObject, None]:
        self.cache.set_storage(storage_object)

    @abstractmethod
    def get_size(self, storage_object: StorageObject) -> int:
        pass

    @abstractmethod
    def store(self, storage_object: StorageObject) -> Tuple[StorageObject, int]:
        pass

    @abstractmethod
    def retrieve(self, storage_object: StorageObject) -> Union[StorageObject, None]:
        """Retrieve file, returns StorageObject with file-like StorageObject.fd"""

    @abstractmethod
    def delete(self, storage_object: StorageObject) -> int:
        pass


class S3Transfer(AbstractTransfer):
    def __init__(self, cache):
        super().__init__(cache)
        self.s3 = boto3.resource('s3')

    def get_size(self, storage_object: StorageObject) -> int:
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            with mon.SUMMARY_S3_REQUESTS.time():
                obj = self.s3.Object(BUCKET, file_key(storage_object))
                etag, size = self._get_meta_info(obj)
                if etag is not None:
                    self._to_cache(storage_object._replace(size=size, etag=etag))
        else:
            return cached.size

    @mon.TIME_IN_TRANSFER_STORE.time()
    def store(self, storage_object: StorageObject):
        obj = self.s3.Object(BUCKET, file_key(storage_object))
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            with mon.SUMMARY_S3_REQUESTS.time():
                _, size = self._get_meta_info(obj)
        else:
            size = cached.size

        new_size = os.path.getsize(storage_object.local_file)

        with open(storage_object.local_file, 'rb') as f_in:
            with mon.SUMMARY_S3_REQUESTS.time():
                response = obj.put(Body=f_in)
            size_diff = new_size - size
            new_object = storage_object._replace(etag=response['ETag'], size=new_size)
            self._to_cache(new_object)
            return new_object, size_diff

    def _get_meta_info(self, obj):
        try:
            return obj.e_tag, obj.content_length
        except ClientError as e:
            status = e.response['ResponseMetadata']['HTTPStatusCode']
            if status == 404:
                return None, 0
            else:
                raise

    @mon.TIME_IN_TRANSFER_RETRIEVE.time()
    def retrieve(self, storage_object: StorageObject):
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            pass
        else:
            if cached.etag == storage_object.etag:
                return storage_object._replace(fd=None)
        obj = self.s3.Object(BUCKET, file_key(storage_object))
        with mon.SUMMARY_S3_REQUESTS.time():
            try:
                if storage_object.etag:
                    response = obj.get(IfNoneMatch=storage_object.etag)
                else:
                    response = obj.get()
            except ClientError as e:
                status = e.response['ResponseMetadata']['HTTPStatusCode']
                if status == 304:
                    return storage_object._replace(fd=None)
                else:
                    return None
            size = response['ContentLength']
            return storage_object._replace(fd=response['Body'], etag=response['ETag'], size=size)

    @mon.TIME_IN_TRANSFER_DELETE.time()
    def delete(self, storage_object):
        obj = self.s3.Object(BUCKET, file_key(storage_object))
        with mon.SUMMARY_S3_REQUESTS.time():
            _, size = self._get_meta_info(obj)
        with mon.SUMMARY_S3_REQUESTS.time():
            obj.delete()
        return size


files = {}


class DummyTransfer(AbstractTransfer):

    def __init__(self, cache):
        super().__init__(cache)
        self._tempdir = tempfile.mkdtemp()

    def get_size(self, storage_object: StorageObject) -> int:
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            try:
                return os.path.getsize(
                    files[file_key(storage_object)].local_file)
            except KeyError:
                return None
        else:
            return cached.size

    def store(self, storage_object: StorageObject) -> Tuple[StorageObject, int]:
        old_size = self.get_size(storage_object)
        if old_size is None:
            old_size = 0
        new_size = os.path.getsize(storage_object.local_file)
        new_path = os.path.join(self._tempdir, file_key(storage_object))
        dirname = os.path.dirname(new_path)
        os.makedirs(dirname, exist_ok=True)
        if new_size != 0:
            shutil.copyfile(storage_object.local_file, new_path)
        else:
            Path(new_path).touch()
        new_object = storage_object._replace(
            local_file=new_path,
            size=new_size,
            etag=str(random.randint(1, 20000)))
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
                return storage_object._replace(fd=None)

        try:
            object = files[file_key(storage_object)]
        except KeyError:
            return None
        if storage_object.etag == object.etag:
            return storage_object._replace(fd=None)
        else:
            return object._replace(fd=open(object.local_file, 'rb'))

    def delete(self, storage_object: StorageObject):
        try:
            return os.path.getsize(files.pop(file_key(storage_object)).local_file)
        except KeyError:
            return 0
