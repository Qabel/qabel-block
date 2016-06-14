from typing import Tuple, Union, NamedTuple

import boto3
import errno
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
    def meta(self, storage_object: StorageObject):
        """Retrieve file metadata, return StorageObject with size and etag set. Return None if the object doesn't exist."""

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

    @mon.TIME_IN_TRANSFER_META.time()
    def meta(self, storage_object: StorageObject):
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            with mon.SUMMARY_S3_REQUESTS.time():
                obj = self.s3.Object(BUCKET, file_key(storage_object))
                etag, size = self._get_meta_info(obj)
                if etag is not None:
                    meta_object = storage_object._replace(size=size, etag=etag)
                    self._to_cache(meta_object)
                    return meta_object
        else:
            return cached

    @mon.TIME_IN_TRANSFER_DELETE.time()
    def delete(self, storage_object):
        obj = self.s3.Object(BUCKET, file_key(storage_object))
        with mon.SUMMARY_S3_REQUESTS.time():
            _, size = self._get_meta_info(obj)
        with mon.SUMMARY_S3_REQUESTS.time():
            obj.delete()
        return size


class LocalTransfer(AbstractTransfer):

    def __init__(self, basedir, cache):
        super().__init__(cache)
        self.basepath = Path(basedir)

    def get_size(self, storage_object: StorageObject) -> int:
        return getattr(self.meta(storage_object), 'size', 0)

    def store(self, storage_object: StorageObject) -> Tuple[StorageObject, int]:
        old_size = self.get_size(storage_object)
        if old_size is None:
            old_size = 0
        new_size = os.path.getsize(storage_object.local_file)
        target_path = self.basepath / file_key(storage_object)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Three ways we create the final file:
        try:
            # (1) just rename(2) it (fast: no data copy, but only inside the same FS)
            etag = os.stat(storage_object.local_file).st_mtime_ns
            os.rename(storage_object.local_file, str(target_path))
            # The contract is that the file still has to exist
            open(storage_object.local_file, 'wb').close()
        except OSError as os_error:
            if os_error.errno in [errno.ENOTSUP, errno.EXDEV]:
                # (2) If that didn't work, copy to temporary ...
                fd, new_file = tempfile.mkstemp(dir=str(target_path.parent))
                with open(storage_object.local_file, 'rb') as input_file, open(fd, 'wb') as output_file:
                    shutil.copyfileobj(input_file, output_file)
                #     ... and rename(2) the temporary
                new_path = Path(new_file)
                etag = new_path.stat().st_mtime_ns
                new_path.replace(target_path)
            else:
                raise
        new_object = storage_object._replace(
            local_file=str(target_path),
            size=new_size,
            etag=str(etag))
        self._to_cache(new_object)
        return new_object, new_size - old_size

    def retrieve(self, storage_object: StorageObject) -> StorageObject:
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            pass
        else:
            if cached.etag == storage_object.etag:
                return storage_object._replace(fd=None)

        path = self.basepath / file_key(storage_object)
        try:
            st = path.stat()
        except FileNotFoundError:
            return None
        object = storage_object._replace(size=st.st_size, etag=str(st.st_mtime_ns))
        if storage_object.etag == object.etag:
            return storage_object._replace(fd=None)
        else:
            return object._replace(fd=path.open('rb'))

    def meta(self, storage_object: StorageObject):
        try:
            cached = self._from_cache(storage_object)
        except KeyError:
            path = self.basepath / file_key(storage_object)
            try:
                st = path.stat()
            except FileNotFoundError:
                return None
            object = storage_object._replace(size=st.st_size, etag=str(st.st_mtime_ns))
            self._to_cache(object)
            return object
        else:
            return cached

    def delete(self, storage_object: StorageObject):
        path = self.basepath / file_key(storage_object)
        try:
            st = path.stat()
        except FileNotFoundError:
            return 0
        try:
            path.unlink()
        except OSError:
            return 0  # raced delete
        return st.st_size
