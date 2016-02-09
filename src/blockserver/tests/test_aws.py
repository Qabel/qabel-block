from blockserver.backends.s3 import S3Transfer
from blockserver.backends.util import StorageObject
import os


def test_basic(testfile):
    t = S3Transfer()
    size = os.path.getsize(testfile)
    storage_object = StorageObject('foo', 'bar', local_file=testfile)
    uploaded, size_diff = t.store(storage_object)
    assert size_diff == size
    assert uploaded.etag is not None
    downloaded = t.retrieve(StorageObject('foo', 'bar'))
    assert uploaded.etag == downloaded.etag
    delete_size = t.delete(storage_object)
    assert size == delete_size
