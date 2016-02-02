from blockserver.backends.s3 import Transfer
from blockserver.backends.util import StorageObject


def test_basic(testfile):
    t = Transfer()
    storage_object = StorageObject('foo', 'bar', None, testfile)
    uploaded = t.store(storage_object)
    assert uploaded.etag is not None
    downloaded = t.retrieve(StorageObject('foo', 'bar', None, None))
    assert uploaded.etag == downloaded.etag
