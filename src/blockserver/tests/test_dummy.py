from blockserver.backends import dummy
from blockserver.backends.util import StorageObject
import os


def test_basic(testfile):
    t = dummy.DummyTransfer()
    storage_object = StorageObject('foo', 'bar', local_file=testfile)
    size = os.path.getsize(testfile)
    uploaded, size_diff = t.store(storage_object)
    assert isinstance(uploaded.etag, str)
    downloaded = t.retrieve(StorageObject('foo', 'bar'))
    assert downloaded.etag is not None
    assert isinstance(uploaded.etag, str)
    assert uploaded == downloaded
    delete_size = t.delete(storage_object)
    assert size == delete_size
