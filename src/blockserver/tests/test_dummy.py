from blockserver.backends import dummy
from blockserver.backends.util import StorageObject


def test_basic(testfile):
    t = dummy.Transfer()
    storage_object = StorageObject('foo', 'bar', None, testfile)
    uploaded = t.store(storage_object)
    assert isinstance(uploaded.etag, str)
    downloaded = t.retrieve(StorageObject('foo', 'bar', None, None))
    assert downloaded.etag is not None
    assert isinstance(uploaded.etag, str)
    assert uploaded == downloaded
