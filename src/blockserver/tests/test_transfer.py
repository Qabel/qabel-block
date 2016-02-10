from blockserver.backend.transfer import StorageObject
from blockserver.backend import transfer as transfer_module
import os


def test_basic(testfile, cache, transfer):
    t = transfer
    size = os.path.getsize(testfile)
    storage_object = StorageObject('foo', 'bar', local_file=testfile)
    uploaded, size_diff = t.store(storage_object)
    assert size_diff == size
    assert uploaded.etag is not None
    downloaded = t.retrieve(StorageObject('foo', 'bar'))
    assert uploaded.etag == downloaded.etag
    delete_size = t.delete(storage_object)
    assert size == delete_size


def test_cache_is_filled(testfile, cache, transfer):
    storage_object = StorageObject('foo', 'bar', local_file=testfile)
    uploaded, size_diff = transfer.store(storage_object)
    assert cache.get(storage_object) == storage_object._replace(
        etag=uploaded.etag, size=uploaded.size)


def test_cache_for_etag(testfile, cache, transfer):
    storage_object = StorageObject('foo', 'bar', local_file=testfile)
    uploaded, _ = transfer.store(storage_object)
    # the request should work without connection to the backend
    # this is simulated by disabling the reference to the internal backend
    transfer_module.files = {}
    transfer.s3 = None
    assert transfer.retrieve(uploaded)
