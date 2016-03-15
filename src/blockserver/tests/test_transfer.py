from blockserver.backend.transfer import StorageObject
from blockserver.backend import transfer as transfer_module
import os


def test_basic(testfile, cache, transfer):
    t = transfer
    size = os.path.getsize(testfile)
    storage_object = StorageObject('foo', 'bar', local_file=testfile)
    t.delete(storage_object)
    uploaded, size_diff = t.store(storage_object)
    assert size_diff == size
    assert uploaded.etag is not None
    downloaded = t.retrieve(StorageObject('foo', 'bar'))
    assert uploaded.etag == downloaded.etag
    delete_size = t.delete(storage_object)
    assert size == delete_size


def test_delete_non_existing_file(testfile, cache, transfer):
    t = transfer
    storage_object = StorageObject('foo', 'bar')
    t.delete(storage_object)
    assert t.delete(storage_object) == 0


def test_cache_is_filled(testfile, cache, transfer):
    storage_object = StorageObject('foo', 'bar', local_file=testfile)
    transfer.delete(storage_object)
    uploaded, size_diff = transfer.store(storage_object)
    assert cache.get_storage(storage_object) == storage_object._replace(
        etag=uploaded.etag, size=uploaded.size)


def test_cache_for_etag(testfile, cache, transfer):
    storage_object = StorageObject('foo', 'bar', local_file=testfile)
    transfer.delete(storage_object)
    uploaded, _ = transfer.store(storage_object)
    # the request should work without connection to the backend
    # this is simulated by disabling the reference to the internal backend
    transfer_module.files = {}
    transfer.s3 = None
    assert transfer.retrieve(uploaded)


def test_get_size(testfile, cache, transfer):
    t = transfer
    size = os.path.getsize(testfile)
    assert size != 0
    storage_object = StorageObject('foo', 'bar', local_file=testfile)
    transfer.delete(storage_object)
    uploaded, size_diff = t.store(storage_object)
    uploaded_size = t.get_size(StorageObject('foo', 'bar'))
    assert size == uploaded_size
    assert uploaded.size == size


def test_get_size_does_not_corrupt_cache(cache, transfer, testfile):
    storage_object = StorageObject('foo', 'baz', local_file=testfile)
    transfer.store(storage_object)
    cache.flush()
    transfer.get_size(StorageObject('foo', 'baz'))

