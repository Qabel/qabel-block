from blockserver.backends.s3 import Transfer

def test_basic(testfile):
    t = Transfer()
    t.store('foo', 'bar', testfile)
    assert open(t.retrieve('foo', 'bar'), 'rb').read() == \
           open(testfile, 'rb').read()
