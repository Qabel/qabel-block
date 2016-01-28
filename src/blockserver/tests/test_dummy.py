from blockserver.backends import dummy


def test_basic(testfile):
    t = dummy.Transfer()
    t.store('foo', 'bar', testfile)
    assert t.retrieve('foo', 'bar') == testfile
