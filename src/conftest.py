import pytest
import tempfile
import os


@pytest.yield_fixture
def testfile():
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        temp.write(b'Dummy\n')
        temp.close
        yield temp.name
    os.remove(temp.name)
