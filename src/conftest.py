import pytest
import tempfile
import os


@pytest.yield_fixture
def testfile():
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        temp.write(b'Dummy\n')
        yield temp.name
    os.remove(temp.name)


def pytest_addoption(parser):
    parser.addoption("--dummy", action="store_true",
        help="run only with the dummy backend")


def pytest_generate_tests(metafunc):
    if 'backend' in metafunc.fixturenames:
        backends = ['dummy']
        if not metafunc.config.option.dummy:
            backends.append('s3')
        metafunc.parametrize("backend", backends, indirect=True)
