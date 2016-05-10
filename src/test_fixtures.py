
import os
import tempfile

import pytest


def test_temp_check_basic(temp_check):
    temp = tempfile.NamedTemporaryFile()
    with pytest.raises(AssertionError):
        temp_check.assert_clean()
    temp.close()


def test_temp_check_empty_block(temp_check):
    with pytest.raises(AssertionError):
        temp_check.assert_clean()

    with pytest.raises(AssertionError):
        with temp_check:
            pass


def test_temp_check_context_manager(temp_check):
    with pytest.raises(AssertionError):
        with temp_check:
            tempfile.NamedTemporaryFile()
    with temp_check:
        tempfile.NamedTemporaryFile().close()


def test_temp_check_deleted(temp_check):
    temp = tempfile.NamedTemporaryFile(delete=False)
    temp.close()
    os.unlink(temp.name)
    temp_check.assert_clean()
