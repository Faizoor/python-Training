import os
import sys
import pytest

# Ensure the lab folder is on sys.path so tests can import modules directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lab7_exceptions import validate_row, DataValidationError

@pytest.fixture
def sample_row():
    return {'id': 1, 'value': 42}

def test_validate_ok(sample_row):
    assert validate_row(sample_row) is None

@pytest.mark.parametrize('bad_row', [{}, {'value': 1}, 'not-a-dict'])
def test_validate_raises(bad_row):
    with pytest.raises(DataValidationError):
        validate_row(bad_row)
