import pytest
from transform import validate_record, transform_record, ValidationError


@pytest.mark.parametrize("rec,expected", [
    ({"id": 1, "value": 2}, {"id": 1, "value": 20}),
    ({"id": 2}, {"id": 2, "value": 0}),
])
def test_transform_valid(rec, expected):
    assert transform_record(rec) == expected


def test_validate_raises_on_missing_id():
    with pytest.raises(ValidationError):
        validate_record({})


def test_validate_raises_on_wrong_type():
    with pytest.raises(ValidationError):
        validate_record({"id": "x"})


@pytest.fixture
def sample_rec():
    return {"id": 5, "value": 3}


def test_transform_with_fixture(sample_rec):
    assert transform_record(sample_rec)["value"] == 30
