from typing import Dict


class ValidationError(Exception):
    pass


def validate_record(rec: Dict) -> bool:
    if "id" not in rec:
        raise ValidationError("id missing")
    if not isinstance(rec["id"], int):
        raise ValidationError("id must be int")
    return True


def transform_record(rec: Dict) -> Dict:
    validate_record(rec)
    return {"id": rec["id"], "value": rec.get("value", 0) * 10}
