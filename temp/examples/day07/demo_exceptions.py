class PipelineError(Exception):
    pass


class ValidationError(PipelineError):
    pass


class DependencyError(PipelineError):
    pass


class TransientError(PipelineError):
    pass


def validate_record(record):
    if "id" not in record:
        raise ValidationError("Missing id")
    if record.get("value") is None:
        raise DependencyError("Dependent enrichment missing")
    if record.get("id") == "temp-fail":
        raise TransientError("Temporary validation hiccup")
    return True


def run_pipeline(records):
    for r in records:
        try:
            validate_record(r)
            print("Processed", r)
        except ValidationError as e:
            print("Validation failed, skipping record:", e)
        except TransientError as e:
            print("Transient; should retry but skipping in demo:", e)
        except DependencyError as e:
            print("External dependency issue — escalate:", e)


if __name__ == "__main__":
    inputs = [{"id": 1, "value": 10}, {"value": 2}, {"id": "temp-fail", "value": 3}, {"id": 4}]
    run_pipeline(inputs)
