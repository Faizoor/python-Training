class DataValidationError(Exception):
    pass

class ExternalServiceError(Exception):
    pass

def validate_row(row):
    if not isinstance(row, dict):
        raise DataValidationError('row must be dict')
    if 'id' not in row:
        raise DataValidationError('missing id')

def call_external(sim_fail=False):
    if sim_fail:
        raise ExternalServiceError('service unavailable')
    return {'ok': True}

if __name__ == '__main__':
    try:
        validate_row({'id': 1})
        print('validate ok')
    except DataValidationError as e:
        print('validation failed', e)
