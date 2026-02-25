import traceback

def run_quick():
    print('--- lab1: decorators ---')
    try:
        from lab1_decorators import fetch_data
        try:
            print('fetch_data ->', fetch_data())
        except Exception as e:
            print('fetch_data raised', e)
    except Exception:
        traceback.print_exc()

    print('\n--- lab2: class retry ---')
    try:
        from lab2_class_retry import load_data
        try:
            print('load_data ->', load_data())
        except Exception as e:
            print('load_data raised', e)
    except Exception:
        traceback.print_exc()

    print('\n--- lab3: dynamic connectors ---')
    try:
        from lab3_dynamic_connectors import make_connector
        C = make_connector('T', 'x')
        c = C()
        print('connector', c.connect(), c.read())
    except Exception:
        traceback.print_exc()

    print('\n--- lab4: cpu parallel ---')
    try:
        from lab4_cpu_parallel import run_sequential, run_parallel
        inputs = list(range(0, 6))
        _, seq_t = run_sequential(inputs)
        _, par_t = run_parallel(inputs)
        print(f'seq {seq_t:.2f}s, par {par_t:.2f}s')
    except Exception:
        traceback.print_exc()

    print('\n--- lab5: asyncio ---')
    try:
        import asyncio
        from lab5_asyncio import run_concurrent
        t0 = asyncio.get_event_loop().time() if asyncio.get_event_loop().is_running() else None
        res = asyncio.run(run_concurrent(6))
        print('async concurrent len', len(res))
    except Exception:
        traceback.print_exc()

    print('\n--- lab6: hybrid pipeline ---')
    try:
        import asyncio
        from lab6_hybrid_pipeline import run_pipeline
        res = asyncio.run(run_pipeline(4))
        print('hybrid res count', len(res))
    except Exception:
        traceback.print_exc()

    print('\n--- lab7: exceptions ---')
    try:
        from lab7_exceptions import validate_row, DataValidationError
        try:
            validate_row({'id': 1})
            print('validate ok')
        except DataValidationError as e:
            print('validation failed', e)
    except Exception:
        traceback.print_exc()

    print('\n--- lab8: logging ---')
    try:
        from lab8_logging import pipeline_step
        pipeline_step('quick-step')
    except Exception:
        traceback.print_exc()

    print('\n--- lab9: retry backoff ---')
    try:
        from lab9_retry_backoff import fragile_call
        try:
            print('fragile_call ->', fragile_call(should_fail=False))
        except Exception as e:
            print('fragile_call failed', e)
    except Exception:
        traceback.print_exc()

if __name__ == '__main__':
    run_quick()
