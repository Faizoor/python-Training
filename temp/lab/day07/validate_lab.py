import importlib
import io
import sys
import contextlib

results = []

def note(name, ok, msg=''):
    results.append((name, ok, msg))
    print(f"[{ 'OK' if ok else 'FAIL' }] {name}: {msg}")

def run_lab1():
    name = 'Lab1: decorators'
    try:
        mod = importlib.import_module('lab1_decorators')
        import random
        # force success
        random.random = lambda: 0.9
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            res = mod.fetch_data()
        ok = isinstance(res, dict) and 'rows' in res
        note(name, ok, 'fetch_data success' if ok else 'unexpected return')
    except Exception as e:
        note(name, False, str(e))

def run_lab2():
    name = 'Lab2: class retry'
    try:
        mod = importlib.import_module('lab2_class_retry')
        import random
        random.random = lambda: 1.0
        ok = mod.load_data() == 'ok'
        note(name, ok, 'load_data succeeded' if ok else 'load_data failed')
    except Exception as e:
        note(name, False, str(e))

def run_lab3():
    name = 'Lab3: dynamic connectors'
    try:
        mod = importlib.import_module('lab3_dynamic_connectors')
        C = mod.make_connector('T', 'x')
        inst = C()
        ok = getattr(C, 'source_type', None) == 'x' and inst.connect().startswith('connected')
        note(name, ok, 'connector ok' if ok else 'connector mismatch')
    except Exception as e:
        note(name, False, str(e))

def run_lab4():
    name = 'Lab4: cpu parallel'
    try:
        mod = importlib.import_module('lab4_cpu_parallel')
        inputs = list(range(0, 4))
        seq_res, _ = mod.run_sequential(inputs)
        par_res, _ = mod.run_parallel(inputs)
        ok = len(seq_res) == len(par_res) == len(inputs)
        note(name, ok, 'results lengths match' if ok else 'mismatch')
    except Exception as e:
        note(name, False, str(e))

def run_lab5():
    name = 'Lab5: asyncio'
    try:
        mod = importlib.import_module('lab5_asyncio')
        import asyncio
        res = asyncio.run(mod.run_concurrent(4))
        ok = isinstance(res, list) and len(res) == 4
        note(name, ok, 'concurrent returned 4' if ok else 'unexpected')
    except Exception as e:
        note(name, False, str(e))

def run_lab6():
    name = 'Lab6: hybrid pipeline'
    try:
        mod = importlib.import_module('lab6_hybrid_pipeline')
        import asyncio
        res = asyncio.run(mod.run_pipeline(3))
        ok = isinstance(res, list) and all('transformed' in it for it in res)
        note(name, ok, 'pipeline transformed' if ok else 'missing transformed')
    except Exception as e:
        note(name, False, str(e))

def run_lab7():
    name = 'Lab7: exceptions'
    try:
        mod = importlib.import_module('lab7_exceptions')
        ok = True
        try:
            mod.validate_row({'id':1})
        except Exception:
            ok = False
        try:
            import pytest
            with pytest.raises(mod.DataValidationError):
                mod.validate_row({})
        except Exception:
            ok = False
        note(name, ok, 'validate_row behavior' if ok else 'validation mismatch')
    except Exception as e:
        note(name, False, str(e))

def run_lab8():
    name = 'Lab8: logging'
    try:
        mod = importlib.import_module('lab8_logging')
        # Call pipeline_step to ensure no exception
        try:
            mod.pipeline_step('validator-step')
            note(name, True, 'pipeline_step ran')
        except Exception as e:
            note(name, False, str(e))
    except Exception as e:
        note(name, False, str(e))

def run_lab9():
    name = 'Lab9: retry backoff'
    try:
        mod = importlib.import_module('lab9_retry_backoff')
        ok = mod.fragile_call(should_fail=False) == 'ok'
        note(name, ok, 'fragile_call ok' if ok else 'fragile_call failed')
    except Exception as e:
        note(name, False, str(e))

def run_lab10():
    name = 'Lab10: tests present'
    try:
        import os
        ok = os.path.exists(os.path.join(os.path.dirname(__file__), 'tests', 'test_validation.py'))
        note(name, ok, 'tests found' if ok else 'tests missing')
    except Exception as e:
        note(name, False, str(e))

def main():
    run_lab1()
    run_lab2()
    run_lab3()
    run_lab4()
    run_lab5()
    run_lab6()
    run_lab7()
    run_lab8()
    run_lab9()
    run_lab10()

    ok_count = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    print('\nSummary: {}/{} checks passed'.format(ok_count, total))
    sys.exit(0 if ok_count == total else 2)

if __name__ == '__main__':
    main()
