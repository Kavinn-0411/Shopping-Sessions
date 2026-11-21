#!/usr/bin/env python3
"""Compare outputs produced in OUT_ROOT/<case>/ against fixtures/<case>/expected. Usage: check_outputs.py FIXTURES OUT_ROOT"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from run_fixtures import SCHEMAS, load_actual, load_expected, rows

fx, root = sys.argv[1], sys.argv[2]
cases = sorted(d for d in os.listdir(fx) if os.path.isdir(os.path.join(fx, d)))
failed = []
for case in cases:
    ok = True
    for table, schema in SCHEMAS.items():
        exp = rows(load_expected(os.path.join(fx, case, "expected", f"{table}.csv"), schema))
        try:
            act = rows(load_actual(os.path.join(root, case, table), schema))
        except Exception as e:
            ok = False; print(f"FAIL {case} {table}: {e}"); continue
        if exp != act:
            ok = False; print(f"FAIL {case} {table}\n  expected {exp}\n  actual   {act}")
    if ok: print(f"ok   {case}")
    if not ok: failed.append(case)
print(f"\n{len(cases)-len(failed)}/{len(cases)} fixtures passed")
sys.exit(1 if failed else 0)
