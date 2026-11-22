"""Apply conftest._require_specific to every gap object/table found in the test modules."""
import importlib, pathlib, sys
sys.path[:0] = ["tests", "tests/lab", "tests/security"]
import conftest
bad = n = 0
def check(match, reason, where):
    global bad, n
    n += 1
    try: conftest._require_specific(match, reason)
    except ValueError as e: bad += 1; print(where, e)
def walk(o, where, depth=0):
    if depth > 6: return
    if hasattr(o, "match") and hasattr(o, "reason") and isinstance(getattr(o, "reason"), str):
        check(o.match, o.reason, where); return
    if isinstance(o, dict):
        if "match" in o and "reason" in o: check(o["match"], o["reason"], where); return
        for k, v in o.items(): walk(v, f"{where}[{k!r}]", depth+1)
    elif isinstance(o, (list, tuple)):
        for i, v in enumerate(o): walk(v, f"{where}[{i}]", depth+1)
for p in sorted(pathlib.Path("tests").glob("test_*.py")):
    m = importlib.import_module(p.stem)
    for name, val in vars(m).items():
        if name.isupper(): walk(val, f"{p.stem}.{name}", 0)
import test_access_delegation as ad
for e, rx in ad.NO_SIGNING_ERROR.items(): check(rx, e, "NO_SIGNING_ERROR")
print(f"checked {n}, too generic {bad}")
