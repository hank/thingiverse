import os, sys, json, pathlib

p = pathlib.Path(sys.argv[1])

for i in p.glob("*.json"):
    # print(f"Validating {i}")
    with i.open() as f: j = json.load(f)
    if not isinstance(j, dict) and not isinstance(j, list):
        print(f"VERYBAD!, {type(j).__name__}, deleting")
        i.unlink()
