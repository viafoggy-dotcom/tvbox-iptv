#!/usr/bin/env python3
"""Пересчитывает box/manifest.json: sha256 файлов и версию (+1, если что-то поменялось).
Запускается GitHub Action после каждого изменения в box/, можно и руками."""
import hashlib, json, os, sys

BOX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "box")
FILES = ["agg.py", "channels.py", "config.json", "run.sh"]
path = os.path.join(BOX, "manifest.json")
try:
    old = json.load(open(path))
except Exception:
    old = {"version": 0, "files": {}}
files = {f: hashlib.sha256(open(os.path.join(BOX, f), "rb").read()).hexdigest() for f in FILES}
if files == old["files"]:
    print(f"без изменений, версия {old['version']}")
    sys.exit(0)
new = {"version": old["version"] + 1, "files": files}
with open(path, "w") as f:
    json.dump(new, f, indent=1)
    f.write("\n")
print(f"версия {new['version']}")
