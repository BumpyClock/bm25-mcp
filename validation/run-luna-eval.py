#!/usr/bin/env python3
"""Run isolated evaluation partitions with the existing sibling harness."""
import importlib.util
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
spec = importlib.util.spec_from_file_location('siblings', ROOT / 'scripts/evaluate-siblings.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
m.PRIVATE = ROOT / 'validation/private-cache/luna-eval'
if sys.argv[1] == 'prepare':
    m.prepare()
else:
    import json
    partition = sys.argv[1]
    projects = json.loads((m.PRIVATE / 'partitions.json').read_text())[partition]
    m.evaluate(ROOT / 'validation/bin/bm25-mcp-luna-eval', 'luna-' + partition, 1, only=projects, timeout=900)
