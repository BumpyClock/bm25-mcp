#!/usr/bin/env python3
"""Run the release benchmark against prepared temporary corpora."""
import json
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parent
manifest = json.loads((root / 'corpus-manifest.json').read_text())
subprocess.run([
    'cargo', 'build', '--release', '--manifest-path',
    str(root / 'engine/Cargo.toml'), '--bin', 'benchmark', '--quiet',
], check=True)
binary = root / 'engine/target/release/benchmark'
for corpus in manifest['corpora']:
    source = Path(corpus['temporary_corpus'])
    if not source.is_file():
        sys.exit('Temporary corpus missing. Run prepare-benchmark.py first.')
    name = corpus['repo']
    with (root / f'benchmark-{name}.jsonl').open('w') as output:
        subprocess.run([str(binary), str(source), name], stdout=output, check=True)
    print(f'Completed {name}', flush=True)
