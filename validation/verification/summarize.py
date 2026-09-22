#!/usr/bin/env python3
"""Summarize the local benchmark logs without selecting individual fast trials."""
import json
import re
import statistics
from pathlib import Path

root = Path(__file__).parent
output = {}
for name in ['baseline', 'cloning', 'paired-head', 'paired-64k', 'paired-16k', 'statistics-final', 'concurrent-head', 'concurrent-final']:
    path = root / (name + '.log')
    groups = {}
    for line in path.read_text().splitlines():
        if 'concurrent trial=' in line:
            line = line[line.index('concurrent trial='):]
        elif 'stats_import unique=' in line:
            line = line[line.index('stats_import unique='):]
        elif 'cold records=' in line:
            line = line[line.index('cold records='):]
        if line.startswith(('prefix=', 'cold ', 'append ', 'stats ', 'concurrent ')):
            key = re.sub(r' trial=\d+.*', '', line)
            values = re.findall(r'(elapsed_us|invalidate_us|restore_us|idle_p95_us|active_p95_us|active_max_us|import_us)=(\d+)', line)
            if key == 'concurrent':
                key = 'concurrent search'
            for metric, value in values:
                groups.setdefault(key + '/' + metric, []).append(int(value))
    output[name] = {
        key: {'n': len(values), 'median_us': statistics.median(values), 'min_us': min(values), 'max_us': max(values)}
        for key, values in groups.items()
    }
(root / 'summary.json').write_text(json.dumps(output, indent=2) + '\n')
print(json.dumps(output, indent=2))
