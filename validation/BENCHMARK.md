# Differential indexing benchmark

Run from the new project directory:

```sh
python3 validation/prepare-benchmark.py
python3 validation/run-benchmark.py
```

The Rust validation crate depends on the sibling BM25 Turbo checkout. Preparation
reads repositories without changing branches or source files. It writes temporary
JSONL corpora containing repository text; remove those exact files listed in the
manifest after benchmarking. The retained reports contain measurements, not source
text. Regenerating a corpus measures the then-current working tree.

The benchmark compares a full precomputed BM25 rebuild from cached tokens with
updates to a prototype raw-postings index using the existing BM25 scoring kernels.
It uses fixed 60-line chunks and the default tokenizer, not a finalized code parser.
Files over 2 MiB and chunks over 64 KiB are excluded to bound this experiment.
Strict ignore filtering applies even to tracked files. Text filtering accepts
UTF-8 without binary control bytes; this is not universal format detection.

Each batch selects files deterministically, appends two synthetic tokens to their
chunks, and deletes every fifth affected chunk. It measures one-file, ten-file,
and approximately ten-percent-of-files batches. These are simulations over actual
repository text, not recorded branch changes. Each update scenario has three
timed repetitions. The update timings exclude constructing the starting index,
preparing edited tokens, and destroying the result.

Query timings use ten fixed lexical queries repeated six times and top ten
results. Every measured query compares result identities and scores between the
two strategies. They establish equivalence for this sample, not search relevance.
The run uses release optimization on macOS ARM64. Timing is in-process and warm;
there is no independent process repetition or randomized algorithm order.

These measurements exclude watcher latency, changed-file reads, parsing,
durable commits, coherent concurrent snapshot publication, and source verification
before serving results. Full-rebuild time includes token cloning required by the
current builder API. Initial-build timings have one sample. Any captured process
peak memory includes both representations, source tokens, and validation copies;
it cannot compare the individual indexes' memory usage.

Raw JSONL results and the corpus manifest are the measurement evidence. An update
speedup in this probe is not an end-to-end freshness guarantee or Windows evidence.
