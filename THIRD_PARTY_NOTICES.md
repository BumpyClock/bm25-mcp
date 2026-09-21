# Third-party notices

## BM25 Turbo

This project includes a reviewed adaptation of the pure Lucene BM25 scoring
kernel from [BM25-Turbo-Rust-Python-WASM-CLI](https://github.com/alessandrobenigni/BM25-Turbo-Rust-Python-WASM-CLI),
revision `ecd28e3babb57cce63636f099a72ba26dc4cf643`.

The adapted source is `src/upstream/scoring.rs`. It is narrowed to Lucene IDF
and term-frequency arithmetic with the upstream arithmetic order preserved.
The durable SQLite store, numeric postings, query accumulator, and versioned
normalizer are project code. The upstream project and this adaptation are
licensed under AGPL-3.0-only; the complete license text is in `LICENSE`.

Rust dependency versions are pinned in Cargo.lock. Their licenses remain
those declared by the upstream packages in Cargo registry metadata.

The complete core crate is also pinned under `vendor/bm25_turbo` for fresh-build
correctness-oracle tests and the historical validation harness. Its Rust source
is unchanged; its Cargo workspace metadata is localized for an independent
build. The live server does not use the upstream WAL.
