# Upstream attribution

`scoring.rs` is a reviewed adaptation of the pure Lucene BM25 scoring kernel
from [BM25-Turbo-Rust-Python-WASM-CLI](https://github.com/alessandrobenigni/BM25-Turbo-Rust-Python-WASM-CLI),
revision `ecd28e3babb57cce63636f099a72ba26dc4cf643`.

The source project is distributed under the GNU Affero General Public License
version 3, and this adaptation is part of the AGPL-3.0-only crate. The copied
work was narrowed to Lucene IDF and term-frequency arithmetic; store-specific
SQLite persistence, numeric postings, query accumulation, and versioned text
normalization are original code in this project. The complete license text is
in the crate's top-level `LICENSE`, with the adaptation summary in
`THIRD_PARTY_NOTICES.md`.
