//! Adapted Lucene BM25 scoring kernel from BM25 Turbo.
//!
//! The upstream implementation supports five variants and a configurable
//! tokenizer.  The durable store deliberately selects its fixed Lucene
//! parameters, so only the pure scoring functions needed by this crate are
//! carried here.  The arithmetic order is retained to keep fresh-build and
//! durable-index scores comparable across platforms.
//!
//! Source: https://github.com/alessandrobenigni/BM25-Turbo-Rust-Python-WASM-CLI
//! Revision: ecd28e3babb57cce63636f099a72ba26dc4cf643
//! License: AGPL-3.0-only (same as the source repository).

/// Lucene's inverse-document-frequency term.
#[inline]
pub(crate) fn lucene_idf(num_docs: u64, doc_freq: u64) -> f32 {
    let n = num_docs as f64;
    let df = doc_freq as f64;
    (1.0 + (n - df + 0.5) / (df + 0.5)).ln() as f32
}

/// Lucene term-frequency normalization, with the upstream defaults k1=1.5 and
/// b=0.75 used by the production index.
#[inline]
pub(crate) fn lucene_tfc(tf: u32, doc_len: u64, avg_doc_len: f64) -> f32 {
    if avg_doc_len <= 0.0 {
        return 0.0;
    }
    let tf = tf as f64;
    let doc_len = doc_len as f64;
    let k1 = 1.5_f64;
    let b = 0.75_f64;
    let ratio = doc_len / avg_doc_len;
    let b_ratio = b * ratio;
    let norm = 1.0 - b + b_ratio;
    (tf / (k1 * norm + tf)) as f32
}

#[inline]
pub(crate) fn lucene_score(
    tf: u32,
    doc_len: u64,
    avg_doc_len: f64,
    num_docs: u64,
    doc_freq: u64,
) -> f32 {
    let idf = lucene_idf(num_docs, doc_freq) as f64;
    let tfc = lucene_tfc(tf, doc_len, avg_doc_len) as f64;
    (idf * tfc) as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lucene_reference_value() {
        let got = lucene_score(3, 100, 80.0, 1000, 100);
        let idf = (1.0_f64 + (1000.0 - 100.0 + 0.5) / (100.0 + 0.5)).ln();
        let norm = 1.0_f64 - 0.75 + 0.75 * (100.0 / 80.0);
        let tfc = 3.0_f64 / (1.5 * norm + 3.0);
        assert_eq!(got.to_bits(), (idf as f32 * tfc as f32).to_bits());
    }
}
