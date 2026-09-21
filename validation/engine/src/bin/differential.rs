use std::collections::{BTreeMap, HashMap};
use bm25_turbo::{BM25Builder, BM25Params, scoring};

#[derive(Default)]
struct PostingProbe {
    docs: BTreeMap<u32, Vec<String>>,
    postings: HashMap<String, BTreeMap<u32, u32>>,
    total_tokens: usize,
}

impl PostingProbe {
    fn replace(&mut self, id: u32, tokens: Option<Vec<String>>) {
        if let Some(old) = self.docs.remove(&id) {
            self.total_tokens -= old.len();
            for term in old {
                if let Some(postings) = self.postings.get_mut(&term) {
                    postings.remove(&id);
                }
            }
        }
        if let Some(tokens) = tokens {
            self.total_tokens += tokens.len();
            for term in &tokens {
                *self.postings.entry(term.clone()).or_default().entry(id).or_default() += 1;
            }
            self.docs.insert(id, tokens);
        }
    }

    fn scores(&self, query: &[String]) -> BTreeMap<u32, f32> {
        let mut scores = BTreeMap::new();
        if self.docs.is_empty() { return scores; }
        let p = BM25Params::default();
        let avg = self.total_tokens as f32 / self.docs.len() as f32;
        for term in query {
            if let Some(postings) = self.postings.get(term) {
                for (&id, &tf) in postings {
                    *scores.entry(id).or_default() += scoring::score(
                        p.method, tf as f32, self.docs[&id].len() as f32, avg,
                        self.docs.len() as u32, postings.len() as u32, p.k1, p.b, p.delta);
                }
            }
        }
        scores
    }
}

fn main() {
    let mut probe = PostingProbe::default();
    let mut comparisons = 0;
    let mut max_error = 0.0_f32;
    for step in 0..120_u32 {
        let id = step % 19;
        let tokens = if step % 7 == 0 { None } else {
            Some((0..(step % 23 + 1)).map(|n| format!("term{}", (step+n*n)%11)).collect())
        };
        probe.replace(id,tokens);
        if probe.docs.is_empty() { continue; }
        let ids: Vec<_> = probe.docs.keys().copied().collect();
        let tokens: Vec<_> = probe.docs.values().cloned().collect();
        let fresh = BM25Builder::new().build_from_tokens(&tokens).unwrap();
        for q in 0..11 {
            let query = vec![format!("term{q}"), format!("term{}", (q+3)%11)];
            let expected = fresh.search_tokens(&query,ids.len()).unwrap();
            let actual = probe.scores(&query);
            assert_eq!(actual.len(),expected.doc_ids.len());
            for (&doc,&score) in expected.doc_ids.iter().zip(&expected.scores) {
                let error = (actual[&ids[doc as usize]]-score).abs();
                max_error = max_error.max(error);
                assert!(error<1e-6,"differential score mismatch: {error}");
            }
            comparisons+=1;
        }
    }
    println!("{}",serde_json::json!({"check":"raw_postings_with_live_statistics_match_fresh_bm25","passed":true,"update_steps":120,"query_comparisons":comparisons,"max_absolute_score_error":max_error,"scope":"Synthetic algorithm probe reusing scoring kernels; not a production index, performance benchmark, persistence, or concurrency test."}));
}
