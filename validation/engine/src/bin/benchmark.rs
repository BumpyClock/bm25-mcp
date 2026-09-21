use bm25_turbo::{BM25Builder, BM25Index, BM25Params, Tokenizer, scoring, selection};
use serde_json::{Value, json};
use std::{collections::{BTreeMap, HashMap}, io::{BufRead, BufReader}, time::Instant};

type Docs = BTreeMap<u32, Vec<String>>;
#[derive(Default)]
struct Compact {
    vocabulary: HashMap<String,u32>,
    postings: Vec<Vec<(u32,u32)>>,
    terms: Vec<Vec<(u32,u32)>>,
    lengths: Vec<u32>,
    total: u64,
}
impl Compact {
    fn append(&mut self,tokens:&[String]) {
        let id=self.lengths.len() as u32;
        let mut counts=HashMap::new();
        for token in tokens {
            let next=self.vocabulary.len() as u32;
            let term=*self.vocabulary.entry(token.clone()).or_insert(next);
            if term as usize==self.postings.len() {self.postings.push(Vec::new());}
            *counts.entry(term).or_insert(0_u32)+=1;
        }
        let mut terms:Vec<_>=counts.into_iter().collect();
        terms.sort_unstable();
        for &(term,tf) in &terms {self.postings[term as usize].push((id,tf));}
        self.terms.push(terms);
        self.lengths.push(tokens.len() as u32);
        self.total+=tokens.len() as u64;
    }
    fn search(&self,query:&[String])->bm25_turbo::Results {
        let mut scores=vec![0.0;self.lengths.len()];
        let p=BM25Params::default();
        for term in query {
            if let Some(&tid)=self.vocabulary.get(term) {
                let postings=&self.postings[tid as usize];
                for &(id,tf) in postings {
                    scores[id as usize]+=scoring::score(p.method,tf as f32,self.lengths[id as usize] as f32,self.total as f32/self.lengths.len() as f32,self.lengths.len() as u32,postings.len() as u32,p.k1,p.b,p.delta);
                }
            }
        }
        selection::top_k(&scores,10)
    }
}
#[derive(Default)]
struct Live {
    lengths: HashMap<u32, usize>,
    terms: HashMap<u32, HashMap<String, u32>>,
    postings: HashMap<String, HashMap<u32, u32>>,
    total: usize,
    span: usize,
}
impl Live {
    fn update(&mut self, id: u32, tokens: &[String]) {
        if let Some(old) = self.terms.remove(&id) {
            self.total -= self.lengths.remove(&id).unwrap();
            for term in old.keys() {
                let p = self.postings.get_mut(term).unwrap();
                p.remove(&id);
                if p.is_empty() { self.postings.remove(term); }
            }
        }
        if tokens.is_empty() { return; }
        self.span = self.span.max(id as usize+1);
        self.total += tokens.len();
        self.lengths.insert(id,tokens.len());
        let mut terms = HashMap::new();
        for term in tokens { *terms.entry(term.clone()).or_insert(0) += 1; }
        for (term, &tf) in &terms { self.postings.entry(term.clone()).or_default().insert(id,tf); }
        self.terms.insert(id,terms);
    }
    fn build(docs: &Docs) -> Self {
        let mut live = Self::default();
        for (&id,tokens) in docs { live.update(id,tokens); }
        live
    }
    fn search(&self, query: &[String]) -> bm25_turbo::Results {
        let mut scores = vec![0.0;self.span];
        let p = BM25Params::default();
        let avg = self.total as f32/self.lengths.len() as f32;
        for term in query {
            if let Some(postings) = self.postings.get(term) {
                for (&id,&tf) in postings {
                    scores[id as usize] += scoring::score(p.method,tf as f32,self.lengths[&id] as f32,avg,self.lengths.len() as u32,postings.len() as u32,p.k1,p.b,p.delta);
                }
            }
        }
        selection::top_k(&scores,10)
    }
}
fn rebuild(docs:&Docs) -> BM25Index {
    let tokens:Vec<_> = docs.values().cloned().collect();
    BM25Builder::new().build_from_tokens(&tokens).unwrap()
}
fn ms(t:Instant)->f64 { t.elapsed().as_secs_f64()*1000.0 }
fn stats(mut samples:Vec<f64>)->Value {
    samples.sort_by(f64::total_cmp);
    json!({"median_ms":samples[samples.len()/2],"p95_ms":samples[((samples.len() as f64*0.95).ceil() as usize-1).min(samples.len()-1)],"samples":samples.len()})
}
fn queries(index:&BM25Index,live:&Live,docs:&Docs,queries:&[Vec<String>])->Value {
    let ids:Vec<_> = docs.keys().copied().collect();
    let mut a=Vec::new(); let mut b=Vec::new();
    for i in 0..60 {
        let query=&queries[i%queries.len()];
        let t=Instant::now(); let expected=index.search_tokens(query,10).unwrap(); a.push(ms(t));
        let t=Instant::now(); let actual=live.search(query); b.push(ms(t));
        let mapped:Vec<_>=expected.doc_ids.iter().map(|&id|ids[id as usize]).collect();
        assert_eq!(mapped,actual.doc_ids,"top-k identities differ");
        for (a,b) in expected.scores.iter().zip(&actual.scores) { assert!((a-b).abs()<=1e-5*a.abs().max(1.0)); }
        std::hint::black_box(actual);
    }
    json!({"precomputed":stats(a),"raw_postings":stats(b),"top_k_equivalent":true})
}
fn main()->Result<(),Box<dyn std::error::Error>> {
    let args:Vec<_>=std::env::args().collect();
    let tokenizer=Tokenizer::default();
    if args.get(3).map(String::as_str)==Some("--memory-pair") {
        let mut indexes=Vec::new();let mut counts=Vec::new();
        for path in [&args[1],&args[4]] {
            let mut index=Compact::default();
            for line in BufReader::new(std::fs::File::open(path)?).lines() {
                let row:Value=serde_json::from_str(&line?)?;
                let tokens=tokenizer.tokenize(row["text"].as_str().unwrap());
                if !tokens.is_empty(){index.append(&tokens);}
            }
            counts.push(json!({"chunks":index.lengths.len(),"tokens":index.total,"vocabulary":index.vocabulary.len()}));
            indexes.push(index);
        }
        for index in &indexes {std::hint::black_box(index.search(&tokenizer.tokenize("request error")));}
        println!("{}",json!({"phase":"memory_pair","repo":args[2],"indexes_code_then_sessions":counts,"note":"Separate append-built code and session indexes retained together. Excludes differential generations, source text cache, SQLite cache, MCP and file watchers."}));
        return Ok(());
    }
    let memory_only=args.get(3).map(String::as_str)==Some("--memory-only");
    let memory_compact=args.get(3).map(String::as_str)==Some("--memory-compact");
    if memory_compact {
        let fixture:Vec<_>=["alpha beta alpha","beta gamma","alpha gamma gamma"].iter().map(|s|tokenizer.tokenize(s)).collect();
        let reference=BM25Builder::new().build_from_tokens(&fixture).unwrap();
        let mut check=Compact::default();
        for tokens in &fixture {check.append(tokens);}
        for text in ["alpha","gamma beta","alpha gamma"] {
            let q=tokenizer.tokenize(text);
            let a=reference.search_tokens(&q,10).unwrap(); let b=check.search(&q);
            assert_eq!(a.doc_ids,b.doc_ids); assert_eq!(a.scores,b.scores);
        }
    }
    let mut compact=Compact::default();
    let mut memory_live=Live::default();
    let mut next_id=0_u32;
    let t=Instant::now(); let mut docs=Docs::new(); let mut files:BTreeMap<u64,Vec<u32>>=BTreeMap::new();
    for line in BufReader::new(std::fs::File::open(&args[1])?).lines() {
        let row:Value=serde_json::from_str(&line?)?;
        let tokens=tokenizer.tokenize(row["text"].as_str().unwrap());
        if tokens.is_empty() {continue;}
        let id=next_id;
        next_id+=1;
        if memory_compact {
            compact.append(&tokens);
            continue;
        }
        if memory_only {
            memory_live.update(id,&tokens);
            continue;
        }
        files.entry(row["file_id"].as_u64().unwrap()).or_default().push(id);
        docs.insert(id,tokens);
    }
    let tokenize_ms=ms(t);
    if memory_compact {
        let query=tokenizer.tokenize("request response");
        std::hint::black_box(compact.search(&query));
        println!("{}",json!({"phase":"memory_compact","repo":args[2],"chunks":compact.lengths.len(),"vocabulary":compact.vocabulary.len(),"tokens":compact.total,"streaming_load_tokenize_build_ms":tokenize_ms,"note":"Term-interned numeric vectors, append-built only. Does not measure differential updates, tombstones, compaction, persistence, source text, sessions or concurrent publication."}));
        return Ok(());
    }
    if memory_only {
        let query=tokenizer.tokenize("request response");
        std::hint::black_box(memory_live.search(&query));
        println!("{}",json!({"phase":"memory_only","repo":args[2],"chunks":memory_live.lengths.len(),"vocabulary":memory_live.postings.len(),"tokens":memory_live.total,"streaming_load_tokenize_build_ms":tokenize_ms,"note":"Single raw-postings index built by streaming chunks. No full input token corpus, precomputed index or validation copies. Excludes source text cache, session history and concurrent publication."}));
        return Ok(());
    }
    let t=Instant::now(); let base=rebuild(&docs); let rebuild_ms=ms(t);
    let t=Instant::now(); let live=Live::build(&docs); let live_ms=ms(t);
    let queries_set:Vec<_>=["error","request response","config","test","timeout","initialize","async","return","function","benchmarkeditmarker"].iter().map(|q|tokenizer.tokenize(q)).collect();
    let initial_queries=queries(&base,&live,&docs,&queries_set);
    println!("{}",json!({"phase":"initial","repo":args[2],"chunks":docs.len(),"files":files.len(),"tokens":live.total,"vocabulary":live.postings.len(),"load_and_tokenize_ms":tokenize_ms,"precomputed_build_ms":rebuild_ms,"raw_postings_build_ms":live_ms,"queries":initial_queries}));
    drop(base); drop(live);
    let mut file_ids:Vec<_>=files.keys().copied().collect();
    file_ids.sort_by_key(|id| id.wrapping_mul(2654435761)%4294967296);
    for count in [1,10,(files.len()/10).max(1)] {
        let count=count.min(files.len());
        let changed:Vec<_>=file_ids.iter().take(count).flat_map(|id|files[id].iter().copied()).collect();
        let mut modified=docs.clone();
        for (i,id) in changed.iter().enumerate() {
            if i%5==4 {modified.remove(id);} else {modified.get_mut(id).unwrap().extend(["benchmarkeditmarker".into(),"request".into()]);}
        }
        let mut delta_times=Vec::new(); let mut rebuild_times=Vec::new(); let mut query_stats=Value::Null;
        for repeat in 0..3 {
            let mut live=Live::build(&docs);
            let t=Instant::now();
            for id in &changed { live.update(*id,modified.get(id).map(Vec::as_slice).unwrap_or(&[])); }
            delta_times.push(ms(t));
            let t=Instant::now(); let index=rebuild(&modified); rebuild_times.push(ms(t));
            if repeat==2 { query_stats=queries(&index,&live,&modified,&queries_set); }
        }
        println!("{}",json!({"phase":"synthetic_edit_batch","repo":args[2],"changed_files":count,"affected_chunks":changed.len(),"update_raw_postings":stats(delta_times),"rebuild_precomputed":stats(rebuild_times),"queries":query_stats,"note":"Cached-token replacement/deletion simulation over actual text; excludes parsing, watcher delay, persistence, snapshot publication and actual checkout."}));
    }
    Ok(())
}
