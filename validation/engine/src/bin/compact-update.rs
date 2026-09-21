use bm25_turbo::{BM25Builder, BM25Params, Results, scoring, selection};
use std::{collections::{BTreeMap,HashMap}, sync::{Arc,RwLock,atomic::{AtomicBool,Ordering}},time::Instant};

#[derive(Clone,Default)]
struct Snapshot {
    vocab:Arc<HashMap<String,u32>>,
    postings:Vec<Arc<Vec<(u32,u32)>>>,
    terms:Vec<Option<Arc<Vec<(u32,u32)>>>>,
    lengths:Vec<u32>,
    total:u64,
    count:u32,
}
impl Snapshot {
    fn replace(&mut self,id:u32,tokens:Option<&[String]>) {
        let i=id as usize;
        if i>=self.terms.len() {self.terms.resize(i+1,None);self.lengths.resize(i+1,0);}
        if let Some(old)=self.terms[i].take() {
            self.count-=1; self.total-=self.lengths[i] as u64;
            for &(term,_) in old.iter() {
                let p=Arc::make_mut(&mut self.postings[term as usize]);
                let at=p.binary_search_by_key(&id,|x|x.0).unwrap();p.remove(at);
            }
        }
        self.lengths[i]=0;
        let Some(tokens)=tokens else{return;};
        let mut counts=BTreeMap::new();
        for token in tokens {
            let tid=if let Some(&tid)=self.vocab.get(token) {tid} else {
                let tid=self.vocab.len() as u32;
                Arc::make_mut(&mut self.vocab).insert(token.clone(),tid);
                self.postings.push(Arc::new(Vec::new()));tid
            };
            *counts.entry(tid).or_insert(0_u32)+=1;
        }
        let terms:Vec<_>=counts.into_iter().collect();
        for &(tid,tf) in &terms {
            let p=Arc::make_mut(&mut self.postings[tid as usize]);
            let at=p.binary_search_by_key(&id,|x|x.0).unwrap_err();p.insert(at,(id,tf));
        }
        self.terms[i]=Some(Arc::new(terms));self.lengths[i]=tokens.len() as u32;
        self.count+=1;self.total+=tokens.len() as u64;
    }
    fn search(&self,q:&[String])->Results {
        let mut scores=vec![0.0;self.terms.len()];let p=BM25Params::default();
        if self.count==0{return selection::top_k(&scores,10);}
        for term in q {
            if let Some(&tid)=self.vocab.get(term) {
                let postings=&self.postings[tid as usize];
                for &(id,tf) in postings.iter() {
                    scores[id as usize]+=scoring::score(p.method,tf as f32,self.lengths[id as usize] as f32,self.total as f32/self.count as f32,self.count,postings.len() as u32,p.k1,p.b,p.delta);
                }
            }
        }
        selection::top_k(&scores,10)
    }
}
fn check(a:&Results,b:&Results) {
    assert_eq!(a.doc_ids,b.doc_ids);
    for (a,b) in a.scores.iter().zip(&b.scores){assert!((a-b).abs()<1e-5);}
}
fn main() {
    let queries:Arc<Vec<Vec<String>>>=Arc::new((0..11).map(|i|vec![format!("t{i}"),format!("t{}",(i+2)%11)]).collect());
    let current=Arc::new(RwLock::new(Arc::new((Snapshot::default(),Vec::<Results>::new()))));
    let done=Arc::new(AtomicBool::new(false));
    let mut threads=Vec::new();
    for _ in 0..4 {
        let current=current.clone();let done=done.clone();let queries=queries.clone();
        threads.push(std::thread::spawn(move || {
            let mut checked=0;
            while !done.load(Ordering::Acquire) {
                let generation=current.read().unwrap().clone();
                for (q,expected) in queries.iter().zip(&generation.1) {
                    check(&generation.0.search(q),expected);checked+=1;
                }
                std::thread::yield_now();
            }
            checked
        }));
    }
    let mut documents=BTreeMap::<u32,Vec<String>>::new();
    let mut publication_us=Vec::new();let mut update_us=Vec::new();
    for step in 0..200_u32 {
        let id=step%43;
        let tokens:Option<Vec<String>>=if step%7==0{None}else{Some((0..step%29+1).map(|j|format!("t{}",(step+j*j)%11)).collect())};
        if let Some(t)=&tokens {documents.insert(id,t.clone());}else{documents.remove(&id);}
        let old=current.read().unwrap().clone();
        let start=Instant::now();let mut next=old.0.clone();next.replace(id,tokens.as_deref());update_us.push(start.elapsed().as_secs_f64()*1e6);
        let mut expected=Vec::new();
        if !documents.is_empty() {
            let corpus:Vec<_>=documents.values().cloned().collect();let ids:Vec<_>=documents.keys().copied().collect();
            let oracle=BM25Builder::new().build_from_tokens(&corpus).unwrap();
            for q in queries.iter(){let mut r=oracle.search_tokens(q,10).unwrap();for id in &mut r.doc_ids{*id=ids[*id as usize];}check(&next.search(q),&r);expected.push(r);}
        }
        // Retained old readers must remain valid after construction of the replacement.
        for (q,r) in queries.iter().zip(&old.1){check(&old.0.search(q),r);}
        let start=Instant::now();*current.write().unwrap()=Arc::new((next,expected));publication_us.push(start.elapsed().as_secs_f64()*1e6);
    }
    done.store(true,Ordering::Release);
    let counts:Vec<_>=threads.into_iter().map(|t|t.join().unwrap()).collect();assert!(counts.iter().all(|&n|n>0));
    update_us.sort_by(f64::total_cmp);publication_us.sort_by(f64::total_cmp);
    println!("{}",serde_json::json!({"passed":true,"generations":200,"reader_threads":4,"concurrent_query_checks":counts.iter().sum::<usize>(),"reader_checks":counts,"update_and_cow_clone_median_us":update_us[100],"publish_lock_median_us":publication_us[100],"limits":"Small synthetic corpus, no SQLite commit, no file verification, no source metadata. Does not establish production latency, bounded memory or crash recovery."}));
}
