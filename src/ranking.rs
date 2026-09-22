//! Bounded field-aware relevance followed by structural dedupe and MMR.
//!
//! Identity/comment extraction is heuristic. Field TF is combined before BM25
//! saturation, using corpus body statistics and fixed metadata length priors.
//! This is BM25F-style reranking, not a new field-statistics index.
use crate::{
    model::Hit,
    query::{QueryClass, QueryPlan, is_stop},
    text::{fold, tokenize_checked, tokenize_with_surfaces_checked},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

pub const CANDIDATE_LIMIT: usize = 200;
pub const THIN_POOL: usize = 20;
pub const WEAK_BM25_THRESHOLD: f32 = 0.25;
pub const EXPANSION_RESERVE: usize = 40;
pub const MEANINGFUL_RESERVE: usize = 40;
pub const PROXIMITY_CAP: f64 = 0.15;
pub const HALF_LIFE_DAYS: f64 = 45.;
pub const DECAY_FLOOR: f64 = 0.25;
pub const MMR_LAMBDA: f64 = 0.85;
/// Expansion is recall support.  It may contribute to an original match, but
/// cannot swamp evidence from the user's actual terms.  Expansion-only hits
/// retain a small floor so a useful synonym can still enter a thin pool.
const EXPANSION_CAP_RATIO: f64 = 0.35;
const EXPANSION_ONLY_CAP: f64 = 0.5;
const MAX_SALIENT: usize = 128;
const MAX_FIELD_BYTES: usize = 16 * 1024;
const K1: f64 = 1.5;
const OVERLAP: f64 = 0.8;
const ADJACENT_SESSION: f64 = 0.7;

#[derive(Clone, Copy, Debug)]
pub struct RankingOptions {
    pub fields: bool,
    pub classification: bool,
    pub exact: bool,
    pub proximity: bool,
    pub expansion: bool,
    pub decay: bool,
    pub dedupe: bool,
    pub weighted_similarity: bool,
    pub mmr: bool,
}
impl Default for RankingOptions {
    fn default() -> Self {
        Self {
            fields: true,
            classification: true,
            exact: true,
            proximity: true,
            expansion: true,
            decay: true,
            dedupe: true,
            weighted_similarity: true,
            mmr: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExactClass {
    QualifiedSymbol,
    Symbol,
    FullPath,
    Basename,
    Diagnostic,
    HistoricalIdentity,
    None,
}

#[derive(Clone, Debug, Serialize)]
pub struct Trace {
    pub match_id: String,
    pub source_kind: String,
    pub query_class: QueryClass,
    pub exact_class: ExactClass,
    pub baseline_bm25: f32,
    pub admission: AdmissionEvidence,
    pub matched_fields: BTreeMap<String, f64>,
    pub original_contribution: f64,
    pub expanded_contribution: f64,
    pub lexical_score: f64,
    pub proximity_bonus: f64,
    pub timestamp: Option<String>,
    pub decay_factor: f64,
    pub relevance: f64,
    pub collapsed_ids: Vec<String>,
    pub structural_similarity: f64,
    pub lexical_similarity: f64,
    pub mmr_penalty: f64,
    pub selection_score: f64,
    pub position: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AdmissionEvidence {
    pub lexical: bool,
    pub definition: bool,
    pub path: bool,
    pub meaningful_bm25: Option<f32>,
    pub expansion_bm25: Option<f32>,
}

#[derive(Clone, Debug, Default)]
pub struct BodyEvidence {
    pub terms: BTreeMap<String, usize>,
    pub length: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct CandidateCounts {
    pub lexical: usize,
    pub meaningful: usize,
    pub definitions: usize,
    pub path: usize,
    pub expansion: usize,
}

#[derive(Debug)]
pub struct RankedSearch {
    pub generation: u64,
    pub hits: Vec<Hit>,
    pub traces: Vec<Trace>,
    pub probes: Vec<crate::query::Probe>,
    pub candidate_count: usize,
    pub admission_counts: CandidateCounts,
    pub meaningful_retrievals: usize,
    pub additional_retrievals: usize,
}

#[derive(Default)]
pub struct CorpusStats {
    pub documents: u64,
    pub average_length: f64,
    pub idf: BTreeMap<String, f64>,
}
impl CorpusStats {
    fn weight(&self, term: &str) -> f64 {
        // A surface absent from persisted postings has no trustworthy corpus df. Use
        // neutral evidence rather than pretending it is corpus-rare.
        self.idf.get(term).copied().unwrap_or(1.)
    }
}

struct Field {
    name: &'static str,
    weight: f64,
    normalization: f64,
    average: f64,
    terms: BTreeMap<String, usize>,
    length: usize,
}
impl Field {
    fn from_tokens(
        name: &'static str,
        tokens: &[String],
        weight: f64,
        normalization: f64,
        average: f64,
    ) -> Self {
        let mut terms = BTreeMap::new();
        for term in tokens {
            *terms.entry(term.clone()).or_default() += 1;
        }
        Self::from_terms(name, terms, tokens.len(), weight, normalization, average)
    }

    fn from_terms(
        name: &'static str,
        terms: BTreeMap<String, usize>,
        length: usize,
        weight: f64,
        normalization: f64,
        average: f64,
    ) -> Self {
        Self {
            name,
            weight,
            normalization,
            average,
            length,
            terms,
        }
    }

    fn from_evidence(
        name: &'static str,
        evidence: BodyEvidence,
        weight: f64,
        normalization: f64,
        average: f64,
    ) -> Self {
        Self::from_terms(
            name,
            evidence.terms,
            evidence.length,
            weight,
            normalization,
            average,
        )
    }
}

#[derive(Default)]
struct TokenCache {
    entries: HashMap<String, Arc<CachedTokens>>,
}

struct CachedTokens {
    terms: Vec<String>,
    surfaces: Vec<String>,
}

impl TokenCache {
    fn get(&mut self, text: &str) -> Result<Arc<CachedTokens>> {
        if let Some(tokens) = self.entries.get(text) {
            return Ok(tokens.clone());
        }
        let (terms, surfaces) = tokenize_with_surfaces_checked(text)?;
        let tokens = Arc::new(CachedTokens { terms, surfaces });
        self.entries.insert(text.to_owned(), tokens.clone());
        Ok(tokens)
    }
}

fn cached_field(
    cache: &mut TokenCache,
    name: &'static str,
    text: &str,
    weight: f64,
    normalization: f64,
    average: f64,
) -> Result<Field> {
    let tokens = cache.get(text)?;
    Ok(Field::from_tokens(
        name,
        &tokens.terms,
        weight,
        normalization,
        average,
    ))
}

pub struct Prepared {
    hit: Hit,
    fields: Vec<Field>,
    symbols: BTreeSet<String>,
    /// Surface forms extracted once from the bounded chunk.  Exact historical
    /// identity checks must use the same normalization as field extraction;
    /// re-tokenizing the chunk for every candidate during reranking made the
    /// session path needlessly expensive.
    identities: BTreeSet<String>,
    surfaces: BTreeSet<String>,
    body: String,
    sequence: Vec<String>,
    salient: BTreeMap<String, f64>,
    exact: ExactClass,
    tier: u8,
    trace: Trace,
}

fn bounded(text: &str) -> &str {
    let mut end = text.len().min(MAX_FIELD_BYTES);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn complete_identifiers(surfaces: &[String]) -> BTreeSet<String> {
    surfaces
        .iter()
        .filter(|surface| surface.len() <= MAX_FIELD_BYTES)
        .cloned()
        .collect()
}

// Recognize explicit declaration syntax, not arbitrary mentions. This avoids
// giving repeated prose a definition tier. No claim of compiler-level parsing.
fn declaration_details(text: &str) -> (Vec<String>, String, BTreeSet<String>) {
    let mut names = Vec::new();
    let mut symbols = BTreeSet::new();
    let mut signatures = String::new();
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with(['/', '*', '#', '\'', '"']) {
            continue;
        }
        let words: Vec<_> = line.split_whitespace().collect();
        let mut name = None;
        for (index, pair) in words.windows(2).enumerate() {
            if matches!(
                pair[0],
                "fn" | "func"
                    | "function"
                    | "class"
                    | "struct"
                    | "enum"
                    | "trait"
                    | "interface"
                    | "def"
                    | "type"
                    | "namespace"
            ) && words[..index].iter().all(|word| {
                matches!(
                    *word,
                    "pub"
                        | "public"
                        | "private"
                        | "protected"
                        | "internal"
                        | "static"
                        | "async"
                        | "unsafe"
                        | "export"
                        | "default"
                        | "virtual"
                        | "inline"
                        | "final"
                        | "abstract"
                        | "extern"
                        | "const"
                        | "pub(crate)"
                        | "mut"
                )
            }) {
                name = Some(pair[1]);
                break;
            }
        }
        // Qualified C++/Rust definitions can lack a declaration keyword.
        if name.is_none()
            && line.contains('{')
            && let Some((prefix, _)) = line.split_once('(')
        {
            let last = prefix.split_whitespace().last().unwrap_or("");
            if last.contains("::") && !line.starts_with("return ") {
                name = Some(last);
            }
        }
        if let Some(name) = name {
            let name = name
                .split(|c: char| !(c.is_alphanumeric() || matches!(c, '_' | ':' | '.')))
                .next()
                .unwrap_or("");
            if !name.is_empty() && name.len() <= MAX_FIELD_BYTES {
                names.push(name.to_owned());
                symbols.insert(fold(name));
                signatures.push_str(line.split('{').next().unwrap_or(line));
                signatures.push('\n');
            }
        }
    }
    (names, signatures, symbols)
}

fn declarations(text: &str) -> (String, String, BTreeSet<String>) {
    let (names, signatures, symbols) = declaration_details(text);
    (names.join(" "), signatures, symbols)
}

pub(crate) fn declared_symbols(text: &str) -> BTreeSet<String> {
    declaration_details(bounded(text)).2
}

fn body_evidence_from_tokens(tokens: &[String]) -> BodyEvidence {
    let mut terms = BTreeMap::new();
    for token in tokens {
        *terms.entry(token.clone()).or_default() += 1;
    }
    BodyEvidence {
        terms,
        length: tokens.len(),
    }
}

pub fn prepare(hits: Vec<Hit>, plan: &QueryPlan) -> Result<Vec<Prepared>> {
    let mut indexed = Vec::new();
    for hit in hits.into_iter().take(CANDIDATE_LIMIT) {
        let tokens = tokenize_checked(bounded(&hit.chunk.text))?;
        indexed.push((hit, body_evidence_from_tokens(&tokens)));
    }
    prepare_indexed_inner(indexed, plan)
}

pub fn prepare_indexed(hits: Vec<(Hit, BodyEvidence)>, plan: &QueryPlan) -> Result<Vec<Prepared>> {
    prepare_indexed_inner(hits.into_iter().take(CANDIDATE_LIMIT), plan)
}

fn prepare_indexed_inner(
    hits: impl IntoIterator<Item = (Hit, BodyEvidence)>,
    plan: &QueryPlan,
) -> Result<Vec<Prepared>> {
    let mut cache = TokenCache::default();
    hits.into_iter()
        .map(|(hit, body_evidence)| {
            let text = bounded(&hit.chunk.text);
            let body = fold(text);
            let mut fields = Vec::new();
            let mut symbols = BTreeSet::new();
            if hit.source.kind == "project" {
                let (names, signatures, declared) = declarations(text);
                symbols = declared;
                let basename = hit
                    .source
                    .path
                    .rsplit(['/', '\\'])
                    .next()
                    .unwrap_or(&hit.source.path);
                fields.push(cached_field(&mut cache, "symbol", &names, 7., 0., 1.)?);
                fields.push(cached_field(
                    &mut cache,
                    "signature",
                    &signatures,
                    4.,
                    0.2,
                    24.,
                )?);
                fields.push(cached_field(&mut cache, "basename", basename, 5., 0., 1.)?);
                fields.push(cached_field(
                    &mut cache,
                    "path",
                    &hit.source.path,
                    4.,
                    0.,
                    1.,
                )?);
                let docs = text
                    .lines()
                    .filter(|l| l.trim().starts_with("///") || l.trim().starts_with("/**"))
                    .collect::<Vec<_>>()
                    .join("\n");
                let comments = text
                    .lines()
                    .filter(|l| l.trim().starts_with("//") || l.trim().starts_with('#'))
                    .collect::<Vec<_>>()
                    .join("\n");
                fields.push(cached_field(
                    &mut cache,
                    "doc_comment",
                    &docs,
                    2.,
                    0.5,
                    64.,
                )?);
                fields.push(cached_field(
                    &mut cache, "comment", &comments, 1.5, 0.5, 64.,
                )?);
                fields.push(Field::from_evidence("body", body_evidence, 1., 0.75, 0.));
            } else {
                let (name, weight) =
                    match (hit.chunk.field_kind.as_deref(), hit.chunk.role.as_deref()) {
                        (Some("tool_call"), _) => ("command", 4.),
                        (Some("tool_result"), _) | (_, Some("tool")) => ("tool_output", 0.5),
                        (_, Some("user")) => ("user_request", 3.),
                        // Conclusions are not separately stored; don't invent a
                        // conclusion classifier from arbitrary assistant prose.
                        _ => ("assistant_prose", 1.5),
                    };
                // Session tool output is often a very large repeated payload.  A
                // bounded metadata prior keeps it from winning solely because the
                // corpus average was inflated by one pathological response.
                fields.push(Field::from_evidence(name, body_evidence, weight, 0.9, 128.));
                let diagnostics = text
                    .lines()
                    .filter(|line| {
                        let line = fold(line);
                        line.contains("error")
                            || line.contains("exception")
                            || line.contains("not found")
                            || line.contains("panic")
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                fields.push(cached_field(
                    &mut cache,
                    "diagnostic",
                    &diagnostics,
                    5.,
                    0.5,
                    64.,
                )?);
                let mentions = text
                    .split_whitespace()
                    .filter(|w| {
                        w.contains('/')
                            || w.contains('_')
                            || w.contains("::")
                            || w.chars().skip(1).any(|c| c.is_uppercase())
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                fields.push(cached_field(
                    &mut cache,
                    "mentioned_identity",
                    &mentions,
                    5.,
                    0.25,
                    32.,
                )?);
            }
            let cached_tokens = cache.get(text)?;
            let sequence = cached_tokens.terms.clone();
            let surfaces: BTreeSet<_> = cached_tokens.terms.iter().cloned().collect();
            let identities = complete_identifiers(&cached_tokens.surfaces);
            let mut salient = BTreeMap::new();
            for field in &fields {
                for term in field.terms.keys() {
                    if !is_generic(term) {
                        salient.insert(term.clone(), 1.);
                    }
                }
            }
            let trace = Trace {
                match_id: hit.match_id.clone(),
                source_kind: hit.source.kind.clone(),
                query_class: plan.class,
                exact_class: ExactClass::None,
                baseline_bm25: hit.score,
                admission: AdmissionEvidence::default(),
                matched_fields: BTreeMap::new(),
                original_contribution: 0.,
                expanded_contribution: 0.,
                lexical_score: 0.,
                proximity_bonus: 0.,
                timestamp: hit.chunk.timestamp.clone(),
                decay_factor: 1.,
                relevance: 0.,
                collapsed_ids: Vec::new(),
                structural_similarity: 0.,
                lexical_similarity: 0.,
                mmr_penalty: 0.,
                selection_score: 0.,
                position: 0,
            };
            Ok(Prepared {
                hit,
                fields,
                symbols,
                identities,
                surfaces,
                body,
                sequence,
                salient,
                exact: ExactClass::None,
                tier: 0,
                trace,
            })
        })
        .collect()
}

pub fn statistics_terms(candidates: &[Prepared], plan: &QueryPlan) -> Result<BTreeSet<String>> {
    let mut terms: BTreeSet<_> = plan.original.iter().cloned().collect();
    for probe in &plan.probes {
        terms.extend(tokenize_checked(&probe.query)?);
    }
    for candidate in candidates {
        terms.extend(candidate.salient.keys().cloned());
    }
    Ok(terms)
}

fn is_generic(t: &str) -> bool {
    is_stop(t)
        || t.len() < 2
        || matches!(
            t,
            "fn" | "func"
                | "function"
                | "let"
                | "const"
                | "var"
                | "pub"
                | "public"
                | "private"
                | "return"
                | "class"
                | "struct"
                | "impl"
                | "self"
                | "this"
                | "true"
                | "false"
                | "null"
                | "none"
                | "some"
                | "if"
                | "else"
                | "match"
                | "use"
                | "import"
                | "export"
                | "int"
                | "string"
                | "str"
                | "void"
                | "new"
                | "value"
                | "data"
        )
}

fn exact_class(c: &Prepared, p: &QueryPlan) -> (ExactClass, u8) {
    let literal = &p.literal;
    if literal.is_empty() {
        return (ExactClass::None, 0);
    }
    match p.class {
        QueryClass::Identifier => {
            if c.symbols.contains(literal) {
                return (
                    if literal.contains("::") {
                        ExactClass::QualifiedSymbol
                    } else {
                        ExactClass::Symbol
                    },
                    2,
                );
            }
            if literal.contains("::") && c.identities.contains(literal) {
                return (ExactClass::HistoricalIdentity, 1);
            }
            if c.hit.source.kind == "session" && c.identities.contains(literal) {
                return (ExactClass::HistoricalIdentity, 1);
            }
        }
        QueryClass::Path => {
            if c.hit.source.kind == "project" {
                let path = fold(&c.hit.source.path.replace('\\', "/"));
                let literal = literal.replace('\\', "/");
                if path == literal || path.ends_with(&format!("/{literal}")) {
                    return (
                        if literal.contains('/') {
                            ExactClass::FullPath
                        } else {
                            ExactClass::Basename
                        },
                        2,
                    );
                }
            } else if c.surfaces.contains(literal) {
                return (ExactClass::HistoricalIdentity, 1);
            }
        }
        QueryClass::Diagnostic if contains_literal(&c.body, literal) => {
            return (ExactClass::Diagnostic, 2);
        }
        _ => {}
    }
    (ExactClass::None, 0)
}

fn contains_literal(body: &str, literal: &str) -> bool {
    body.match_indices(literal).any(|(i, _)| {
        let end = i + literal.len();
        body[..i]
            .chars()
            .next_back()
            .is_none_or(|c| !c.is_alphanumeric() && c != '_')
            && body[end..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_alphanumeric() && c != '_')
    })
}

pub fn temporal_factor(
    timestamp: Option<&str>,
    now: DateTime<Utc>,
    exact: bool,
    literal: bool,
) -> f64 {
    if exact {
        return 1.;
    }
    let Some(time) = timestamp.and_then(|s| DateTime::parse_from_rfc3339(s).ok()) else {
        return 1.;
    };
    let age = (now.signed_duration_since(time).num_seconds() as f64 / 86400.).max(0.);
    let decay = DECAY_FLOOR + (1. - DECAY_FLOOR) * 2_f64.powf(-age / HALF_LIFE_DAYS);
    decay.powf(if literal { 0.5 } else { 1. })
}

fn proximity(sequence: &[String], terms: &[String]) -> f64 {
    let wanted: BTreeSet<_> = terms.iter().filter(|t| !is_stop(t)).collect();
    if wanted.len() < 2 {
        return 0.;
    }
    let mut counts = BTreeMap::<&String, usize>::new();
    let mut left = 0;
    let mut best = usize::MAX;
    for (right, t) in sequence.iter().enumerate() {
        if wanted.contains(t) {
            *counts.entry(t).or_default() += 1;
        }
        while counts.len() == wanted.len() {
            best = best.min(right - left + 1);
            if let Some(count) = counts.get_mut(&sequence[left]) {
                *count -= 1;
                if *count == 0 {
                    counts.remove(&sequence[left]);
                }
            }
            left += 1;
        }
    }
    if best == usize::MAX {
        0.
    } else {
        (wanted.len() as f64 / best as f64).min(1.)
    }
}

fn field_score(
    c: &Prepared,
    terms: impl Iterator<Item = (String, f64)>,
    stats: &CorpusStats,
    matched: &mut BTreeMap<String, f64>,
) -> f64 {
    let mut result = 0.;
    for (term, evidence) in terms {
        let mut tf = 0.;
        for field in &c.fields {
            let count = field.terms.get(&term).copied().unwrap_or(0) as f64;
            if count == 0. {
                continue;
            }
            let average = if field.average == 0. {
                stats.average_length.max(1.)
            } else {
                field.average
            };
            let normalized = field.weight * count
                / (1. - field.normalization + field.normalization * field.length as f64 / average);
            *matched.entry(field.name.into()).or_default() += normalized * evidence;
            tf += normalized;
        }
        result += stats.weight(&term) * tf / (K1 + tf) * evidence;
    }
    result
}

pub fn weighted_jaccard(a: &BTreeMap<String, f64>, b: &BTreeMap<String, f64>) -> f64 {
    let intersection: f64 = a
        .iter()
        .map(|(t, w)| w.min(b.get(t).copied().unwrap_or(0.)))
        .sum();
    let union: f64 = a.values().sum::<f64>() + b.values().sum::<f64>() - intersection;
    if union > 0. { intersection / union } else { 0. }
}

fn overlap(a: &Hit, b: &Hit) -> f64 {
    if a.source.key != b.source.key || a.source.version != b.source.version {
        return 0.;
    }
    // Byte overlap is reliable even when a giant line spans many chunks.
    let length = a
        .chunk
        .end_byte
        .saturating_sub(a.chunk.start_byte)
        .min(b.chunk.end_byte.saturating_sub(b.chunk.start_byte));
    if length == 0 {
        return 0.;
    }
    a.chunk
        .end_byte
        .min(b.chunk.end_byte)
        .saturating_sub(a.chunk.start_byte.max(b.chunk.start_byte)) as f64
        / length as f64
}
fn structural(a: &Prepared, b: &Prepared) -> f64 {
    if a.hit.source.key == b.hit.source.key {
        if overlap(&a.hit, &b.hit) >= OVERLAP {
            return 0.95;
        }
        if a.hit.source.kind == "session"
            && a.hit.chunk.end_line.abs_diff(b.hit.chunk.start_line) <= 1
        {
            return ADJACENT_SESSION;
        }
        if !a.symbols.is_empty() && !a.symbols.is_disjoint(&b.symbols) {
            return 1.;
        }
    }
    0.
}
fn duplicate(a: &Prepared, b: &Prepared) -> bool {
    if a.hit.source.kind != b.hit.source.kind {
        return false;
    }
    // Exact repeated content is generally a physical replica (generated
    // files, mirrored tool output, or an indexed copy).  Collapse it across
    // sources, while keeping distinct same-event fields and overlapping
    // ranges eligible below.  Exact-tier sorting chooses the useful
    // representative before this check runs.
    if !a.hit.chunk.text.is_empty() && a.hit.chunk.text == b.hit.chunk.text {
        return true;
    }
    // Session fields share event ranges but can contain distinct evidence.
    // Never collapse them based on their source ranges alone.
    a.hit.source.kind == "project" && overlap(&a.hit, &b.hit) >= OVERLAP
}

pub fn rerank(
    mut candidates: Vec<Prepared>,
    plan: &QueryPlan,
    stats: &CorpusStats,
    options: RankingOptions,
    now: DateTime<Utc>,
    limit: usize,
) -> Result<(Vec<Hit>, Vec<Trace>)> {
    let mut expanded = BTreeMap::<String, f64>::new();
    if options.expansion {
        for probe in &plan.probes {
            for term in tokenize_checked(&probe.query)? {
                if !plan.original.contains(&term) {
                    expanded
                        .entry(term)
                        .and_modify(|w| *w = w.max(probe.weight))
                        .or_insert(probe.weight);
                }
            }
        }
    }
    for c in &mut candidates {
        if options.exact {
            (c.exact, c.tier) = exact_class(c, plan);
        }
        let mut matched = BTreeMap::new();
        let original = field_score(
            c,
            plan.original.iter().cloned().map(|t| (t, 1.)),
            stats,
            &mut matched,
        );
        let mut expansion_fields = BTreeMap::new();
        let raw_expansion = field_score(
            c,
            expanded.iter().map(|(t, w)| (t.clone(), *w)),
            stats,
            &mut expansion_fields,
        );
        let expansion = if original > 0. {
            raw_expansion.min(original * EXPANSION_CAP_RATIO)
        } else {
            raw_expansion.min(EXPANSION_ONLY_CAP)
        };
        let expansion_scale = if raw_expansion > 0. {
            expansion / raw_expansion
        } else {
            0.
        };
        for (field, value) in expansion_fields {
            *matched.entry(field).or_default() += value * expansion_scale;
        }
        let lexical = if options.fields {
            original + expansion
        } else {
            f64::from(c.hit.score)
        };
        let bonus = if options.proximity {
            lexical * PROXIMITY_CAP * proximity(&c.sequence, &plan.original)
        } else {
            0.
        };
        let strong_literal = contains_literal(&c.body, &plan.literal) && !plan.literal.is_empty();
        let decay = if options.decay && c.hit.source.kind == "session" {
            temporal_factor(
                c.hit.chunk.timestamp.as_deref(),
                now,
                c.exact != ExactClass::None,
                strong_literal,
            )
        } else {
            1.
        };
        c.trace.exact_class = c.exact;
        c.trace.matched_fields = matched;
        c.trace.original_contribution = original;
        c.trace.expanded_contribution = expansion;
        c.trace.lexical_score = lexical;
        c.trace.proximity_bonus = bonus;
        c.trace.decay_factor = decay;
        c.trace.relevance = (lexical + bonus) * decay;
        c.hit.score = c.trace.relevance as f32;
        let mut salient: Vec<_> = std::mem::take(&mut c.salient)
            .into_keys()
            .map(|t| {
                let w = if options.weighted_similarity {
                    stats.weight(&t)
                } else {
                    1.
                };
                (t, w)
            })
            .collect();
        salient.sort_by(|(a, wa), (b, wb)| wb.total_cmp(wa).then(a.cmp(b)));
        salient.truncate(MAX_SALIENT);
        c.salient = salient.into_iter().collect();
    }
    candidates.retain(|c| {
        c.trace.original_contribution > 0.
            || c.trace.expanded_contribution > 0.
            || c.exact != ExactClass::None
    });
    candidates.sort_by(|a, b| {
        b.tier
            .cmp(&a.tier)
            .then(b.trace.relevance.total_cmp(&a.trace.relevance))
            .then(a.hit.match_id.cmp(&b.hit.match_id))
    });
    let mut unique: Vec<Prepared> = Vec::new();
    for c in candidates {
        if options.dedupe
            && let Some(prior) = unique.iter_mut().find(|p| duplicate(p, &c))
        {
            prior.trace.collapsed_ids.push(c.hit.match_id.clone());
            continue;
        }
        unique.push(c);
    }
    let max = unique
        .iter()
        .map(|c| c.trace.relevance)
        .fold(0., f64::max)
        .max(f64::EPSILON);
    let mut selected = Vec::new();
    // Each candidate keeps its maximum similarity so each selected pair is
    // evaluated only once. Scores in Hit remain relevance, never MMR scores.
    let mut similarities = vec![(0_f64, 0_f64, 0_f64); unique.len()];
    while !unique.is_empty() && selected.len() < limit.min(CANDIDATE_LIMIT) {
        let tier = unique.iter().map(|c| c.tier).max().unwrap();
        let mut best = 0;
        let mut best_score = f64::NEG_INFINITY;
        for (i, c) in unique.iter().enumerate() {
            if c.tier != tier {
                continue;
            }
            let score = if options.mmr {
                MMR_LAMBDA * c.trace.relevance / max - (1. - MMR_LAMBDA) * similarities[i].0
            } else {
                c.trace.relevance / max
            };
            if score > best_score {
                best = i;
                best_score = score;
            }
        }
        let mut c = unique.remove(best);
        let (similarity, lexical, structure) = similarities.remove(best);
        c.trace.selection_score = best_score;
        c.trace.lexical_similarity = lexical;
        c.trace.structural_similarity = structure;
        c.trace.mmr_penalty = if options.mmr {
            (1. - MMR_LAMBDA) * similarity
        } else {
            0.
        };
        c.trace.position = selected.len() + 1;
        if options.mmr {
            for (other, sim) in unique.iter().zip(&mut similarities) {
                let lexical = weighted_jaccard(&c.salient, &other.salient);
                let structure = structural(&c, other);
                let combined = lexical.max(structure);
                if combined > sim.0 {
                    *sim = (combined, lexical, structure);
                }
            }
        }
        selected.push(c);
    }
    Ok(selected.into_iter().map(|c| (c.hit, c.trace)).unzip())
}
