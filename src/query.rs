//! Deterministic intent and bounded, provenance-carrying lexical fallback.
use crate::text::{fold, tokenize_with_surfaces_checked};
use anyhow::Result;
use serde::Serialize;
use std::collections::BTreeSet;

pub const MAX_PROBES: usize = 6;
pub const MAX_QUERY_TERMS: usize = 256;
pub const DERIVED_WEIGHT: f64 = 0.8;
pub const MORPH_WEIGHT: f64 = 0.6;
pub const ALIAS_WEIGHT: f64 = 0.4;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryClass {
    Identifier,
    Path,
    Diagnostic,
    Natural,
    Mixed,
}

#[derive(Clone, Debug, Serialize)]
pub struct Probe {
    pub query: String,
    pub weight: f64,
    pub origin: &'static str,
}

#[derive(Debug)]
pub struct QueryPlan {
    pub class: QueryClass,
    pub literal: String,
    pub original: Vec<String>,
    pub probes: Vec<Probe>,
}

pub fn classify(query: &str) -> QueryClass {
    let q = query.trim();
    if (q.starts_with('"') && q.ends_with('"') && q.len() > 2) || diagnostic_code(q) {
        return QueryClass::Diagnostic;
    }
    if !q.contains(char::is_whitespace) {
        if q.contains('/')
            || q.contains('\\')
            || q.rsplit_once('.')
                .is_some_and(|(_, ext)| is_path_extension(ext))
        {
            return QueryClass::Path;
        }
        if q.contains('_')
            || q.contains("::")
            || q.contains('.')
            || q.chars().any(|c| c.is_uppercase())
        {
            return QueryClass::Identifier;
        }
    }
    if q.split_whitespace()
        .any(|w| w.contains('_') || w.contains("::") || w.contains('/'))
    {
        QueryClass::Mixed
    } else {
        QueryClass::Natural
    }
}

fn is_path_extension(ext: &str) -> bool {
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "rs" | "ts"
            | "tsx"
            | "js"
            | "jsx"
            | "py"
            | "go"
            | "java"
            | "kt"
            | "swift"
            | "c"
            | "h"
            | "cpp"
            | "hpp"
            | "cs"
            | "rb"
            | "php"
            | "json"
            | "toml"
            | "yaml"
            | "yml"
            | "xml"
            | "md"
            | "txt"
            | "sql"
            | "sh"
            | "bash"
            | "zsh"
            | "lock"
    )
}

/// Recognize conventional compiler/tool diagnostic identifiers without
/// misclassifying ordinary versioned API names such as `ManagerV2`.
fn diagnostic_code(q: &str) -> bool {
    let folded = q.to_ascii_uppercase();
    let starts_with_digits = |prefix: &str| {
        folded
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.len() >= 2 && rest.chars().all(|c| c.is_ascii_digit()))
    };
    starts_with_digits("E")
        || starts_with_digits("TS")
        || starts_with_digits("CS")
        || starts_with_digits("C")
        || folded.starts_with("ERR_")
        || folded.starts_with("ERR-")
        || folded.starts_with("ERROR[")
        || folded.starts_with("WARNING[")
}

pub fn is_stop(term: &str) -> bool {
    matches!(
        term,
        "a" | "an"
            | "the"
            | "is"
            | "are"
            | "was"
            | "were"
            | "be"
            | "been"
            | "to"
            | "of"
            | "in"
            | "on"
            | "at"
            | "by"
            | "for"
            | "and"
            | "or"
            | "as"
            | "it"
            | "its"
            | "we"
            | "i"
            | "you"
            | "that"
            | "this"
            | "those"
            | "what"
            | "why"
            | "how"
            | "when"
            | "where"
            | "did"
            | "do"
            | "does"
            | "have"
            | "has"
            | "had"
            | "with"
            | "from"
            | "about"
            | "after"
            | "before"
            | "yesterday"
            | "thing"
            | "can"
            | "could"
            | "would"
            | "should"
    )
}

impl QueryPlan {
    pub fn new(query: &str, classification: bool) -> Result<Self> {
        let class = if classification {
            classify(query)
        } else {
            QueryClass::Mixed
        };
        let (terms, surfaces) = tokenize_with_surfaces_checked(query)?;
        let literal = if class == QueryClass::Identifier && surfaces.len() == 1 {
            surfaces[0].clone()
        } else {
            fold(query.trim().trim_matches('"'))
        };
        let mut seen = BTreeSet::new();
        let all_terms: Vec<_> = terms
            .iter()
            .filter(|t| seen.insert((*t).clone()))
            .take(MAX_QUERY_TERMS)
            .cloned()
            .collect();
        let original: Vec<_> = if class == QueryClass::Natural {
            all_terms.iter().filter(|t| !is_stop(t)).cloned().collect()
        } else {
            all_terms.clone()
        };
        // Stopword reduction is a ranking aid, never a guaranteed miss.  A
        // literal query such as `is` can still be a meaningful code/text
        // lookup and must reach the existing lexical index.
        let original = if original.is_empty() {
            all_terms
        } else {
            original
        };
        let mut probes = Vec::new();
        if matches!(class, QueryClass::Natural | QueryClass::Mixed) {
            let reduced = terms
                .iter()
                .filter(|term| !is_stop(term))
                .take(16)
                .cloned()
                .collect::<Vec<_>>()
                .join(" ");
            if !reduced.is_empty() && reduced != literal {
                probes.push(Probe {
                    query: reduced,
                    weight: DERIVED_WEIGHT,
                    origin: "reduced",
                });
            }
            // Deliberately small inflection families, not general stemming.
            const FAMILIES: &[&[&str]] = &[
                &["delete", "deleted", "deleting", "deletion"],
                &["fix", "fixed", "fixing"],
                &["crash", "crashed", "crashing"],
                &["serialize", "serialized", "serialization"],
            ];
            for term in &original {
                for family in FAMILIES {
                    if family.contains(&term.as_str()) {
                        for variant in *family {
                            if !original.iter().any(|t| t == variant) {
                                probes.push(Probe {
                                    query: (*variant).into(),
                                    weight: MORPH_WEIGHT,
                                    origin: "morphology",
                                });
                            }
                        }
                    }
                }
                let alias = match term.as_str() {
                    "remove" => Some("delete"),
                    "delete" => Some("remove"),
                    "terminate" => Some("kill"),
                    "kill" => Some("terminate"),
                    "panic" => Some("crash"),
                    "crash" => Some("panic"),
                    "ctor" => Some("constructor"),
                    "constructor" => Some("ctor"),
                    _ => None,
                };
                if let Some(alias) = alias {
                    probes.push(Probe {
                        query: alias.into(),
                        weight: ALIAS_WEIGHT,
                        origin: "alias",
                    });
                }
            }
        }
        // Identifier decomposition already participates in the original postings
        // query. Reissuing it would produce exactly the same candidate pool.
        let mut seen = BTreeSet::new();
        probes.retain(|p| seen.insert(p.query.clone()));
        probes.truncate(MAX_PROBES);
        Ok(Self {
            class,
            literal,
            original,
            probes,
        })
    }
}
