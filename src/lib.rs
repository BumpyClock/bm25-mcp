#![forbid(unsafe_code)]

pub mod content_cache;
pub mod coverage;
pub mod identity;
pub mod ingest;
pub mod model;
pub mod progress;
pub mod sessions;
pub mod store;
pub mod text;

pub mod runtime;
pub mod tools;

mod git_changes;

pub mod query;
pub mod ranking;

mod reconciliation;
