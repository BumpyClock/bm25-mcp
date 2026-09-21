#![forbid(unsafe_code)]

use anyhow::Result;
use bm25_mcp::{
    runtime::{self, OwnerClient},
    tools,
};
use clap::{Parser, Subcommand};
use rmcp::{ErrorData, RoleServer, ServerHandler, ServiceExt, model::*, service::RequestContext};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    /// Run a stdio MCP frontend bound to a project.
    Serve {
        #[arg(long)]
        project: Option<PathBuf>,
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    #[command(hide = true)]
    Owner {
        #[arg(long)]
        project: PathBuf,
        #[arg(long)]
        cache_dir: PathBuf,
    },
}
struct Service {
    client: Arc<Mutex<OwnerClient>>,
}
impl ServerHandler for Service {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("bm25-mcp",env!("CARGO_PKG_VERSION")))
            .with_instructions("Search project text and coding-agent sessions using lexical BM25. Inspect status and coverage; results may be partial during reconciliation.")
    }
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: tools::tool_definitions(),
            ..Default::default()
        })
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResult, ErrorData> {
        let client = self.client.clone();
        let result = tokio::task::spawn_blocking(move || {
            client.lock().unwrap().call(
                &request.name,
                serde_json::Value::Object(request.arguments.unwrap_or_default()),
            )
        })
        .await;
        match result {
            Ok(Ok(value)) => Ok(CallToolResult::structured(value)),
            Ok(Err(error)) => Ok(CallToolResult::structured_error(
                serde_json::json!({"error":format!("{error:#}")}),
            )),
            Err(error) => Ok(CallToolResult::structured_error(
                serde_json::json!({"error":error.to_string()}),
            )),
        }
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Commands::Owner { project, cache_dir } => runtime::run_owner(&project, &cache_dir),
        Commands::Serve { project, cache_dir } => {
            let root = match project {
                Some(path) => path,
                None => {
                    let cwd = std::env::current_dir()?;
                    std::process::Command::new("git")
                        .arg("-C")
                        .arg(&cwd)
                        .args(["rev-parse", "--show-toplevel"])
                        .output()
                        .ok()
                        .filter(|o| o.status.success())
                        .and_then(|o| String::from_utf8(o.stdout).ok())
                        .map(|s| PathBuf::from(s.trim()))
                        .unwrap_or(cwd)
                }
            };
            let cache = cache_dir
                .or_else(|| dirs::cache_dir().map(|p| p.join("bm25-mcp")))
                .ok_or_else(|| anyhow::anyhow!("no cache directory; specify --cache-dir"))?;
            let client = Arc::new(Mutex::new(OwnerClient::connect(&root, &cache)?));
            let heartbeat_client = Arc::downgrade(&client);
            let heartbeat = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    let Some(client) = heartbeat_client.upgrade() else {
                        break;
                    };
                    let _ = tokio::task::spawn_blocking(move || client.lock().unwrap().heartbeat())
                        .await;
                }
            });
            let service = Service { client }.serve(rmcp::transport::stdio()).await?;
            service.waiting().await?;
            heartbeat.abort();
            Ok(())
        }
    }
}
