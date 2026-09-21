param([string]$Project = "")
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
$log = Join-Path $PWD 'windows-validation.txt'
Start-Transcript -Path $log -Force
try {
    [System.Environment]::OSVersion.VersionString
    [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    rustc --version
    if ($LASTEXITCODE -ne 0) { throw 'Rust is required.' }
    cargo --version
    git --version
    if ($LASTEXITCODE -ne 0) { throw 'Git is required for worktree validation.' }
    cargo fmt --package bm25-mcp -- --check
    if ($LASTEXITCODE -ne 0) { throw 'Formatting check failed.' }
    cargo test --locked --all-targets
    if ($LASTEXITCODE -ne 0) { throw 'Tests failed.' }
    cargo clippy --locked --all-targets -- -D warnings -D unsafe-code
    if ($LASTEXITCODE -ne 0) { throw 'Clippy failed.' }
    cargo build --locked --release
    if ($LASTEXITCODE -ne 0) { throw 'Release build failed.' }
    & '.\target\release\bm25-mcp.exe' --help
    if ($LASTEXITCODE -ne 0) { throw 'Executable launch failed.' }
    if ($Project) {
        py -3 scripts/acceptance.py --project $Project --snapshot-sessions
        if ($LASTEXITCODE -ne 0) { throw 'Real-project acceptance failed.' }
        py -3 scripts/exercise-updates.py
        if ($LASTEXITCODE -ne 0) { throw 'Update acceptance failed.' }
        py -3 scripts/check-pressure.py --binary '.\target\release\bm25-mcp.exe'
        if ($LASTEXITCODE -ne 0) { throw 'Pressure acceptance failed.' }
    }
    Write-Output 'Automated Windows checks passed. Record manual MCP-client attachment and first-client termination results separately.'
} finally {
    Stop-Transcript
}
