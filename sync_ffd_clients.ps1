# Sync FFD MCP to Codex, Claude Code, and Claude Desktop without copying the API key.
$ErrorActionPreference = 'Stop'

$workspace = Split-Path -Parent $MyInvocation.MyCommand.Path
$wrapper = (Join-Path $workspace 'ffd_mcp_wrapper.py').Replace('\', '/')
$ffdConfig = Join-Path $HOME '.ffd\mcp-config.json'
$ffdServer = Join-Path $HOME '.ffd\ffd_mcp_server.py'

if (-not (Test-Path $ffdConfig) -or -not (Test-Path $ffdServer)) {
  throw 'FFD local installation is missing. Run the official FFD installer first.'
}

function Backup-File([string]$Path) {
  if (Test-Path $Path) {
    $stamp = Get-Date -Format 'yyyyMMddHHmmss'
    $backup = '{0}.backup-ffd-{1}' -f $Path, $stamp
    Copy-Item -LiteralPath $Path -Destination $backup -Force
  }
}

function Sync-JsonMcp([string]$Path) {
  $dir = Split-Path -Parent $Path
  if (-not (Test-Path $dir)) { return }
  Backup-File $Path
  if (Test-Path $Path) {
    $config = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
  } else {
    $config = [PSCustomObject]@{}
  }
  if (-not $config.PSObject.Properties['mcpServers']) {
    $config | Add-Member -MemberType NoteProperty -Name 'mcpServers' -Value ([PSCustomObject]@{})
  }
  $entry = [PSCustomObject]@{ command = 'python'; args = @($wrapper) }
  $config.mcpServers | Add-Member -Force -MemberType NoteProperty -Name 'ffd' -Value $entry
  $config | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $Path -Encoding UTF8
  Write-Host "Synced MCP: $Path"
}

Sync-JsonMcp (Join-Path $HOME '.claude.json')
Sync-JsonMcp (Join-Path $env:APPDATA 'Claude\claude_desktop_config.json')

$codexPath = Join-Path $HOME '.codex\config.toml'
if (Test-Path $codexPath) {
  $toml = Get-Content -LiteralPath $codexPath -Raw
  if ($toml -notmatch '(?m)^\[mcp_servers\.ffd\]$') {
    Backup-File $codexPath
    $tomlBlock = "`n[mcp_servers.ffd]`ncommand = `"python`"`nargs = [`"$wrapper`"]`nenabled = true`n"
    Add-Content -LiteralPath $codexPath -Encoding UTF8 -Value $tomlBlock
    Write-Host "Synced MCP: $codexPath"
  } else {
    Write-Host 'Codex FFD MCP already exists; it was not overwritten.'
  }
} else {
  Write-Warning "Codex config not found: $codexPath"
}

Write-Host 'Done. Restart Codex and Claude Code.'
