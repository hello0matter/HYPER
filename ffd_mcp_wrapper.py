#!/usr/bin/env python3
"""Launch FFD MCP using the API key stored only in ~/.ffd/mcp-config.json.

This wrapper lets Codex and Claude Code share the locally installed FFD setup
without duplicating the key into either client configuration.
"""

import json
import os
import runpy
import sys
from pathlib import Path


ffd_dir = Path.home() / ".ffd"
config_path = ffd_dir / "mcp-config.json"
server_path = ffd_dir / "ffd_mcp_server.py"

if not config_path.exists() or not server_path.exists():
    raise SystemExit("FFD 尚未安装。请先在本地运行官方安装程序并输入 API Key。")

try:
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    ffd = config["mcpServers"]["ffd"]
    ffd_env = ffd["env"]
except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
    raise SystemExit(f"FFD 本地配置不可用：{exc}") from exc

environment = os.environ.copy()
environment.update({str(key): str(value) for key, value in ffd_env.items()})
os.environ.update(environment)
# runpy avoids Windows os.exec* process replacement quirks while preserving stdio for MCP.
sys.argv = [str(server_path)]
runpy.run_path(str(server_path), run_name="__main__")
