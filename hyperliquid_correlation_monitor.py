#!/usr/bin/env python3
"""Hyperliquid 行情研究、模拟交易与可选真实双腿策略服务。"""

import argparse
import base64
import concurrent.futures
import csv
import getpass
import hashlib
import hmac
import itertools
import json
import math
import os
import re
import secrets
import sqlite3
import statistics
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import URLError, HTTPError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import websocket

from crypto_strategy_lab import run_strategy_lab

try:
    import tkinter as tk
    from tkinter import messagebox, ttk
except Exception:
    class _NoTk:
        Tk = object
    tk = _NoTk()
    messagebox = None
    ttk = None


ROOT = Path(__file__).resolve().parent
CONFIG_FILE = ROOT / "monitor_config.json"
EV_JOURNAL_FILE = ROOT / "positive_ev_journal.csv"
ALT_DB_FILE = ROOT / "altcoin_monitor.sqlite3"
LIVE_SECRET_FILE = ROOT / "live_api_secret.json"
HL_INFO = "https://api.hyperliquid.xyz/info"
HL_WS = "wss://api.hyperliquid.xyz/ws"

DEFAULT_CONFIG = {
    "coin": "xyz:GOLD",
    "reference_provider": "hyperliquid_oracle",
    "reference_coin": "xyz:GOLD",
    "custom_url": "",
    "custom_json_path": "price",
    "custom_bid_path": "",
    "custom_ask_path": "",
    "custom_timestamp_path": "",
    "custom_headers_json": "",
    "custom_http_method": "GET",
    "custom_body_json": "",
    "ffd_crypto_id": "BTC",
    "interval_seconds": 5,
    "window_minutes": 60,
    "alert_bps": 25,
    "round_trip_cost_bps": 18,
    "extra_buffer_bps": 10,
    "max_lag_seconds": 60,
    "server_url": "http://127.0.0.1:8787",
    # 保守筛选：至少观察这么久才允许把外部可交易价格差标成“人工核对”。
    "min_observation_minutes": 30,
}

DEFAULT_ALT_LEADERS = "BTC, ETH"
DEFAULT_ALT_ASSETS = "ALL"
DEFAULT_PAPER_NOTIONAL = 1000.0
LIVE_MIN_ORDER_USDC = 10.50
LIVE_EXECUTION_STEPS = (
    "cached_account", "prepare_leverage", "final_l2",
    "submit_real", "record_paper", "refresh_account_async",
)


def base_asset(symbol):
    return symbol.split(":", 1)[-1].strip().upper()


def read_config():
    try:
        saved = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        return {**DEFAULT_CONFIG, **saved}
    except (OSError, json.JSONDecodeError):
        return DEFAULT_CONFIG.copy()


def write_config(config):
    CONFIG_FILE.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def get_json(url, *, payload=None, timeout=12, headers=None, retries=2):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request_headers = {"Content-Type": "application/json", "User-Agent": "HL-Correlation-Monitor/1.0"}
    if headers:
        request_headers.update(headers)
    request = Request(url, data=data, headers=request_headers)
    last_exc = None
    for attempt in range(max(1, retries + 1)):
        try:
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            last_exc = exc
            if exc.code != 429 or attempt >= retries:
                raise
            time.sleep(2 + attempt * 4)
    raise last_exc


def server_endpoint(base_url, path):
    base = base_url.strip().rstrip("/")
    if not base.startswith(("http://", "https://")):
        base = "http://" + base
    return base + path


def split_symbols(value):
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def path_value(obj, path):
    """读取 price 或 data.last.price 形式的 JSON 路径。"""
    value = obj
    for part in path.split("."):
        if isinstance(value, list):
            value = value[int(part)]
        else:
            value = value[part]
    return float(value)


def optional_path_value(obj, path):
    return None if not path.strip() else path_value(obj, path.strip())


def expand_environment(value):
    """把 ${NAME} 替换成环境变量。API 密钥不写入项目配置文件。"""
    return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", lambda match: os.environ.get(match.group(1), ""), value)


def source_timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) or str(value).replace(".", "", 1).isdigit():
        timestamp = float(value)
        return timestamp / 1000 if timestamp > 10_000_000_000 else timestamp
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def fetch_custom_quote(config):
    """通用 REST JSON 适配器：支持最新价或 bid/ask，及可选源时间戳。"""
    url = config["custom_url"].strip()
    if not url.startswith(("https://", "http://")):
        raise ValueError("外部行情 URL 必须以 https:// 或 http:// 开头")
    raw_headers = config.get("custom_headers_json", "").strip()
    try:
        headers = {} if not raw_headers else json.loads(expand_environment(raw_headers))
    except json.JSONDecodeError as exc:
        raise ValueError(f"请求头 JSON 格式不正确：{exc.msg}") from exc
    if not isinstance(headers, dict) or not all(isinstance(key, str) and isinstance(value, str) for key, value in headers.items()):
        raise ValueError("请求头必须是字符串键和值组成的 JSON 对象")
    method = config.get("custom_http_method", "GET").strip().upper()
    raw_body = config.get("custom_body_json", "").strip()
    if method not in ("GET", "POST"):
        raise ValueError("通用适配器当前只支持 GET 或 POST JSON")
    try:
        body = None if not raw_body else json.loads(expand_environment(raw_body))
    except json.JSONDecodeError as exc:
        raise ValueError(f"请求体 JSON 格式不正确：{exc.msg}") from exc
    if method == "GET" and body is not None:
        raise ValueError("GET 请求请把参数写入 URL 查询串；POST 才使用请求体 JSON")
    payload = get_json(url, payload=body, headers=headers)
    bid = optional_path_value(payload, config.get("custom_bid_path", ""))
    ask = optional_path_value(payload, config.get("custom_ask_path", ""))
    price = optional_path_value(payload, config.get("custom_json_path", ""))
    if bid is not None and ask is not None:
        if bid <= 0 or ask <= 0 or bid > ask:
            raise ValueError("外部 bid/ask 无效（需为正数且 bid 不大于 ask）")
        mid = (bid + ask) / 2
    elif price is not None and price > 0:
        mid = price
    else:
        raise ValueError("至少填写一个有效价格字段，或同时填写买一和卖一字段")
    raw_timestamp = None
    timestamp_path = config.get("custom_timestamp_path", "").strip()
    if timestamp_path:
        value = payload
        for part in timestamp_path.split("."):
            value = value[int(part)] if isinstance(value, list) else value[part]
        raw_timestamp = source_timestamp(value)
    return {"mid": mid, "bid": bid, "ask": ask, "source_time": raw_timestamp}


def ffd_mcp_call(tool_name, arguments, timeout=25):
    """Call the local FFD MCP wrapper without exposing its locally stored API key."""
    wrapper = ROOT / "ffd_mcp_wrapper.py"
    if not wrapper.exists():
        raise ValueError("未找到 FFD 包装器；请先完成 FFD 本地安装与同步")
    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": tool_name, "arguments": arguments}},
    ]
    try:
        process = subprocess.run(
            [os.sys.executable, str(wrapper)], input="\n".join(json.dumps(item) for item in requests) + "\n",
            text=True, encoding="utf-8", errors="replace", capture_output=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"FFD 查询超过 {timeout} 秒；这个源只适合低频研究，不适合实时价差") from exc
    if process.returncode != 0:
        raise RuntimeError("FFD 本地 MCP 启动失败，请确认已完成安装并重启客户端")
    rows = [json.loads(line) for line in process.stdout.splitlines() if line.strip()]
    if len(rows) < 2:
        raise RuntimeError("FFD 未返回可用数据")
    result = rows[-1].get("result", {})
    if result.get("isError"):
        raise RuntimeError("FFD 查询失败，请检查当前权益或标的")
    for item in result.get("content", []):
        if item.get("type") == "text":
            return json.loads(item.get("text", "{}"))
    raise RuntimeError("FFD 返回格式无法识别")


def fetch_ffd_crypto_quote(config):
    """FFD crypto snapshots are research-only: no executable bid/ask is provided."""
    coin = config.get("ffd_crypto_id", "").strip()
    if not coin:
        raise ValueError("请填写 FFD 加密标的，例如 BTC 或 ETH")
    value = ffd_mcp_call("ffd_crypto_market_price", {"ids": coin, "vs_currencies": "usd", "format": "json"})
    rows = value.get("data", {}).get("rows", [])
    if not rows or rows[0].get("price") in (None, ""):
        raise ValueError("FFD 未返回该加密标的的价格")
    row = rows[0]
    return {"mid": float(row["price"]), "bid": None, "ask": None, "source_time": source_timestamp(row.get("last_updated_at"))}


def hl_book(coin):
    started = time.time()
    data = get_json(HL_INFO, payload={"type": "l2Book", "coin": coin})
    elapsed_ms = (time.time() - started) * 1000
    bids, asks = data["levels"]
    bid, ask = float(bids[0]["px"]), float(asks[0]["px"])
    return {
        "bid": bid, "ask": ask, "mid": (bid + ask) / 2,
        "spread_bps": (ask / bid - 1) * 10_000,
        "server_time": float(data.get("time", 0)) / 1000,
        "request_ms": elapsed_ms,
    }


def context_from_hl(context):
    return {
        "oracle": float(context["oraclePx"]),
        "mid": float(context.get("midPx") or context["markPx"]),
        "funding_hourly": float(context.get("funding") or 0),
        "premium": float(context.get("premium") or 0),
        "day_ntl_vlm": float(context.get("dayNtlVlm") or 0),
        "open_interest": float(context.get("openInterest") or 0),
    }


def hl_meta_contexts():
    data = get_json(HL_INFO, payload={"type": "metaAndAssetCtxs"})
    universe, contexts = data[0]["universe"], data[1]
    result = {}
    for asset, context in zip(universe, contexts):
        name = asset["name"]
        result[name.upper()] = {"meta": asset, "context": context_from_hl(context)}
    return result


def discover_hl_assets(leaders=None, *, min_volume=0, max_assets=0):
    leaders = {item.upper() for item in (leaders or [])}
    meta = hl_meta_contexts()
    rows = []
    for name, item in meta.items():
        if name in leaders:
            continue
        if item["meta"].get("isDelisted"):
            continue
        volume = item["context"].get("day_ntl_vlm", 0)
        if volume < min_volume:
            continue
        rows.append((volume, name))
    rows.sort(reverse=True)
    assets = [name for _volume, name in rows]
    if max_assets and max_assets > 0:
        assets = assets[:max_assets]
    return assets, meta


def hl_context(coin):
    if ":" not in coin:
        dex, wanted = None, coin
    else:
        dex, wanted = coin.split(":", 1)
    payload = {"type": "metaAndAssetCtxs"}
    if dex:
        payload["dex"] = dex
    data = get_json(HL_INFO, payload=payload)
    universe, contexts = data[0]["universe"], data[1]
    for asset, context in zip(universe, contexts):
        if asset["name"] in (coin, wanted):
            return context_from_hl(context)
    raise ValueError(f"未在 Hyperliquid 找到基准币种：{coin}")


def hl_candles(coin, *, hours=24, interval="5m"):
    """读取 Hyperliquid 公开历史 K 线，供相关性研究使用。"""
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - int(hours) * 60 * 60 * 1000
    data = get_json(HL_INFO, payload={"type": "candleSnapshot", "req": {
        "coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms,
    }})
    return {int(candle["t"]): float(candle["c"]) for candle in data}


def pearson(xs, ys):
    if len(xs) < 8 or len(xs) != len(ys):
        return None
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    numerator = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return None if dx == 0 or dy == 0 else numerator / (dx * dy)


def aligned_returns(left_series, right_series):
    common = sorted(set(left_series) & set(right_series))
    if len(common) < 12:
        return [], [], []
    left_prices = [left_series[t] for t in common]
    right_prices = [right_series[t] for t in common]
    left_returns = [math.log(left_prices[i] / left_prices[i - 1]) for i in range(1, len(left_prices))]
    right_returns = [math.log(right_prices[i] / right_prices[i - 1]) for i in range(1, len(right_prices))]
    return common[1:], left_returns, right_returns


def beta_against(asset_returns, hedge_returns):
    if len(asset_returns) < 8 or len(asset_returns) != len(hedge_returns):
        return None
    mean_asset, mean_hedge = statistics.fmean(asset_returns), statistics.fmean(hedge_returns)
    hedge_var = sum((item - mean_hedge) ** 2 for item in hedge_returns)
    if hedge_var == 0:
        return None
    covariance = sum((a - mean_asset) * (h - mean_hedge) for a, h in zip(asset_returns, hedge_returns))
    return covariance / hedge_var


def residual_snapshot(asset_series, hedge_series):
    _, asset_returns, hedge_returns = aligned_returns(asset_series, hedge_series)
    if len(asset_returns) < 12:
        return None
    corr = pearson(asset_returns, hedge_returns)
    beta = beta_against(asset_returns, hedge_returns)
    if corr is None or beta is None:
        return None
    mean_asset = statistics.fmean(asset_returns)
    mean_hedge = statistics.fmean(hedge_returns)
    residuals = [(a - mean_asset) - beta * (h - mean_hedge) for a, h in zip(asset_returns, hedge_returns)]
    if len(residuals) < 12:
        return None
    sigma = statistics.pstdev(residuals)
    zscore = 0 if sigma == 0 else (residuals[-1] - statistics.fmean(residuals)) / sigma
    recent_asset = sum(asset_returns[-3:]) * 10_000 if len(asset_returns) >= 3 else asset_returns[-1] * 10_000
    recent_hedge = sum(hedge_returns[-3:]) * 10_000 if len(hedge_returns) >= 3 else hedge_returns[-1] * 10_000
    common = sorted(set(asset_series) & set(hedge_series))
    return {"corr": corr, "beta": beta, "zscore": zscore, "asset_15m_bps": recent_asset,
            "hedge_15m_bps": recent_hedge, "samples": len(asset_returns),
            "residual_mean": statistics.fmean(residuals), "residual_sigma": sigma,
            "mean_asset_return": mean_asset, "mean_hedge_return": mean_hedge,
            "last_asset_px": float(asset_series[common[-1]]) if common else None,
            "last_hedge_px": float(hedge_series[common[-1]]) if common else None,
            "last_bar_ts": common[-1] if common else None}


def fetch_many(items, fetcher, max_workers=3):
    results, failures = {}, []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(fetcher, item): item for item in items}
        for future in concurrent.futures.as_completed(future_map):
            item = future_map[future]
            try:
                results[item] = future.result()
            except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                failures.append(f"{item}: {exc}")
    return results, failures


def expand_scan_assets(leaders, assets, *, min_volume=0, max_assets=0):
    requested = [item.upper() for item in assets]
    if any(item in ("ALL", "*", "全部") for item in requested):
        discovered, meta = discover_hl_assets(leaders, min_volume=min_volume, max_assets=max_assets)
        return discovered, meta, [f"自动发现 Hyperliquid 可交易合约 {len(discovered)} 个（已排除保护腿和已下架合约）"]
    return list(dict.fromkeys(requested)), hl_meta_contexts(), []


def altcoin_scan_report(leaders, assets, *, hours, min_corr, min_z, max_spread_bps=None, min_volume=0, max_assets=0):
    assets, meta_contexts, notes = expand_scan_assets(leaders, assets, min_volume=min_volume, max_assets=max_assets)
    wanted = list(dict.fromkeys(leaders + assets))
    series, candle_failures = fetch_many(wanted, lambda asset: hl_candles(asset, hours=hours))
    contexts, context_failures = {}, []
    for asset in assets:
        if asset in meta_contexts:
            contexts[asset] = meta_contexts[asset]["context"]
        else:
            context_failures.append(f"{asset}: 未在 Hyperliquid 找到该合约")
    books, book_failures = fetch_many(assets, hl_book)
    rows = []
    for asset in assets:
        if asset not in series:
            continue
        best = None
        for leader in leaders:
            if leader == asset or leader not in series:
                continue
            snapshot = residual_snapshot(series[asset], series[leader])
            if not snapshot:
                continue
            snapshot["leader"] = leader
            if best is None or abs(snapshot["corr"]) * abs(snapshot["zscore"]) > abs(best["corr"]) * abs(best["zscore"]):
                best = snapshot
        if not best:
            continue
        ctx = contexts.get(asset, {})
        book = books.get(asset, {})
        spread = book.get("spread_bps")
        abs_z = abs(best["zscore"])
        passes = best["corr"] >= min_corr and abs_z >= min_z
        score = best["corr"] * abs_z
        if spread is not None:
            score -= min(spread, 100) / 100
        if max_spread_bps is not None and spread is not None and spread > max_spread_bps:
            passes = False
        rows.append((passes, score, asset, best, ctx, book))
    rows.sort(key=lambda row: (row[0], row[1]), reverse=True)
    return rows, notes + candle_failures + context_failures + book_failures


def format_altcoin_scan(rows, failures, *, hours, min_z, title="小币联动扫描"):
    lines = [f"{title}：最近 {hours} 小时 5分钟K线", ""]
    lines.append("读法：观察 = 没过门槛，只记录；候选 = 过了相关性和偏离门槛，可进入纸面跟踪。")
    lines.append("Z 为正 = 小币相对保护腿偏强；Z 为负 = 小币相对保护腿偏弱。")
    lines.append("beta 是保护比例：做 1,000 USDC 小币，保护腿约 beta*1,000 USDC 反向。")
    lines.append("")
    if not rows:
        lines.append("没有得到可用组合。可能是合约名不存在、K线样本不足，或网络/API 暂时失败。")
    for passes, _score, asset, stat, ctx, book in rows[:20]:
        leader = stat["leader"]
        zscore = stat["zscore"]
        beta = stat["beta"]
        corr = stat["corr"]
        spread = book.get("spread_bps")
        spread_text = "盘口点差未知" if spread is None else f"盘口点差 {spread:.2f} bps"
        funding = ctx.get("funding_hourly")
        if funding is None:
            funding_text = "资金费未知"
        elif funding > 0:
            funding_text = f"资金费 {funding * 10_000:+.3f} bps/小时：做多付费，做空收费"
        elif funding < 0:
            funding_text = f"资金费 {funding * 10_000:+.3f} bps/小时：做空付费，做多收费"
        else:
            funding_text = "资金费接近 0"
        if zscore <= -min_z:
            plan = f"直接腿：观察做多 {asset}；保护腿：做空约 {abs(beta):.2f} 倍 {leader}"
        elif zscore >= min_z:
            plan = f"直接腿：观察做空 {asset}；保护腿：做多约 {abs(beta):.2f} 倍 {leader}"
        else:
            plan = "偏离不够：只记录，不做动作"
        tag = "候选" if passes else "观察"
        if spread is not None and spread > 25:
            tag = "谨慎"
            plan += "；盘口太宽，容易被滑点吃掉"
        lines.append(f"[{tag}] {asset:<10} vs {leader:<4}  corr {corr:+.3f}  beta {beta:+.2f}  Z {zscore:+.2f}  样本 {stat['samples']}")
        lines.append(f"     近15分钟：{asset} {stat['asset_15m_bps']:+.1f} bps / {leader} {stat['hedge_15m_bps']:+.1f} bps；{spread_text}；{funding_text}")
        lines.append(f"     {plan}")
    if failures:
        lines += ["", "未能读取："] + failures[:20]
    lines += ["", "实际验证：把“候选”先纸面记录 1-2 周，看偏离后是否回归、回归需要多久、最大反向浮亏多大。截图里提到参数会影响亏损结果，所以这里要把过滤器记录清楚。"]
    return "\n".join(lines)


def altcoin_payload_rows(rows, *, min_z):
    payload = []
    for passes, score, asset, stat, ctx, book in rows:
        spread = book.get("spread_bps")
        funding = ctx.get("funding_hourly")
        zscore = stat["zscore"]
        if zscore <= -min_z:
            action = "long_asset_short_hedge"
            plan = f"观察做多 {asset}；做空约 {abs(stat['beta']):.2f} 倍 {stat['leader']}"
        elif zscore >= min_z:
            action = "short_asset_long_hedge"
            plan = f"观察做空 {asset}；做多约 {abs(stat['beta']):.2f} 倍 {stat['leader']}"
        else:
            action = "watch"
            plan = "偏离不够：只记录"
        tag = "candidate" if passes else "watch"
        if spread is not None and spread > 25:
            tag = "caution"
        payload.append({
            "tag": tag, "action": action, "plan": plan, "score": score,
            "asset": asset, "leader": stat["leader"], "corr": stat["corr"], "beta": stat["beta"],
            "zscore": zscore, "samples": stat["samples"], "asset_15m_bps": stat["asset_15m_bps"],
            "hedge_15m_bps": stat["hedge_15m_bps"], "spread_bps": spread, "funding_hourly": funding,
            "bid": book.get("bid"), "ask": book.get("ask"), "mid": book.get("mid"),
            "_rt": {key: stat.get(key) for key in (
                "residual_mean", "residual_sigma", "mean_asset_return", "mean_hedge_return",
                "last_asset_px", "last_hedge_px", "last_bar_ts"
            )},
        })
    return payload


def init_alt_db(path=ALT_DB_FILE):
    with sqlite3.connect(path) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                leaders TEXT NOT NULL,
                assets TEXT NOT NULL,
                hours INTEGER NOT NULL,
                min_corr REAL NOT NULL,
                min_z REAL NOT NULL,
                max_spread_bps REAL
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS scan_rows (
                scan_id INTEGER NOT NULL,
                ts REAL NOT NULL,
                tag TEXT NOT NULL,
                action TEXT NOT NULL,
                asset TEXT NOT NULL,
                leader TEXT NOT NULL,
                score REAL NOT NULL,
                corr REAL NOT NULL,
                beta REAL NOT NULL,
                zscore REAL NOT NULL,
                samples INTEGER NOT NULL,
                asset_15m_bps REAL NOT NULL,
                hedge_15m_bps REAL NOT NULL,
                spread_bps REAL,
                funding_hourly REAL,
                bid REAL,
                ask REAL,
                mid REAL,
                plan TEXT NOT NULL
            )
        """)
        db.execute("CREATE INDEX IF NOT EXISTS idx_scan_rows_scan ON scan_rows(scan_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_scan_rows_asset_ts ON scan_rows(asset, ts)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_key TEXT NOT NULL,
                status TEXT NOT NULL,
                asset TEXT NOT NULL,
                leader TEXT NOT NULL,
                action TEXT NOT NULL,
                notional_usdc REAL NOT NULL,
                beta REAL NOT NULL,
                asset_notional_usdc REAL,
                hedge_notional_usdc REAL,
                asset_entry_px REAL,
                hedge_entry_px REAL,
                entry_ts REAL NOT NULL,
                exit_ts REAL,
                entry_z REAL NOT NULL,
                exit_z REAL,
                entry_corr REAL,
                exit_corr REAL,
                entry_spread_bps REAL,
                exit_spread_bps REAL,
                entry_funding_hourly REAL,
                exit_funding_hourly REAL,
                pnl_bps REAL NOT NULL DEFAULT 0,
                pnl_usdc REAL NOT NULL DEFAULT 0,
                close_reason TEXT,
                opened_scan_id INTEGER,
                closed_scan_id INTEGER,
                plan TEXT,
                mode TEXT NOT NULL DEFAULT 'legacy',
                pnl_model TEXT
            )
        """)
        for column in (
            "mode TEXT NOT NULL DEFAULT 'legacy'",
            "asset_notional_usdc REAL", "hedge_notional_usdc REAL",
            "asset_entry_px REAL", "hedge_entry_px REAL", "pnl_model TEXT",
        ):
            try:
                db.execute(f"ALTER TABLE paper_trades ADD COLUMN {column}")
            except sqlite3.OperationalError:
                pass
        db.execute("CREATE INDEX IF NOT EXISTS idx_paper_trades_status ON paper_trades(status, trade_key)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_paper_trades_ts ON paper_trades(entry_ts, exit_ts)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS paper_equity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                scan_id INTEGER,
                realized_usdc REAL NOT NULL,
                unrealized_usdc REAL NOT NULL,
                total_usdc REAL NOT NULL,
                open_count INTEGER NOT NULL,
                mode TEXT NOT NULL DEFAULT 'legacy',
                pnl_model TEXT
            )
        """)
        for column in ("mode TEXT NOT NULL DEFAULT 'legacy'", "pnl_model TEXT"):
            try:
                db.execute(f"ALTER TABLE paper_equity ADD COLUMN {column}")
            except sqlite3.OperationalError:
                pass
        db.execute("CREATE INDEX IF NOT EXISTS idx_paper_equity_ts ON paper_equity(ts)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS live_account_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                account_address TEXT NOT NULL,
                account_value REAL,
                total_margin_used REAL,
                total_notional REAL,
                spot_usdc REAL,
                spot_available_usdc REAL,
                account_mode TEXT,
                positions_json TEXT NOT NULL
            )
        """)
        # Existing installations already have this table; SQLite needs an explicit migration.
        for column in ("spot_usdc REAL", "spot_available_usdc REAL", "account_mode TEXT"):
            try:
                db.execute(f"ALTER TABLE live_account_snapshots ADD COLUMN {column}")
            except sqlite3.OperationalError:
                pass
        db.execute("CREATE INDEX IF NOT EXISTS idx_live_account_snapshots_ts ON live_account_snapshots(ts)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS live_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_key TEXT NOT NULL,
                status TEXT NOT NULL,
                asset TEXT NOT NULL,
                leader TEXT NOT NULL,
                action TEXT NOT NULL,
                asset_notional_usdc REAL NOT NULL,
                hedge_notional_usdc REAL NOT NULL,
                total_notional_usdc REAL NOT NULL,
                beta REAL NOT NULL,
                entry_ts REAL NOT NULL,
                exit_ts REAL,
                entry_z REAL NOT NULL,
                exit_z REAL,
                entry_corr REAL,
                exit_corr REAL,
                entry_spread_bps REAL,
                exit_spread_bps REAL,
                asset_size REAL NOT NULL DEFAULT 0,
                hedge_size REAL NOT NULL DEFAULT 0,
                asset_entry_px REAL,
                hedge_entry_px REAL,
                asset_exit_px REAL,
                hedge_exit_px REAL,
                pnl_usdc REAL NOT NULL DEFAULT 0,
                pnl_bps REAL NOT NULL DEFAULT 0,
                fee_usdc REAL,
                funding_usdc REAL,
                net_pnl_usdc REAL,
                net_pnl_bps REAL,
                asset_net_pnl_usdc REAL,
                hedge_net_pnl_usdc REAL,
                close_reason TEXT,
                opened_scan_id INTEGER,
                closed_scan_id INTEGER,
                entry_json TEXT,
                exit_json TEXT,
                note TEXT
            )
        """)
        for column in (
            "fee_usdc REAL", "funding_usdc REAL", "net_pnl_usdc REAL", "net_pnl_bps REAL",
            "asset_net_pnl_usdc REAL", "hedge_net_pnl_usdc REAL",
        ):
            try:
                db.execute(f"ALTER TABLE live_trades ADD COLUMN {column}")
            except sqlite3.OperationalError:
                pass
        db.execute("CREATE INDEX IF NOT EXISTS idx_live_trades_status ON live_trades(status, trade_key)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_live_trades_ts ON live_trades(entry_ts, exit_ts)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS leadlag_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL,
                asset TEXT NOT NULL,
                leader TEXT NOT NULL,
                side TEXT NOT NULL,
                notional_usdc REAL NOT NULL,
                entry_ts REAL NOT NULL,
                exit_ts REAL,
                entry_px REAL NOT NULL,
                exit_px REAL,
                size REAL NOT NULL,
                leader_3s_bps REAL,
                leader_15s_bps REAL,
                asset_3s_bps REAL,
                lag_bps REAL,
                expected_edge_bps REAL,
                corr REAL,
                beta REAL,
                entry_spread_bps REAL,
                entry_imbalance REAL,
                gross_bps REAL NOT NULL DEFAULT 0,
                fee_bps REAL NOT NULL DEFAULT 0,
                funding_bps REAL NOT NULL DEFAULT 0,
                net_bps REAL NOT NULL DEFAULT 0,
                pnl_usdc REAL NOT NULL DEFAULT 0,
                max_net_bps REAL NOT NULL DEFAULT 0,
                close_reason TEXT,
                signal_json TEXT
            )
        """)
        db.execute("CREATE INDEX IF NOT EXISTS idx_leadlag_status ON leadlag_trades(status, asset)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_leadlag_ts ON leadlag_trades(entry_ts, exit_ts)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS strategy_lab_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                coins TEXT NOT NULL,
                interval TEXT NOT NULL,
                days INTEGER NOT NULL,
                round_trip_cost_bps REAL NOT NULL,
                payload_json TEXT NOT NULL
            )
        """)
        db.execute("CREATE INDEX IF NOT EXISTS idx_strategy_lab_runs_ts ON strategy_lab_runs(ts)")


def save_alt_scan(payload, config, *, db_path=ALT_DB_FILE):
    init_alt_db(db_path)
    ts = payload["ts"]
    with sqlite3.connect(db_path) as db:
        cursor = db.execute(
            "INSERT INTO scans (ts, leaders, assets, hours, min_corr, min_z, max_spread_bps) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ts, ",".join(config["leaders"]), ",".join(config["assets"]), config["hours"],
             config["min_corr"], config["min_z"], config.get("max_spread_bps")),
        )
        scan_id = cursor.lastrowid
        for row in payload["rows"]:
            db.execute("""
                INSERT INTO scan_rows (
                    scan_id, ts, tag, action, asset, leader, score, corr, beta, zscore, samples,
                    asset_15m_bps, hedge_15m_bps, spread_bps, funding_hourly, bid, ask, mid, plan
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                scan_id, ts, row["tag"], row["action"], row["asset"], row["leader"], row["score"],
                row["corr"], row["beta"], row["zscore"], row["samples"], row["asset_15m_bps"],
                row["hedge_15m_bps"], row["spread_bps"], row["funding_hourly"], row["bid"],
                row["ask"], row["mid"], row["plan"],
            ))
    return scan_id


def save_strategy_lab_run(payload, *, db_path=ALT_DB_FILE):
    init_alt_db(db_path)
    with sqlite3.connect(db_path) as db:
        cursor = db.execute("""
            INSERT INTO strategy_lab_runs (
                ts, coins, interval, days, round_trip_cost_bps, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            float(payload.get("ts") or time.time()), ",".join(payload.get("coins") or []),
            str(payload.get("interval") or "15m"), int(payload.get("days") or 30),
            float(payload.get("round_trip_cost_bps") or 0), json.dumps(payload, ensure_ascii=False),
        ))
        return cursor.lastrowid


def load_latest_strategy_lab_run(*, db_path=ALT_DB_FILE):
    init_alt_db(db_path)
    with sqlite3.connect(db_path) as db:
        row = db.execute("SELECT payload_json FROM strategy_lab_runs ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except (TypeError, json.JSONDecodeError):
        return None


def fetch_live_account_snapshot(account_address, account_mode_hint=None):
    """Public/read-only account view. It never needs an API private key."""
    if not valid_evm_address(account_address):
        raise ValueError("未配置有效的主钱包公开地址")
    requests = {
        "perps": {"type": "clearinghouseState", "user": account_address},
        "spot": {"type": "spotClearinghouseState", "user": account_address},
    }
    if not account_mode_hint:
        requests["mode"] = {"type": "userAbstraction", "user": account_address}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(requests)) as pool:
        futures = {
            key: pool.submit(get_json, HL_INFO, payload=body, timeout=12)
            for key, body in requests.items()
        }
        results = {key: future.result() for key, future in futures.items()}
    raw = results["perps"]
    spot_raw = results["spot"]
    account_mode = str(account_mode_hint or results.get("mode") or "standard")
    margin = raw.get("marginSummary") or {}
    spot_usdc = 0.0
    spot_available_usdc = 0.0
    for balance in spot_raw.get("balances") or []:
        if str(balance.get("coin", "")).upper() == "USDC":
            spot_usdc = float(balance.get("total") or 0)
            spot_available_usdc = max(0.0, spot_usdc - float(balance.get("hold") or 0))
            break
    positions = []
    for item in raw.get("assetPositions") or []:
        pos = item.get("position") or {}
        try:
            size = float(pos.get("szi") or 0)
        except (ValueError, TypeError):
            size = 0.0
        if size == 0:
            continue
        positions.append({
            "coin": pos.get("coin", ""), "size": size,
            "entry_px": float(pos.get("entryPx") or 0),
            "position_value": float(pos.get("positionValue") or 0),
            "unrealized_pnl": float(pos.get("unrealizedPnl") or 0),
            "liquidation_px": pos.get("liquidationPx"),
            "leverage": (pos.get("leverage") or {}).get("value"),
        })
    return {
        "ts": time.time(), "account_address": account_address,
        "account_value": float(margin.get("accountValue") or 0),
        "total_margin_used": float(margin.get("totalMarginUsed") or 0),
        "total_notional": float(margin.get("totalNtlPos") or 0),
        "spot_usdc": spot_usdc, "spot_available_usdc": spot_available_usdc,
        "account_mode": account_mode,
        "positions": positions,
    }


def save_live_account_snapshot(snapshot, db_path=ALT_DB_FILE):
    init_alt_db(db_path)
    with sqlite3.connect(db_path) as db:
        db.execute("""
            INSERT INTO live_account_snapshots
            (ts, account_address, account_value, total_margin_used, total_notional, spot_usdc, spot_available_usdc, account_mode, positions_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (snapshot["ts"], snapshot["account_address"], snapshot["account_value"],
              snapshot["total_margin_used"], snapshot["total_notional"],
              snapshot.get("spot_usdc", 0), snapshot.get("spot_available_usdc", 0),
              snapshot.get("account_mode", "standard"),
              json.dumps(snapshot["positions"], ensure_ascii=False)))


def load_latest_live_account_snapshot(db_path=ALT_DB_FILE):
    init_alt_db(db_path)
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM live_account_snapshots ORDER BY ts DESC LIMIT 1").fetchone()
    if not row:
        return None
    return {
        "ts": row["ts"], "account_address": row["account_address"], "account_value": row["account_value"],
        "total_margin_used": row["total_margin_used"], "total_notional": row["total_notional"],
        "spot_usdc": row["spot_usdc"], "spot_available_usdc": row["spot_available_usdc"],
        "account_mode": row["account_mode"],
        "positions": json.loads(row["positions_json"]),
    }


def _live_sdk_exchange(config):
    try:
        from eth_account import Account
        from hyperliquid.exchange import Exchange
        from hyperliquid.utils import constants
    except ImportError as exc:
        raise RuntimeError("服务器缺少 Hyperliquid 官方 SDK") from exc
    private_key = load_live_api_key()
    if not private_key:
        raise RuntimeError("服务器未配置 API 钱包私钥")
    if not valid_evm_address(config.get("live_account_address")):
        raise RuntimeError("未配置主钱包公开地址")
    wallet = Account.from_key(private_key)
    return Exchange(wallet, constants.MAINNET_API_URL, account_address=config["live_account_address"])


def _round_down_size(value, decimals):
    scale = 10 ** int(decimals)
    return math.floor(float(value) * scale + 1e-12) / scale


def _round_up_size(value, decimals):
    scale = 10 ** int(decimals)
    return math.ceil(float(value) * scale - 1e-12) / scale


def _live_ioc_order(exchange, coin, is_buy, notional_usdc, slippage_bps, *, reduce_only=False, size_override=None, min_notional_usdc=0, px_override=None):
    coin_key = exchange.info.name_to_coin[coin]
    mids = exchange.info.all_mids()
    mid = float(px_override if px_override else mids[coin_key])
    asset_id = exchange.info.name_to_asset(coin)
    decimals = exchange.info.asset_to_sz_decimals[asset_id]
    if size_override is not None:
        size = _round_down_size(size_override, decimals)
    else:
        size = _round_down_size(float(notional_usdc) / mid, decimals)
        if min_notional_usdc > 0:
            size = max(size, _round_up_size(float(min_notional_usdc) / mid, decimals))
    if size <= 0:
        raise ValueError(f"{coin} 下单数量过小")
    limit_px = exchange._slippage_price(coin, is_buy, max(0.0001, float(slippage_bps) / 10_000), px=mid)
    return {
        "coin": coin, "is_buy": bool(is_buy), "sz": size, "limit_px": limit_px,
        "order_type": {"limit": {"tif": "Ioc"}}, "reduce_only": bool(reduce_only),
    }


def _live_fills_from_response(response, expected_coins):
    statuses = (((response or {}).get("response") or {}).get("data") or {}).get("statuses") or []
    result = {}
    for index, coin in enumerate(expected_coins):
        status = statuses[index] if index < len(statuses) else {"error": "交易所未返回该腿状态"}
        filled = status.get("filled") if isinstance(status, dict) else None
        if filled:
            result[coin] = {"filled": True, "size": float(filled.get("totalSz") or 0),
                            "price": float(filled.get("avgPx") or 0), "oid": filled.get("oid"), "raw": status}
        else:
            result[coin] = {"filled": False, "size": 0.0, "price": 0.0, "raw": status}
    return result


def _stored_order_fills(raw_json):
    try:
        payload = json.loads(raw_json or "{}") if isinstance(raw_json, str) else (raw_json or {})
    except json.JSONDecodeError:
        return {}
    return payload.get("fills") or {}


def official_trade_costs(trade, fills_by_oid, funding_rows):
    """Calculate official fees/funding and per-leg net PnL for one closed trade."""
    entry_fills = _stored_order_fills(trade.get("entry_json"))
    exit_fills = _stored_order_fills(trade.get("exit_json"))
    start_ms = float(trade.get("entry_ts") or 0) * 1000
    end_ms = float(trade.get("exit_ts") or trade.get("entry_ts") or 0) * 1000
    legs = {}
    for coin in (trade.get("asset"), trade.get("leader")):
        entry = entry_fills.get(coin) or {}
        exit_fill = exit_fills.get(coin) or {}
        entry_official = fills_by_oid.get(str(entry.get("oid"))) or {}
        exit_official = fills_by_oid.get(str(exit_fill.get("oid"))) or {}
        fees = float(entry_official.get("fee") or 0) + float(exit_official.get("fee") or 0)
        if exit_official:
            gross = float(exit_official.get("closedPnl") or 0)
        else:
            is_asset = coin == trade.get("asset")
            entry_px = float(trade.get("asset_entry_px" if is_asset else "hedge_entry_px") or 0)
            exit_px = float(trade.get("asset_exit_px" if is_asset else "hedge_exit_px") or 0)
            size = float(trade.get("asset_size" if is_asset else "hedge_size") or 0)
            entry_buy = (trade.get("action") == "long_asset_short_hedge") if is_asset else (trade.get("action") != "long_asset_short_hedge")
            gross = (exit_px - entry_px) * size * (1 if entry_buy else -1)
        funding = sum(
            float((item.get("delta") or {}).get("usdc") or 0)
            for item in funding_rows
            if start_ms <= float(item.get("time") or 0) <= end_ms
            and str((item.get("delta") or {}).get("coin") or "").upper() == str(coin or "").upper()
        )
        legs[coin] = {"gross": gross, "fee": fees, "funding": funding, "net": gross - fees + funding}
    asset_leg = legs.get(trade.get("asset"), {"net": 0})
    hedge_leg = legs.get(trade.get("leader"), {"net": 0})
    gross = sum(item["gross"] for item in legs.values())
    fee = sum(item["fee"] for item in legs.values())
    funding = sum(item["funding"] for item in legs.values())
    net = gross - fee + funding
    total_notional = max(float(trade.get("total_notional_usdc") or 0), 1e-9)
    return {
        "pnl_usdc": gross, "pnl_bps": gross / total_notional * 10_000,
        "fee_usdc": fee, "funding_usdc": funding,
        "net_pnl_usdc": net, "net_pnl_bps": net / total_notional * 10_000,
        "asset_net_pnl_usdc": asset_leg["net"], "hedge_net_pnl_usdc": hedge_leg["net"],
    }


def fetch_official_trade_costs(config, trade):
    account = str(config.get("live_account_address") or "")
    if not valid_evm_address(account):
        return None
    fills = get_json(HL_INFO, payload={"type": "userFills", "user": account}, timeout=15)
    fills_by_oid = {str(item.get("oid")): item for item in fills if item.get("oid") is not None}
    funding_rows = get_json(HL_INFO, payload={
        "type": "userFunding", "user": account,
        "startTime": max(0, int(float(trade.get("entry_ts") or 0) * 1000) - 1000),
    }, timeout=15)
    return official_trade_costs(trade, fills_by_oid, funding_rows)


def reconcile_live_trade_costs(state):
    """Backfill official net PnL for historical closed trades without placing orders."""
    try:
        account = str(state.config.get("live_account_address") or "")
        if not valid_evm_address(account):
            return
        with sqlite3.connect(state.db_path) as db:
            db.row_factory = sqlite3.Row
            trades = [dict(row) for row in db.execute(
                "SELECT * FROM live_trades WHERE status='closed' ORDER BY entry_ts"
            ).fetchall()]
        if not trades:
            return
        fills = get_json(HL_INFO, payload={"type": "userFills", "user": account}, timeout=20)
        fills_by_oid = {str(item.get("oid")): item for item in fills if item.get("oid") is not None}
        funding_rows = get_json(HL_INFO, payload={
            "type": "userFunding", "user": account,
            "startTime": max(0, int(min(float(trade["entry_ts"]) for trade in trades) * 1000) - 1000),
        }, timeout=20)
        with sqlite3.connect(state.db_path) as db:
            for trade in trades:
                costs = official_trade_costs(trade, fills_by_oid, funding_rows)
                db.execute("""
                    UPDATE live_trades SET pnl_usdc=?, pnl_bps=?, fee_usdc=?, funding_usdc=?,
                        net_pnl_usdc=?, net_pnl_bps=?, asset_net_pnl_usdc=?, hedge_net_pnl_usdc=?
                    WHERE id=?
                """, (
                    costs["pnl_usdc"], costs["pnl_bps"], costs["fee_usdc"], costs["funding_usdc"],
                    costs["net_pnl_usdc"], costs["net_pnl_bps"], costs["asset_net_pnl_usdc"],
                    costs["hedge_net_pnl_usdc"], trade["id"],
                ))
        reconcile_shared_paper_from_live(state.db_path, state.config.get("paper_fee_bps", 9.0))
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] reconciled official costs for {len(trades)} live trades", flush=True)
    except Exception as exc:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] live cost reconciliation failed: {exc}", flush=True)


def reconcile_shared_paper_from_live(db_path=ALT_DB_FILE, fee_bps=9.0):
    """Repair old unified paper rows that correspond to an actual live fill."""
    init_alt_db(db_path)
    repaired = 0
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        legacy_rows = [dict(row) for row in db.execute("""
            SELECT * FROM paper_trades
            WHERE mode='shared_strategy' AND COALESCE(pnl_model, 'z_approx') != 'l2_executable'
        """).fetchall()]
        live_rows = [dict(row) for row in db.execute("""
            SELECT * FROM live_trades WHERE status IN ('open', 'closed')
        """).fetchall()]
        for paper in legacy_rows:
            matches = [
                live for live in live_rows
                if live.get("asset") == paper.get("asset")
                and live.get("leader") == paper.get("leader")
                and live.get("action") == paper.get("action")
                and abs(float(live.get("entry_ts") or 0) - float(paper.get("entry_ts") or 0)) <= 30
            ]
            if not matches:
                continue
            live = min(matches, key=lambda item: abs(float(item.get("entry_ts") or 0) - float(paper.get("entry_ts") or 0)))
            total = max(float(live.get("total_notional_usdc") or paper.get("notional_usdc") or 0), 1e-9)
            if live.get("status") == "closed":
                pnl_usdc = live.get("net_pnl_usdc")
                if pnl_usdc is None:
                    pnl_usdc = float(live.get("pnl_usdc") or 0) - total * float(fee_bps) / 10_000
                pnl_bps = float(pnl_usdc) / total * 10_000
                status = "closed"
                exit_ts = live.get("exit_ts")
                exit_z = live.get("exit_z")
                close_reason = paper.get("close_reason") or live.get("close_reason") or "与真实成交记录对账"
            else:
                pnl_usdc = pnl_bps = 0.0
                status = "open"
                exit_ts = exit_z = None
                close_reason = None
            db.execute("""
                UPDATE paper_trades SET status=?, notional_usdc=?, asset_notional_usdc=?, hedge_notional_usdc=?,
                    asset_entry_px=?, hedge_entry_px=?, entry_ts=?, exit_ts=?, exit_z=?, pnl_usdc=?, pnl_bps=?,
                    close_reason=?, pnl_model='l2_executable'
                WHERE id=?
            """, (
                status, total, live.get("asset_notional_usdc"), live.get("hedge_notional_usdc"),
                live.get("asset_entry_px"), live.get("hedge_entry_px"), live.get("entry_ts"), exit_ts,
                exit_z, pnl_usdc, pnl_bps, close_reason, paper["id"],
            ))
            repaired += 1
    if repaired:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] repaired {repaired} unified paper rows from live fills", flush=True)
    return repaired


def load_latest_scan(db_path=ALT_DB_FILE):
    if not Path(db_path).exists():
        return None
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        scan = db.execute("SELECT * FROM scans ORDER BY id DESC LIMIT 1").fetchone()
        if not scan:
            return None
        rows = [dict(row) for row in db.execute("SELECT * FROM scan_rows WHERE scan_id = ? ORDER BY tag = 'candidate' DESC, score DESC", (scan["id"],))]
    return {"scan": dict(scan), "rows": rows}


def load_asset_history(asset, limit=200, db_path=ALT_DB_FILE):
    if not Path(db_path).exists():
        return []
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT * FROM scan_rows WHERE asset = ? ORDER BY ts DESC LIMIT ?",
            (asset.upper(), int(limit)),
        ).fetchall()
    return [dict(row) for row in rows]


def load_asset_pair_series(asset, leader=None, limit=240, db_path=ALT_DB_FILE):
    if not Path(db_path).exists():
        return []
    sql = "SELECT * FROM scan_rows WHERE asset = ?"
    params = [asset.upper()]
    if leader:
        sql += " AND leader = ?"
        params.append(leader.upper())
    sql += " ORDER BY ts DESC LIMIT ?"
    params.append(int(limit))
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(sql, params).fetchall()
    return [dict(row) for row in rows][::-1]


def load_asset_pair_series_since(asset, leader=None, hours=168, limit=5000, db_path=ALT_DB_FILE):
    if not Path(db_path).exists():
        return []
    since = time.time() - max(1, int(hours)) * 3600
    sql = "SELECT * FROM scan_rows WHERE asset = ? AND ts >= ?"
    params = [asset.upper(), since]
    if leader:
        sql += " AND leader = ?"
        params.append(leader.upper())
    sql += " ORDER BY ts ASC LIMIT ?"
    params.append(int(limit))
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def load_latest_rows_for_pairs(pairs, db_path=ALT_DB_FILE):
    if not pairs or not Path(db_path).exists():
        return {}
    result = {}
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        for asset, leader in pairs:
            row = db.execute(
                "SELECT * FROM scan_rows WHERE asset = ? AND leader = ? ORDER BY ts DESC LIMIT 1",
                (asset.upper(), leader.upper()),
            ).fetchone()
            if row:
                item = dict(row)
                result[f"{asset.upper()}:{leader.upper()}"] = item
    return result


def summarize_pair_history(rows):
    if not rows:
        return {}
    zscores = [float(row["zscore"]) for row in rows if row.get("zscore") is not None]
    corrs = [float(row["corr"]) for row in rows if row.get("corr") is not None]
    spreads = [float(row["spread_bps"]) for row in rows if row.get("spread_bps") is not None]
    candidates = sum(1 for row in rows if row.get("tag") == "candidate")
    def avg(values):
        return statistics.fmean(values) if values else None
    def stdev(values):
        return statistics.pstdev(values) if len(values) > 1 else 0
    return {
        "points": len(rows), "candidate_count": candidates,
        "candidate_ratio": candidates / len(rows),
        "z_min": min(zscores) if zscores else None, "z_max": max(zscores) if zscores else None,
        "z_avg": avg(zscores), "z_std": stdev(zscores),
        "corr_avg": avg(corrs), "corr_std": stdev(corrs),
        "spread_avg": avg(spreads), "spread_max": max(spreads) if spreads else None,
    }


def replay_backtest_from_scan_rows(rows, *, entry_z=2.0, exit_z=0.5, max_hold=36, fee_bps=9.0, z_value_bps=18.0):
    rows = [row for row in rows if row.get("zscore") is not None]
    if len(rows) < 12:
        raise ValueError(f"数据库样本不足：当前只有 {len(rows)} 个点。等服务器多跑一段时间再复盘。")
    trades, position = [], None
    for i, row in enumerate(rows):
        z = float(row["zscore"])
        if position is None:
            if z >= entry_z:
                position = {
                    "side": "short_asset", "entry": i, "entry_ts": row["ts"], "entry_z": z,
                    "entry_corr": row.get("corr"), "entry_spread_bps": row.get("spread_bps"),
                }
            elif z <= -entry_z:
                position = {
                    "side": "long_asset", "entry": i, "entry_ts": row["ts"], "entry_z": z,
                    "entry_corr": row.get("corr"), "entry_spread_bps": row.get("spread_bps"),
                }
            continue
        hold = i - position["entry"]
        if position["side"] == "short_asset":
            pnl = (float(position["entry_z"]) - z) * z_value_bps - fee_bps
        else:
            pnl = (z - float(position["entry_z"])) * z_value_bps - fee_bps
        should_exit = abs(z) <= exit_z or hold >= max_hold or i == len(rows) - 1
        if should_exit:
            position.update({
                "exit": i, "exit_ts": row["ts"], "exit_z": z, "hold_bars": hold,
                "pnl": pnl, "exit_corr": row.get("corr"), "exit_spread_bps": row.get("spread_bps"),
            })
            trades.append(position)
            position = None
    if not trades:
        return {
            "source": "db_replay", "points": len(rows), "trades": [], "total_bps": 0,
            "win_rate": 0, "avg_bps": 0, "worst_bps": 0,
            "note": "数据库里没有触发完整的入场/出场。不是坏了，而是这段历史没有满足规则的机会。",
        }
    total = sum(float(trade["pnl"]) for trade in trades)
    wins = sum(float(trade["pnl"]) > 0 for trade in trades)
    return {
        "source": "db_replay", "points": len(rows), "trades": trades, "total_bps": total,
        "win_rate": wins / len(trades), "avg_bps": total / len(trades),
        "worst_bps": min(float(trade["pnl"]) for trade in trades),
        "note": "这是基于服务器数据库扫描点的复盘，避免实时请求交易所；收益是Z偏离收敛近似，不是真实成交回测。",
    }


def paper_trade_key(row):
    return f"{row['asset']}:{row['leader']}:{row['action']}"


def paper_pair_key(row_or_trade):
    return f"{row_or_trade['asset']}:{row_or_trade['leader']}"


def paper_direction_label(action):
    if action == "short_asset_long_hedge":
        return "模拟做空小币 / 做多保护腿"
    if action == "long_asset_short_hedge":
        return "模拟做多小币 / 做空保护腿"
    return "模拟观察"


def paper_trade_leg_notionals(trade):
    """Return asset/hedge notionals, including a safe derivation for old rows."""
    asset = float(trade.get("asset_notional_usdc") or 0)
    hedge = float(trade.get("hedge_notional_usdc") or 0)
    if asset > 0 and hedge > 0:
        return asset, hedge
    total = float(trade.get("total_notional_usdc") or trade.get("notional_usdc") or 0)
    beta = abs(float(trade.get("beta") or 0))
    if total > 0:
        asset = total / max(1.0 + beta, 1e-9)
        hedge = max(0.0, total - asset)
    return asset, hedge


def strategy_row_execution_prices(row, action, *, closing=False):
    """Use executable bid/ask sides for the two-leg simulated fill."""
    check_books = ((row.get("_strategy_l2_check") or {}).get("books") or {})
    asset_book = check_books.get(row.get("asset")) or {}
    hedge_book = check_books.get(row.get("leader")) or {}

    def px(name, side, fallback_book):
        value = fallback_book.get(side)
        if value in (None, ""):
            value = row.get(name)
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        return value if value > 0 else None

    if action == "long_asset_short_hedge":
        asset_side, hedge_side = (("bid", "ask") if closing else ("ask", "bid"))
    elif action == "short_asset_long_hedge":
        asset_side, hedge_side = (("ask", "bid") if closing else ("bid", "ask"))
    else:
        return None, None
    return (
        px(f"asset_l2_{asset_side}", asset_side, asset_book),
        px(f"hedge_l2_{hedge_side}", hedge_side, hedge_book),
    )


def paper_trade_pnl_details(trade, row, config, now_ts=None):
    """Prefer executable two-leg price PnL; fall back only for historical rows."""
    fee_bps = float(config.get("paper_fee_bps", 9.0))
    now_ts = now_ts or time.time()
    hours = max(0.0, (now_ts - float(trade["entry_ts"])) / 3600)
    funding_hourly_bps = float(row.get("funding_hourly") or 0) * 10_000
    if trade["action"] == "short_asset_long_hedge":
        funding_sign = 1.0
    elif trade["action"] == "long_asset_short_hedge":
        funding_sign = -1.0
    else:
        funding_sign = 0.0
    funding_bps = funding_sign * funding_hourly_bps * hours

    asset_entry = float(trade.get("asset_entry_px") or 0)
    hedge_entry = float(trade.get("hedge_entry_px") or 0)
    asset_exit, hedge_exit = strategy_row_execution_prices(row, trade.get("action"), closing=True)
    asset_notional, hedge_notional = paper_trade_leg_notionals(trade)
    total_notional = asset_notional + hedge_notional
    if all((asset_entry > 0, hedge_entry > 0, asset_exit, hedge_exit, total_notional > 0)):
        asset_buy = trade["action"] == "long_asset_short_hedge"
        hedge_buy = not asset_buy
        asset_size = asset_notional / asset_entry
        hedge_size = hedge_notional / hedge_entry
        gross_usdc = ((float(asset_exit) - asset_entry) * asset_size * (1 if asset_buy else -1)
                      + (float(hedge_exit) - hedge_entry) * hedge_size * (1 if hedge_buy else -1))
        gross_bps = gross_usdc / total_notional * 10_000
        pnl_bps = gross_bps + funding_bps - fee_bps
        return {
            "pnl_bps": pnl_bps, "pnl_usdc": total_notional * pnl_bps / 10_000,
            "gross_bps": gross_bps, "funding_bps": funding_bps, "fee_bps": fee_bps,
            "pnl_model": "l2_executable", "asset_exit_px": asset_exit, "hedge_exit_px": hedge_exit,
        }

    entry_z = float(trade["entry_z"])
    current_z = float(row.get("zscore") or 0)
    z_value_bps = float(config.get("paper_z_value_bps", 18.0))
    if trade["action"] == "short_asset_long_hedge":
        gross_bps = (entry_z - current_z) * z_value_bps
    elif trade["action"] == "long_asset_short_hedge":
        gross_bps = (current_z - entry_z) * z_value_bps
    else:
        gross_bps = 0.0
    pnl_bps = gross_bps + funding_bps - fee_bps
    total = float(trade.get("notional_usdc") or total_notional or 0)
    return {
        "pnl_bps": pnl_bps, "pnl_usdc": total * pnl_bps / 10_000,
        "gross_bps": gross_bps, "funding_bps": funding_bps, "fee_bps": fee_bps,
        "pnl_model": "z_approx", "asset_exit_px": None, "hedge_exit_px": None,
    }


def paper_trade_pnl_bps(trade, row, config, now_ts=None):
    return paper_trade_pnl_details(trade, row, config, now_ts)["pnl_bps"]


def paper_close_reason(trade, row, config, now_ts=None):
    now_ts = now_ts or time.time()
    z = float(row.get("zscore") or 0)
    corr = float(row.get("corr") or 0)
    spread = row.get("spread_bps")
    pnl = paper_trade_pnl_bps(trade, row, config, now_ts)
    max_spread = config.get("paper_max_spread_bps") or config.get("max_spread_bps")
    if abs(z) <= float(config.get("paper_exit_z", 0.5)):
        return "偏离回归", pnl
    # A fixed take-profit is optional.  The normal exit is still Z returning
    # to neutral; this is a safety valve for cases where the simulated PnL has
    # already reached the configured target before the residual fully normalizes.
    take_profit_bps = float(config.get("paper_take_profit_bps", 50.0) or 0)
    if take_profit_bps > 0 and pnl >= take_profit_bps:
        return "达到固定止盈", pnl
    if pnl <= -abs(float(config.get("paper_stop_bps", 80.0))):
        return "触发止损", pnl
    if now_ts - float(trade["entry_ts"]) >= float(config.get("paper_max_hold_minutes", 360)) * 60:
        return "超过最长持仓", pnl
    if corr < float(config.get("paper_min_corr", config.get("min_corr", 0.65))) * 0.85:
        return "相关性恶化", pnl
    if max_spread is not None and spread is not None and float(spread) > float(max_spread) * 2.0:
        return "点差恶化", pnl
    return None, pnl


def load_paper_snapshot(db_path=ALT_DB_FILE, limit=200, current_rows=None, config=None):
    init_alt_db(db_path)
    sync_live = bool((config or {}).get("paper_sync_live", True))
    trade_mode_where = (" AND mode = 'shared_strategy' AND pnl_model='l2_executable'"
                        if sync_live else " AND mode = 'legacy'")
    equity_mode_where = (" WHERE mode = 'shared_strategy' AND pnl_model='l2_executable'"
                         if sync_live else " WHERE mode = 'legacy'")
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        open_rows = [dict(row) for row in db.execute(
            "SELECT * FROM paper_trades WHERE status = 'open'" + trade_mode_where + " ORDER BY entry_ts DESC"
        ).fetchall()]
        closed_rows = [dict(row) for row in db.execute(
            "SELECT * FROM paper_trades WHERE status = 'closed'" + trade_mode_where + " ORDER BY exit_ts DESC LIMIT ?", (int(limit),)
        ).fetchall()]
        equity_rows = [dict(row) for row in db.execute(
            "SELECT * FROM paper_equity" + equity_mode_where + " ORDER BY ts DESC LIMIT ?", (int(limit),)
        ).fetchall()][::-1]
        stats = db.execute("""
            SELECT
                COUNT(*) AS trades,
                SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END) AS wins,
                COALESCE(SUM(pnl_usdc), 0) AS realized,
                COALESCE(AVG(pnl_bps), 0) AS avg_bps,
                COALESCE(MIN(pnl_bps), 0) AS worst_bps
            FROM paper_trades WHERE status = 'closed'
        """ + trade_mode_where).fetchone()
    trades = int(stats["trades"] or 0)
    wins = int(stats["wins"] or 0)
    if current_rows and config:
        row_by_pair = {paper_pair_key(row): row for row in current_rows}
        missing_pairs = [
            (trade["asset"], trade["leader"]) for trade in open_rows
            if paper_pair_key(trade) not in row_by_pair
        ]
        row_by_pair.update(load_latest_rows_for_pairs(missing_pairs, db_path))
        now_ts = time.time()
        for trade in open_rows:
            row = row_by_pair.get(paper_pair_key(trade))
            if not row:
                continue
            pnl_details = paper_trade_pnl_details(trade, row, config, now_ts)
            trade["current_z"] = row.get("zscore")
            trade["current_corr"] = row.get("corr")
            trade["current_spread_bps"] = row.get("spread_bps")
            trade["pnl_bps"] = pnl_details["pnl_bps"]
            trade["pnl_usdc"] = pnl_details["pnl_usdc"]
            trade["pnl_model"] = pnl_details["pnl_model"]
    return {
        "enabled": True,
        "mode": "shared_strategy" if sync_live else "independent",
        "open": open_rows,
        "closed": closed_rows,
        "equity": equity_rows,
        "stats": {
            "trades": trades,
            "wins": wins,
            "win_rate": wins / trades if trades else 0,
            "realized_usdc": float(stats["realized"] or 0),
            "avg_bps": float(stats["avg_bps"] or 0),
            "worst_bps": float(stats["worst_bps"] or 0),
        },
    }


def notify_dingtalk_paper_trade(state, alert_type, trade, row=None, reason=None):
    setting = "notify_paper_open" if alert_type == "模拟开仓" else "notify_paper_close"
    if not state.config.get(setting, True):
        return
    webhook, keyword = dingtalk_channel_config(state.config, "paper")
    if not webhook:
        return
    dash = _dashboard_url(state.config)
    asset = trade.get("asset") or (row or {}).get("asset")
    leader = trade.get("leader") or (row or {}).get("leader")
    asset_notional, hedge_notional = paper_trade_leg_notionals(trade)
    lines = [
        f"{keyword} Hyperliquid 模拟盘提醒",
        f"类型：{alert_type}",
        f"触发时间：{beijing_time_text()}（北京时间）",
        f"币对：{asset} vs {leader}",
        f"方向：{paper_direction_label(trade.get('action'))}",
        f"模拟名义金额：{_fmt_plain(asset_notional, 2, ' U')} + {_fmt_plain(hedge_notional, 2, ' U')}",
        f"入场Z：{_fmt_signed(trade.get('entry_z'), 2)}",
        "盈亏算法：两腿盘口可成交价，扣除配置成本",
    ]
    if row:
        lines.append(f"当前Z：{_fmt_signed(row.get('zscore'), 2)} | corr：{_fmt_signed(row.get('corr'), 3)} | 点差：{_fmt_plain(row.get('spread_bps'), 2, ' bps')}")
    if trade.get("pnl_bps") is not None and alert_type != "模拟开仓":
        lines.append(f"模拟成本后盈亏：{_fmt_signed(trade.get('pnl_bps'), 1, ' bps')} / {_fmt_signed(trade.get('pnl_usdc'), 4, ' USDC')}")
    if reason:
        lines.append(f"原因：{reason}")
    if dash:
        lines.append(f"看图：{dash}")
    lines.append("性质：模拟盘按当时买一/卖一估算成交；仍可能与真实IOC成交、手续费和资金费不同。")
    try:
        dingtalk_post(webhook, "\n".join(lines))
    except Exception as exc:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] dingtalk paper notify failed: {exc}", flush=True)


def notify_dingtalk_leadlag(state, alert_type, trade):
    setting = "notify_leadlag_open" if alert_type == "V2模拟开仓" else "notify_leadlag_close"
    if not state.config.get(setting, True):
        return
    webhook, keyword = dingtalk_channel_config(state.config, "paper")
    if not webhook:
        return
    lines = [
        f"{keyword} Hyperliquid 联动传播 V2",
        f"类型：{alert_type}",
        f"时间：{beijing_time_text()}（北京时间）",
        f"小币 / 龙头：{trade.get('asset')} / {trade.get('leader')}",
        f"方向：{'模拟做多小币' if trade.get('side') == 'long' else '模拟做空小币'}",
        f"单腿金额：{_fmt_plain(trade.get('notional_usdc'), 2, ' U')}",
        f"跟随缺口：{_fmt_signed(trade.get('lag_bps'), 1, ' bps')} | 扣费预期空间：{_fmt_signed(trade.get('expected_edge_bps'), 1, ' bps')}",
    ]
    if alert_type != "V2模拟开仓":
        lines.append(f"成本后盈亏：{_fmt_signed(trade.get('net_bps'), 1, ' bps')} / {_fmt_signed(trade.get('pnl_usdc'), 4, ' U')}")
        lines.append(f"平仓原因：{trade.get('close_reason') or '-'}")
    dash = _dashboard_url(state.config)
    if dash:
        lines.append(f"面板：{dash}")
    try:
        dingtalk_post(webhook, "\n".join(lines))
    except Exception as exc:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] dingtalk leadlag notify failed: {exc}", flush=True)


def live_direction_label(action):
    if action == "short_asset_long_hedge":
        return "真实做空小币 / 做多保护腿"
    if action == "long_asset_short_hedge":
        return "真实做多小币 / 做空保护腿"
    return "真实观察"


def _live_position_size_map(account):
    return {str(pos.get("coin") or "").upper(): float(pos.get("size") or 0) for pos in (account.get("positions") or [])}


def _live_trade_legs(trade):
    asset_buy = trade["action"] == "long_asset_short_hedge"
    hedge_buy = not asset_buy
    return [
        {
            "coin": trade["asset"],
            "entry_px": float(trade["asset_entry_px"] or 0),
            "size": float(trade["asset_size"] or 0),
            "entry_buy": asset_buy,
            "expected_sign": 1 if asset_buy else -1,
            "close_is_buy": not asset_buy,
        },
        {
            "coin": trade["leader"],
            "entry_px": float(trade["hedge_entry_px"] or 0),
            "size": float(trade["hedge_size"] or 0),
            "entry_buy": hedge_buy,
            "expected_sign": 1 if hedge_buy else -1,
            "close_is_buy": not hedge_buy,
        },
    ]


def load_live_trades_snapshot(db_path=ALT_DB_FILE, limit=200, current_rows=None, config=None):
    init_alt_db(db_path)
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        open_rows = [dict(row) for row in db.execute("SELECT * FROM live_trades WHERE status = 'open' ORDER BY entry_ts DESC").fetchall()]
        closed_rows = [dict(row) for row in db.execute("SELECT * FROM live_trades WHERE status != 'open' ORDER BY COALESCE(exit_ts, entry_ts) DESC LIMIT ?", (int(limit),)).fetchall()]
        stats = db.execute("""
            SELECT
                COUNT(*) AS trades,
                SUM(CASE WHEN COALESCE(net_pnl_usdc, pnl_usdc) > 0 THEN 1 ELSE 0 END) AS wins,
                COALESCE(SUM(COALESCE(net_pnl_usdc, pnl_usdc)), 0) AS realized,
                COALESCE(AVG(COALESCE(net_pnl_bps, pnl_bps)), 0) AS avg_bps,
                COALESCE(MIN(COALESCE(net_pnl_bps, pnl_bps)), 0) AS worst_bps,
                COALESCE(SUM(pnl_usdc), 0) AS gross_realized,
                COALESCE(SUM(fee_usdc), 0) AS fees,
                COALESCE(SUM(funding_usdc), 0) AS funding,
                COALESCE(SUM(asset_net_pnl_usdc), 0) AS asset_only_net,
                COALESCE(SUM(hedge_net_pnl_usdc), 0) AS hedge_leg_net,
                COALESCE(SUM(total_notional_usdc), 0) AS total_entry_notional,
                COALESCE(AVG(fee_usdc), 0) AS avg_fee
            FROM live_trades WHERE status = 'closed'
        """).fetchone()
    if current_rows and config:
        row_by_pair = {paper_pair_key(row): row for row in current_rows}
        missing_pairs = [
            (trade["asset"], trade["leader"]) for trade in open_rows
            if paper_pair_key(trade) not in row_by_pair
        ]
        row_by_pair.update(load_latest_rows_for_pairs(missing_pairs, db_path))
        now_ts = time.time()
        for trade in open_rows:
            row = row_by_pair.get(paper_pair_key(trade))
            if not row:
                continue
            pnl_details = paper_trade_pnl_details(
                {**trade, "notional_usdc": trade.get("total_notional_usdc")}, row, config, now_ts,
            )
            trade["current_z"] = row.get("zscore")
            trade["current_corr"] = row.get("corr")
            trade["current_beta"] = row.get("beta")
            trade["current_spread_bps"] = row.get("spread_bps")
            trade["current_asset_15m_bps"] = row.get("asset_15m_bps")
            trade["current_hedge_15m_bps"] = row.get("hedge_15m_bps")
            trade["current_funding_hourly"] = row.get("funding_hourly")
            trade["current_plan"] = row.get("plan")
            trade["current_tag"] = row.get("tag")
            trade["signal_pnl_bps"] = pnl_details["pnl_bps"]
            trade["signal_pnl_usdc"] = pnl_details["pnl_usdc"]
            trade["signal_pnl_model"] = pnl_details["pnl_model"]
    trades = int(stats["trades"] or 0)
    wins = int(stats["wins"] or 0)
    return {
        "open": open_rows,
        "closed": closed_rows,
        "stats": {
            "trades": trades,
            "wins": wins,
            "win_rate": wins / trades if trades else 0,
            "realized_usdc": float(stats["realized"] or 0),
            "avg_bps": float(stats["avg_bps"] or 0),
            "worst_bps": float(stats["worst_bps"] or 0),
            "gross_realized_usdc": float(stats["gross_realized"] or 0),
            "fee_usdc": float(stats["fees"] or 0),
            "funding_usdc": float(stats["funding"] or 0),
            "asset_only_net_usdc": float(stats["asset_only_net"] or 0),
            "hedge_leg_net_usdc": float(stats["hedge_leg_net"] or 0),
            "total_entry_notional_usdc": float(stats["total_entry_notional"] or 0),
            "avg_fee_usdc": float(stats["avg_fee"] or 0),
        },
    }


def notify_dingtalk_live_trade(state, alert_type, trade, row=None, reason=None):
    if "异常" in alert_type or "失败" in alert_type:
        setting = "notify_live_error"
    elif alert_type == "真实开仓":
        setting = "notify_live_open"
    elif alert_type.startswith("真实平仓"):
        setting = "notify_live_close"
    else:
        setting = "notify_live_test"
    if not state.config.get(setting, state.config.get("notify_live_test", True)):
        return
    webhook, keyword = dingtalk_channel_config(state.config, "live")
    if not webhook:
        return
    lines = [
        f"{keyword} Hyperliquid 真实策略提醒",
        f"类型：{alert_type}",
        f"时间：{beijing_time_text()}（北京时间）",
        f"币对：{trade.get('asset')} vs {trade.get('leader')}",
        f"方向：{live_direction_label(trade.get('action'))}",
        f"实际名义金额：{_fmt_plain(trade.get('asset_notional_usdc'), 2, ' U')} + {_fmt_plain(trade.get('hedge_notional_usdc'), 2, ' U')}",
        f"入场Z：{_fmt_signed(trade.get('entry_z'), 2)}",
    ]
    if row:
        lines.append(f"当前Z：{_fmt_signed(row.get('zscore'), 2)} | corr：{_fmt_signed(row.get('corr'), 3)} | 点差：{_fmt_plain(row.get('spread_bps'), 2, ' bps')}")
    if trade.get("pnl_usdc") is not None and (alert_type != "真实开仓"):
        lines.append(f"实际价格毛盈亏：{_fmt_signed(trade.get('pnl_usdc'), 4, ' U')} / {_fmt_signed(trade.get('pnl_bps'), 1, ' bps')}（未扣官方手续费、资金费）")
        if trade.get("net_pnl_usdc") is not None:
            lines.append(
                f"官方手续费：{_fmt_plain(trade.get('fee_usdc'), 4, ' U')} | "
                f"实际资金费：{_fmt_signed(trade.get('funding_usdc'), 4, ' U')}"
            )
            lines.append(f"官方成本后净盈亏：{_fmt_signed(trade.get('net_pnl_usdc'), 4, ' U')} / {_fmt_signed(trade.get('net_pnl_bps'), 1, ' bps')}")
        else:
            fee_bps = float(state.config.get("paper_fee_bps", 9.0) or 0)
            total_notional = float(trade.get("total_notional_usdc") or 0)
            if total_notional > 0 and fee_bps > 0:
                net_bps = float(trade.get("pnl_bps") or 0) - fee_bps
                net_usdc = float(trade.get("pnl_usdc") or 0) - total_notional * fee_bps / 10_000
                lines.append(f"暂按配置成本估算净收益：{_fmt_signed(net_usdc, 4, ' U')} / {_fmt_signed(net_bps, 1, ' bps')}")
    if reason:
        lines.append(f"原因：{reason}")
    dash = _dashboard_url(state.config)
    if dash:
        lines.append(f"查看面板：{dash}")
    lines.append("性质：真实订单已经发送；最终以 Hyperliquid 官方成交、手续费和仓位为准。")
    try:
        dingtalk_post(webhook, "\n".join(lines))
    except Exception as exc:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] dingtalk live trade notify failed: {exc}", flush=True)


def notify_dingtalk_live_emergency(config, title, lines):
    if not config.get("notify_live_error", True):
        return
    webhook, keyword = dingtalk_channel_config(config, "live")
    if not webhook:
        return
    body = [
        f"{keyword} Hyperliquid 真实策略提醒",
        f"类型：{title}",
        f"时间：{beijing_time_text()}（北京时间）",
        *lines,
    ]
    dash = _dashboard_url(config)
    if dash:
        body.append(f"查看面板：{dash}")
    body.append("性质：紧急真实订单已经发送；最终以 Hyperliquid 官方成交、手续费和仓位为准。")
    try:
        dingtalk_post(webhook, "\n".join(body))
    except Exception as exc:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] dingtalk emergency notify failed: {exc}", flush=True)


def _live_sized_notionals(row, requested_asset_notional, available_usdc, *, auto_min_notional=False):
    beta = abs(float(row.get("beta") or 0))
    if beta <= 0:
        raise ValueError("该候选 Beta 无效")
    asset_notional = float(requested_asset_notional)
    if asset_notional <= 0:
        raise ValueError("真实交易每笔小币腿 USDC 必须大于 0")
    required_asset_notional = max(LIVE_MIN_ORDER_USDC, LIVE_MIN_ORDER_USDC / beta)
    if asset_notional < required_asset_notional:
        if not auto_min_notional:
            raise RuntimeError(
                "跳过真实开仓：Hyperliquid 每条腿最低订单价值约 $10；"
                f"当前小币腿 {asset_notional:.2f}U、保护腿 {asset_notional * beta:.2f}U。"
                f"该币对 beta={beta:.2f}，若要成交，小币腿至少约 {required_asset_notional:.2f}U；"
                "也可以在真实交易面板开启“自动补到交易所最低”。"
            )
        asset_notional = required_asset_notional
    hedge_notional = asset_notional * beta
    gross = asset_notional + hedge_notional
    cap = max(0.0, min(60.0, float(available_usdc or 0) * 0.75))
    if cap > 0 and gross > cap:
        raise RuntimeError(f"双腿名义金额约 {gross:.2f} USDC，超过当前真实策略单笔上限 {cap:.2f} USDC")
    return asset_notional, hedge_notional


def _live_target_leverage(config):
    return max(1, min(50, int(config.get("live_leverage", 1) or 1)))


def _live_apply_leverage(exchange, coins, leverage):
    results = {}
    for coin in sorted({str(item).upper() for item in coins if item}):
        try:
            results[coin] = exchange.update_leverage(int(leverage), coin, is_cross=True)
        except Exception as exc:
            results[coin] = {"error": str(exc)}
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] set leverage failed: {coin} {leverage}x {exc}", flush=True)
    return results


def live_execution_steps(config):
    # Fixed, reviewed order.  The former browser step editor was too easy to
    # misunderstand and could move or remove safety checks.
    return list(LIVE_EXECUTION_STEPS)


def cached_live_account(state, *, max_age_ms=None):
    with state.lock:
        account = dict(state.live_account) if state.live_account else None
    if not account:
        return None, None
    age_ms = max(0.0, (time.time() - float(account.get("ts") or 0)) * 1000)
    if max_age_ms is not None and age_ms > float(max_age_ms):
        return None, age_ms
    return account, age_ms


def refresh_live_account_cache(state):
    address = state.config.get("live_account_address")
    if not valid_evm_address(address):
        return None
    with state.lock:
        mode_hint = (state.live_account or {}).get("account_mode")
    snapshot = fetch_live_account_snapshot(address, account_mode_hint=mode_hint)
    should_save = time.time() - float(getattr(state, "last_live_account_db_save", 0) or 0) >= 30
    if should_save:
        save_live_account_snapshot(snapshot, state.db_path)
    with state.lock:
        state.live_account, state.live_error = snapshot, None
        if should_save:
            state.last_live_account_db_save = time.time()
    return snapshot


def refresh_live_account_cache_async(state):
    def work():
        try:
            refresh_live_account_cache(state)
        except Exception as exc:
            with state.lock:
                state.live_error = str(exc)
    threading.Thread(target=work, daemon=True).start()


def live_account_cache_loop(state):
    while state.running:
        if valid_evm_address(state.config.get("live_account_address")):
            try:
                refresh_live_account_cache(state)
            except Exception as exc:
                with state.lock:
                    state.live_error = str(exc)
        interval = max(1.0, float(state.config.get("live_account_poll_seconds", 5.0) or 5.0))
        wait_started = time.time()
        while state.running and time.time() - wait_started < interval:
            time.sleep(0.2)


def live_prepare_leverage(state, exchange, coins, leverage):
    ttl = max(0.0, float(state.config.get("live_leverage_cache_seconds", 86_400) or 0))
    now = time.time()
    results, missing = {}, []
    with state.lock:
        for coin in sorted({str(item).upper() for item in coins if item}):
            cached = state.live_leverage_cache.get((coin, int(leverage)))
            if cached and now - float(cached.get("ts") or 0) <= ttl:
                results[coin] = {"status": "ok", "cached": True, "raw": cached.get("raw")}
            else:
                missing.append(coin)
    if missing:
        fresh = _live_apply_leverage(exchange, missing, leverage)
        results.update(fresh)
        with state.lock:
            for coin, result in fresh.items():
                if isinstance(result, dict) and result.get("status") == "ok":
                    state.live_leverage_cache[(coin, int(leverage))] = {"ts": now, "raw": result}
    return results


def apply_live_leverage_to_current_positions(config):
    if not config.get("live_enabled") or not live_config_public(config).get("execution_ready"):
        return {}
    account = fetch_live_account_snapshot(config["live_account_address"])
    coins = [pos.get("coin") for pos in account.get("positions") or []]
    if not coins:
        return {}
    exchange = _live_sdk_exchange(config)
    return _live_apply_leverage(exchange, coins, _live_target_leverage(config))


def live_expected_edge_bps(row, config):
    z = abs(float(row.get("zscore") or 0))
    exit_z = float(config.get("paper_exit_z", 0.5) or 0.5)
    z_value_bps = float(config.get("paper_z_value_bps", 18.0) or 18.0)
    spread_bps = float(row.get("spread_bps") or 0)
    fee_bps = float(config.get("paper_fee_bps", 9.0) or 9.0)
    gross = max(0.0, z - exit_z) * z_value_bps
    estimated_cost = fee_bps + spread_bps * 2
    return gross - estimated_cost


def live_candidate_reject_reasons(row, config):
    """Return every basic signal filter that blocks a real entry.

    This deliberately excludes account/order-book state.  Keeping the pure
    signal checks here makes the trading loop, API diagnostics and UI use the
    exact same rules instead of each reimplementing a slightly different set.
    """
    z = abs(float(row.get("zscore") or 0))
    corr = float(row.get("corr") or 0)
    spread = float(row.get("spread_bps")) if row.get("spread_bps") is not None else 999.0
    min_z = float(config.get("live_min_entry_z", 3.0) or 3.0)
    min_corr = float(config.get("live_min_corr", 0.75) or 0.75)
    max_spread = float(config.get("live_l2_max_spread_bps", 2.5) or 2.5)
    min_edge = float(config.get("live_min_expected_edge_bps", 25.0) or 25.0)
    edge = live_expected_edge_bps(row, config)
    reasons = []
    if z < min_z:
        reasons.append(("z", f"|Z| {z:.2f} < 实盘最低 {min_z:.2f}"))
    if corr < min_corr:
        reasons.append(("corr", f"相关 {corr:.3f} < 实盘最低 {min_corr:.3f}"))
    if spread > max_spread:
        reasons.append(("spread", f"点差 {spread:.2f}bps > 实盘最高 {max_spread:.2f}bps"))
    if edge < min_edge:
        reasons.append(("edge", f"预期边际 {edge:.1f}bps < 实盘最低 {min_edge:.1f}bps"))
    return reasons


def live_candidate_reject_reason(row, config):
    reasons = live_candidate_reject_reasons(row, config)
    return reasons[0][1] if reasons else ""


def realtime_row_from_l2book(row, l2book):
    rt = row.get("_rt") or {}
    sigma = float(rt.get("residual_sigma") or 0)
    last_asset = float(rt.get("last_asset_px") or 0)
    last_hedge = float(rt.get("last_hedge_px") or 0)
    if sigma <= 0 or last_asset <= 0 or last_hedge <= 0:
        return dict(row)
    asset_book = l2book.get_book(row.get("asset"))
    hedge_book = l2book.get_book(row.get("leader"))
    if not asset_book or not hedge_book:
        return dict(row)
    try:
        asset_mid = float(asset_book["mid"])
        hedge_mid = float(hedge_book["mid"])
        asset_ret = math.log(asset_mid / last_asset)
        hedge_ret = math.log(hedge_mid / last_hedge)
        beta = float(row.get("beta") or 0)
        residual = (asset_ret - float(rt.get("mean_asset_return") or 0)) - beta * (hedge_ret - float(rt.get("mean_hedge_return") or 0))
        z = (residual - float(rt.get("residual_mean") or 0)) / sigma
        out = dict(row)
        out["kline_zscore"] = row.get("kline_zscore", row.get("zscore"))
        out["zscore"] = z
        out["realtime_zscore"] = z
        out["realtime"] = True
        out["asset_l2_mid"] = asset_mid
        out["hedge_l2_mid"] = hedge_mid
        out["asset_l2_bid"] = float(asset_book.get("bid") or 0)
        out["asset_l2_ask"] = float(asset_book.get("ask") or 0)
        out["hedge_l2_bid"] = float(hedge_book.get("bid") or 0)
        out["hedge_l2_ask"] = float(hedge_book.get("ask") or 0)
        out["asset_l2_age_ms"] = asset_book.get("age_ms")
        out["hedge_l2_age_ms"] = hedge_book.get("age_ms")
        out["spread_bps"] = max(float(asset_book.get("spread_bps") or 0), float(hedge_book.get("spread_bps") or 0))
        return out
    except (ValueError, KeyError, TypeError, ZeroDivisionError):
        return dict(row)


def realtime_rows_from_l2book(rows, l2book):
    return [realtime_row_from_l2book(row, l2book) for row in rows]


def prepare_live_rows(rows, config, l2book=None):
    """Build the rows used by real trading and by its diagnostics.

    The scan's candidate/watch tag is based on the last completed 5-minute
    candle.  When WS Z is enabled the Z value can change every tick, so the
    real direction must be recalculated from the current Z instead of reusing
    that old candle tag/action.
    """
    prepared = realtime_rows_from_l2book(rows, l2book) if config.get("live_use_realtime_z", True) and l2book else [dict(row) for row in rows]
    # Even when realtime Z is disabled, price-based simulation and stop-loss
    # still need current executable bid/ask prices.
    if l2book:
        enriched = []
        for source in prepared:
            row = dict(source)
            asset_book = l2book.get_book(row.get("asset"))
            hedge_book = l2book.get_book(row.get("leader"))
            if asset_book and hedge_book:
                row.update({
                    "asset_l2_bid": asset_book.get("bid"), "asset_l2_ask": asset_book.get("ask"),
                    "asset_l2_mid": asset_book.get("mid"), "hedge_l2_bid": hedge_book.get("bid"),
                    "hedge_l2_ask": hedge_book.get("ask"), "hedge_l2_mid": hedge_book.get("mid"),
                    "asset_l2_age_ms": asset_book.get("age_ms"), "hedge_l2_age_ms": hedge_book.get("age_ms"),
                    "spread_bps": max(float(asset_book.get("spread_bps") or 0), float(hedge_book.get("spread_bps") or 0)),
                })
            enriched.append(row)
        prepared = enriched
    min_z = float(config.get("live_min_entry_z", 3.0) or 3.0)
    result = []
    for source in prepared:
        row = dict(source)
        z = float(row.get("zscore") or 0)
        row["scan_tag"] = row.get("tag")
        row["scan_action"] = row.get("action")
        if z > 0:
            row["action"] = "short_asset_long_hedge"
            row["plan"] = f"实盘方向：做空 {row.get('asset')}；做多约 {abs(float(row.get('beta') or 0)):.2f} 倍 {row.get('leader')}"
        elif z < 0:
            row["action"] = "long_asset_short_hedge"
            row["plan"] = f"实盘方向：做多 {row.get('asset')}；做空约 {abs(float(row.get('beta') or 0)):.2f} 倍 {row.get('leader')}"
        else:
            row["action"] = "watch"
            row["plan"] = "实时偏离接近 0，只观察"
        reasons = live_candidate_reject_reasons(row, config)
        row["live_status"] = "pass" if not reasons else "blocked"
        row["live_reject_reason"] = reasons[0][1] if reasons else "基础过滤通过，等待实时盘口与账户校验"
        row["live_reject_all"] = [text for _key, text in reasons]
        row["live_expected_edge_bps"] = live_expected_edge_bps(row, config)
        row["live_min_z_met"] = abs(z) >= min_z
        result.append(row)
    return result


def live_opportunity_diagnostics(state, rows, account=None, open_trades=None, limit=20):
    """Explain, in machine- and human-readable form, why no real order opened."""
    config = state.config
    prepared = prepare_live_rows(rows, config, state.l2book)
    counts = {"pass": 0, "z": 0, "corr": 0, "spread": 0, "edge": 0, "l2": 0}
    opportunities = []
    for row in prepared:
        reasons = live_candidate_reject_reasons(row, config)
        reason_key = reasons[0][0] if reasons else "pass"
        reason_text = reasons[0][1] if reasons else "基础过滤通过"
        l2_reason = ""
        if not reasons and config.get("live_use_l2book", True):
            try:
                available = float((account or {}).get("spot_available_usdc") or (account or {}).get("account_value") or 0)
                asset_notional, hedge_notional = _live_sized_notionals(
                    row, config.get("live_notional_usdc", 10.0), available,
                    auto_min_notional=bool(config.get("live_auto_min_notional", False)),
                )
                l2_reason, _books = live_l2book_reject_reason(state, row, asset_notional, hedge_notional)
            except (ValueError, RuntimeError, TypeError) as exc:
                l2_reason = str(exc)
            if l2_reason:
                reason_key, reason_text = "l2", l2_reason
        counts[reason_key] = counts.get(reason_key, 0) + 1
        opportunities.append({
            "asset": row.get("asset"), "leader": row.get("leader"),
            "zscore": row.get("zscore"), "kline_zscore": row.get("kline_zscore", row.get("zscore")),
            "corr": row.get("corr"), "beta": row.get("beta"), "spread_bps": row.get("spread_bps"),
            "expected_edge_bps": live_expected_edge_bps(row, config), "action": row.get("action"),
            "status": "pass" if reason_key == "pass" else "blocked", "reason_key": reason_key,
            "reason": "可以进入下单阶段" if reason_key == "pass" else reason_text,
        })
    opportunities.sort(key=lambda row: (0 if row["status"] == "pass" else 1, -abs(float(row.get("zscore") or 0))))

    open_trades = list(open_trades or [])
    global_reasons = []
    public = live_config_public(config)
    if not config.get("live_enabled"):
        global_reasons.append("真实下单总开关已关闭")
    if not config.get("live_strategy_enabled"):
        global_reasons.append("真实策略开关已关闭")
    if not public.get("execution_ready"):
        global_reasons.append(public.get("blocker") or "真实交易配置未就绪")
    if len(open_trades) >= int(config.get("live_max_open", 1) or 1):
        global_reasons.append(f"已达到最多真实仓位 {int(config.get('live_max_open', 1) or 1)} 组")
    if not config.get("live_auto_min_notional", False) and float(config.get("live_notional_usdc") or 0) < LIVE_MIN_ORDER_USDC:
        global_reasons.append(f"每笔小币腿低于交易所单腿最低约 {LIVE_MIN_ORDER_USDC:.0f}U")
    positions = list((account or {}).get("positions") or [])
    if positions and not open_trades:
        global_reasons.append("官方账户存在程序数据库未跟踪的仓位，为避免净仓冲突暂停新开仓")
    return {
        "ts": time.time(), "total": len(prepared), "counts": counts,
        "pass_count": counts.get("pass", 0), "global_reasons": global_reasons,
        "can_open_now": not global_reasons and counts.get("pass", 0) > 0,
        "opportunities": opportunities[:max(1, int(limit))],
    }


def l2book_subscription_coins(rows, open_trades=None, leaders=None):
    coins = {str(item).upper() for item in (leaders or []) if item}
    for trade in open_trades or []:
        coins.add(str(trade.get("asset") or "").upper())
        coins.add(str(trade.get("leader") or "").upper())
    ranked = list(rows or [])
    ranked.sort(key=lambda r: (0 if r.get("tag") == "candidate" else 1, -abs(float(r.get("zscore") or 0))))
    for row in ranked[:1000]:
        coins.add(str(row.get("asset") or "").upper())
        coins.add(str(row.get("leader") or "").upper())
    return {coin for coin in coins if coin}


def configured_ws_leaders(config):
    return set(config.get("leaders", [])) | set(split_symbols(str(config.get("leadlag_leaders") or ""))) | {"BTC", "ETH"}


def live_l2book_reject_reason(state, row, asset_notional, hedge_notional, *, allow_strategy_grace=False):
    config = state.config
    if not config.get("live_use_l2book", True):
        return "", {}
    max_age_ms = float(config.get("live_l2_max_age_ms", 3000) or 3000)
    grace_ms = float(config.get("live_strategy_entry_grace_ms", 10_000) or 0)
    max_spread_bps = float(config.get("live_l2_max_spread_bps", 2.5) or 2.5)
    strategy_check = row.get("_strategy_l2_check") or {}
    checked_at = float(strategy_check.get("checked_at") or 0)
    checked_books = strategy_check.get("books") or {}
    ws_connected = bool(state.l2book.snapshot(["__STATUS_ONLY__"]).get("status", {}).get("connected"))
    books = {}
    for coin, notional in ((row["asset"], asset_notional), (row["leader"], hedge_notional)):
        book = state.l2book.get_book(coin)
        books[coin] = book
        if not book:
            return f"l2Book 未收到 {coin} 盘口，跳过真实开仓", books
        age_ms = float(book.get("age_ms") or 999999)
        if age_ms > max_age_ms:
            accepted = checked_books.get(coin) or {}
            elapsed_ms = max(0.0, (time.time() - checked_at) * 1000) if checked_at else 999999.0
            same_top = (
                accepted
                and float(accepted.get("bid") or 0) == float(book.get("bid") or 0)
                and float(accepted.get("ask") or 0) == float(book.get("ask") or 0)
            )
            grace_ok = allow_strategy_grace and ws_connected and same_top and elapsed_ms <= grace_ms
            if not grace_ok:
                return f"{coin} l2Book 数据过旧 {age_ms:.0f}ms > {max_age_ms:.0f}ms", books
        if float(book.get("spread_bps") or 999999) > max_spread_bps:
            return f"{coin} 实时盘口点差 {float(book.get('spread_bps') or 0):.2f}bps > {max_spread_bps:.2f}bps", books
        mid = float(book.get("mid") or 0)
        top_size = float((book.get("ask_size") if row["action"] == "long_asset_short_hedge" and coin == row["asset"] else book.get("bid_size")) or 0)
        if coin == row["leader"]:
            top_size = float((book.get("bid_size") if row["action"] == "long_asset_short_hedge" else book.get("ask_size")) or 0)
        top_notional = top_size * mid
        if top_notional and top_notional < float(notional) * 0.5:
            return f"{coin} 顶层盘口深度偏薄：约 {top_notional:.2f}U，不足目标腿 {float(notional):.2f}U 的一半", books
    return "", books


def open_live_strategy_trade(state, row, scan_id):
    config = state.config
    steps = live_execution_steps(config)
    account = None
    account_age_ms = None
    asset_notional = hedge_notional = None
    exchange = None
    leverage_result = {}
    l2_books = {}
    response = None
    execution_timing = []
    asset_buy = row["action"] == "long_asset_short_hedge"
    hedge_buy = not asset_buy
    slippage = float(config.get("live_max_slippage_bps", 15))
    target_leverage = _live_target_leverage(config)
    orders = None

    def ensure_notionals():
        nonlocal asset_notional, hedge_notional
        if asset_notional is not None:
            return
        available = 0.0
        if account:
            available = (float(account.get("spot_available_usdc") or 0)
                         if account.get("account_mode") == "unifiedAccount"
                         else float(account.get("account_value") or 0))
        asset_notional, hedge_notional = _live_sized_notionals(
            row, config.get("live_notional_usdc", 10.0), available,
            auto_min_notional=bool(config.get("live_auto_min_notional", False)),
        )

    for step in (item for item in steps if item != "record_paper"):
        step_started = time.perf_counter()
        if step == "cached_account":
            max_age = float(config.get("live_account_cache_max_age_ms", 15_000) or 15_000)
            account, account_age_ms = cached_live_account(state, max_age_ms=max_age)
            if not account:
                raise RuntimeError(f"后台账户缓存不可用或过旧（约 {float(account_age_ms or 0):.0f}ms），极速流程不进行同步HTTP查询")
        elif step == "prepare_leverage":
            exchange = exchange or _live_sdk_exchange(config)
            leverage_result = live_prepare_leverage(
                state, exchange, [row["asset"], row["leader"]], target_leverage,
            )
            if bool(config.get("live_require_leverage_ok", True)):
                failed_leverage = {
                    coin: result for coin, result in leverage_result.items()
                    if not (isinstance(result, dict) and result.get("status") == "ok")
                }
                if failed_leverage:
                    raise RuntimeError(f"杠杆设置失败，已跳过真实开仓：目标 {target_leverage}x；失败 {failed_leverage}")
        elif step == "final_l2":
            ensure_notionals()
            l2_reject, l2_books = live_l2book_reject_reason(
                state, row, asset_notional, hedge_notional, allow_strategy_grace=True,
            )
            if l2_reject:
                raise RuntimeError(l2_reject)
        elif step == "submit_real":
            ensure_notionals()
            exchange = exchange or _live_sdk_exchange(config)
            if not l2_books:
                l2_books = {
                    row["asset"]: state.l2book.get_book(row["asset"]),
                    row["leader"]: state.l2book.get_book(row["leader"]),
                }
            asset_book = l2_books.get(row["asset"]) or {}
            hedge_book = l2_books.get(row["leader"]) or {}
            asset_px = asset_book.get("ask") if asset_buy else asset_book.get("bid")
            hedge_px = hedge_book.get("ask") if hedge_buy else hedge_book.get("bid")
            orders = [
                _live_ioc_order(exchange, row["asset"], asset_buy, asset_notional, slippage, min_notional_usdc=LIVE_MIN_ORDER_USDC, px_override=asset_px),
                _live_ioc_order(exchange, row["leader"], hedge_buy, hedge_notional, slippage, min_notional_usdc=LIVE_MIN_ORDER_USDC, px_override=hedge_px),
            ]
            response = exchange.bulk_orders(orders)
        elif step == "refresh_account_async":
            refresh_live_account_cache_async(state)
        execution_timing.append({"step": step, "ms": (time.perf_counter() - step_started) * 1000})
    if response is None or orders is None:
        raise RuntimeError("真实执行流程缺少“发送真实IOC订单”步骤")
    fills = _live_fills_from_response(response, [row["asset"], row["leader"]])
    if not all(item["filled"] and item["size"] > 0 for item in fills.values()):
        emergency = []
        for coin, original in zip((row["asset"], row["leader"]), orders):
            filled = fills.get(coin, {})
            if filled.get("filled") and filled.get("size", 0) > 0:
                emergency.append(_live_ioc_order(exchange, coin, not original["is_buy"], 0, slippage,
                                                  reduce_only=True, size_override=filled["size"]))
        unwind_response = exchange.bulk_orders(emergency) if emergency else None
        note = "入场未完整成交；已对已成交腿发送紧急减仓请求"
        with sqlite3.connect(state.db_path) as db:
            db.execute("""
                INSERT INTO live_trades (
                    trade_key, status, asset, leader, action, asset_notional_usdc, hedge_notional_usdc,
                    total_notional_usdc, beta, entry_ts, exit_ts, entry_z, entry_corr, entry_spread_bps,
                    entry_json, exit_json, note, opened_scan_id
                ) VALUES (?, 'entry_failed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                paper_trade_key(row), row["asset"], row["leader"], row["action"], asset_notional, hedge_notional,
                asset_notional + hedge_notional, row["beta"], time.time(), time.time(), row["zscore"], row["corr"],
                row.get("spread_bps"), json.dumps({"response": response, "fills": fills}, ensure_ascii=False),
                json.dumps(unwind_response, ensure_ascii=False) if unwind_response is not None else None,
                note, scan_id,
            ))
        notify_dingtalk_live_trade(state, "真实开仓失败", {
            "asset": row["asset"], "leader": row["leader"], "action": row["action"],
            "asset_notional_usdc": asset_notional, "hedge_notional_usdc": hedge_notional,
            "entry_z": row.get("zscore"), "pnl_usdc": None,
        }, row, note)
        raise RuntimeError(note)
    trade = {
        "trade_key": paper_trade_key(row), "status": "open", "asset": row["asset"], "leader": row["leader"],
        "action": row["action"], "asset_notional_usdc": asset_notional, "hedge_notional_usdc": hedge_notional,
        "total_notional_usdc": asset_notional + hedge_notional, "beta": row["beta"], "entry_ts": time.time(),
        "entry_z": row["zscore"], "entry_corr": row["corr"], "entry_spread_bps": row.get("spread_bps"),
        "asset_size": fills[row["asset"]]["size"], "hedge_size": fills[row["leader"]]["size"],
        "asset_entry_px": fills[row["asset"]]["price"], "hedge_entry_px": fills[row["leader"]]["price"],
        "entry_json": json.dumps({
            "response": response, "fills": fills, "leverage_result": leverage_result,
            "l2_books": l2_books, "execution_steps": steps,
            "execution_timing": execution_timing, "account_cache_age_ms": account_age_ms,
        }, ensure_ascii=False),
        "opened_scan_id": scan_id,
        "note": (f"{row.get('plan', '')}；目标杠杆 {target_leverage}x；入场预期边际 "
                 f"{live_expected_edge_bps(row, config):.1f}bps；固定安全流程耗时 "
                 f"{sum(item['ms'] for item in execution_timing):.0f}ms"),
    }
    with sqlite3.connect(state.db_path) as db:
        cursor = db.execute("""
            INSERT INTO live_trades (
                trade_key, status, asset, leader, action, asset_notional_usdc, hedge_notional_usdc,
                total_notional_usdc, beta, entry_ts, entry_z, entry_corr, entry_spread_bps,
                asset_size, hedge_size, asset_entry_px, hedge_entry_px, entry_json, opened_scan_id, note
            ) VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade["trade_key"], trade["asset"], trade["leader"], trade["action"], trade["asset_notional_usdc"],
            trade["hedge_notional_usdc"], trade["total_notional_usdc"], trade["beta"], trade["entry_ts"],
            trade["entry_z"], trade["entry_corr"], trade["entry_spread_bps"], trade["asset_size"],
            trade["hedge_size"], trade["asset_entry_px"], trade["hedge_entry_px"], trade["entry_json"],
            scan_id, trade["note"],
        ))
        trade["id"] = cursor.lastrowid
    notify_dingtalk_live_trade(state, "真实开仓", trade, row, "候选满足条件，按模拟盘策略真实开仓")
    return trade


def close_live_strategy_trade(state, trade, row, reason, scan_id):
    exchange = _live_sdk_exchange(state.config)
    slippage = float(state.config.get("live_max_slippage_bps", 15))
    account = fetch_live_account_snapshot(state.config["live_account_address"])
    position_sizes = _live_position_size_map(account)
    close_orders, close_coins, skipped = [], [], {}
    for leg in _live_trade_legs(trade):
        net_size = float(position_sizes.get(leg["coin"], 0.0))
        if net_size * leg["expected_sign"] <= 0:
            skipped[leg["coin"]] = {
                "reason": "官方净仓没有本策略对应方向，跳过 reduce-only",
                "net_size": net_size,
                "expected_sign": leg["expected_sign"],
            }
            continue
        size = min(abs(net_size), leg["size"])
        if size <= 0:
            skipped[leg["coin"]] = {"reason": "可平数量为 0", "net_size": net_size}
            continue
        close_orders.append(_live_ioc_order(exchange, leg["coin"], leg["close_is_buy"], 0, slippage,
                                           reduce_only=True, size_override=size))
        close_coins.append(leg["coin"])
    if not close_orders:
        note = f"官方净仓已无本策略对应方向；按对账关闭记录：{reason}"
        exit_payload = {"positions_before_close": position_sizes, "skipped": skipped, "reason": reason}
        with sqlite3.connect(state.db_path) as db:
            db.execute("""
                UPDATE live_trades
                SET status='reconciled', exit_ts=?, exit_z=?, exit_corr=?, exit_spread_bps=?,
                    close_reason=?, closed_scan_id=?, exit_json=?, note=?
                WHERE id=?
            """, (time.time(), row.get("zscore"), row.get("corr"), row.get("spread_bps"),
                  reason, scan_id, json.dumps(exit_payload, ensure_ascii=False), note, trade["id"]))
        trade.update({"status": "reconciled", "exit_ts": time.time(), "exit_z": row.get("zscore"),
                      "close_reason": reason, "note": note})
        notify_dingtalk_live_trade(state, "真实平仓对账", trade, row, note)
        return trade
    response = exchange.bulk_orders(close_orders)
    fills = _live_fills_from_response(response, close_coins)
    failed = {coin: item for coin, item in fills.items() if not (item.get("filled") and item.get("size", 0) > 0)}
    if failed:
        note = f"平仓未完整成交：{reason}；请核对官方仓位"
        with sqlite3.connect(state.db_path) as db:
            db.execute("UPDATE live_trades SET note=?, exit_json=? WHERE id=?", (
                note, json.dumps({"response": response, "fills": fills, "skipped": skipped,
                                  "positions_before_close": position_sizes}, ensure_ascii=False), trade["id"],
            ))
        notify_dingtalk_live_trade(state, "真实平仓异常", trade, row, note)
        raise RuntimeError(note)
    pnl_usdc = 0.0
    asset_exit = None
    hedge_exit = None
    for leg in _live_trade_legs(trade):
        fill = fills.get(leg["coin"])
        if not fill:
            continue
        signed = 1 if leg["entry_buy"] else -1
        pnl_usdc += (float(fill["price"]) - leg["entry_px"]) * min(float(fill["size"]), leg["size"]) * signed
        if leg["coin"] == trade["asset"]:
            asset_exit = fill["price"]
        if leg["coin"] == trade["leader"]:
            hedge_exit = fill["price"]
    pnl_bps = pnl_usdc / max(float(trade["total_notional_usdc"]), 1e-9) * 10_000
    final_status = "closed" if not skipped else "reconciled"
    note = trade.get("note") or ""
    if skipped:
        note = f"{note}；部分腿按官方净仓对账跳过：{', '.join(skipped)}"
    exit_ts = time.time()
    exit_json = json.dumps({"response": response, "fills": fills, "skipped": skipped,
                            "positions_before_close": position_sizes}, ensure_ascii=False)
    with sqlite3.connect(state.db_path) as db:
        db.execute("""
            UPDATE live_trades
            SET status=?, exit_ts=?, exit_z=?, exit_corr=?, exit_spread_bps=?,
                asset_exit_px=?, hedge_exit_px=?, pnl_usdc=?, pnl_bps=?, close_reason=?,
                closed_scan_id=?, exit_json=?, note=?
            WHERE id=?
        """, (
            final_status, exit_ts, row.get("zscore"), row.get("corr"), row.get("spread_bps"),
            asset_exit, hedge_exit, pnl_usdc, pnl_bps, reason, scan_id,
            exit_json, note, trade["id"],
        ))
    trade.update({"status": final_status, "exit_ts": exit_ts, "exit_z": row.get("zscore"),
                  "asset_exit_px": asset_exit, "hedge_exit_px": hedge_exit,
                  "pnl_usdc": pnl_usdc, "pnl_bps": pnl_bps, "close_reason": reason,
                  "exit_json": exit_json, "note": note})
    if final_status == "closed":
        try:
            costs = fetch_official_trade_costs(state.config, trade)
            if costs:
                with sqlite3.connect(state.db_path) as db:
                    db.execute("""
                        UPDATE live_trades SET pnl_usdc=?, pnl_bps=?, fee_usdc=?, funding_usdc=?,
                            net_pnl_usdc=?, net_pnl_bps=?, asset_net_pnl_usdc=?, hedge_net_pnl_usdc=?
                        WHERE id=?
                    """, (
                        costs["pnl_usdc"], costs["pnl_bps"], costs["fee_usdc"], costs["funding_usdc"],
                        costs["net_pnl_usdc"], costs["net_pnl_bps"], costs["asset_net_pnl_usdc"],
                        costs["hedge_net_pnl_usdc"], trade["id"],
                    ))
                trade.update(costs)
        except Exception as exc:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] official trade cost lookup failed: {exc}", flush=True)
    notify_dingtalk_live_trade(state, "真实平仓" if final_status == "closed" else "真实平仓对账", trade, row, reason)
    return trade


def execute_live_emergency_flatten(state, reason="manual emergency close"):
    config = state.config
    if not config.get("live_enabled"):
        raise RuntimeError("真实下单总开关关闭，拒绝发送紧急平仓订单")
    if not live_config_public(config).get("execution_ready"):
        raise RuntimeError("真实交易尚未就绪：" + live_config_public(config).get("blocker", ""))
    account = fetch_live_account_snapshot(config["live_account_address"])
    positions = account.get("positions") or []
    if not positions:
        return {"status": "no_positions", "positions": [], "fills": {}, "note": "官方账户没有可平仓位"}
    exchange = _live_sdk_exchange(config)
    slippage = max(float(config.get("live_max_slippage_bps", 15)), 30.0)
    orders, coins = [], []
    for pos in positions:
        coin = str(pos.get("coin") or "").upper()
        size = float(pos.get("size") or 0)
        if not coin or size == 0:
            continue
        orders.append(_live_ioc_order(exchange, coin, size < 0, 0, slippage,
                                      reduce_only=True, size_override=abs(size)))
        coins.append(coin)
    if not orders:
        return {"status": "no_orders", "positions": positions, "fills": {}, "note": "没有生成可执行平仓单"}
    response = exchange.bulk_orders(orders)
    fills = _live_fills_from_response(response, coins)
    status = "submitted"
    if not all(item.get("filled") and item.get("size", 0) > 0 for item in fills.values()):
        status = "partial_or_failed"
    payload = {
        "status": status,
        "positions": positions,
        "response": response,
        "fills": fills,
        "reason": reason,
    }
    now_ts = time.time()
    with sqlite3.connect(state.db_path) as db:
        db.execute("""
            UPDATE live_trades
            SET status='emergency_closed', exit_ts=?, close_reason=?, note=?,
                exit_json=?
            WHERE status='open'
        """, (
            now_ts, "紧急全部平仓", "已发送紧急全部平仓；请以官方仓位为准",
            json.dumps(payload, ensure_ascii=False),
        ))
    with state.lock:
        latest_rows = list((state.latest or {}).get("rows", []))
    strategy_rows = prepare_live_rows(latest_rows, state.config, state.l2book)
    row_by_pair = {paper_pair_key(row): row for row in strategy_rows}
    with sqlite3.connect(state.db_path) as db:
        db.row_factory = sqlite3.Row
        paper_open = [dict(row) for row in db.execute("""
            SELECT * FROM paper_trades
            WHERE status='open' AND mode='shared_strategy' AND pnl_model='l2_executable'
        """).fetchall()]
        for trade in paper_open:
            row = row_by_pair.get(paper_pair_key(trade))
            if not row:
                db.execute("""
                    UPDATE paper_trades SET status='archived', exit_ts=?, close_reason=? WHERE id=?
                """, (now_ts, "真实紧急全部平仓；缺少当前盘口，模拟记录归档", trade["id"]))
                continue
            details = paper_trade_pnl_details(trade, row, state.config, now_ts)
            db.execute("""
                UPDATE paper_trades SET status='closed', exit_ts=?, exit_z=?, exit_corr=?,
                    exit_spread_bps=?, exit_funding_hourly=?, pnl_bps=?, pnl_usdc=?,
                    close_reason=?, pnl_model='l2_executable'
                WHERE id=?
            """, (
                now_ts, row.get("zscore"), row.get("corr"), row.get("spread_bps"),
                row.get("funding_hourly"), details["pnl_bps"], details["pnl_usdc"],
                "真实紧急全部平仓同步", trade["id"],
            ))
    notify_dingtalk_live_emergency(config, "紧急全部平仓",
                                   [f"状态：{status}",
                                    "仓位：" + "，".join(f"{p.get('coin')} {p.get('size')}" for p in positions),
                                    "说明：已对官方当前所有仓位发送 reduce-only 平仓单"])
    return payload


def update_live_trading(state, payload, scan_id):
    config = state.config
    payload.setdefault("strategy_live_entries", {})
    if not config.get("live_enabled") or not config.get("live_strategy_enabled"):
        return load_live_trades_snapshot(state.db_path, current_rows=payload.get("rows", []), config=config)
    if not live_config_public(config).get("execution_ready"):
        return load_live_trades_snapshot(state.db_path, current_rows=payload.get("rows", []), config=config)
    rows = list(payload.get("strategy_rows") or prepare_live_rows(payload.get("rows", []), config, state.l2book))
    row_by_pair = {paper_pair_key(row): row for row in rows}
    init_alt_db(state.db_path)
    with sqlite3.connect(state.db_path) as db:
        db.row_factory = sqlite3.Row
        open_trades = [dict(row) for row in db.execute("SELECT * FROM live_trades WHERE status='open' ORDER BY entry_ts ASC").fetchall()]
    missing_pairs = [
        (trade["asset"], trade["leader"]) for trade in open_trades
        if paper_pair_key(trade) not in row_by_pair
    ]
    row_by_pair.update(load_latest_rows_for_pairs(missing_pairs, state.db_path))
    if open_trades:
        try:
            exchange = _live_sdk_exchange(config)
            coins = []
            for trade in open_trades:
                coins.extend([trade["asset"], trade["leader"]])
            _live_apply_leverage(exchange, coins, _live_target_leverage(config))
        except Exception as exc:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] live leverage refresh failed: {exc}", flush=True)
    for trade in open_trades:
        row = row_by_pair.get(paper_pair_key(trade))
        if not row:
            continue
        reason, _pnl_bps = paper_close_reason({**trade, "notional_usdc": trade["asset_notional_usdc"]}, row, config, payload["ts"])
        if reason:
            try:
                close_live_strategy_trade(state, trade, row, reason, scan_id)
            except Exception as exc:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] live close failed: {exc}", flush=True)
    with sqlite3.connect(state.db_path) as db:
        db.row_factory = sqlite3.Row
        open_trades = [dict(row) for row in db.execute("SELECT * FROM live_trades WHERE status='open' ORDER BY entry_ts ASC").fetchall()]
    max_open = int(config.get("live_max_open", 1))
    open_keys = {trade["trade_key"] for trade in open_trades}
    if len(open_keys) >= max_open:
        return load_live_trades_snapshot(state.db_path, current_rows=rows, config=config)
    if (
        not config.get("live_auto_min_notional", False)
        and float(config.get("live_notional_usdc") or 0) < LIVE_MIN_ORDER_USDC
    ):
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] live open skipped: "
            f"live_notional_usdc={float(config.get('live_notional_usdc') or 0):.2f}U "
            f"is below Hyperliquid minimum leg value {LIVE_MIN_ORDER_USDC:.2f}U; "
            "existing live trades are still managed",
            flush=True,
        )
        return load_live_trades_snapshot(state.db_path, current_rows=rows, config=config)
    execution_steps = live_execution_steps(config)
    if "cached_account" in execution_steps:
        account, account_age_ms = cached_live_account(
            state, max_age_ms=float(config.get("live_account_cache_max_age_ms", 15_000) or 15_000),
        )
        if not account:
            print(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] live open skipped: "
                f"background account cache unavailable/stale ({float(account_age_ms or 0):.0f}ms)",
                flush=True,
            )
            return load_live_trades_snapshot(state.db_path, current_rows=rows, config=config)
    else:
        account = {"positions": []}
    external_positions = account.get("positions") or []
    if external_positions and not open_trades:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] live open skipped: account has positions not tracked by live_trades", flush=True)
        return load_live_trades_snapshot(state.db_path, current_rows=rows, config=config)
    used_coins = set()
    for trade in open_trades:
        used_coins.add(str(trade["asset"]).upper())
        used_coins.add(str(trade["leader"]).upper())
    for pos in external_positions:
        coin = str(pos.get("coin") or "").upper()
        if coin:
            used_coins.add(coin)
    # Do not reuse the scan's 5-minute candidate tag here.  WS Z is updated
    # between candles, so every row must be evaluated with the current live
    # thresholds and current direction or valid tick-level signals get missed.
    if config.get("paper_sync_live", True):
        # In unified mode the continuously-running simulated strategy is the
        # canonical decision engine.  Real execution may act only on entry
        # events created in this exact scan; it must not invent a substitute
        # trade when an order fails or when real trading was previously off.
        candidates = list(payload.get("strategy_open_rows") or [])
    else:
        candidates = [row for row in rows if row.get("action") in ("short_asset_long_hedge", "long_asset_short_hedge")]
    candidates.sort(key=lambda r: abs(float(r.get("zscore") or 0)), reverse=True)
    rejected_live = 0
    for row in candidates:
        if len(open_keys) >= max_open:
            break
        key = paper_trade_key(row)
        if key in open_keys:
            continue
        if str(row.get("asset") or "").upper() in used_coins or str(row.get("leader") or "").upper() in used_coins:
            continue
        reject_reason = live_candidate_reject_reason(row, config)
        if reject_reason:
            rejected_live += 1
            if rejected_live <= 3:
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] live candidate rejected: "
                    f"{row.get('asset')} vs {row.get('leader')} {reject_reason}",
                    flush=True,
                )
            continue
        try:
            trade = open_live_strategy_trade(state, row, scan_id)
            payload["strategy_live_entries"][trade["trade_key"]] = trade
            open_keys.add(trade["trade_key"])
            used_coins.add(str(trade["asset"]).upper())
            used_coins.add(str(trade["leader"]).upper())
        except Exception as exc:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] live open skipped/failed: {exc}", flush=True)
    return load_live_trades_snapshot(state.db_path, current_rows=rows, config=config)


def shared_strategy_l2_reject_reason(state, row):
    """Use the real l2Book quality rules without requiring a funded account."""
    try:
        asset_notional, hedge_notional = _live_sized_notionals(
            row, state.config.get("live_notional_usdc", LIVE_MIN_ORDER_USDC), 1_000_000_000,
            auto_min_notional=True,
        )
    except (ValueError, RuntimeError, TypeError) as exc:
        return str(exc)
    reason, books = live_l2book_reject_reason(state, row, asset_notional, hedge_notional)
    if not reason:
        row["_strategy_l2_check"] = {
            "checked_at": time.time(),
            "books": {
                coin: {key: book.get(key) for key in ("bid", "ask", "mid", "spread_bps", "age_ms")}
                for coin, book in books.items()
            },
        }
    return reason


def prepare_shared_strategy_cycle(state, payload, scan_id):
    """Close due paper positions and create entry decisions without recording entries yet."""
    config = state.config
    init_alt_db(state.db_path)
    now_ts = float(payload["ts"])
    rows = list(payload.get("strategy_rows") or prepare_live_rows(payload.get("rows", []), config, state.l2book))
    row_by_pair = {paper_pair_key(row): row for row in rows}
    max_open = max(0, int(config.get("live_max_open", 1) or 0))
    opened_now, closed_now = [], []
    with sqlite3.connect(state.db_path) as db:
        db.row_factory = sqlite3.Row
        # Preserve older modes for audit, but never let them occupy canonical
        # strategy slots or pollute the new synchronized statistics.
        if not getattr(state, "shared_legacy_archived", False):
            db.execute("""
                UPDATE paper_trades SET status='archived', exit_ts=?, close_reason='切换为统一策略模式'
                WHERE status='open' AND mode != 'shared_strategy'
            """, (now_ts,))
            db.execute("""
                UPDATE paper_trades SET status='archived', exit_ts=?, close_reason='旧Z理论记录已退出交易界面'
                WHERE status='open' AND mode='shared_strategy'
                  AND COALESCE(pnl_model, 'z_approx') != 'l2_executable'
            """, (now_ts,))
            state.shared_legacy_archived = True
        open_trades = [dict(row) for row in db.execute("""
            SELECT * FROM paper_trades
            WHERE status='open' AND mode='shared_strategy'
            ORDER BY entry_ts ASC
        """).fetchall()]
        cooldown_seconds = max(0, int(config.get("live_reentry_cooldown_minutes", 15) or 0)) * 60
        recent_closed_pairs = {
            f"{row['asset']}:{row['leader']}": float(row["last_exit_ts"] or 0)
            for row in db.execute("""
                SELECT asset, leader, MAX(exit_ts) AS last_exit_ts
                FROM paper_trades
                WHERE status='closed' AND mode='shared_strategy' AND pnl_model='l2_executable'
                GROUP BY asset, leader
            """).fetchall()
        }
        open_keys = {trade["trade_key"] for trade in open_trades}
        missing_pairs = [
            (trade["asset"], trade["leader"]) for trade in open_trades
            if paper_pair_key(trade) not in row_by_pair
        ]
        row_by_pair.update(load_latest_rows_for_pairs(missing_pairs, state.db_path))
        closed_keys_this_round = set()
        for trade in open_trades:
            row = row_by_pair.get(paper_pair_key(trade))
            if not row:
                continue
            reason, pnl_bps = paper_close_reason(trade, row, config, now_ts)
            if not reason:
                continue
            pnl_details = paper_trade_pnl_details(trade, row, config, now_ts)
            pnl_bps = float(pnl_details["pnl_bps"])
            pnl_usdc = float(pnl_details["pnl_usdc"])
            db.execute("""
                UPDATE paper_trades
                SET status='closed', exit_ts=?, exit_z=?, exit_corr=?, exit_spread_bps=?, exit_funding_hourly=?,
                    pnl_bps=?, pnl_usdc=?, close_reason=?, closed_scan_id=?, pnl_model=?
                WHERE id=?
            """, (
                now_ts, row.get("zscore"), row.get("corr"), row.get("spread_bps"), row.get("funding_hourly"),
                pnl_bps, pnl_usdc, reason, scan_id, pnl_details["pnl_model"], trade["id"],
            ))
            trade.update({"status": "closed", "exit_ts": now_ts, "exit_z": row.get("zscore"),
                          "pnl_bps": pnl_bps, "pnl_usdc": pnl_usdc, "close_reason": reason,
                          "pnl_model": pnl_details["pnl_model"]})
            closed_now.append((trade, row, reason))
            closed_keys_this_round.add(trade["trade_key"])
            open_keys.discard(trade["trade_key"])

        used_coins = set()
        for trade in open_trades:
            if trade["trade_key"] not in open_keys:
                continue
            used_coins.update((str(trade["asset"]).upper(), str(trade["leader"]).upper()))
        candidates = [
            row for row in rows
            if row.get("action") in ("short_asset_long_hedge", "long_asset_short_hedge")
            and not live_candidate_reject_reason(row, config)
        ]
        candidates.sort(key=lambda row: abs(float(row.get("zscore") or 0)), reverse=True)
        for row in candidates:
            if len(open_keys) >= max_open:
                break
            key = paper_trade_key(row)
            asset = str(row.get("asset") or "").upper()
            leader = str(row.get("leader") or "").upper()
            if key in open_keys or key in closed_keys_this_round or asset in used_coins or leader in used_coins:
                continue
            if cooldown_seconds and now_ts - recent_closed_pairs.get(paper_pair_key(row), 0) < cooldown_seconds:
                continue
            l2_reason = shared_strategy_l2_reject_reason(state, row)
            if l2_reason:
                continue
            try:
                asset_notional, hedge_notional = _live_sized_notionals(
                    row, config.get("live_notional_usdc", LIVE_MIN_ORDER_USDC), 1_000_000_000,
                    auto_min_notional=True,
                )
                notional = asset_notional + hedge_notional
            except (ValueError, RuntimeError, TypeError):
                notional = float(config.get("paper_notional_usdc", DEFAULT_PAPER_NOTIONAL))
                beta_abs = abs(float(row.get("beta") or 0))
                asset_notional = notional / max(1.0 + beta_abs, 1e-9)
                hedge_notional = max(0.0, notional - asset_notional)
            asset_entry_px, hedge_entry_px = strategy_row_execution_prices(row, row.get("action"), closing=False)
            if not asset_entry_px or not hedge_entry_px:
                continue
            pnl_model = "l2_executable"
            plan = f"统一策略：{row.get('plan', '')}；模拟与真实共用同一信号，真实开关只控制是否发送订单"
            trade = {
                "id": None, "trade_key": key, "status": "open", "asset": row["asset"],
                "leader": row["leader"], "action": row["action"], "notional_usdc": notional,
                "asset_notional_usdc": asset_notional, "hedge_notional_usdc": hedge_notional,
                "asset_entry_px": asset_entry_px, "hedge_entry_px": hedge_entry_px,
                "beta": row["beta"], "entry_ts": now_ts, "entry_z": row["zscore"],
                "pnl_bps": 0, "pnl_usdc": 0, "plan": plan, "pnl_model": pnl_model,
            }
            opened_now.append((trade, row))
            open_keys.add(key)
            used_coins.update((asset, leader))

    payload["strategy_pending_entries"] = opened_now
    payload["strategy_open_rows"] = [row for _trade, row in opened_now]
    payload["strategy_closed_events"] = closed_now
    payload["strategy_closed_keys"] = [trade["trade_key"] for trade, _row, _reason in closed_now]
    return {"rows": rows, "row_by_pair": row_by_pair, "now_ts": now_ts}


def finalize_shared_strategy_cycle(state, payload, scan_id, cycle):
    """Persist the already-decided paper entries and equity after the chosen real-order phase."""
    config = state.config
    rows = cycle["rows"]
    row_by_pair = cycle["row_by_pair"]
    now_ts = cycle["now_ts"]
    opened_now = list(payload.get("strategy_pending_entries") or [])
    closed_now = list(payload.get("strategy_closed_events") or [])
    live_entries = payload.get("strategy_live_entries") or {}
    with sqlite3.connect(state.db_path) as db:
        db.row_factory = sqlite3.Row
        for trade, row in opened_now:
            live_trade = live_entries.get(trade["trade_key"])
            if live_trade:
                trade.update({
                    "notional_usdc": live_trade["total_notional_usdc"],
                    "asset_notional_usdc": live_trade["asset_notional_usdc"],
                    "hedge_notional_usdc": live_trade["hedge_notional_usdc"],
                    "asset_entry_px": live_trade["asset_entry_px"],
                    "hedge_entry_px": live_trade["hedge_entry_px"],
                    "entry_ts": live_trade["entry_ts"],
                    "pnl_model": "l2_executable",
                    "plan": trade["plan"] + "；真实已成交，本模拟记录使用真实入场成交价",
                })
            existing = db.execute("""
                SELECT id FROM paper_trades
                WHERE status='open' AND mode='shared_strategy' AND trade_key=?
            """, (trade["trade_key"],)).fetchone()
            if existing:
                trade["id"] = existing["id"]
                continue
            cursor = db.execute("""
                INSERT INTO paper_trades (
                    trade_key, status, asset, leader, action, notional_usdc, beta,
                    asset_notional_usdc, hedge_notional_usdc, asset_entry_px, hedge_entry_px, entry_ts,
                    entry_z, entry_corr, entry_spread_bps, entry_funding_hourly,
                    pnl_bps, pnl_usdc, opened_scan_id, plan, mode, pnl_model
                ) VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, 'shared_strategy', ?)
            """, (
                trade["trade_key"], trade["asset"], trade["leader"], trade["action"],
                trade["notional_usdc"], trade["beta"], trade.get("asset_notional_usdc"),
                trade.get("hedge_notional_usdc"), trade.get("asset_entry_px"), trade.get("hedge_entry_px"),
                trade["entry_ts"], trade["entry_z"],
                row.get("corr"), row.get("spread_bps"), row.get("funding_hourly"), scan_id, trade["plan"],
                trade.get("pnl_model"),
            ))
            trade["id"] = cursor.lastrowid
        current_open = [dict(row) for row in db.execute("""
            SELECT * FROM paper_trades WHERE status='open' AND mode='shared_strategy' ORDER BY entry_ts ASC
        """).fetchall()]
        unrealized = 0.0
        for trade in current_open:
            row = row_by_pair.get(paper_pair_key(trade))
            if row and str(trade.get("pnl_model") or "") == "l2_executable":
                unrealized += float(paper_trade_pnl_details(trade, row, config, now_ts)["pnl_usdc"])
        realized = float(db.execute("""
            SELECT COALESCE(SUM(pnl_usdc), 0) FROM paper_trades
            WHERE status='closed' AND mode='shared_strategy' AND pnl_model='l2_executable'
        """).fetchone()[0])
        db.execute("""
            INSERT INTO paper_equity (ts, scan_id, realized_usdc, unrealized_usdc, total_usdc, open_count, mode, pnl_model)
            VALUES (?, ?, ?, ?, ?, ?, 'shared_strategy', 'l2_executable')
        """, (now_ts, scan_id, realized, unrealized, realized + unrealized,
              sum(str(trade.get("pnl_model") or "") == "l2_executable" for trade in current_open)))

    for trade, row in opened_now:
        notify_dingtalk_paper_trade(state, "模拟开仓", trade, row, "统一策略信号开仓；真实开关决定是否同时发送真实订单")
    for trade, row, reason in closed_now:
        notify_dingtalk_paper_trade(state, "模拟平仓", trade, row, reason)
    return load_paper_snapshot(state.db_path, current_rows=rows, config=config)


def update_shared_strategy_paper(state, payload, scan_id):
    cycle = prepare_shared_strategy_cycle(state, payload, scan_id)
    return finalize_shared_strategy_cycle(state, payload, scan_id, cycle)


def update_paper_trading(state, payload, scan_id):
    config = state.config
    if config.get("paper_sync_live", True):
        # Unified strategy mode is intentionally always-on.  The real switches
        # control only whether the same new-entry events are sent as orders.
        return update_shared_strategy_paper(state, payload, scan_id)
    if not config.get("paper_enabled", True):
        return {"enabled": False, "open": [], "closed": [], "equity": [], "stats": {}}
    init_alt_db(state.db_path)
    now_ts = float(payload["ts"])
    rows = payload.get("rows", [])
    row_by_pair = {paper_pair_key(row): row for row in rows}
    max_open = int(config.get("paper_max_open", 12))
    closed_now = []
    opened_now = []
    with sqlite3.connect(state.db_path) as db:
        db.row_factory = sqlite3.Row
        open_trades = [dict(row) for row in db.execute("SELECT * FROM paper_trades WHERE status = 'open' ORDER BY entry_ts ASC").fetchall()]
        open_keys = {trade["trade_key"] for trade in open_trades}
        missing_pairs = [
            (trade["asset"], trade["leader"]) for trade in open_trades
            if paper_pair_key(trade) not in row_by_pair
        ]
        row_by_pair.update(load_latest_rows_for_pairs(missing_pairs, state.db_path))
        closed_keys_this_round = set()
        for trade in open_trades:
            row = row_by_pair.get(paper_pair_key(trade))
            if not row:
                continue
            reason, pnl_bps = paper_close_reason(trade, row, config, now_ts)
            if not reason:
                continue
            pnl_usdc = float(trade["notional_usdc"]) * pnl_bps / 10_000
            db.execute("""
                UPDATE paper_trades
                SET status='closed', exit_ts=?, exit_z=?, exit_corr=?, exit_spread_bps=?, exit_funding_hourly=?,
                    pnl_bps=?, pnl_usdc=?, close_reason=?, closed_scan_id=?
                WHERE id=?
            """, (
                now_ts, row.get("zscore"), row.get("corr"), row.get("spread_bps"), row.get("funding_hourly"),
                pnl_bps, pnl_usdc, reason, scan_id, trade["id"],
            ))
            trade.update({"status": "closed", "exit_ts": now_ts, "exit_z": row.get("zscore"),
                          "pnl_bps": pnl_bps, "pnl_usdc": pnl_usdc, "close_reason": reason})
            closed_now.append((trade, row, reason))
            closed_keys_this_round.add(trade["trade_key"])
            open_keys.discard(trade["trade_key"])

        open_count = len(open_keys)
        candidates = [row for row in rows if row.get("tag") == "candidate" and row.get("action") in ("short_asset_long_hedge", "long_asset_short_hedge")]
        candidates.sort(key=lambda r: abs(float(r.get("zscore") or 0)), reverse=True)
        for row in candidates:
            if open_count >= max_open:
                break
            key = paper_trade_key(row)
            if key in open_keys or key in closed_keys_this_round:
                continue
            notional = float(config.get("paper_notional_usdc", DEFAULT_PAPER_NOTIONAL))
            cursor = db.execute("""
                INSERT INTO paper_trades (
                    trade_key, status, asset, leader, action, notional_usdc, beta, entry_ts,
                    entry_z, entry_corr, entry_spread_bps, entry_funding_hourly,
                    pnl_bps, pnl_usdc, opened_scan_id, plan
                ) VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
            """, (
                key, row["asset"], row["leader"], row["action"], notional, row["beta"], now_ts,
                row["zscore"], row["corr"], row.get("spread_bps"), row.get("funding_hourly"),
                scan_id, row.get("plan", ""),
            ))
            trade = {
                "id": cursor.lastrowid, "trade_key": key, "status": "open", "asset": row["asset"],
                "leader": row["leader"], "action": row["action"], "notional_usdc": notional,
                "beta": row["beta"], "entry_ts": now_ts, "entry_z": row["zscore"],
                "pnl_bps": 0, "pnl_usdc": 0, "plan": row.get("plan", ""),
            }
            opened_now.append((trade, row))
            open_keys.add(key)
            open_count += 1

        open_trades = [dict(row) for row in db.execute("SELECT * FROM paper_trades WHERE status = 'open' ORDER BY entry_ts ASC").fetchall()]
        unrealized = 0.0
        for trade in open_trades:
            row = row_by_pair.get(paper_pair_key(trade))
            if not row:
                continue
            pnl_bps = paper_trade_pnl_bps(trade, row, config, now_ts)
            unrealized += float(trade["notional_usdc"]) * pnl_bps / 10_000
        realized = float(db.execute("SELECT COALESCE(SUM(pnl_usdc), 0) FROM paper_trades WHERE status='closed'").fetchone()[0])
        db.execute(
            "INSERT INTO paper_equity (ts, scan_id, realized_usdc, unrealized_usdc, total_usdc, open_count) VALUES (?, ?, ?, ?, ?, ?)",
            (now_ts, scan_id, realized, unrealized, realized + unrealized, len(open_trades)),
        )

    for trade, row in opened_now:
        notify_dingtalk_paper_trade(state, "模拟开仓", trade, row, "候选满足条件，模拟盘默认开仓")
    for trade, row, reason in closed_now:
        notify_dingtalk_paper_trade(state, "模拟平仓", trade, row, reason)
    return load_paper_snapshot(state.db_path, current_rows=rows, config=config)


def pair_candle_payload(asset, leader, hours=24):
    asset_series = hl_candles(asset, hours=hours)
    leader_series = hl_candles(leader, hours=hours)
    common = sorted(set(asset_series) & set(leader_series))
    if not common:
        return []
    base_asset = asset_series[common[0]]
    base_leader = leader_series[common[0]]
    rows = []
    for ts in common:
        asset_px = asset_series[ts]
        leader_px = leader_series[ts]
        rows.append({
            "ts": ts / 1000 if ts > 10_000_000_000 else ts,
            "asset": asset_px, "leader": leader_px,
            "asset_norm": 100 * asset_px / base_asset,
            "leader_norm": 100 * leader_px / base_leader,
            "relative_bps": (asset_px / base_asset / (leader_px / base_leader) - 1) * 10_000,
        })
    return rows


def leadlag_pair_state(row, l2book, config):
    """Evaluate whether a leader impulse is propagating into a lagging asset."""
    asset = str(row.get("asset") or "").upper()
    leader = str(row.get("leader") or "").upper()
    asset_motion = l2book.motion(asset)
    leader_motion = l2book.motion(leader)
    base = {
        "asset": asset, "leader": leader, "eligible": False, "reason": "",
        "corr": float(row.get("corr") or 0), "beta": abs(float(row.get("beta") or 0)),
    }
    if not asset_motion or not leader_motion:
        return {**base, "reason": "等待两边 WS 盘口历史"}
    needed = ("ret_1s_bps", "ret_3s_bps", "ret_15s_bps")
    if any(asset_motion.get(key) is None or leader_motion.get(key) is None for key in needed):
        return {**base, "reason": "正在积累至少 15 秒盘口历史"}
    max_age_ms = float(config.get("leadlag_max_data_age_ms", 10_000) or 10_000)
    if max(float(asset_motion.get("age_ms") or 0), float(leader_motion.get("age_ms") or 0)) > max_age_ms:
        return {**base, "reason": "盘口数据过旧"}
    l1, l3, l15 = (float(leader_motion[key]) for key in needed)
    a1, a3, a15 = (float(asset_motion[key]) for key in needed)
    beta = max(0.25, min(3.0, base["beta"]))
    candidates = []
    threshold_3s = float(config.get("leadlag_leader_3s_bps", 2) or 2)
    threshold_15s = float(config.get("leadlag_leader_15s_bps", 4) or 4)
    if abs(l3) >= threshold_3s:
        direction = 1 if l3 > 0 else -1
        if direction * l1 >= 0:
            candidates.append(("3秒", direction, l3, a3, direction * (beta * l3 - a3)))
    if abs(l15) >= threshold_15s:
        direction = 1 if l15 > 0 else -1
        if direction * l3 >= 2:
            candidates.append(("15秒", direction, l15, a15, direction * (beta * l15 - a15)))
    if not candidates:
        return {
            **base, "reason": "龙头尚未形成同方向快速启动",
            "leader_1s_bps": l1, "leader_3s_bps": l3, "leader_15s_bps": l15,
            "asset_1s_bps": a1, "asset_3s_bps": a3, "asset_15s_bps": a15,
        }
    window, direction, leader_move, asset_move, lag_bps = max(candidates, key=lambda item: item[4])
    spread = float(asset_motion.get("spread_bps") or 999)
    imbalance = float(asset_motion.get("imbalance") or 0)
    side_size = float((asset_motion.get("ask_size") if direction > 0 else asset_motion.get("bid_size")) or 0)
    depth_usdc = side_size * float(asset_motion.get("mid") or 0)
    notional = float(config.get("leadlag_notional_usdc", 20) or 20)
    depth_multiple = depth_usdc / max(notional, 1e-9)
    fee_bps = float(config.get("leadlag_fee_bps", 5) or 0)
    expected_edge = lag_bps - fee_bps - spread * 2
    out = {
        **base, "window": window, "direction": direction,
        "side": "long" if direction > 0 else "short",
        "leader_move_bps": leader_move, "asset_move_bps": asset_move,
        "leader_1s_bps": l1, "leader_3s_bps": l3, "leader_15s_bps": l15,
        "asset_1s_bps": a1, "asset_3s_bps": a3, "asset_15s_bps": a15,
        "lag_bps": lag_bps, "expected_edge_bps": expected_edge,
        "spread_bps": spread, "imbalance": imbalance,
        "depth_usdc": depth_usdc, "depth_multiple": depth_multiple,
        "entry_px": float((asset_motion.get("ask") if direction > 0 else asset_motion.get("bid")) or 0),
    }
    checks = [
        (base["corr"] >= float(config.get("leadlag_min_corr", 0.60) or 0.60),
         f"相关 {base['corr']:.3f} 低于门槛"),
        (lag_bps >= float(config.get("leadlag_min_lag_bps", 6) or 6),
         f"跟随缺口 {lag_bps:.1f}bps 不足"),
        (direction * a1 >= 0, "小币 1 秒盘口尚未同向确认"),
        (direction * imbalance >= float(config.get("leadlag_min_imbalance", -1.0)),
         f"盘口倾斜 {imbalance:+.2f} 未同向确认"),
        (spread <= float(config.get("leadlag_max_spread_bps", 2.5) or 2.5),
         f"点差 {spread:.2f}bps 过大"),
        (depth_multiple >= float(config.get("leadlag_min_depth_multiple", 2) or 0),
         f"同向顶层深度只有目标金额 {depth_multiple:.1f} 倍"),
        (expected_edge >= float(config.get("leadlag_min_edge_bps", 6) or 6),
         f"扣费预期空间 {expected_edge:.1f}bps 不足"),
        (out["entry_px"] > 0, "没有可执行入场价"),
    ]
    failed = next((reason for passed, reason in checks if not passed), "")
    out["eligible"] = not failed
    out["reason"] = failed or f"{window}龙头启动，小币落后且盘口同向"
    return out


def leadlag_trade_metrics(trade, row, l2book, config, now_ts=None):
    now_ts = float(now_ts or time.time())
    motion = l2book.motion(trade.get("asset"))
    if not motion:
        return None
    direction = 1 if trade.get("side") == "long" else -1
    exit_px = float((motion.get("bid") if direction > 0 else motion.get("ask")) or 0)
    entry_px = float(trade.get("entry_px") or 0)
    if entry_px <= 0 or exit_px <= 0:
        return None
    gross_bps = direction * (exit_px / entry_px - 1) * 10_000
    hours = max(0.0, now_ts - float(trade.get("entry_ts") or now_ts)) / 3600
    funding_hourly_bps = float((row or {}).get("funding_hourly") or 0) * 10_000
    funding_bps = -direction * funding_hourly_bps * hours
    fee_bps = float(trade.get("fee_bps") or config.get("leadlag_fee_bps", 5) or 0)
    net_bps = gross_bps - fee_bps + funding_bps
    return {
        "exit_px": exit_px, "gross_bps": gross_bps, "fee_bps": fee_bps,
        "funding_bps": funding_bps, "net_bps": net_bps,
        "pnl_usdc": float(trade.get("notional_usdc") or 0) * net_bps / 10_000,
    }


def leadlag_close_reason(trade, metrics, leader_motion, config, now_ts=None):
    now_ts = float(now_ts or time.time())
    net_bps = float(metrics["net_bps"])
    max_net = max(float(trade.get("max_net_bps") or 0), net_bps)
    if net_bps >= float(config.get("leadlag_take_profit_bps", 18) or 18):
        return "达到单腿止盈"
    if net_bps <= -abs(float(config.get("leadlag_stop_bps", 14) or 14)):
        return "触发单腿止损"
    trail_start = float(config.get("leadlag_trail_start_bps", 8) or 0)
    trail_gap = float(config.get("leadlag_trail_gap_bps", 6) or 6)
    if trail_start > 0 and max_net >= trail_start and net_bps <= max_net - trail_gap:
        return "移动止盈回撤"
    direction = 1 if trade.get("side") == "long" else -1
    if leader_motion and leader_motion.get("ret_1s_bps") is not None and direction * float(leader_motion["ret_1s_bps"]) <= -5:
        return "龙头短线反向"
    if now_ts - float(trade.get("entry_ts") or now_ts) >= float(config.get("leadlag_max_hold_minutes", 8) or 8) * 60:
        return "超过最长持仓"
    return None


def load_leadlag_snapshot(db_path=ALT_DB_FILE, limit=200, status=None):
    init_alt_db(db_path)
    with sqlite3.connect(db_path) as db:
        db.row_factory = sqlite3.Row
        open_rows = [dict(row) for row in db.execute(
            "SELECT * FROM leadlag_trades WHERE status='open' ORDER BY entry_ts DESC"
        ).fetchall()]
        closed_rows = [dict(row) for row in db.execute(
            "SELECT * FROM leadlag_trades WHERE status='closed' ORDER BY exit_ts DESC LIMIT ?", (int(limit),)
        ).fetchall()]
        stats = db.execute("""
            SELECT COUNT(*) AS trades,
                   SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END) AS wins,
                   COALESCE(SUM(pnl_usdc), 0) AS realized,
                   COALESCE(AVG(net_bps), 0) AS avg_bps,
                   COALESCE(MIN(net_bps), 0) AS worst_bps,
                   COALESCE(SUM(notional_usdc * fee_bps / 10000.0), 0) AS estimated_fees
            FROM leadlag_trades WHERE status='closed'
        """).fetchone()
    trades = int(stats["trades"] or 0)
    wins = int(stats["wins"] or 0)
    return {
        "open": open_rows, "closed": closed_rows,
        "stats": {
            "trades": trades, "wins": wins, "win_rate": wins / trades if trades else 0,
            "realized_usdc": float(stats["realized"] or 0),
            "avg_bps": float(stats["avg_bps"] or 0), "worst_bps": float(stats["worst_bps"] or 0),
            "estimated_fees_usdc": float(stats["estimated_fees"] or 0),
        },
        "status": status or {},
    }


def update_leadlag_strategy(state, rows, now_ts=None):
    """Run the independent WS lead-lag single-leg paper strategy."""
    now_ts = float(now_ts or time.time())
    config = state.config
    init_alt_db(state.db_path)
    row_by_asset = {str(row.get("asset") or "").upper(): row for row in rows}
    leader_coins = split_symbols(str(config.get("leadlag_leaders") or "BTC,ETH,SOL,HYPE,DOGE,BNB"))
    signals = []
    for row in rows:
        asset = str(row.get("asset") or "").upper()
        alternatives = []
        for leader in leader_coins:
            if not asset or leader == asset:
                continue
            relationship = (state.l2book.recent_relationship(asset, leader)
                            if hasattr(state.l2book, "recent_relationship") else None)
            if relationship:
                virtual_row = {**row, "leader": leader, "corr": relationship["corr"], "beta": relationship["beta"]}
            elif leader == str(row.get("leader") or "").upper():
                virtual_row = row
            else:
                continue
            signal = leadlag_pair_state(virtual_row, state.l2book, config)
            signal["relationship_samples"] = (relationship or {}).get("samples")
            alternatives.append(signal)
        if alternatives:
            alternatives.sort(key=lambda item: (
                bool(item.get("eligible")), float(item.get("expected_edge_bps") or -999999),
                max(abs(float(item.get("leader_3s_bps") or 0)), abs(float(item.get("leader_15s_bps") or 0))),
            ), reverse=True)
            signals.append(alternatives[0])
    signals.sort(key=lambda item: float(item.get("expected_edge_bps") or -999999), reverse=True)
    opened_now, closed_now = [], []
    with state.leadlag_lock:
        with sqlite3.connect(state.db_path) as db:
            db.row_factory = sqlite3.Row
            open_trades = [dict(row) for row in db.execute(
                "SELECT * FROM leadlag_trades WHERE status='open' ORDER BY entry_ts"
            ).fetchall()]
            for trade in open_trades:
                row = row_by_asset.get(str(trade.get("asset") or "").upper()) or {}
                metrics = leadlag_trade_metrics(trade, row, state.l2book, config, now_ts)
                if not metrics:
                    continue
                max_net = max(float(trade.get("max_net_bps") or 0), float(metrics["net_bps"]))
                leader_motion = state.l2book.motion(trade.get("leader"))
                reason = leadlag_close_reason({**trade, "max_net_bps": max_net}, metrics, leader_motion, config, now_ts)
                if reason:
                    db.execute("""
                        UPDATE leadlag_trades SET status='closed', exit_ts=?, exit_px=?, gross_bps=?,
                            funding_bps=?, net_bps=?, pnl_usdc=?, max_net_bps=?, close_reason=? WHERE id=?
                    """, (now_ts, metrics["exit_px"], metrics["gross_bps"], metrics["funding_bps"],
                          metrics["net_bps"], metrics["pnl_usdc"], max_net, reason, trade["id"]))
                    closed_now.append(({**trade, **metrics, "status": "closed", "exit_ts": now_ts,
                                        "max_net_bps": max_net, "close_reason": reason}, row))
                else:
                    db.execute("""
                        UPDATE leadlag_trades SET exit_px=?, gross_bps=?, funding_bps=?, net_bps=?,
                            pnl_usdc=?, max_net_bps=? WHERE id=?
                    """, (metrics["exit_px"], metrics["gross_bps"], metrics["funding_bps"],
                          metrics["net_bps"], metrics["pnl_usdc"], max_net, trade["id"]))

            if config.get("leadlag_enabled", True):
                open_assets = {str(row[0]).upper() for row in db.execute(
                    "SELECT asset FROM leadlag_trades WHERE status='open'"
                ).fetchall()}
                open_count = len(open_assets)
                max_open = max(0, int(config.get("leadlag_max_open", 3) or 0))
                cooldown = max(0, int(config.get("leadlag_cooldown_minutes", 20) or 0)) * 60
                recent = {str(row[0]).upper(): float(row[1] or 0) for row in db.execute("""
                    SELECT asset, MAX(exit_ts) FROM leadlag_trades WHERE status='closed' GROUP BY asset
                """).fetchall()}
                for signal in signals:
                    if open_count >= max_open:
                        break
                    asset = str(signal.get("asset") or "").upper()
                    if not signal.get("eligible") or not asset or asset in open_assets:
                        continue
                    if cooldown and asset in recent and now_ts - recent[asset] < cooldown:
                        continue
                    entry_px = float(signal.get("entry_px") or 0)
                    notional = float(config.get("leadlag_notional_usdc", 20) or 20)
                    if entry_px <= 0 or notional <= 0:
                        continue
                    fee_bps = float(config.get("leadlag_fee_bps", 5) or 0)
                    cursor = db.execute("""
                        INSERT INTO leadlag_trades (
                            status, asset, leader, side, notional_usdc, entry_ts, entry_px, size,
                            leader_3s_bps, leader_15s_bps, asset_3s_bps, lag_bps, expected_edge_bps,
                            corr, beta, entry_spread_bps, entry_imbalance, fee_bps, signal_json
                        ) VALUES ('open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        asset, signal["leader"], signal["side"], notional, now_ts, entry_px,
                        notional / entry_px, signal.get("leader_3s_bps"), signal.get("leader_15s_bps"),
                        signal.get("asset_3s_bps"), signal.get("lag_bps"), signal.get("expected_edge_bps"),
                        signal.get("corr"), signal.get("beta"), signal.get("spread_bps"),
                        signal.get("imbalance"), fee_bps,
                        json.dumps({**signal, "funding_hourly": row_by_asset.get(asset, {}).get("funding_hourly")}, ensure_ascii=False),
                    ))
                    opened_now.append({**signal, "id": cursor.lastrowid, "status": "open",
                                       "entry_ts": now_ts, "entry_px": entry_px,
                                       "notional_usdc": notional, "fee_bps": fee_bps,
                                       "size": notional / entry_px})
                    open_assets.add(asset)
                    open_count += 1
        state.leadlag_status.update({
            "running": True, "last_eval_ts": now_ts, "last_error": None,
            "signals": signals[:50], "eligible": sum(bool(item.get("eligible")) for item in signals),
            "opened": int(state.leadlag_status.get("opened") or 0) + len(opened_now),
            "closed": int(state.leadlag_status.get("closed") or 0) + len(closed_now),
        })
    for trade in opened_now:
        notify_dingtalk_leadlag(state, "V2模拟开仓", trade)
    for trade, _row in closed_now:
        notify_dingtalk_leadlag(state, "V2模拟平仓", trade)
    return load_leadlag_snapshot(state.db_path, status=dict(state.leadlag_status))


def env_bool(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        value = env_file_value(name)
    if value is None or value == "":
        return default
    return value.strip().lower() not in ("0", "false", "no", "off", "关闭")


def env_float(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        value = env_file_value(name)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def env_int(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        value = env_file_value(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_file_value(name, env_path=None):
    """Read one .env value without importing secrets into process-wide state."""
    path = Path(env_path or (ROOT / ".env"))
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == name:
                return value.strip()
    except OSError:
        pass
    return ""


def env_text(name, default=""):
    value = os.environ.get(name)
    if value is None or value == "":
        value = env_file_value(name)
    return value if value not in (None, "") else default


def _live_fernet():
    """Encryption key lives in server .env; API private key lives separately encrypted."""
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise RuntimeError("缺少 cryptography；请先执行 pip install -r requirements.txt") from exc
    master = os.environ.get("HLM_LIVE_SECRET_MASTER_KEY") or env_file_value("HLM_LIVE_SECRET_MASTER_KEY")
    if not master:
        master = Fernet.generate_key().decode("ascii")
        update_env_vars({"HLM_LIVE_SECRET_MASTER_KEY": master})
    try:
        return Fernet(master.encode("ascii"))
    except Exception as exc:
        raise RuntimeError("HLM_LIVE_SECRET_MASTER_KEY 格式无效") from exc


def _clean_api_private_key(value):
    key = str(value or "").strip()
    if key.startswith("0x"):
        key = key[2:]
    if len(key) != 64 or not all(char in "0123456789abcdefABCDEF" for char in key):
        raise ValueError("API 钱包私钥应为 64 位十六进制字符")
    return "0x" + key.lower()


def _live_key_digest(key, salt):
    return base64.urlsafe_b64encode(hashlib.pbkdf2_hmac("sha256", key.encode("utf-8"), salt, 310_000)).decode("ascii")


def live_api_key_configured():
    return LIVE_SECRET_FILE.exists()


def load_live_api_key():
    try:
        payload = json.loads(LIVE_SECRET_FILE.read_text(encoding="utf-8"))
        return _live_fernet().decrypt(payload["ciphertext"].encode("ascii")).decode("utf-8")
    except FileNotFoundError:
        return ""
    except (OSError, KeyError, ValueError) as exc:
        raise RuntimeError("无法读取已加密的 API 私钥") from exc


def store_live_api_key(new_key, old_key=None):
    """Store an API wallet key encrypted; replacement requires the previous key."""
    new_key = _clean_api_private_key(new_key)
    if LIVE_SECRET_FILE.exists():
        if not old_key:
            raise ValueError("替换 API 私钥必须输入旧私钥")
        old_key = _clean_api_private_key(old_key)
        try:
            previous = json.loads(LIVE_SECRET_FILE.read_text(encoding="utf-8"))
            salt = base64.urlsafe_b64decode(previous["salt"].encode("ascii"))
            expected = previous["verify"]
        except (OSError, KeyError, ValueError) as exc:
            raise RuntimeError("已加密 API 私钥配置损坏，不能安全替换") from exc
        if not hmac.compare_digest(_live_key_digest(old_key, salt), expected):
            raise ValueError("旧 API 私钥不正确")
    salt = secrets.token_bytes(16)
    payload = {
        "version": 1,
        "salt": base64.urlsafe_b64encode(salt).decode("ascii"),
        "verify": _live_key_digest(new_key, salt),
        "ciphertext": _live_fernet().encrypt(new_key.encode("utf-8")).decode("ascii"),
    }
    temp = LIVE_SECRET_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    try:
        os.chmod(temp, 0o600)
    except OSError:
        pass
    os.replace(temp, LIVE_SECRET_FILE)


def configure_live_api_key_cli(change=False):
    if change and not LIVE_SECRET_FILE.exists():
        raise SystemExit("尚未配置 API 私钥；请先使用 --set-live-api-key")
    old = getpass.getpass("请输入旧 API 私钥（不会显示）：") if LIVE_SECRET_FILE.exists() else None
    new = getpass.getpass("请输入新的 Hyperliquid API 钱包私钥（不会显示）：")
    confirm = getpass.getpass("再次输入新的 API 钱包私钥：")
    if new != confirm:
        raise SystemExit("两次输入不一致，未写入任何内容")
    store_live_api_key(new, old)
    print("API 私钥已加密保存。私钥不会写入数据库、网页接口或日志。")


def generate_live_api_key_cli():
    """Create a dedicated API wallet secret on the server; print only its public address."""
    if LIVE_SECRET_FILE.exists():
        raise SystemExit("服务器已经有加密的 API 私钥；如需替换请使用 --change-live-api-key")
    try:
        from eth_account import Account
    except ImportError as exc:
        raise SystemExit("缺少 eth-account；请先执行 pip install -r requirements.txt") from exc
    wallet = Account.create()
    store_live_api_key(wallet.key.hex())
    print("已生成并加密保存服务器 API 钱包。请在 Hyperliquid API 页面授权以下公开地址：")
    print(wallet.address)


PAPER_CONFIG_FIELDS = {
    "paper_enabled": {"env": "PAPER_ENABLED", "type": "bool", "label": "模拟盘开关"},
    "paper_sync_live": {"env": "PAPER_SYNC_LIVE", "type": "bool", "label": "统一策略模式"},
    "paper_notional_usdc": {"env": "PAPER_NOTIONAL_USDC", "type": "float", "min": 1, "max": 1_000_000, "label": "每笔名义本金"},
    "paper_exit_z": {"env": "PAPER_EXIT_Z", "type": "float", "min": 0, "max": 10, "label": "回归平仓 Z"},
    "paper_take_profit_bps": {"env": "PAPER_TAKE_PROFIT_BPS", "type": "float", "min": 0, "max": 10_000, "label": "固定止盈 bps（0关闭）"},
    "paper_stop_bps": {"env": "PAPER_STOP_BPS", "type": "float", "min": 1, "max": 10_000, "label": "止损 bps"},
    "paper_max_hold_minutes": {"env": "PAPER_MAX_HOLD_MINUTES", "type": "int", "min": 1, "max": 100_000, "label": "最长持仓分钟"},
    "paper_max_open": {"env": "PAPER_MAX_OPEN", "type": "int", "min": 0, "max": 500, "label": "最多持仓数"},
    "paper_fee_bps": {"env": "PAPER_FEE_BPS", "type": "float", "min": 0, "max": 1_000, "label": "模拟成本 bps"},
    "paper_z_value_bps": {"env": "PAPER_Z_VALUE_BPS", "type": "float", "min": 0.1, "max": 1_000, "label": "每 1Z 折算 bps"},
    "paper_min_corr": {"env": "PAPER_MIN_CORR", "type": "float", "min": -1, "max": 1, "label": "最低相关性"},
}


LIVE_CONFIG_FIELDS = {
    "live_enabled": {"env": "LIVE_ENABLED", "type": "bool", "label": "真实下单总开关"},
    "live_account_poll_seconds": {"env": "LIVE_ACCOUNT_POLL_SECONDS", "type": "float", "min": 1, "max": 300, "label": "后台账户刷新秒数"},
    "live_account_cache_max_age_ms": {"env": "LIVE_ACCOUNT_CACHE_MAX_AGE_MS", "type": "float", "min": 1000, "max": 300_000, "label": "账户缓存最大年龄 ms"},
    "live_leverage_cache_seconds": {"env": "LIVE_LEVERAGE_CACHE_SECONDS", "type": "float", "min": 0, "max": 604_800, "label": "杠杆预设缓存秒数"},
    "live_account_address": {"env": "HLM_ACCOUNT_ADDRESS", "type": "address", "label": "主钱包公开地址"},
    "live_notional_usdc": {"env": "LIVE_NOTIONAL_USDC", "type": "float", "min": 1, "max": 100_000, "label": "每笔小币腿 USDC"},
    "live_max_open": {"env": "LIVE_MAX_OPEN", "type": "int", "min": 0, "max": 20, "label": "最多真实仓位"},
    "live_reentry_cooldown_minutes": {"env": "LIVE_REENTRY_COOLDOWN_MINUTES", "type": "int", "min": 0, "max": 10080, "label": "同币对重新开仓冷却分钟"},
    "live_max_slippage_bps": {"env": "LIVE_MAX_SLIPPAGE_BPS", "type": "float", "min": 1, "max": 500, "label": "最大下单滑点 bps"},
    "live_leverage": {"env": "LIVE_LEVERAGE", "type": "int", "min": 1, "max": 50, "label": "真实交易杠杆倍数"},
    "live_auto_min_notional": {"env": "LIVE_AUTO_MIN_NOTIONAL", "type": "bool", "label": "低于交易所最低时自动补足"},
    "live_min_entry_z": {"env": "LIVE_MIN_ENTRY_Z", "type": "float", "min": 0, "max": 20, "label": "实盘最低入场 |Z|"},
    "live_min_corr": {"env": "LIVE_MIN_CORR", "type": "float", "min": -1, "max": 1, "label": "实盘最低相关性"},
    "live_min_expected_edge_bps": {"env": "LIVE_MIN_EXPECTED_EDGE_BPS", "type": "float", "min": -1000, "max": 10000, "label": "实盘最低预期边际 bps"},
    "live_use_l2book": {"env": "LIVE_USE_L2BOOK", "type": "bool", "label": "真实开仓使用 l2Book 盘口校验"},
    "live_l2_max_age_ms": {"env": "LIVE_L2_MAX_AGE_MS", "type": "float", "min": 100, "max": 60_000, "label": "l2Book 最大延迟 ms"},
    "live_strategy_entry_grace_ms": {"env": "LIVE_STRATEGY_ENTRY_GRACE_MS", "type": "float", "min": 0, "max": 60_000, "label": "统一信号真实执行宽限 ms"},
    "live_l2_max_spread_bps": {"env": "LIVE_L2_MAX_SPREAD_BPS", "type": "float", "min": 0, "max": 100, "label": "最大允许点差 bps"},
    "live_use_realtime_z": {"env": "LIVE_USE_REALTIME_Z", "type": "bool", "label": "使用 l2Book 实时近似 Z"},
    "live_realtime_strategy_interval_ms": {"env": "LIVE_REALTIME_STRATEGY_INTERVAL_MS", "type": "float", "min": 100, "max": 10_000, "label": "WS策略判断间隔 ms"},
    "live_require_leverage_ok": {"env": "LIVE_REQUIRE_LEVERAGE_OK", "type": "bool", "label": "杠杆设置失败时跳过"},
    "live_strategy_enabled": {"env": "LIVE_STRATEGY_ENABLED", "type": "bool", "label": "真实策略开关"},
}


LEADLAG_CONFIG_FIELDS = {
    "leadlag_enabled": {"env": "LEADLAG_ENABLED", "type": "bool", "label": "V2 模拟策略开关"},
    "leadlag_notional_usdc": {"env": "LEADLAG_NOTIONAL_USDC", "type": "float", "min": 1, "max": 100_000, "label": "单腿模拟金额 USDC"},
    "leadlag_max_open": {"env": "LEADLAG_MAX_OPEN", "type": "int", "min": 0, "max": 50, "label": "最多同时持仓"},
    "leadlag_leaders": {"env": "LEADLAG_LEADERS", "type": "str", "label": "V2 龙头币列表"},
    "leadlag_leader_3s_bps": {"env": "LEADLAG_LEADER_3S_BPS", "type": "float", "min": 1, "max": 1000, "label": "龙头3秒启动 bps"},
    "leadlag_leader_15s_bps": {"env": "LEADLAG_LEADER_15S_BPS", "type": "float", "min": 1, "max": 5000, "label": "龙头15秒启动 bps"},
    "leadlag_min_lag_bps": {"env": "LEADLAG_MIN_LAG_BPS", "type": "float", "min": 1, "max": 5000, "label": "最小跟随缺口 bps"},
    "leadlag_min_corr": {"env": "LEADLAG_MIN_CORR", "type": "float", "min": -1, "max": 1, "label": "最低相关性"},
    "leadlag_max_spread_bps": {"env": "LEADLAG_MAX_SPREAD_BPS", "type": "float", "min": 0, "max": 100, "label": "最大点差 bps"},
    "leadlag_max_data_age_ms": {"env": "LEADLAG_MAX_DATA_AGE_MS", "type": "float", "min": 500, "max": 60000, "label": "V2 行情最大年龄 ms"},
    "leadlag_min_imbalance": {"env": "LEADLAG_MIN_IMBALANCE", "type": "float", "min": -1, "max": 1, "label": "最小同向盘口倾斜（-1关闭）"},
    "leadlag_min_depth_multiple": {"env": "LEADLAG_MIN_DEPTH_MULTIPLE", "type": "float", "min": 0, "max": 1000, "label": "最小盘口深度倍数"},
    "leadlag_fee_bps": {"env": "LEADLAG_FEE_BPS", "type": "float", "min": 0, "max": 1000, "label": "单腿往返成本 bps"},
    "leadlag_min_edge_bps": {"env": "LEADLAG_MIN_EDGE_BPS", "type": "float", "min": 0, "max": 5000, "label": "最低扣费预期空间 bps"},
    "leadlag_take_profit_bps": {"env": "LEADLAG_TAKE_PROFIT_BPS", "type": "float", "min": 1, "max": 5000, "label": "止盈 bps"},
    "leadlag_stop_bps": {"env": "LEADLAG_STOP_BPS", "type": "float", "min": 1, "max": 5000, "label": "止损 bps"},
    "leadlag_trail_start_bps": {"env": "LEADLAG_TRAIL_START_BPS", "type": "float", "min": 0, "max": 5000, "label": "移动止盈启动 bps"},
    "leadlag_trail_gap_bps": {"env": "LEADLAG_TRAIL_GAP_BPS", "type": "float", "min": 1, "max": 5000, "label": "移动止盈回撤 bps"},
    "leadlag_max_hold_minutes": {"env": "LEADLAG_MAX_HOLD_MINUTES", "type": "int", "min": 1, "max": 1440, "label": "最长持仓分钟"},
    "leadlag_cooldown_minutes": {"env": "LEADLAG_COOLDOWN_MINUTES", "type": "int", "min": 0, "max": 10080, "label": "同币冷却分钟"},
}


NOTIFY_CONFIG_FIELDS = {
    "dingtalk_paper_webhook": {"env": "DINGTALK_PAPER_WEBHOOK", "type": "str", "default": "", "label": "模拟/候选 Webhook"},
    "dingtalk_paper_keyword": {"env": "DINGTALK_PAPER_KEYWORD", "type": "str", "default": "小测试", "label": "模拟/候选关键词"},
    "dingtalk_live_webhook": {"env": "DINGTALK_LIVE_WEBHOOK", "type": "str", "default": "", "label": "真实交易 Webhook"},
    "dingtalk_live_keyword": {"env": "DINGTALK_LIVE_KEYWORD", "type": "str", "default": "小测试", "label": "真实交易关键词"},
    "public_url": {"env": "HLM_PUBLIC_URL", "type": "str", "default": "", "label": "面板公网地址"},
    "notify_cooldown": {"env": "NOTIFY_COOLDOWN", "type": "int", "default": 1800, "min": 0, "max": 86_400, "label": "同类提醒冷却秒"},
    "notify_candidate_open": {"env": "NOTIFY_CANDIDATE_OPEN", "type": "bool", "default": True, "label": "候选首次出现"},
    "notify_candidate_repeat": {"env": "NOTIFY_CANDIDATE_REPEAT", "type": "bool", "default": False, "label": "候选持续提醒"},
    "notify_candidate_resolved": {"env": "NOTIFY_CANDIDATE_RESOLVED", "type": "bool", "default": False, "label": "候选解除"},
    "notify_caution": {"env": "NOTIFY_CAUTION", "type": "bool", "default": False, "label": "谨慎风险"},
    "notify_paper_open": {"env": "NOTIFY_PAPER_OPEN", "type": "bool", "default": True, "label": "模拟开仓"},
    "notify_paper_close": {"env": "NOTIFY_PAPER_CLOSE", "type": "bool", "default": True, "label": "模拟平仓"},
    "notify_leadlag_open": {"env": "NOTIFY_LEADLAG_OPEN", "type": "bool", "default": True, "label": "V2 模拟开仓"},
    "notify_leadlag_close": {"env": "NOTIFY_LEADLAG_CLOSE", "type": "bool", "default": True, "label": "V2 模拟平仓"},
    "notify_live_test": {"env": "NOTIFY_LIVE_TEST", "type": "bool", "default": True, "label": "真实交易开平仓"},
    "notify_live_open": {"env": "NOTIFY_LIVE_OPEN", "type": "bool", "default": True, "label": "真实开仓"},
    "notify_live_close": {"env": "NOTIFY_LIVE_CLOSE", "type": "bool", "default": True, "label": "真实平仓"},
    "notify_live_error": {"env": "NOTIFY_LIVE_ERROR", "type": "bool", "default": True, "label": "真实异常"},
    "notify_candidate_max_per_scan": {"env": "NOTIFY_CANDIDATE_MAX_PER_SCAN", "type": "int", "default": 1, "min": 0, "max": 50, "label": "每轮候选最多推送数"},
    "notify_candidate_min_z": {"env": "NOTIFY_CANDIDATE_MIN_Z", "type": "float", "default": 3.0, "min": 0, "max": 20, "label": "候选最低 |Z|"},
}


def leadlag_config_public(config):
    return {key: config.get(key) for key in LEADLAG_CONFIG_FIELDS}


def coerce_leadlag_config(raw):
    updates = {}
    for key, meta in LEADLAG_CONFIG_FIELDS.items():
        if key not in raw:
            continue
        value = raw[key]
        if meta["type"] == "bool":
            parsed = value.strip().lower() not in ("0", "false", "no", "off", "关闭") if isinstance(value, str) else bool(value)
        elif meta["type"] == "str":
            parsed = ",".join(split_symbols(str(value or "")))
            if not parsed:
                raise ValueError(f"{meta['label']} 不能为空")
        elif meta["type"] == "int":
            parsed = int(value)
        else:
            parsed = float(value)
        if meta["type"] not in ("bool", "str") and not (meta["min"] <= parsed <= meta["max"]):
            raise ValueError(f"{meta['label']} 超出允许范围")
        updates[key] = parsed
    if not updates:
        raise ValueError("没有可更新的联动传播 V2 参数")
    return updates


def update_leadlag_env_file(values, env_path=None):
    update_env_vars({
        LEADLAG_CONFIG_FIELDS[key]["env"]: 1 if isinstance(value, bool) and value else 0 if isinstance(value, bool) else value
        for key, value in values.items()
    }, env_path=env_path)


def notify_config_public(config):
    return {key: config.get(key, meta["default"]) for key, meta in NOTIFY_CONFIG_FIELDS.items()}


def coerce_notify_config(raw):
    updates = {}
    for key, meta in NOTIFY_CONFIG_FIELDS.items():
        if key not in raw:
            continue
        value = raw[key]
        if meta["type"] == "bool":
            parsed = value.strip().lower() not in ("0", "false", "no", "off", "关闭") if isinstance(value, str) else bool(value)
        elif meta["type"] == "int":
            parsed = int(value)
        elif meta["type"] == "str":
            parsed = str(value or "").strip()
            if "WEBHOOK" in meta["env"] and parsed and not parsed.startswith("https://oapi.dingtalk.com/robot/send?"):
                raise ValueError(f"{meta['label']} 看起来不是钉钉机器人 Webhook")
            if meta["env"] == "HLM_PUBLIC_URL" and parsed and not re.match(r"^https?://", parsed):
                raise ValueError("面板公网地址必须以 http:// 或 https:// 开头")
            if "KEYWORD" in meta["env"] and not parsed:
                parsed = "小测试"
        else:
            parsed = float(value)
        if meta["type"] not in ("bool", "str") and not (meta["min"] <= parsed <= meta["max"]):
            raise ValueError(f"{meta['label']} 超出允许范围")
        updates[key] = parsed
    if not updates:
        raise ValueError("没有可更新的推送设置")
    return updates


def update_notify_env_file(values, env_path=None):
    update_env_vars({NOTIFY_CONFIG_FIELDS[key]["env"]: 1 if isinstance(value, bool) and value else 0 if isinstance(value, bool) else value for key, value in values.items()}, env_path=env_path)


def valid_evm_address(value):
    return bool(re.fullmatch(r"0x[0-9a-fA-F]{40}", str(value or "").strip()))


def live_config_public(config):
    account = str(config.get("live_account_address") or "")
    key_configured = live_api_key_configured()
    sdk_ready = False
    try:
        import hyperliquid  # noqa: F401
        sdk_ready = True
    except ImportError:
        pass
    ready = key_configured and valid_evm_address(account) and sdk_ready
    if not key_configured:
        blocker = "尚未通过 SSH 加密配置 API 钱包私钥"
    elif not valid_evm_address(account):
        blocker = "请填写主钱包公开地址（不是 API 钱包地址）"
    elif not sdk_ready:
        blocker = "服务器缺少 hyperliquid-python-sdk"
    else:
        blocker = ("已就绪；真实策略当前" + ("已开启" if config.get("live_strategy_enabled") else "关闭") +
                   "。开启后会按模拟盘逻辑真实开仓，并由回归、止盈止损、最长持仓等规则平仓。")
        if (
            config.get("live_strategy_enabled")
            and not config.get("live_auto_min_notional", False)
            and float(config.get("live_notional_usdc") or 0) < LIVE_MIN_ORDER_USDC
        ):
            blocker += f" 当前每笔小币腿 {float(config.get('live_notional_usdc') or 0):.2f}U 低于 Hyperliquid 单腿最低约 {LIVE_MIN_ORDER_USDC:.2f}U，新开仓会跳过；已有仓位仍会按规则管理。"
    return {
        **{key: config.get(key) for key in LIVE_CONFIG_FIELDS},
        "api_key_configured": key_configured,
        "sdk_ready": sdk_ready,
        "execution_ready": ready,
        "blocker": blocker,
    }


def coerce_live_config(raw):
    updates = {}
    for key, meta in LIVE_CONFIG_FIELDS.items():
        if key not in raw:
            continue
        value = raw[key]
        if meta["type"] == "bool":
            parsed = value.strip().lower() not in ("0", "false", "no", "off", "关闭") if isinstance(value, str) else bool(value)
        elif meta["type"] == "int":
            parsed = int(value)
        elif meta["type"] == "address":
            parsed = str(value or "").strip()
            if parsed and not valid_evm_address(parsed):
                raise ValueError("主钱包公开地址格式不正确，应为 0x 开头的 42 位地址")
        else:
            parsed = float(value)
        if meta["type"] not in ("bool", "address"):
            if parsed < meta["min"] or parsed > meta["max"]:
                raise ValueError(f"{meta['label']} 超出允许范围")
        updates[key] = parsed
    if not updates:
        raise ValueError("没有可更新的真实交易参数")
    return updates


def paper_config_public(config):
    return {key: config.get(key) for key in PAPER_CONFIG_FIELDS}


def coerce_paper_config(raw):
    updates = {}
    for key, meta in PAPER_CONFIG_FIELDS.items():
        if key not in raw:
            continue
        value = raw[key]
        if meta["type"] == "bool":
            if isinstance(value, str):
                parsed = value.strip().lower() not in ("0", "false", "no", "off", "关闭")
            else:
                parsed = bool(value)
        elif meta["type"] == "int":
            parsed = int(value)
        else:
            parsed = float(value)
        if meta["type"] != "bool":
            if "min" in meta and parsed < meta["min"]:
                raise ValueError(f"{meta['label']} 不能小于 {meta['min']}")
            if "max" in meta and parsed > meta["max"]:
                raise ValueError(f"{meta['label']} 不能大于 {meta['max']}")
        updates[key] = parsed
    if not updates:
        raise ValueError("没有可更新的模拟盘参数")
    return updates


def update_env_file(values, env_path=None):
    env_path = Path(env_path or (ROOT / ".env"))
    existing = []
    if env_path.exists():
        existing = env_path.read_text(encoding="utf-8").splitlines()
    env_values = {PAPER_CONFIG_FIELDS[key]["env"]: value for key, value in values.items()}
    seen = set()
    output = []
    for line in existing:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output.append(line)
            continue
        name = line.split("=", 1)[0].strip()
        if name in env_values:
            value = env_values[name]
            output.append(f"{name}={1 if isinstance(value, bool) and value else 0 if isinstance(value, bool) else value}")
            seen.add(name)
        else:
            output.append(line)
    for name, value in env_values.items():
        if name not in seen:
            output.append(f"{name}={1 if isinstance(value, bool) and value else 0 if isinstance(value, bool) else value}")
    env_path.write_text("\n".join(output) + "\n", encoding="utf-8")


def update_live_env_file(values, env_path=None):
    env_values = {}
    for key, value in values.items():
        env_name = LIVE_CONFIG_FIELDS[key]["env"]
        if isinstance(value, (list, tuple)):
            value = ",".join(str(item) for item in value)
        env_values[env_name] = 1 if isinstance(value, bool) and value else 0 if isinstance(value, bool) else value
    update_env_vars(env_values, env_path=env_path)


def update_env_vars(env_values, env_path=None):
    env_path = Path(env_path or (ROOT / ".env"))
    existing = []
    if env_path.exists():
        existing = env_path.read_text(encoding="utf-8").splitlines()
    seen = set()
    output = []
    for line in existing:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output.append(line)
            continue
        name = line.split("=", 1)[0].strip()
        if name in env_values:
            output.append(f"{name}={env_values[name]}")
            seen.add(name)
        else:
            output.append(line)
    for name, value in env_values.items():
        if name not in seen:
            output.append(f"{name}={value}")
    env_path.write_text("\n".join(output) + "\n", encoding="utf-8")


def valid_admin_token(config, supplied):
    expected = str(config.get("admin_token") or "")
    return bool(expected) and bool(supplied) and hmac.compare_digest(str(supplied), expected)


def dingtalk_post(webhook, content, timeout=8):
    if not webhook:
        return False
    payload = json.dumps({"msgtype": "text", "text": {"content": content}}, ensure_ascii=False).encode("utf-8")
    request = Request(webhook, data=payload, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        data = json.loads(response.read().decode("utf-8"))
    return data.get("errcode") == 0


def dingtalk_channel_config(config, channel):
    if channel == "live":
        webhook = config.get("dingtalk_live_webhook") or os.environ.get("DINGTALK_LIVE_WEBHOOK", "")
        keyword = config.get("dingtalk_live_keyword") or os.environ.get("DINGTALK_LIVE_KEYWORD", "小测试")
    else:
        webhook = (
            config.get("dingtalk_paper_webhook")
            or os.environ.get("DINGTALK_PAPER_WEBHOOK", "")
            or config.get("dingtalk_webhook")
            or os.environ.get("DINGTALK_WEBHOOK", "")
        )
        keyword = (
            config.get("dingtalk_paper_keyword")
            or os.environ.get("DINGTALK_PAPER_KEYWORD", "")
            or config.get("dingtalk_keyword")
            or os.environ.get("DINGTALK_KEYWORD", "小测试")
        )
    return str(webhook or "").strip(), str(keyword or "小测试").strip() or "小测试"


def _fmt_signed(value, digits=2, suffix=""):
    if value is None:
        return "未知"
    try:
        return f"{float(value):+.{digits}f}{suffix}"
    except (TypeError, ValueError):
        return "未知"


def _fmt_plain(value, digits=2, suffix=""):
    if value is None:
        return "未知"
    try:
        return f"{float(value):.{digits}f}{suffix}"
    except (TypeError, ValueError):
        return "未知"


def _alert_direction_text(action):
    if action == "short_asset_long_hedge":
        return "做空小币 / 做多保护腿"
    if action == "long_asset_short_hedge":
        return "做多小币 / 做空保护腿"
    return "只观察"


def _dashboard_url(config):
    base = (
        config.get("public_url")
        or os.environ.get("HLM_PUBLIC_URL")
        or os.environ.get("PUBLIC_URL")
        or ""
    ).strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/dashboard"):
        return base
    return base + "/dashboard"


def beijing_time_text(timestamp=None):
    """Timestamp shown in alerts is explicit, independent of server timezone."""
    ts = time.time() if timestamp is None else float(timestamp)
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")


def _dingtalk_candidate_content(keyword, alert_type, row, config):
    max_spread = config.get("max_spread_bps")
    reason = [
        f"|Z| ≥ {float(config.get('min_z', 0)):.2f}",
        f"corr ≥ {float(config.get('min_corr', 0)):.2f}",
    ]
    if max_spread is not None:
        reason.append(f"点差 ≤ {float(max_spread):.2f} bps")
    dash = _dashboard_url(config)
    lines = [
        f"{keyword} Hyperliquid 小币联动提醒",
        f"类型：{alert_type}",
        f"触发时间：{beijing_time_text()}（北京时间）",
        f"币对：{row.get('asset')} vs {row.get('leader')}",
        f"方向：{_alert_direction_text(row.get('action'))}",
        f"触发：{'；'.join(reason)}",
        f"Z：{_fmt_signed(row.get('zscore'), 2)} | corr：{_fmt_signed(row.get('corr'), 3)} | beta：{_fmt_signed(row.get('beta'), 2)}",
        f"近15分钟：小币 {_fmt_signed(row.get('asset_15m_bps'), 1, ' bps')} / 保护腿 {_fmt_signed(row.get('hedge_15m_bps'), 1, ' bps')}",
        f"盘口点差：{_fmt_plain(row.get('spread_bps'), 2, ' bps')} | 资金费：{_fmt_signed((row.get('funding_hourly') or 0) * 10_000, 3, ' bps/小时')}",
        f"计划：{row.get('plan', '只观察')}",
    ]
    if dash:
        lines.append(f"看图：{dash}")
    if config.get("live_enabled") and config.get("live_strategy_enabled"):
        lines.append("性质：候选信号提醒；真实策略是否下单由服务器按仓位、资金、滑点和风控规则单独判断。")
    else:
        lines.append("性质：只读监控提醒；真实策略开关未同时开启时不会自动下单。")
    return "\n".join(lines)


def notify_dingtalk_candidates(state, payload):
    webhook, keyword = dingtalk_channel_config(state.config, "paper")
    if not webhook:
        return
    cooldown = int(state.config.get("notify_cooldown", 1800))
    now = time.time()
    candidates = [row for row in payload.get("rows", []) if row.get("tag") == "candidate"]
    cautions = [
        row for row in payload.get("rows", [])
        if row.get("tag") == "caution" and abs(float(row.get("zscore") or 0)) >= float(state.config.get("min_z", 2.0))
    ]
    current_keys = {f"{row['asset']}:{row['leader']}:{row['action']}" for row in candidates}
    previous_keys = set(getattr(state, "active_candidates", set()))

    if state.config.get("notify_candidate_resolved", False):
        for key in sorted(previous_keys - current_keys):
            asset, leader, action = (key.split(":", 2) + ["", "", ""])[:3]
            content = (
                f"{keyword} Hyperliquid 小币联动提醒\n"
                f"类型：候选解除\n"
                f"触发时间：{beijing_time_text(now)}（北京时间）\n"
                f"币对：{asset} vs {leader}\n"
                f"原方向：{_alert_direction_text(action)}\n"
                f"说明：这组已经不再满足当前候选条件，可能是偏离回归、相关性下降或点差不合格。\n"
                f"性质：候选状态提醒；真实策略是否下单由服务器按仓位、资金、滑点和风控规则单独判断。"
            )
            try:
                if dingtalk_post(webhook, content):
                    state.last_notify[f"resolved:{key}"] = now
            except Exception as exc:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] dingtalk notify failed: {exc}", flush=True)

    max_candidate_pushes = int(state.config.get("notify_candidate_max_per_scan", 1))
    min_candidate_z = float(state.config.get("notify_candidate_min_z", 3.0))
    candidate_alerts = [row for row in candidates if abs(float(row.get("zscore") or 0)) >= min_candidate_z]
    candidate_alerts.sort(key=lambda row: abs(float(row.get("zscore") or 0)), reverse=True)
    pushed_candidates = 0
    for row in candidate_alerts:
        if pushed_candidates >= max_candidate_pushes:
            break
        key = f"{row['asset']}:{row['leader']}:{row['action']}"
        last = state.last_notify.get(key, 0)
        if now - last < cooldown:
            continue
        alert_type = "候选持续提醒" if key in previous_keys else "候选首次出现"
        allowed = "notify_candidate_repeat" if key in previous_keys else "notify_candidate_open"
        if not state.config.get(allowed, True):
            continue
        content = _dingtalk_candidate_content(keyword, alert_type, row, state.config)
        try:
            if dingtalk_post(webhook, content):
                state.last_notify[key] = now
                pushed_candidates += 1
        except Exception as exc:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] dingtalk notify failed: {exc}", flush=True)

    for row in cautions[:10] if state.config.get("notify_caution", False) else []:
        key = f"caution:{row['asset']}:{row['leader']}:{row['action']}"
        last = state.last_notify.get(key, 0)
        if now - last < cooldown:
            continue
        content = _dingtalk_candidate_content(keyword, "谨慎风险", row, state.config)
        try:
            if dingtalk_post(webhook, content):
                state.last_notify[key] = now
        except Exception as exc:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] dingtalk notify failed: {exc}", flush=True)

    state.active_candidates = current_keys


def html_response(handler, body, status=200):
    data = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def dashboard_html():
    return r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hyperliquid 小币联动监控</title>
<style>
body{margin:0;background:#f6f7f9;color:#111827;font-family:Arial,"Microsoft YaHei",sans-serif}
header{padding:14px 18px;background:#111827;color:#fff;display:flex;align-items:center;gap:16px}
header h1{font-size:18px;margin:0;font-weight:600}
header span{color:#cbd5e1;font-size:13px}
main{padding:14px;display:block}
section{background:#fff;border:1px solid #e5e7eb;border-radius:6px;padding:12px;margin-bottom:12px}
h2{font-size:15px;margin:0 0 10px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{border-bottom:1px solid #edf0f3;padding:7px 6px;text-align:right;white-space:nowrap}
th:first-child,td:first-child{text-align:left}
tr{cursor:pointer}
tr:hover{background:#f8fafc}
.candidate{color:#dc2626;font-weight:600}.watch{color:#334155}.caution{color:#b45309;font-weight:600}
.muted{color:#64748b}.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.metric{background:#f8fafc;border:1px solid #edf0f3;border-radius:6px;padding:8px}
canvas{width:100%;height:220px;border:1px solid #e5e7eb;background:#fff;border-radius:6px;margin-bottom:8px;cursor:crosshair}
button,input{font-size:13px;padding:6px 8px}button{cursor:pointer}
.toolbar{display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap}
.mini{height:170px}.scoreGood{color:#16a34a}.scoreBad{color:#dc2626}.scoreMid{color:#b45309}
.subtle{font-size:12px;color:#64748b}.controls{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:6px 0 10px}
.topPanel{width:auto}
.tableWrap{overflow:auto;max-height:76vh;border:1px solid #edf0f3;border-radius:6px}
.mainTable{min-width:1500px}
#liveTradeTbl{min-width:2200px}
.mainTable th{position:sticky;top:0;background:#f8fafc;z-index:2}
.actionCell{text-align:left;min-width:420px;max-width:680px;overflow:hidden;text-overflow:ellipsis}
.paperTop{display:grid;grid-template-columns:1fr 1fr;gap:12px;align-items:start}
.paperTableWrap{overflow:auto;max-height:210px;border:1px solid #edf0f3;border-radius:6px;margin-top:8px}
.detailGrid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}.detailBox{background:#f8fafc;border:1px solid #e5e7eb;border-radius:6px;padding:8px}
.detailCharts{display:grid;grid-template-columns:1fr;gap:8px;margin-top:10px}
.detailText{white-space:pre-wrap;line-height:1.6}
.tooltip{position:fixed;pointer-events:none;background:#111827;color:white;padding:8px 10px;border-radius:6px;font-size:12px;line-height:1.45;display:none;z-index:20;box-shadow:0 8px 24px rgba(15,23,42,.25)}
.selectedRow{background:#eef2ff}
.filter{width:110px}
.paperForm{display:grid;grid-template-columns:repeat(5,minmax(90px,1fr));gap:6px;margin:8px 0}
.paperForm label{font-size:12px;color:#475569}.paperForm input,.paperForm select{width:100%;box-sizing:border-box;margin-top:2px}
.paperActions{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:6px 0 8px}.paperActions input{width:220px}
.tokenBox{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:8px 0}.tokenBox input{width:190px}
.dangerBtn{background:#dc2626;color:white;border-color:#991b1b}
.settingBand{border:1px solid #e5e7eb;border-radius:8px;padding:10px;margin:10px 0;background:#f8fafc}
dialog{border:0;border-radius:8px;max-width:820px;width:92%;padding:0;box-shadow:0 20px 60px rgba(15,23,42,.35)}
.wideDialog{max-width:1280px}
.helpHead{display:flex;justify-content:space-between;align-items:center;background:#111827;color:white;padding:12px 16px}.helpHead h2{margin:0;color:white}
.helpBody{padding:14px 18px;max-height:72vh;overflow:auto;line-height:1.65;font-size:14px}.helpBody h3{margin:16px 0 6px}.helpBody p{margin:6px 0}.helpBody code{background:#f1f5f9;padding:2px 4px;border-radius:4px}
.pill{display:inline-block;padding:2px 6px;border-radius:999px;background:#eef2ff;color:#3730a3;font-size:12px}
.passChip{color:#15803d;font-weight:600}.blockChip{color:#b45309}.reasonCell{text-align:left;white-space:normal;min-width:260px}
@media(max-width:1000px){th,td{font-size:12px;padding:6px 4px}}
</style>
</head>
<body>
<header><h1>Hyperliquid 小币联动监控</h1><span id="status">加载中...</span><button onclick="openStrategyLabDialog()">策略实验室</button><button onclick="openLeadlagDialog()">联动传播 V2</button><button onclick="openNotifyDialog()">推送设置</button><button onclick="openGlobalDialog()">全局设置</button><button onclick="help.showModal()">? 说明</button></header>
<main>
<section class="topPanel">
<h2>模拟盘 / 纸面交易</h2>
<div class="paperTop">
<div>
<div class="grid">
<div class="metric"><div class="muted">模拟盘状态</div><div id="pStatus">-</div></div>
<div class="metric"><div class="muted">累计模拟收益</div><div id="pPnl">-</div></div>
<div class="metric"><div class="muted">胜率 / 次数</div><div id="pWin">-</div></div>
<div class="metric"><div class="muted">当前参数</div><div id="pConfig">-</div></div>
</div>
<div class="paperActions">
<button onclick="openLiveDialog()">打开策略 / 实盘设置</button>
<span id="pSaveStatus" class="subtle">模拟与真实只使用这一套策略控件；真实专属执行项放在同一弹窗后半部分。</span>
</div>
<div class="subtle">模拟盘始终运行，只显示按两腿买一/卖一形成的可成交价格记录；旧 Z 理论记录已从交易界面移除。</div>
</div>
<div>
<canvas id="paperChart" class="mini" width="760" height="190"></canvas>
<div class="subtle">模拟盘只用于观察策略质量，不是真实成交。点击下面持仓/平仓也会弹出详情。</div>
</div>
</div>
<div class="paperTableWrap">
<table id="paperTbl"><thead><tr><th>状态</th><th>币对</th><th>成本后盈亏</th><th>方向</th><th>入场Z</th><th>当前/出场Z</th><th>原因</th></tr></thead><tbody></tbody></table>
</div>
</section>
<section>
<div class="toolbar">
<button onclick="loadLatest()">刷新</button>
<input id="filterText" class="filter" placeholder="筛选币" oninput="renderTable()">
<select id="tagFilter" onchange="renderTable()"><option value="">全部</option><option value="candidate">候选</option><option value="watch">观察</option><option value="caution">谨慎</option></select>
<span class="muted">点击行弹出完整详情；动作列可横向滚动查看</span>
</div>
<div class="tableWrap">
<table id="tbl" class="mainTable"><thead><tr><th onclick="sortBy('tag')">K线状态</th><th>实盘判断</th><th onclick="sortBy('asset')">小币</th><th onclick="sortBy('leader')">保护腿</th><th onclick="sortBy('corr')">相关</th><th onclick="sortBy('beta')">Beta</th><th onclick="sortBy('zscore')">WS实时Z</th><th onclick="sortBy('kline_zscore')">K线Z</th><th onclick="sortBy('asset_15m_bps')">小币15m</th><th onclick="sortBy('hedge_15m_bps')">保护15m</th><th onclick="sortBy('spread_bps')">点差</th><th onclick="sortBy('funding_hourly')">资金费/小时</th><th>动作</th></tr></thead><tbody></tbody></table>
</div>
</section>
</main>
<div id="tip" class="tooltip"></div>
<dialog id="detailDlg" class="wideDialog">
<div class="helpHead"><h2 id="detailTitle">详情</h2><button onclick="detailDlg.close()">关闭</button></div>
<div class="helpBody" id="detailBody"></div>
</dialog>
<dialog id="strategyLabDlg" class="wideDialog">
<div class="helpHead"><h2>虚拟货币策略实验室</h2><button onclick="strategyLabDlg.close()">关闭</button></div>
<div class="helpBody">
  <p>统一使用 Hyperliquid K线测试常见趋势、动量、突破和均值回归策略。信号在本根收盘计算、下一根开盘成交，避免偷看未来；排名只看后40%的样本外区间，并扣除配置的往返成本。</p>
  <div class="paperActions">
    <label>币种<input id="labCoins" value="BTC,ETH,SOL" style="width:180px"></label>
    <label>周期<select id="labInterval"><option value="5m">5分钟</option><option value="15m" selected>15分钟</option><option value="1h">1小时</option><option value="4h">4小时</option></select></label>
    <label>历史天数<input id="labDays" type="number" min="7" max="180" value="30" style="width:75px"></label>
    <label>往返成本bps<input id="labCost" type="number" min="0" max="500" value="12" step="0.5" style="width:75px"></label>
    <button onclick="runStrategyLab(true)">运行新回测</button><button onclick="runStrategyLab(false)">读取上次</button>
    <span id="labStatus" class="subtle">真实交易未授权；先看样本外结果。</span>
  </div>
  <div class="grid">
    <div class="metric"><div class="muted">策略参数组合</div><div id="labEvaluations">-</div></div>
    <div class="metric"><div class="muted">达到研究门槛</div><div id="labPromotable">-</div></div>
    <div class="metric"><div class="muted">数据源</div><div id="labSource">-</div></div>
    <div class="metric"><div class="muted">更新时间</div><div id="labTime">-</div></div>
  </div>
  <p class="scoreMid">“达到研究门槛”只表示训练段和样本外段都为正、样本外交易次数及盈亏比达标；它不是自动开真实仓许可。公开 TradingView 策略还需在桌面版登录后加入图表，再与这里的 Hyperliquid 结果交叉验证。</p>
  <div class="paperTableWrap" style="max-height:520px"><table id="strategyLabTbl"><thead><tr><th>门槛</th><th>币种</th><th>实际历史</th><th>策略</th><th>类型</th><th>当前信号</th><th>训练净收益</th><th>样本外净收益</th><th>样本外回撤</th><th>交易数</th><th>胜率</th><th>盈亏比</th><th>平均每笔</th><th>参数</th></tr></thead><tbody></tbody></table></div>
  <p id="labNote" class="subtle"></p>
</div>
</dialog>
<dialog id="globalDlg">
<div class="helpHead"><h2>全局设置</h2><button onclick="globalDlg.close()">关闭</button></div>
<div class="helpBody">
  <div class="settingBand">
    <h3>管理口令</h3>
    <div class="paperActions"><input id="globalAdminToken" type="password" placeholder="当前管理口令"><button onclick="saveAdminTokenLocal()">保存到本机浏览器</button><span id="globalTokenStatus" class="subtle"></span></div>
    <div class="tokenBox"><input id="oldAdminToken" type="password" placeholder="旧管理口令"><input id="newAdminToken" type="password" placeholder="新管理口令，至少8位"><button onclick="changeAdminToken()">修改服务器管理口令</button></div>
    <div id="tokenStatus" class="subtle"></div>
    <p class="subtle">管理口令用于保存参数和执行紧急平仓。保存到本机浏览器只存在 localStorage，不写服务器。</p>
  </div>
  <div class="settingBand">
    <h3>Hyperliquid API 钱包</h3>
    <div class="grid">
      <div class="metric"><div class="muted">API 钱包状态</div><div id="globalApiStatus">读取中...</div></div>
      <div class="metric"><div class="muted">主钱包公开地址</div><div id="globalAccountAddress">-</div></div>
    </div>
    <p>本软件只使用一个服务器端 API 钱包。私钥不在网页填写、不进入 SQLite、不回传浏览器。</p>
    <p>配置或替换 API 钱包只能通过 SSH 在服务器执行：</p>
    <p><code>cd /opt/hyperliquid-monitor &amp;&amp; .venv/bin/python hyperliquid_correlation_monitor.py --set-live-api-key</code></p>
    <p>替换时执行：<code>--change-live-api-key</code>，必须输入旧私钥。</p>
  </div>
</div>
</dialog>
<dialog id="liveDlg" class="wideDialog">
<div class="helpHead"><h2>统一策略 / 真实交易</h2><button onclick="liveDlg.close()">关闭</button></div>
<div class="helpBody">
  <div class="grid">
    <div class="metric"><div class="muted">真实下单状态</div><div id="liveStatus">读取中...</div></div>
    <div class="metric"><div class="muted">账户模式 / 可用 USDC</div><div id="liveBalance">-</div></div>
    <div class="metric"><div class="muted">当前真实仓位数</div><div id="livePositionCount">-</div></div>
    <div class="metric"><div class="muted">真实已平仓 / 胜率</div><div id="liveClosedWin">-</div></div>
    <div class="metric"><div class="muted">真实已实现</div><div id="liveRealized">-</div></div>
    <div class="metric"><div class="muted">真实平均 / 最差</div><div id="liveAvgWorst">-</div></div>
    <div class="metric"><div class="muted">官方手续费 / 资金费</div><div id="liveCosts">-</div></div>
    <div class="metric"><div class="muted">手续费负担</div><div id="liveFeeBurden">-</div></div>
    <div class="metric"><div class="muted">资金费影响占比</div><div id="liveFundingImpact">-</div></div>
    <div class="metric"><div class="muted">双腿 vs 单腿影子</div><div id="liveHedgeCompare">-</div></div>
    <div class="metric"><div class="muted">l2Book WS 盘口</div><div id="liveL2Status">-</div></div>
    <div class="metric"><div class="muted">本轮真实机会</div><div id="liveOpportunityStatus">-</div></div>
    <div class="metric"><div class="muted">WS实时策略引擎</div><div id="liveRealtimeEngine">-</div></div>
  </div>
  <p id="liveBlocker" class="scoreMid"></p>
  <div class="paperActions"><button onclick="saveLiveConfig()">保存统一策略 / 真实执行参数</button><button onclick="loadLive()">刷新真实账户</button><span id="liveSaveStatus" class="subtle">管理口令在“全局设置”里填写</span></div>
  <div class="settingBand">
    <h3>① 常用策略设置（模拟与真实共用）</h3>
    <div class="detailText">日常只需要选择档位、最多持仓和退出规则。模拟与真实使用同一个开平仓信号；新版模拟盈亏改为按两腿买一/卖一估算，不再把 Z 回归直接当利润。</div>
    <div class="paperForm">
      <label>统一策略档位<select id="liveRiskPreset" onchange="applyLiveRiskPreset(this.value)"><option value="conservative">保守：少交易</option><option value="balanced">中等：测试</option><option value="aggressive">激进：多交易</option><option value="custom">自定义</option></select></label>
      <label>统一策略最多持仓<input id="cfg_live_max_open" type="number" step="1"></label>
      <label>同币对冷却分钟<input id="cfg_live_reentry_cooldown_minutes" type="number" min="0" max="10080" step="1"></label>
      <label>回归平仓Z<input id="cfg_paper_exit_z" type="number" step="0.1"></label>
      <label>固定止盈bps（0关闭）<input id="cfg_paper_take_profit_bps" type="number" step="1" min="0"></label>
      <label>止损bps<input id="cfg_paper_stop_bps" type="number" step="1"></label>
      <label>最长持仓分钟<input id="cfg_paper_max_hold_minutes" type="number" step="1"></label>
    </div>
    <details class="settingBand"><summary><b>高级策略过滤（一般不用改）</b></summary>
      <div class="paperForm">
        <label>开仓最低|Z|<input id="cfg_live_min_entry_z" type="number" min="0" max="20" step="0.1"></label>
        <label>开仓最低相关<input id="cfg_live_min_corr" type="number" min="-1" max="1" step="0.01"></label>
        <label>最低预期边际bps<input id="cfg_live_min_expected_edge_bps" type="number" step="1"></label>
        <label>模拟手续费/额外滑点bps<input id="cfg_paper_fee_bps" type="number" step="0.1"></label>
        <label>Z边际估算系数bps<input id="cfg_paper_z_value_bps" type="number" step="0.1"></label>
        <label>持仓最低相关<input id="cfg_paper_min_corr" type="number" step="0.01"></label>
        <label>l2Book盘口校验<select id="cfg_live_use_l2book"><option value="true">开启（推荐）</option><option value="false">关闭</option></select></label>
        <label>盘口最大年龄ms<input id="cfg_live_l2_max_age_ms" type="number" min="100" max="60000" step="100"></label>
        <label>已确认盘口宽限ms<input id="cfg_live_strategy_entry_grace_ms" type="number" min="0" max="60000" step="100"></label>
        <label>最大允许点差bps<input id="cfg_live_l2_max_spread_bps" type="number" min="0" max="100" step="0.1"></label>
        <label>WS实时Z<select id="cfg_live_use_realtime_z"><option value="true">开启（推荐）</option><option value="false">关闭</option></select></label>
        <label>策略判断节流ms<input id="cfg_live_realtime_strategy_interval_ms" type="number" min="100" max="10000" step="100" title="有新盘口时，最多按这个间隔合并判断一次。500ms不是行情延迟。"></label>
      </div>
    </details>
  </div>
  <div class="settingBand">
    <h3>② 真实下单设置</h3>
    <div class="detailText">这些项目只决定是否真的发送订单、下多大和用多少杠杆，不改变策略信号。</div>
    <div class="paperForm">
      <label>真实下单总开关<select id="cfg_live_enabled"><option value="false">关闭</option><option value="true">开启</option></select></label>
      <label>真实策略开关<select id="cfg_live_strategy_enabled"><option value="false">关闭（默认）</option><option value="true">开启</option></select></label>
      <label>主钱包公开地址<input id="cfg_live_account_address" placeholder="0x...（不是 API 钱包）"></label>
      <label>每笔小币腿USDC<input id="cfg_live_notional_usdc" type="number" min="1" step="0.1"></label>
      <label>低于交易所最低<select id="cfg_live_auto_min_notional"><option value="false">跳过，不下单（默认）</option><option value="true">自动补到最低</option></select></label>
      <label>杠杆倍数<input id="cfg_live_leverage" type="number" min="1" max="50" step="1"></label>
    </div>
    <div class="detailText"><b>固定执行步骤：</b>读取后台账户缓存 → 准备杠杆 → 复查最新盘口 → 发送真实 IOC → 保存模拟记录 → 后台刷新账户。顺序固定，不能编辑。</div>
    <details class="settingBand"><summary><b>高级真实执行（一般不用改）</b></summary>
      <div class="paperForm">
        <label>最大滑点bps<input id="cfg_live_max_slippage_bps" type="number" step="1"></label>
        <label>杠杆失败处理<select id="cfg_live_require_leverage_ok"><option value="true">跳过，不下单（推荐）</option><option value="false">继续下单</option></select></label>
        <label>后台账户刷新秒<input id="cfg_live_account_poll_seconds" type="number" min="1" max="300" step="1"></label>
        <label>账户缓存最大年龄ms<input id="cfg_live_account_cache_max_age_ms" type="number" min="1000" max="300000" step="1000"></label>
        <label>杠杆预设缓存秒<input id="cfg_live_leverage_cache_seconds" type="number" min="0" max="604800" step="60"></label>
      </div>
    </details>
  </div>
  <h3>当前 / 历史真实交易</h3>
  <div class="detailText">持仓显示盘口估算；已平仓显示 Hyperliquid 官方手续费和资金费后的净盈亏。点击任意一行可查看两腿成交价和原始订单回执。</div>
  <div class="paperTableWrap" style="max-height:440px"><table id="liveTradeTbl"><thead><tr><th>状态</th><th>币对</th><th>净盈亏</th><th>手续费</th><th>资金费</th><th>方向</th><th>开仓时间</th><th>平仓时间</th><th>持仓时长</th><th>相关</th><th>Beta</th><th>入场Z</th><th>当前/出场Z</th><th>小币15m</th><th>保护15m</th><th>点差</th><th>资金费/小时</th><th>名义金额</th><th>原因</th></tr></thead><tbody></tbody></table></div>
  <p id="liveStatsNote" class="subtle">真实统计正在读取 Hyperliquid 官方成交手续费和资金费。</p>
  <h3>为什么这一轮没有交易</h3>
  <div id="liveDiagnosticSummary" class="detailText">读取服务器当前过滤结果中...</div>
  <div class="paperTableWrap" style="max-height:360px"><table id="liveDiagnosticTbl"><thead><tr><th>结果</th><th>币对</th><th>WS Z</th><th>K线Z</th><th>相关</th><th>点差</th><th>预期边际</th><th>方向</th><th>具体原因</th></tr></thead><tbody></tbody></table></div>
  <h3>实时 l2Book 盘口</h3>
  <div class="paperActions">
    <label style="display:flex;align-items:center;gap:6px"><input id="liveAutoRefresh" type="checkbox" checked> 自动刷新盘口</label>
    <span id="liveAutoRefreshStatus" class="subtle">盘口每 1 秒刷新；账户/交易每 15 秒刷新。</span>
  </div>
  <div class="paperTableWrap"><table id="liveL2Tbl"><thead><tr><th>币种</th><th>买一 bid</th><th>卖一 ask</th><th>点差</th><th>买一深度</th><th>卖一深度</th><th>数据年龄</th></tr></thead><tbody></tbody></table></div>
  <h3>真实交易走势</h3>
  <div id="liveChartSummary" class="detailText">读取真实交易记录后显示。</div>
  <div class="detailCharts">
    <canvas id="liveEquityChart" width="1100" height="230"></canvas>
    <canvas id="liveWinChart" width="1100" height="210"></canvas>
    <canvas id="liveBpsChart" width="1100" height="210"></canvas>
  </div>
  <div class="settingBand">
    <h3>紧急风控</h3>
    <div class="paperActions"><input id="emergencyConfirm" placeholder="输入 CLOSE"><button class="dangerBtn" onclick="runEmergencyClose()">紧急全部平仓</button><span id="emergencyStatus" class="subtle">只发送 reduce-only 平仓单，不会主动开反向仓。</span></div>
  </div>
  <h3>真实账户仓位（只读快照）</h3>
  <div class="tableWrap"><table id="livePosTbl"><thead><tr><th>合约</th><th>未实现盈亏</th><th>方向/数量</th><th>开仓价</th><th>仓位价值</th><th>杠杆</th><th>强平价</th></tr></thead><tbody></tbody></table></div>
  <p class="subtle">真实下单总开关即使开启，也只允许策略使用服务器已加密保存的唯一 API 钱包。API 钱包状态在“全局设置”查看。</p>
</div>
</dialog>
<dialog id="leadlagDlg" class="wideDialog">
<div class="helpHead"><h2>联动传播 V2 / 单腿模拟</h2><button onclick="leadlagDlg.close()">关闭</button></div>
<div class="helpBody">
  <div class="grid">
    <div class="metric"><div class="muted">策略状态</div><div id="llStatus">-</div></div>
    <div class="metric"><div class="muted">当前持仓</div><div id="llOpen">-</div></div>
    <div class="metric"><div class="muted">累计成本后收益</div><div id="llPnl">-</div></div>
    <div class="metric"><div class="muted">胜率 / 次数</div><div id="llWin">-</div></div>
    <div class="metric"><div class="muted">平均 / 最差</div><div id="llAvgWorst">-</div></div>
    <div class="metric"><div class="muted">估算手续费</div><div id="llFees">-</div></div>
    <div class="metric"><div class="muted">当前合格信号</div><div id="llEligible">-</div></div>
  </div>
  <p class="scoreMid">V2 当前只做独立单腿模拟，不会发送真实订单，也不会占用旧双腿策略仓位。先收集足够样本，再决定是否开放真实单腿开关。</p>
  <div class="paperActions"><label>参数档位 <select id="llPreset" onchange="applyLeadlagPreset(this.value)"><option value="sampling">取样激进（推荐）</option><option value="standard">标准</option><option value="strict">严格</option><option value="custom">自定义</option></select></label><button onclick="saveLeadlagConfig()">保存 V2 参数</button><button onclick="loadLeadlag()">刷新</button><span id="llSaveStatus" class="subtle">保存需要全局管理口令</span></div>
  <details class="settingBand" open><summary><b>V2 参数</b></summary>
    <div class="paperForm">
      <label>模拟策略开关<select id="cfg_leadlag_enabled"><option value="true">开启</option><option value="false">关闭</option></select></label>
      <label>每笔单腿金额U<input id="cfg_leadlag_notional_usdc" type="number" step="1"></label>
      <label>最多持仓<input id="cfg_leadlag_max_open" type="number" step="1"></label>
      <label>龙头币列表<input id="cfg_leadlag_leaders" placeholder="BTC,ETH,SOL,HYPE,DOGE,BNB"></label>
      <label>龙头3秒启动bps<input id="cfg_leadlag_leader_3s_bps" type="number" step="1"></label>
      <label>龙头15秒启动bps<input id="cfg_leadlag_leader_15s_bps" type="number" step="1"></label>
      <label>最小跟随缺口bps<input id="cfg_leadlag_min_lag_bps" type="number" step="1"></label>
      <label>最低相关性<input id="cfg_leadlag_min_corr" type="number" step="0.01"></label>
      <label>最大点差bps<input id="cfg_leadlag_max_spread_bps" type="number" step="0.1"></label>
      <label>行情最大年龄ms<input id="cfg_leadlag_max_data_age_ms" type="number" step="500"></label>
      <label>盘口同向倾斜<input id="cfg_leadlag_min_imbalance" type="number" step="0.01"></label>
      <label>盘口深度倍数<input id="cfg_leadlag_min_depth_multiple" type="number" step="1"></label>
      <label>单腿往返成本bps<input id="cfg_leadlag_fee_bps" type="number" step="0.1"></label>
      <label>最低扣费空间bps<input id="cfg_leadlag_min_edge_bps" type="number" step="1"></label>
      <label>止盈bps<input id="cfg_leadlag_take_profit_bps" type="number" step="1"></label>
      <label>止损bps<input id="cfg_leadlag_stop_bps" type="number" step="1"></label>
      <label>移动止盈启动bps<input id="cfg_leadlag_trail_start_bps" type="number" step="1"></label>
      <label>移动止盈回撤bps<input id="cfg_leadlag_trail_gap_bps" type="number" step="1"></label>
      <label>最长持仓分钟<input id="cfg_leadlag_max_hold_minutes" type="number" step="1"></label>
      <label>同币冷却分钟<input id="cfg_leadlag_cooldown_minutes" type="number" step="1"></label>
    </div>
  </details>
  <h3>当前联动信号</h3>
  <div class="paperTableWrap" style="max-height:320px"><table id="llSignalTbl"><thead><tr><th>结果</th><th>小币/龙头</th><th>方向</th><th>扣费空间</th><th>跟随缺口</th><th>龙头3s/15s</th><th>小币1s/3s</th><th>相关</th><th>点差</th><th>盘口倾斜</th><th>原因</th></tr></thead><tbody></tbody></table></div>
  <h3>V2 模拟交易</h3>
  <canvas id="llEquityChart" width="1100" height="220"></canvas>
  <div class="paperTableWrap" style="max-height:420px"><table id="llTradeTbl"><thead><tr><th>状态</th><th>小币/龙头</th><th>成本后盈亏</th><th>方向</th><th>开仓时间</th><th>平仓时间</th><th>持仓</th><th>入场价</th><th>跟随缺口</th><th>预期空间</th><th>最高浮盈</th><th>原因</th></tr></thead><tbody></tbody></table></div>
</div>
</dialog>
<dialog id="notifyDlg">
<div class="helpHead"><h2>钉钉推送设置</h2><button onclick="notifyDlg.close()">关闭</button></div>
<div class="helpBody">
  <p>勾选才会推送。候选提醒只是信号；真实下单是否发生，看“真实交易开平仓”提醒和真实交易面板。</p>
  <div class="paperActions"><button onclick="saveNotifyConfig()">保存推送设置</button><button onclick="loadNotifyConfig()">重读</button><span id="notifySaveStatus" class="subtle">保存需要先在“全局设置”填写管理口令</span></div>
  <div class="paperForm">
    <label>模拟/候选群 Webhook<input id="cfg_dingtalk_paper_webhook" placeholder="https://oapi.dingtalk.com/robot/send?..."></label>
    <label>模拟/候选关键词<input id="cfg_dingtalk_paper_keyword" placeholder="小测试"></label>
    <label>真实交易群 Webhook<input id="cfg_dingtalk_live_webhook" placeholder="https://oapi.dingtalk.com/robot/send?..."></label>
    <label>真实交易关键词<input id="cfg_dingtalk_live_keyword" placeholder="小测试"></label>
    <label>面板公网地址<input id="cfg_public_url" placeholder="http://50.114.113.121/hl"></label>
  </div>
  <div class="detailText" id="notifyChecks"></div>
  <div class="paperForm">
    <label>每轮候选最多推送<input id="cfg_notify_candidate_max_per_scan" type="number" min="0" max="50" step="1"></label>
    <label>候选最低 |Z|<input id="cfg_notify_candidate_min_z" type="number" min="0" max="20" step="0.1"></label>
    <label>重复提醒冷却秒<input id="cfg_notify_cooldown" type="number" min="0" max="86400" step="60"></label>
  </div>
  <p class="subtle">例如：每轮最多 1 条、最低 |Z|=3，表示一轮里只从偏离最强且 Z 绝对值至少为 3 的候选中推 1 条。填 0 条等于不推候选观察。</p>
</div>
</dialog>
<dialog id="help">
<div class="helpHead"><h2>这些指标到底是什么意思</h2><button onclick="help.close()">关闭</button></div>
<div class="helpBody">
<p><span class="pill">一句话</span> 这个工具不是预测涨跌，而是在找：某个小币平时跟 BTC/ETH 一起走，但现在短时间走得太远，后面可能往正常关系靠回来。</p>

<h3>偏离是什么意思</h3>
<p>偏离就是“小币现在相对 BTC/ETH 是否跑歪了”。例如 SOL 平时和 ETH 很同步，ETH 涨 1%，SOL 大概也跟着涨；但现在 ETH 只涨一点，SOL 突然涨很多，这就叫 SOL 相对 ETH 偏强。</p>
<p>偏离不是“价格贵不贵”。SOL 价格 180，DOGE 价格 0.2，不能直接比。我们比的是它们的涨跌关系。</p>

<h3>Z 是什么意思</h3>
<p>Z 可以理解成“这次跑歪有多夸张”。</p>
<p><code>Z = 0</code>：基本正常，没有明显跑歪。</p>
<p><code>Z = +2</code>：小币比平时明显强，可能涨过头。</p>
<p><code>Z = -2</code>：小币比平时明显弱，可能跌过头。</p>
<p><code>|Z|</code> 越大，说明偏离越极端。但极端不代表马上回归，也可能继续极端，所以一定要配合止损、最长持仓时间和盘口成本。</p>

<h3>Z 为正/为负怎么读</h3>
<p><code>Z 为正</code>：小币相对保护腿偏强。假设赌回归，方向通常是“做空小币 + 做多保护腿”。</p>
<p><code>Z 为负</code>：小币相对保护腿偏弱。假设赌回归，方向通常是“做多小币 + 做空保护腿”。</p>
<p>这里的“方向”只是研究动作，不是自动下单建议。</p>

<h3>保护腿是什么</h3>
<p>保护腿通常是 BTC 或 ETH。它的作用是抵消一部分大盘涨跌影响。</p>
<p>例子：工具显示 <code>做空 SOL；做多 0.82 倍 ETH</code>，意思不是单独赌 SOL 跌，而是赌“SOL 相对 ETH 过强，会回落到正常关系”。ETH 多单是保护，不是主攻。</p>

<h3>beta 是什么</h3>
<p>beta 是大概的对冲比例。比如 beta = 0.82，意思是做 1000 USDC 的 SOL，保护腿大概做 820 USDC 的 ETH 反向。</p>
<p>beta 不是固定真理，会随行情变。小币暴涨暴跌时，beta 很容易失效。</p>

<h3>相关性 corr 是什么</h3>
<p>相关性表示过去这段时间两者是不是经常一起涨跌。</p>
<p><code>corr 接近 1</code>：很同步。</p>
<p><code>corr 接近 0</code>：关系弱。</p>
<p>如果相关性低，就算 Z 很大也不一定有意义，因为它本来就不跟 BTC/ETH 走。</p>

<h3>候选、观察、谨慎</h3>
<p><code>观察</code>：没过门槛，只记录，不考虑动作。</p>
<p><code>候选</code>：相关性和偏离都过门槛，可以加入纸面跟踪。</p>
<p><code>谨慎</code>：看起来有偏离，但盘口点差太大或数据质量差，容易被滑点吃掉。</p>

<h3>盘口点差是什么</h3>
<p>点差就是买一和卖一之间的差。点差越大，进场出场越贵。</p>
<p>小币最坑的地方常常不是方向错，而是看起来有机会，实际一买一卖成本很高。</p>
<p><code>bps</code> 是万分之一。1 bps = 0.01%，10 bps = 0.10%，100 bps = 1%。例如点差 2.5 bps，就是买卖之间大约差 0.025%。</p>

<h3>资金费是什么</h3>
<p>永续合约里，多空之间会定期付费。正资金费通常是多头付空头；负资金费通常是空头付多头。</p>
<p>如果策略要持仓很久，资金费会明显影响收益。比如你做空小币但资金费为负，可能是你在付费。</p>

<h3>怎么用才比较实际</h3>
<p>第一步，只看候选，不下单。</p>
<p>第二步，记录它 5 分钟、30 分钟、2 小时后有没有回归。</p>
<p>第三步，统计至少几十次后，再看胜率、平均收益、最大反向亏损。</p>
<p>第四步，如果要实盘，先极小仓位验证滑点、资金费和爆仓距离。</p>

<h3>真实交易为什么看起来没赚没亏</h3>
<p>你现在每笔很小，通常十几 USDC 一条腿。价格动一点点，显示出来可能只有几分钱，手续费、点差和资金费会很容易吃掉它。</p>
<p>所以小仓位阶段主要是在验证：能不能正确开平仓、滑点多大、异常能不能及时发现，而不是马上看大额利润。</p>
<p><b>模拟怎么计算：</b>做多的一腿按卖一价模拟买入、按买一价模拟卖出；做空的一腿按买一价模拟卖出、按卖一价模拟买回，再按两腿名义金额计算 bps，并扣除“模拟手续费/额外滑点”。因此 ADA 即使 Z 回归，如果 ADA 与 ETH 的真实价格组合亏损，模拟也会显示亏损。</p>
<p>旧版把 Z 变化直接折算成收益的算法已经从交易界面删除，因为 Z 回归不等于两腿价格一定赚钱。真实盘仍可能和模拟差几 bps，原因是 IOC 发送耗时、真实成交档位、手续费和资金费。</p>
<p><b>最近一次官方账单核对：</b>价格毛收益本身为负，手续费累计约 1.8U，资金费接近 0，成本后净收益约 -2.2U。具体数字以真实面板顶部的动态统计为准。说明当前首先要解决的是信号质量和交易频率，不是更换行情 API。</p>

<h3>真实面板顶部每一项是什么</h3>
<p><code>真实下单状态</code>：总开关和策略开关是否同时开启。</p>
<p><code>账户模式 / 可用USDC</code>：Hyperliquid 当前可用于保证金和下单的余额，不等于你愿意承担的最大亏损。</p>
<p><code>当前真实仓位数</code>：官方账户当前非零合约仓位；程序记录和官方仓位不一致时应优先看官方。</p>
<p><code>真实已平仓 / 胜率</code>：只统计程序真实成交并正常平仓的记录。胜率高不代表赚钱，仍要看平均 bps。</p>
<p><code>真实已实现</code>：按 Hyperliquid 官方成交、手续费和实际资金费计算的净盈亏。</p>
<p><code>真实平均 / 最差</code>：平均每笔 bps 和历史最差单笔。平均长期为正比单看胜率更重要。</p>
<p><code>官方手续费 / 资金费</code>：手续费是开、平两条腿的真实收费；资金费是持仓期间多空之间的实际收付。</p>
<p><code>手续费负担</code>：“往返 bps”用累计手续费除以每笔两腿入场名义金额计算，能直接看策略每完成一次开平大约先损失多少；“均每笔”是平均一组双腿交易付了多少 U；亏损时还会显示手续费及资金费成本占净亏损的比例。</p>
<p><code>资金费影响占比</code>：“资金费/手续费”用来判断资金费是否已经大到需要优先处理；“手续费/|毛收益|”表示手续费相当于价格毛盈亏绝对值的多少。超过 100% 说明手续费比策略自身产生的价格波动收益还大。</p>
<p><code>双腿 vs 单腿影子</code>：单腿影子是假设历史只做小币腿的比较，不会发送单腿订单。当前历史中保护腿多数拖累，但单腿结果仍然亏，所以暂不开放真实单腿开关。</p>
<p><code>l2Book WS盘口</code>：WebSocket 是否连接、订阅了多少币、最新消息年龄。</p>
<p><code>本轮真实机会</code>：目前有多少组合通过基础过滤；通过后仍要检查账户、最低金额、盘口深度和杠杆。</p>
<p><code>WS实时策略引擎</code>：实时判断是否在运行、多久判断一次、最近是否报错。</p>

<h3>为什么杠杆 5x 不会让它更容易赚钱</h3>
<p>杠杆只放大仓位，不提高信号胜率。原本一笔亏 0.02U，放大后会亏更多；如果方向正确，也会赚更多。</p>
<p>部分小币不支持你设置的杠杆倍数，交易所会返回 Invalid leverage value。现在软件默认遇到杠杆设置失败就跳过，不再继续下单。</p>

<h3>策略设置怎么理解</h3>
<p><code>统一策略档位</code>：一次填好开仓 Z、相关性、点差和预期边际。保守=机会少、过滤严；中等=用于小额测试；激进=交易多，但更容易交易到噪声。选择后仍要点击保存。</p>
<p><code>保守档</code>：|Z| 3.0、相关 0.75、最大点差 2.5bps、最低预期边际 25bps。</p>
<p><code>中等档</code>：|Z| 2.5、相关 0.70、最大点差 3.5bps、最低预期边际 18bps。</p>
<p><code>激进档</code>：|Z| 2.0、相关 0.65、最大点差 5bps、最低预期边际 10bps。小资金研究可以看，实盘要谨慎。</p>
<p><code>每笔小币腿USDC</code>：小币这一腿想下多少名义金额。保护腿会按 beta 自动折算，不是固定同样金额。</p>
<p><code>低于交易所最低</code>：Hyperliquid 单腿最低约 10U。选择“跳过”时，小额不下单；选择“自动补到最低”时，会把金额抬到两条腿都满足最低订单。</p>
<p><code>统一策略最多持仓</code>：模拟和真实共用的最多组合数。小资金阶段建议 1。</p>
<p><code>同币对冷却分钟</code>：一个组合刚平仓后，至少等这么久才允许再次开仓。它用于阻止几秒内反复开平、被手续费连续磨损；默认 15 分钟。</p>
<p><code>最大滑点bps</code>：IOC 限价单最多允许比当前可成交价差多少。太小容易不成交，太大容易成交很差。</p>
<p><code>杠杆倍数</code>：放大仓位，不提高胜率。小资金测试建议先 1x，确认策略真实转正后再考虑提高。</p>
<p><code>杠杆失败处理</code>：有些小币不支持设置的杠杆。默认“跳过”更安全，避免你以为是 5x，实际交易所没接受。</p>
<p><code>开仓最低|Z|</code>：偏离至少多极端才允许开仓。越高交易越少。</p>
<p><code>开仓最低相关</code>：小币和保护腿过去的联动至少多稳定。</p>
<p><code>最大允许点差</code>：小币腿或保护腿任意一边的实时买卖差超过它就不做。之前重复的“开仓最大点差”和“盘口最大点差”已经合并。</p>
<p><code>最低预期边际</code>：仍是基于 Z 的入场过滤分数，只负责筛选，不再拿来计算新版模拟盈亏。</p>
<p><code>回归平仓Z</code>：例如 0.5，表示 |Z| 回到 0.5 以内就退出。注意：Z 回归时价格组合仍可能亏，所以退出原因和盈亏是两件事。</p>
<p><code>固定止盈/止损bps</code>：新版按两腿盘口价格收益判断；不是按 Z 变化判断。</p>
<p><code>最长持仓分钟</code>：超过时间无论盈亏都退出，避免关系长期不回归。</p>
<p><code>模拟手续费/额外滑点bps</code>：买一卖一已经包含盘口点差；这里再扣交易手续费和额外成交误差。设置太低仍会使模拟偏乐观。</p>
<p><code>Z边际估算系数bps</code>：把 Z 到回归线的空间粗略换算成“预期边际”，只用于开仓过滤；也用于没有保存盘口的历史旧记录。新版模拟实际盈亏不使用它。</p>
<p><code>持仓最低相关</code>：持仓后相关性明显破坏时提前退出。</p>
<p><code>l2Book盘口校验</code>：开仓前检查实时买一/卖一、点差、数据年龄、盘口深度。不合格就跳过真实下单。</p>
<p><code>l2Book最大延迟ms</code>：盘口数据允许多旧。3000ms 表示超过 3 秒没更新就不拿来开仓。</p>
<p><code>已确认盘口宽限ms</code>：盘口刚刚通过新鲜度检查后，准备杠杆和发送订单仍需要时间。只有 WS 连接正常、买一卖一没变、点差和深度仍合格时才允许短暂宽限。</p>
<p><code>固定执行步骤</code>：后台缓存账户 → 准备杠杆 → 复查最新盘口 → 发送真实 IOC → 保存模拟记录 → 后台刷新账户。步骤编辑器已经删除，避免误删风控或把检查放到下单之后。</p>
<p><code>后台账户刷新秒</code>：平时多久更新一次余额和仓位，默认约 3 秒。它在后台进行，不阻塞商机。</p>
<p><code>账户缓存最大年龄ms</code>：缓存超过多旧就拒绝开仓。不是行情延迟。</p>
<p><code>杠杆预设缓存秒</code>：同一币种杠杆已经设置成功后，多长时间内不重复调用设置接口。</p>
<p><code>l2Book最大点差bps</code>：实时盘口点差上限。超过这个值说明进出场成本太高，跳过。</p>
<p><code>WS盘口覆盖</code>：服务器现在自动订阅本轮全部扫描币、保护腿和持仓币，最多 1000 个，不再由“150”参数截断。因此观察表和可做实时盘口校验的币集合保持一致。</p>
<p><code>WS实时Z</code>：用最近一次历史K线计算出的 beta/均值/波动率作为基准，再用 l2Book 最新 mid 估算当前偏离。它适合开仓前/平仓前快速确认，但不是完整历史回测。</p>
<p><code>策略判断节流ms</code>：WS 行情本身持续接收；500ms 表示把这段时间内的新盘口合并判断一次，避免每条消息都重复查库。它不是 500ms 网络延迟。</p>
<p><code>真实下单总开关</code>：最外层保险。关闭后绝不会发送新的真实订单。</p>
<p><code>真实策略开关</code>：总开关开启后还要这个开关开启，才会按规则真实下单。关闭时只监控和模拟。</p>

<h3>策略实验室怎么读</h3>
<p><code>训练段</code>：较早的 60% K 线，用来观察这套固定参数在过去是否有基本逻辑。它不是挑完参数后重新美化的成绩。</p>
<p><code>样本外</code>：较新的 40% K 线，排名只看这一段。可以理解成参数先定好，再参加一次没用于训练的考试。</p>
<p><code>下一根开盘成交</code>：本根收盘后才能知道 RSI、MACD 或突破信号，所以程序最早只能用下一根开盘价成交，避免偷偷使用未来价格。</p>
<p><code>达到研究门槛</code>：训练和样本外净收益都为正，样本外至少 6 笔、盈亏比至少 1.10、回撤未超过门槛。它只代表值得继续做 30/60/90 天、多周期和实时模拟，不是实盘授权。</p>
<p><code>往返成本 bps</code>：估算一笔从开仓到平仓的总手续费和滑点。默认 12 bps。真实成本会随 maker/taker 费率、盘口、币种和订单大小变化。</p>
<p><code>当前信号</code>：最后一根已获取 K 线得出的做多、做空或空仓目标。它不是实时盘口信号；15分钟策略只有在新15分钟K线确认后才应变化。</p>
<p><code>实际历史</code>：必须看这一列，而不是只看你填写的天数。Hyperliquid 公开 candleSnapshot 约有 5000 根上限，所以 5分钟大约只有17天、15分钟大约52天；更长历史需要 TradingView 或授权历史数据源。</p>
<p><code>TradingView 和这里的区别</code>：TradingView 适合发现和阅读策略、使用它的策略测试器；最终要在 Hyperliquid 下单时，仍应使用 Hyperliquid K线重跑，因为交易所价格、K线边界、手续费和成交条件可能不同。</p>
<p>TradingView 公共库可以搜索和手工添加策略，但没有官方接口允许任意批量下载全部社区源码。开源脚本可在页面查看时按作者许可人工移植；受保护或仅邀请脚本不能拉取源码，也不应绕过保护。</p>

<h3>l2Book 表格怎么读</h3>
<p><code>买一 bid</code>：现在别人愿意买的最高价。你如果马上卖，通常接近这个价成交。</p>
<p><code>卖一 ask</code>：现在别人愿意卖的最低价。你如果马上买，通常接近这个价成交。</p>
<p><code>买一深度/卖一深度</code>：当前最优价位上大概有多少可成交量。前面的 U 是约 USDC，后面是币数量。</p>
<p><code>数据年龄</code>：这条盘口距离现在多久。越小越新；超过 l2Book 最大延迟时，实盘会跳过。</p>
<p><code>自动刷新盘口</code>：只刷新网页显示，不改变服务器交易逻辑。后台 WS 本来就是实时收数据，网页每 1 秒拿一次服务器内存快照。</p>

<h3>真实交易走势图怎么读</h3>
<p><code>开仓时间/平仓时间</code>：真实策略表统一按北京时间显示到秒；持仓中的平仓时间显示“持仓中”。</p>
<p><code>持仓时长</code>：从真实开仓成交到平仓成交的时间；持仓中会随页面自动刷新继续计时。</p>
<p><code>累计收益/回撤</code>：蓝线看真实成交后累计盈亏，红线看从高点回落多少。</p>
<p><code>滚动10笔胜率</code>：最近 10 笔表现，能看策略近期有没有改善。</p>
<p><code>每笔 bps/平均 bps</code>：看收益分布。平均 bps 长期为正，才说明策略有真实边际。</p>

<h3>模拟盘参数怎么读</h3>
<p><code>统一策略</code>：模拟盘始终运行并产生唯一的开平仓决定。真实开关关闭时只模拟；真实开关开启时，真实模块只把本轮刚出现的同一个模拟开仓事件发送成真实订单。</p>
<p>统一模式下，WS Z、相关性、点差、预期边际、方向、止盈止损和最多持仓都共用一套参数。真实盘额外检查余额、最低订单、杠杆和实际成交；这些属于执行条件，不会改变模拟策略本身。</p>
<p><code>盘口可成交价</code>：新版记录。模拟多头按卖一买、买一卖；模拟空头按买一卖、卖一买。新版累计收益、胜率和利润曲线只统计这种记录。</p>
<p>旧 Z 理论交易记录仍留在 SQLite 里用于审计，但不会再出现在模拟交易表、胜率或利润曲线中。</p>
<p><code>双腿名义金额</code>：模拟与真实会显示同样的“小币腿 + 保护腿”格式，例如 18.9U + 10.5U，不再一个显示总额、另一个显示两腿。</p>

<h3>保护腿、现货和资金费怎么选</h3>
<p><code>保护腿</code>不是必然降低风险。它只能减少 BTC/ETH 大盘方向影响，也会增加一条腿的点差和两次成交手续费。当前历史中保护腿多数时候拖累；但只做小币腿的影子结果也仍然亏，所以现阶段应继续比较，而不是直接切成真实单腿。</p>
<p><code>现货</code>不能直接替代当前永续策略，因为当小币相对过强时需要做空小币，普通现货没有做空能力。Hyperliquid 虽有现货市场，但当前自动交易代码是永续合约双腿逻辑，不能混用。</p>
<p><code>资金费套利</code>可以单独研究，例如现货多头配永续空头赚正资金费，但它是另一套持仓周期和风险模型。当前实际资金费接近 0，远小于约 1.8U 的累计手续费，不能用资金费把这套超短线策略救回来。</p>

<h3>联动传播 V2 是什么</h3>
<p>V2 不再看到 Z 极端就反向交易。它监听 BTC/ETH 的 1 秒、3 秒、15 秒盘口变化：龙头已经快速启动，而相关小币还没有完全跟上，并且小币盘口开始出现同方向力量时，模拟做多或做空小币。</p>
<p><code>跟随缺口</code>：按历史 beta 推算小币本应移动的幅度，减去小币实际移动幅度。缺口越大，理论补涨/补跌空间越大。</p>
<p><code>龙头币列表</code>：V2 不再只看 BTC/ETH。默认同时比较 BTC、ETH、SOL、HYPE、DOGE、BNB，并用最近约3分钟 allMids 自动计算每个小币与各龙头的实时相关和 beta，选择当前最有传播空间的组合。</p>
<p><code>扣费预期空间</code>：跟随缺口减去单腿往返成本和两次盘口点差。只有剩余空间达到门槛才模拟开仓。</p>
<p><code>盘口同向倾斜</code>：买一和卖一顶层数量的差异。做多时希望买方更强，做空时希望卖方更强；它只是确认条件，不是单独信号。设置为 <code>-1</code> 代表关闭硬过滤：界面仍展示倾斜数据，但不会仅因顶层数量瞬时反向而拦截信号。取样档默认使用 -1，标准档和严格档仍会检查。</p>
<p><code>盘口深度倍数</code>：当前可成交顶层金额是模拟下单金额的多少倍，防止看到信号却交易在太薄的盘口。</p>
<p><code>取样激进档</code>：按服务器真实波动分布调整为3秒2bps、15秒4bps、相关0.60、点差2.5bps、扣费空间6bps。目的是先产生足够模拟样本；它大约关注当前市场最活跃的10%左右短时波动，也更容易包含噪声。</p>
<p><code>标准档</code>：3秒4bps、15秒8bps，并提高相关和扣费空间要求。</p>
<p><code>严格档</code>：保留第一版12bps/25bps门槛，信号非常少，只适合高波动行情。</p>
<p><code>行情最大年龄</code>：allMids 通常按价格变化推送，不保证每秒来一条。取样档允许10秒以内，但真正入场仍使用 l2Book 当前买一卖一和点差。</p>
<p>V2 默认单腿、目标空间更大，并使用固定止盈、止损、移动止盈、龙头反向退出和最长持仓；不会因为 Z 回到零自动平仓。当前版本只模拟，不会发送真实订单。</p>

<h3>推送参数怎么读</h3>
<p><code>候选首次出现</code>：某个币对第一次达到候选条件时推送。</p>
<p><code>候选持续提醒</code>：同一个候选持续存在时，按冷却时间重复提醒。容易刷屏，默认可关。</p>
<p><code>每轮候选最多推送</code>：一轮扫描最多推几条，避免小币行情多时刷爆钉钉。</p>
<p><code>候选最低 |Z|</code>：只有偏离绝对值达到这个值才推送候选。数值越高，提醒越少但更极端。</p>
<p><code>重复提醒冷却秒</code>：同类消息隔多久才能再次推送。</p>

<h3>1U 测试单是什么意思</h3>
<p>Hyperliquid 官方每条腿最低订单价值约 10U。双腿策略不是总共 10U，而是小币腿至少约 10U，保护腿也至少约 10U。</p>
<p>所以填 1U 时，真实交易默认会跳过，不会发真实订单。如果打开“自动补到最低”，软件会把小币腿抬到让两条腿都满足最低订单。比如 beta=0.67 时，为了让保护腿也达到 10U，小币腿可能需要约 15U。</p>
<p>1U 仍然可以作为参数/界面测试，但不能作为 Hyperliquid 实盘成交测试。最小实盘连通性测试通常要准备双腿合计 20U 以上，还要留出保证金和滑点缓冲。</p>

<h3>为什么不能多个策略共用 ETH/BTC</h3>
<p>Hyperliquid 官方账户对同一个币只显示一个净仓位。比如三笔策略都用 ETH 做保护腿，官方只会合并成一个 ETH 仓位。</p>
<p>合并后，每笔策略想独立平自己的 ETH 腿就可能出错。所以现在真实策略会避免新仓和已有仓位共用同一个币。</p>

<h3>紧急全部平仓是什么</h3>
<p>紧急按钮会读取官方当前全部仓位，然后对每个仓位发送 reduce-only 平仓单。reduce-only 表示只能减少仓位，不能反向开新仓。</p>
<p>它用于程序异常、策略看不懂、或者你只想先把仓位收干净的时候。点之前必须输入 <code>CLOSE</code>。</p>

<h3>API 钱包是什么</h3>
<p>API 钱包不是你的主钱包，也不是普通密码。它是被主钱包授权的交易签名钥匙。软件用它给 Hyperliquid 发订单。</p>
<p>这个软件只使用服务器上加密保存的一个 API 钱包。网页不接收私钥，避免浏览器、日志、数据库泄露。</p>

<h3>最容易亏的情况</h3>
<p>小币有独立消息，和 BTC/ETH 脱钩；Z 很高但继续冲；盘口太薄；资金费吃掉利润；beta 失效；参数为了适配过去而过拟合。</p>
</div>
</dialog>
<script>
let latestRows=[];
let selectedRow=null;
let sortKey='score', sortDir=-1;
const charts={};
let liveL2Coins=[];
let liveL2RefreshBusy=false;
function fmt(n,d=2){return n===null||n===undefined||isNaN(Number(n))?'-':Number(n).toFixed(d)}
function fmtBeijingDateTime(ts){
  if(ts===null||ts===undefined||isNaN(Number(ts)))return '-';
  return new Date(Number(ts)*1000).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
function fmtDuration(startTs,endTs){
  if(!startTs)return '-';
  let seconds=Math.max(0,Math.round(Number(endTs||Date.now()/1000)-Number(startTs)));
  const days=Math.floor(seconds/86400);seconds%=86400;
  const hours=Math.floor(seconds/3600);seconds%=3600;
  const minutes=Math.floor(seconds/60);seconds%=60;
  if(days)return `${days}天 ${hours}时 ${minutes}分`;
  if(hours)return `${hours}时 ${minutes}分 ${seconds}秒`;
  if(minutes)return `${minutes}分 ${seconds}秒`;
  return `${seconds}秒`;
}
function tagText(t){return t==='candidate'?'候选':(t==='caution'?'谨慎':'观察')}
function val(row,key){return row[key]===null||row[key]===undefined?'':row[key]}
function dirText(action){
  if(action==='short_asset_long_hedge') return '做空小币 / 做多保护腿';
  if(action==='long_asset_short_hedge') return '做多小币 / 做空保护腿';
  return '只观察';
}
function sortBy(key){sortDir=sortKey===key?-sortDir:-1; sortKey=key; renderTable()}
async function loadLatest(){
  const r=await fetch('latest'); const data=await r.json();
  if(!data.ok){document.getElementById('status').textContent=data.error||'服务暂无数据';return}
  latestRows=data.rows||[];
  const ts=data.ts || (data.scan&&data.scan.ts);
  document.getElementById('status').textContent='最新：'+(ts?new Date(ts*1000).toLocaleString():'未知')+'；记录 '+latestRows.length+' 条';
  renderTable();
  if(latestRows[0] && !selectedRow) selectRow(latestRows[0]);
  loadPaper();
}
function renderTable(){
  const tb=document.querySelector('#tbl tbody'); tb.innerHTML='';
  const f=(document.getElementById('filterText')?.value||'').trim().toUpperCase();
  const tag=document.getElementById('tagFilter')?.value||'';
  const rows=latestRows.filter(r=>(!f||r.asset.includes(f)||r.leader.includes(f))&&(!tag||r.tag===tag));
  rows.sort((a,b)=>{
    const av=val(a,sortKey),bv=val(b,sortKey);
    if(typeof av==='number'&&typeof bv==='number') return (av-bv)*sortDir;
    return String(av).localeCompare(String(bv))*sortDir;
  });
  rows.forEach(row=>{
    const tr=document.createElement('tr');
    if(selectedRow && selectedRow.asset===row.asset && selectedRow.leader===row.leader) tr.className='selectedRow';
    const livePass=row.live_status==='pass';
    tr.innerHTML=`<td class="${row.tag}">${tagText(row.tag)}</td><td class="${livePass?'passChip':'blockChip'}" title="${esc(row.live_reject_reason||'')}">${livePass?'可进入盘口校验':'过滤'}</td><td>${row.asset}</td><td>${row.leader}</td><td>${fmt(row.corr,3)}</td><td>${fmt(row.beta,2)}</td><td>${row.realtime?fmt(row.zscore,2):'-'}</td><td>${fmt(row.kline_zscore??row.zscore,2)}</td><td>${fmt(row.asset_15m_bps,1)} bps</td><td>${fmt(row.hedge_15m_bps,1)} bps</td><td>${fmt(row.spread_bps,2)}</td><td>${fmt((row.funding_hourly||0)*10000,3)} bps</td><td class="actionCell">${row.plan||''}</td>`;
    tr.onclick=()=>selectRow(row,true);
    tb.appendChild(tr);
  });
}
async function selectRow(row, showDetail=false){
  selectedRow=row;
  renderTable();
  if(!showDetail) return;
  const [seriesRes,statsRes,candleRes]=await Promise.all([
    fetch(`series?asset=${encodeURIComponent(row.asset)}&leader=${encodeURIComponent(row.leader)}&limit=240`),
    fetch(`stats?asset=${encodeURIComponent(row.asset)}&leader=${encodeURIComponent(row.leader)}&limit=240`),
    fetch(`candles?asset=${encodeURIComponent(row.asset)}&leader=${encodeURIComponent(row.leader)}&hours=24`)
  ]);
  const series=await seriesRes.json();
  const stats=await statsRes.json();
  const candles=await candleRes.json();
  const s=stats.stats||{};
  showRowDetail(row, s, series.rows||[], candles.rows||[]);
}
function showRowDetail(row, stats={}, seriesRows=[], candleRows=[]){
  document.getElementById('detailTitle').textContent=`${row.asset} vs ${row.leader} 完整详情`;
  const notional=Number(document.getElementById('cfg_live_notional_usdc')?.value||10.5);
  const hedge=Math.abs(Number(row.beta||0))*notional;
  const fundingBps=(Number(row.funding_hourly)||0)*10000;
  const html=`
    <div class="detailGrid">
      <div class="detailBox"><div class="muted">状态</div><div class="${row.tag}">${tagText(row.tag)}</div></div>
      <div class="detailBox"><div class="muted">方向</div><div>${dirText(row.action)}</div></div>
      <div class="detailBox"><div class="muted">目标双腿粗算</div><div>小币 ${fmt(notional,2)}U / 保护腿约 ${fmt(hedge,2)}U</div></div>
      <div class="detailBox"><div class="muted">WS实时Z / K线Z</div><div>${row.realtime?fmt(row.zscore,2):'-'} / ${fmt(row.kline_zscore??row.zscore,2)}</div></div>
      <div class="detailBox"><div class="muted">相关性 corr</div><div>${fmt(row.corr,3)}</div></div>
      <div class="detailBox"><div class="muted">Beta</div><div>${fmt(row.beta,2)}</div></div>
      <div class="detailBox"><div class="muted">小币15m</div><div>${fmt(row.asset_15m_bps,1)} bps</div></div>
      <div class="detailBox"><div class="muted">保护腿15m</div><div>${fmt(row.hedge_15m_bps,1)} bps</div></div>
      <div class="detailBox"><div class="muted">点差 / 资金费</div><div>${fmt(row.spread_bps,2)} bps / ${fmt(fundingBps,3)} bps小时</div></div>
    </div>
    <h3>动作</h3>
    <div class="detailText">${row.plan||'只观察'}</div>
    <h3>怎么理解</h3>
    <div class="detailText">${explainRow(row)}</div>
    <h3>历史质量</h3>
    <div class="detailText">样本：${row.samples||'-'}；相关均值：${fmt(stats.corr_avg,3)}；相关波动：${fmt(stats.corr_std,3)}；候选率：${fmt((stats.candidate_ratio||0)*100,1)}%；平均点差：${fmt(stats.spread_avg,2)} bps；最大点差：${fmt(stats.spread_max,2)} bps。</div>
    <h3>历史走势</h3>
    <div class="subtle">这里只画真实采集到的价格、Z、相关性和点差，不再把旧 Z 理论公式包装成收益回测。图表可滚轮缩放、拖拽平移。</div>
    <div class="detailCharts">
      <canvas id="priceChart" width="1100" height="260"></canvas>
      <canvas id="zChart" width="1100" height="240"></canvas>
      <canvas id="qualityChart" class="mini" width="1100" height="190"></canvas>
    </div>
  `;
  document.getElementById('detailBody').innerHTML=html;
  detailDlg.showModal();
  delete charts.priceChart; delete charts.zChart; delete charts.qualityChart;
  setTimeout(()=>{
    drawPrice(candleRows, true);
    drawZSeries(seriesRows, true);
    drawQuality(seriesRows, true);
  }, 30);
}
function explainRow(row){
  if(row.tag==='candidate'){
    return `它现在满足候选条件：相关性达到门槛，偏离 Z 足够大，并且盘口点差没有超过过滤线。这里赌的是“相对关系回归”，不是单独赌 ${row.asset} 涨跌。`;
  }
  if(row.tag==='caution'){
    return '它有明显偏离，但交易质量不够好，通常是点差太大或盘口太差。模拟盘可以观察，真实交易容易被滑点吃掉。';
  }
  return '它现在只是观察项：偏离、相关性或点差没有同时满足条件。先记录，不适合当作开仓信号。';
}
function canvasMetrics(c){
  const rect=c.getBoundingClientRect();
  const dpr=window.devicePixelRatio||1;
  const w=Math.max(320, rect.width || c.clientWidth || 760);
  const h=Math.max(150, rect.height || c.clientHeight || 220);
  const needW=Math.round(w*dpr), needH=Math.round(h*dpr);
  if(c.width!==needW || c.height!==needH){c.width=needW;c.height=needH}
  const ctx=c.getContext('2d');
  ctx.setTransform(dpr,0,0,dpr,0,0);
  return {ctx,w,h,rect,dpr};
}
function eventX(c,e){
  const rect=c.getBoundingClientRect();
  return Math.max(0, Math.min(rect.width, e.clientX-rect.left));
}
function niceTicks(min,max,count=5){
  if(!isFinite(min)||!isFinite(max)||min===max) return [min||0];
  const span=max-min;
  const raw=span/Math.max(1,count-1);
  const pow=Math.pow(10, Math.floor(Math.log10(Math.abs(raw))));
  const step=[1,2,2.5,5,10].map(x=>x*pow).find(x=>x>=raw) || raw;
  const start=Math.ceil(min/step)*step;
  const ticks=[];
  for(let v=start;v<=max+step*0.5;v+=step) ticks.push(Number(v.toFixed(10)));
  return ticks.slice(0,8);
}
function fmtAxis(v){
  const av=Math.abs(Number(v));
  if(av>=100) return Number(v).toFixed(0);
  if(av>=10) return Number(v).toFixed(1);
  if(av>=1) return Number(v).toFixed(2);
  return Number(v).toFixed(3);
}
function fmtTime(ts){
  if(!ts) return '';
  const d=new Date(ts*1000);
  return `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
}
function drawAxes(ctx,w,h,padL,padR,padT,padB,yTicks,xTicks,yFn,xFn){
  ctx.save();
  ctx.font='11px Arial';
  ctx.lineWidth=1;
  yTicks.forEach(v=>{
    const yy=yFn(v);
    ctx.strokeStyle='#e5e7eb';
    ctx.beginPath();ctx.moveTo(padL,yy);ctx.lineTo(w-padR,yy);ctx.stroke();
    ctx.fillStyle='#475569';
    ctx.textAlign='right';ctx.textBaseline='middle';
    ctx.fillText(fmtAxis(v),padL-8,yy);
  });
  xTicks.forEach(t=>{
    const xx=xFn(t.index);
    ctx.strokeStyle='#f1f5f9';
    ctx.beginPath();ctx.moveTo(xx,padT);ctx.lineTo(xx,h-padB);ctx.stroke();
    ctx.fillStyle='#64748b';
    ctx.textAlign='center';ctx.textBaseline='top';
    ctx.fillText(fmtTime(t.ts),xx,h-padB+8);
  });
  ctx.strokeStyle='#94a3b8';
  ctx.beginPath();ctx.moveTo(padL,padT);ctx.lineTo(padL,h-padB);ctx.lineTo(w-padR,h-padB);ctx.stroke();
  ctx.restore();
}
function makeChart(id, rows, series, options){
  const c=document.getElementById(id);
  if(!c) return;
  const {w,h}=canvasMetrics(c);
  const padL=58,padR=16,padT=18,padB=42;
  const chart=charts[id]||{start:0,end:1,drag:false,lastX:0,hover:null};
  if(!charts[id]) charts[id]=chart;
  chart.rows=rows; chart.series=series; chart.options=options; chart.pad={padL,padR,padT,padB};
  if(options.reset){chart.start=0;chart.end=1;chart.hover=null}
  if(!chart.bound){
    c.addEventListener('wheel', e=>{e.preventDefault(); zoomChart(id,eventX(c,e),e.deltaY)});
    c.addEventListener('mousedown', e=>{chart.drag=true;chart.lastX=eventX(c,e)});
    window.addEventListener('mouseup', ()=>chart.drag=false);
    c.addEventListener('mousemove', e=>{const x=eventX(c,e); if(chart.drag) panChart(id,x); else {chart.hover=x; chart.pointer={x:e.clientX,y:e.clientY}; renderChart(id)}});
    c.addEventListener('mouseleave', ()=>{chart.hover=null; renderChart(id); hideTip()});
    c.addEventListener('dblclick', ()=>{chart.start=0;chart.end=1;renderChart(id)});
    window.addEventListener('resize', ()=>renderChart(id));
    chart.bound=true;
  }
  renderChart(id);
}
function visibleRange(chart){
  const n=chart.rows.length;
  const a=Math.max(0,Math.floor(chart.start*Math.max(n-1,1)));
  const b=Math.min(n-1,Math.ceil(chart.end*Math.max(n-1,1)));
  return [a,b];
}
function zoomChart(id,x,delta){
  const chart=charts[id],{padL,padR}=chart.pad,c=document.getElementById(id),{w}=canvasMetrics(c),plotW=w-padL-padR;
  const rel=Math.max(0,Math.min(1,(x-padL)/plotW));
  const span=chart.end-chart.start, factor=delta>0?1.25:0.8, next=Math.max(0.05,Math.min(1,span*factor));
  const center=chart.start+span*rel;
  chart.start=Math.max(0,center-next*rel); chart.end=Math.min(1,chart.start+next);
  if(chart.end-chart.start<next) chart.start=Math.max(0,chart.end-next);
  renderChart(id);
}
function panChart(id,x){
  const chart=charts[id],c=document.getElementById(id),{w}=canvasMetrics(c),dx=(x-chart.lastX)/(w-chart.pad.padL-chart.pad.padR),span=chart.end-chart.start;
  chart.lastX=x; chart.start-=dx*span; chart.end-=dx*span;
  if(chart.start<0){chart.end-=chart.start;chart.start=0}
  if(chart.end>1){chart.start-=chart.end-1;chart.end=1}
  chart.start=Math.max(0,chart.start); chart.end=Math.min(1,chart.end); renderChart(id);
}
function renderChart(id){
  const chart=charts[id],c=document.getElementById(id);
  if(!chart || !c) return;
  const {ctx,w,h,rect}=canvasMetrics(c),{padL,padR,padT,padB}=chart.pad;
  ctx.clearRect(0,0,w,h); ctx.fillStyle='#fff'; ctx.fillRect(0,0,w,h);
  const [a,b]=visibleRange(chart),vis=chart.rows.slice(a,b+1);
  const vals=vis.flatMap(r=>chart.series.map(s=>Number(s.value(r)))).filter(v=>!isNaN(v));
  if(!vals.length){ctx.fillStyle='#64748b';ctx.fillText('暂无数据',20,40);return}
  let min=chart.options.min!==undefined?chart.options.min:Math.min(...vals);
  let max=chart.options.max!==undefined?chart.options.max:Math.max(...vals);
  const pad=Math.max((max-min)*0.15, chart.options.pad||0.02); min-=pad; max+=pad;
  function x(i){return padL+(w-padL-padR)*(vis.length<=1?0:i/(vis.length-1))}
  function y(v){return padT+(h-padT-padB)*(1-(v-min)/(max-min))}
  const fixedMarks=(chart.options.marks||[]).filter(v=>v>=min&&v<=max);
  const yTicks=[...new Set([...niceTicks(min,max,5),...fixedMarks])].sort((a,b)=>a-b);
  const xTicks=[];
  const xCount=Math.min(6, vis.length);
  for(let i=0;i<xCount;i++){
    const idx=Math.round(i*(vis.length-1)/Math.max(1,xCount-1));
    xTicks.push({index:idx, ts:vis[idx]?.ts||vis[idx]?.t});
  }
  drawAxes(ctx,w,h,padL,padR,padT,padB,yTicks,xTicks,y,x);
  fixedMarks.forEach(v=>{
    ctx.save();
    ctx.strokeStyle=Math.abs(v)===2?'#f59e0b':'#94a3b8';
    ctx.setLineDash([5,4]);
    ctx.beginPath();ctx.moveTo(padL,y(v));ctx.lineTo(w-padR,y(v));ctx.stroke();
    ctx.restore();
  });
  chart.series.forEach(s=>{
    ctx.strokeStyle=s.color; ctx.lineWidth=s.width||2; ctx.beginPath();
    let started=false;
    vis.forEach((r,i)=>{
      const val=Number(s.value(r)); if(isNaN(val)) return;
      const yy=y(val),xx=x(i); if(!started){ctx.moveTo(xx,yy); started=true} else ctx.lineTo(xx,yy);
    });
    ctx.stroke();
  });
  ctx.font='12px Arial';
  ctx.fillStyle='#334155'; ctx.textAlign='right'; ctx.textBaseline='alphabetic';
  ctx.fillText(chart.options.footer||`${vis.length} 个点`, w-12, h-10);
  chart.series.forEach((s,i)=>{ctx.fillStyle=s.color;ctx.textAlign='left';ctx.fillText(s.label, padL+i*88, h-10)});
  if(chart.hover!==null && vis.length){
    const idx=Math.max(0,Math.min(vis.length-1,Math.round((chart.hover-padL)/(w-padL-padR)*(vis.length-1))));
    const row=vis[idx],xx=x(idx);
    ctx.strokeStyle='#64748b';ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(xx,padT);ctx.lineTo(xx,h-padB);ctx.stroke();ctx.setLineDash([]);
    chart.series.forEach(s=>{
      const val=Number(s.value(row)); if(isNaN(val)) return;
      const yy=y(val);
      ctx.fillStyle=s.color;ctx.beginPath();ctx.arc(xx,yy,3,0,Math.PI*2);ctx.fill();
      ctx.strokeStyle='#fff';ctx.lineWidth=1;ctx.stroke();
    });
    let html=`<b>${new Date((row.ts||row.t||0)*1000).toLocaleString()}</b>`;
    chart.series.forEach(s=>{html+=`<br>${s.label}: ${fmt(s.value(row),s.digits??2)}`});
    html+=`<br><span style="color:#cbd5e1">X: ${fmtTime(row.ts||row.t)}；Y轴范围: ${fmtAxis(min)} ~ ${fmtAxis(max)}</span>`;
    const px=(chart.pointer?.x || rect.left+xx)+14;
    const py=(chart.pointer?.y || rect.top+padT)+14;
    showTip(html, px, py);
  }
}
function showTip(html,x,y){const tip=document.getElementById('tip');tip.innerHTML=html;tip.style.left=x+'px';tip.style.top=y+'px';tip.style.display='block'}
function hideTip(){const tip=document.getElementById('tip');tip.style.display='none'}
function drawZSeries(rows, reset=false){
  makeChart('zChart', rows, [{label:'Z偏离', color:'#7c3aed', value:r=>r.zscore}], {marks:[-2,0,2], min:-3, max:3, footer:`Z 偏离：${rows.length} 点`, reset});
}
function drawQuality(rows, reset=false){
  makeChart('qualityChart', rows, [
    {label:'相关性', color:'#2563eb', value:r=>r.corr, digits:3},
    {label:'点差/20', color:'#ea580c', value:r=>Math.min(Number(r.spread_bps)||0,20)/20, digits:3}
  ], {marks:[0,.5,1], min:0, max:1, footer:'蓝=相关性  橙=点差/20', reset});
}
function drawPrice(rows, reset=false){
  makeChart('priceChart', rows, [
    {label:'小币', color:'#16a34a', value:r=>r.asset_norm},
    {label:'保护腿', color:'#0f172a', value:r=>r.leader_norm}
  ], {marks:[100], footer:'归一化价格：小币 vs 保护腿', reset});
}
async function loadPaper(){
  try{
    const r=await fetch('paper?limit=120');
    const data=await r.json();
    if(!data.ok){document.getElementById('pStatus').textContent=data.error||'读取失败';return}
    renderPaper(data);
  }catch(e){
    document.getElementById('pStatus').textContent='读取失败';
  }
}
function paperActionText(action){
  if(action==='short_asset_long_hedge') return '空小币/多保护';
  if(action==='long_asset_short_hedge') return '多小币/空保护';
  return action||'-';
}
function renderPaper(data){
  const cfg=data.config||{}, stats=data.stats||{};
  const open=data.open||[], closed=data.closed||[], equity=data.equity||[];
  fillPaperConfig(cfg, false);
  const synced=cfg.paper_sync_live!==false;
  document.getElementById('pStatus').textContent=(synced?'统一策略模拟：始终运行':(cfg.paper_enabled===false?'独立模拟关闭':'独立模拟运行'))+`；持仓 ${open.length} 个`;
  document.getElementById('pPnl').textContent=`盘口成交模型已实现 ${fmt(stats.realized_usdc,2)} USDC`;
  document.getElementById('pWin').textContent=`${fmt((stats.win_rate||0)*100,1)}% / ${stats.trades||0} 次`;
  const tp=Number(cfg.paper_take_profit_bps||0);
  document.getElementById('pConfig').textContent=(synced?'与真实盘共用入场/退出信号；真实关闭时仍模拟；':`每笔 ${fmt(cfg.paper_notional_usdc,0)}U；`)+`固定止盈 ${tp>0?fmt(tp,0)+'bps':'关闭'}；止损 ${fmt(cfg.paper_stop_bps,0)}bps；回归Z ${fmt(cfg.paper_exit_z,2)}；最长 ${cfg.paper_max_hold_minutes||'-'}分`;
  drawPaperEquity(equity, true);
  const tb=document.querySelector('#paperTbl tbody'); tb.innerHTML='';
  const rows=[...open.map(x=>({...x,_status:'持仓'})), ...closed.slice(0,20).map(x=>({...x,_status:'已平'}))];
  rows.forEach(row=>{
    const tr=document.createElement('tr');
    const zNow=row.exit_z ?? row.current_z ?? '-';
    tr.innerHTML=`<td>${row._status}</td><td>${row.asset} vs ${row.leader}</td><td class="${Number(row.pnl_usdc)>=0?'scoreGood':'scoreBad'}">${fmt(row.pnl_bps,1)} bps / ${fmt(row.pnl_usdc,4)}U</td><td>${paperActionText(row.action)}</td><td>${fmt(row.entry_z,2)}</td><td>${fmt(zNow,2)}</td><td style="text-align:left">${row.close_reason||row.plan||''}</td>`;
    tr.onclick=()=>showPaperTradeDetail(row);
    tb.appendChild(tr);
  });
}
function showPaperTradeDetail(row){
  document.getElementById('detailTitle').textContent=`模拟交易：${row.asset} vs ${row.leader}`;
  const entryTime=row.entry_ts?new Date(row.entry_ts*1000).toLocaleString():'-';
  const exitTime=row.exit_ts?new Date(row.exit_ts*1000).toLocaleString():'未平仓';
  const assetNotional=Number(row.asset_notional_usdc||0),hedgeNotional=Number(row.hedge_notional_usdc||0);
  document.getElementById('detailBody').innerHTML=`
    <div class="detailGrid">
      <div class="detailBox"><div class="muted">状态</div><div>${row._status||row.status}</div></div>
      <div class="detailBox"><div class="muted">方向</div><div>${paperActionText(row.action)}</div></div>
      <div class="detailBox"><div class="muted">双腿名义金额</div><div>${fmt(assetNotional,2)}U + ${fmt(hedgeNotional,2)}U</div></div>
      <div class="detailBox"><div class="muted">入场时间</div><div>${entryTime}</div></div>
      <div class="detailBox"><div class="muted">出场时间</div><div>${exitTime}</div></div>
      <div class="detailBox"><div class="muted">Beta</div><div>${fmt(row.beta,2)}</div></div>
      <div class="detailBox"><div class="muted">入场Z</div><div>${fmt(row.entry_z,2)}</div></div>
      <div class="detailBox"><div class="muted">当前/出场Z</div><div>${fmt(row.exit_z ?? row.current_z,2)}</div></div>
      <div class="detailBox"><div class="muted">模拟盈亏</div><div class="${Number(row.pnl_usdc)>=0?'scoreGood':'scoreBad'}">${fmt(row.pnl_bps,1)} bps / ${fmt(row.pnl_usdc,4)} USDC</div></div>
    </div>
    <h3>原因/计划</h3>
    <div class="detailText">${row.close_reason||row.plan||'持仓观察中'}</div>
    <h3>注意</h3>
    <div class="detailText">这条记录按开仓和当前/平仓时的买一卖一计算两腿收益，并扣除配置成本；真实 IOC 仍会因发送耗时、实际成交档位和官方费用略有不同。</div>
  `;
  detailDlg.showModal();
}
function drawPaperEquity(rows, reset=false){
  makeChart('paperChart', rows||[], [
    {label:'总收益U', color:'#16a34a', value:r=>r.total_usdc, digits:2},
    {label:'已实现U', color:'#2563eb', value:r=>r.realized_usdc, digits:2},
    {label:'未实现U', color:'#f97316', value:r=>r.unrealized_usdc, digits:2}
  ], {marks:[0], footer:`模拟利润曲线：${(rows||[]).length} 点`, reset});
}
const paperConfigKeys=['paper_exit_z','paper_take_profit_bps','paper_stop_bps','paper_max_hold_minutes','paper_fee_bps','paper_z_value_bps','paper_min_corr'];
function cfgEl(key){return document.getElementById('cfg_'+key)}
function fillPaperConfig(cfg, force=true){
  paperConfigKeys.forEach(key=>{
    const el=cfgEl(key); if(!el || cfg[key]===undefined) return;
    if(!force && document.activeElement===el) return;
    el.value=cfg[key];
  });
}
async function loadPaperConfig(){
  try{
    const r=await fetch('paper_config');
    const data=await r.json();
    if(!data.ok){document.getElementById('pSaveStatus').textContent=data.error||'读取失败';return}
    fillPaperConfig(data.config||{}, true);
    document.getElementById('pSaveStatus').textContent=data.admin_enabled?'参数已读取；保存需要管理口令':'服务器未配置管理口令，不能网页保存';
  }catch(e){
    document.getElementById('pSaveStatus').textContent='读取失败：'+e;
  }
}
function readPaperConfigForm(){
  const cfg={};
  paperConfigKeys.forEach(key=>{
    const el=cfgEl(key); if(!el) return;
    if(key==='paper_max_hold_minutes') cfg[key]=parseInt(el.value,10);
    else cfg[key]=parseFloat(el.value);
  });
  return cfg;
}
function adminTokenValue(){
  return (document.getElementById('globalAdminToken')?.value||localStorage.getItem('hlm_admin_token')||'').trim();
}
function saveAdminTokenLocal(){
  const token=(document.getElementById('globalAdminToken')?.value||'').trim();
  const status=document.getElementById('globalTokenStatus');
  if(!token){status.textContent='请输入管理口令';return}
  localStorage.setItem('hlm_admin_token', token);
  status.textContent='已保存到本机浏览器';
}
async function openGlobalDialog(){
  globalDlg.showModal();
  const saved=localStorage.getItem('hlm_admin_token')||'';
  if(saved) document.getElementById('globalAdminToken').value=saved;
  document.getElementById('globalApiStatus').textContent='读取中...';
  try{
    const r=await fetch('live?fresh=1');const data=await r.json();
    const cfg=data.config||{};
    document.getElementById('globalApiStatus').textContent=cfg.api_key_configured?(cfg.sdk_ready?'API 私钥已加密配置，可交易':'API 私钥已配置，但缺 SDK'):'未配置 API 钱包私钥';
    document.getElementById('globalAccountAddress').textContent=cfg.live_account_address||'-';
  }catch(e){
    document.getElementById('globalApiStatus').textContent='读取失败：'+e.message;
  }
}
const liveConfigKeys=['live_enabled','live_account_poll_seconds','live_account_cache_max_age_ms','live_leverage_cache_seconds','live_account_address','live_notional_usdc','live_auto_min_notional','live_max_open','live_reentry_cooldown_minutes','live_max_slippage_bps','live_leverage','live_require_leverage_ok','live_min_entry_z','live_min_corr','live_min_expected_edge_bps','live_use_l2book','live_l2_max_age_ms','live_strategy_entry_grace_ms','live_l2_max_spread_bps','live_use_realtime_z','live_realtime_strategy_interval_ms','live_strategy_enabled'];
function liveCfgEl(key){return document.getElementById('cfg_'+key)}
const liveRiskPresets={
  conservative:{live_min_entry_z:3.0,live_min_corr:0.75,live_min_expected_edge_bps:25,live_l2_max_spread_bps:2.5},
  balanced:{live_min_entry_z:2.5,live_min_corr:0.70,live_min_expected_edge_bps:18,live_l2_max_spread_bps:3.5},
  aggressive:{live_min_entry_z:2.0,live_min_corr:0.65,live_min_expected_edge_bps:10,live_l2_max_spread_bps:5.0}
};
function sameLivePreset(cfg,preset){
  return Object.entries(preset).every(([k,v])=>Math.abs(Number(cfg[k])-Number(v))<1e-9);
}
function updateLiveRiskPresetSelect(cfg){
  const el=document.getElementById('liveRiskPreset'); if(!el)return;
  let matched='custom';
  for(const [name,preset] of Object.entries(liveRiskPresets)){
    if(sameLivePreset(cfg,preset)){matched=name;break}
  }
  el.value=matched;
}
function markLiveRiskCustom(){
  const el=document.getElementById('liveRiskPreset'); if(el)el.value='custom';
}
function applyLiveRiskPreset(name){
  if(name==='custom')return;
  const preset=liveRiskPresets[name]; if(!preset)return;
  Object.entries(preset).forEach(([key,value])=>{
    const el=liveCfgEl(key); if(el)el.value=value;
  });
  const status=document.getElementById('liveSaveStatus');
  if(status)status.textContent=`已套用${name==='conservative'?'保守':name==='balanced'?'中等':'激进'}档；点击“保存真实交易参数”后生效`;
}
function fillLiveConfig(cfg){
  liveConfigKeys.forEach(key=>{
    const el=liveCfgEl(key); if(!el || cfg[key]===undefined) return;
    el.value=['live_enabled','live_strategy_enabled','live_auto_min_notional','live_require_leverage_ok','live_use_l2book','live_use_realtime_z'].includes(key) ? (cfg[key]===true?'true':'false') : cfg[key];
  });
  updateLiveRiskPresetSelect(cfg);
}
function readLiveConfig(){
  return {
    live_enabled:liveCfgEl('live_enabled').value==='true',
    live_account_poll_seconds:parseFloat(liveCfgEl('live_account_poll_seconds').value),
    live_account_cache_max_age_ms:parseFloat(liveCfgEl('live_account_cache_max_age_ms').value),
    live_leverage_cache_seconds:parseFloat(liveCfgEl('live_leverage_cache_seconds').value),
    live_account_address:liveCfgEl('live_account_address').value.trim(),
    live_notional_usdc:parseFloat(liveCfgEl('live_notional_usdc').value),
    live_auto_min_notional:liveCfgEl('live_auto_min_notional').value==='true',
    live_max_open:parseInt(liveCfgEl('live_max_open').value,10),
    live_reentry_cooldown_minutes:parseInt(liveCfgEl('live_reentry_cooldown_minutes').value,10),
    live_max_slippage_bps:parseFloat(liveCfgEl('live_max_slippage_bps').value),
    live_leverage:parseInt(liveCfgEl('live_leverage').value,10),
    live_require_leverage_ok:liveCfgEl('live_require_leverage_ok').value==='true',
    live_min_entry_z:parseFloat(liveCfgEl('live_min_entry_z').value),
    live_min_corr:parseFloat(liveCfgEl('live_min_corr').value),
    live_min_expected_edge_bps:parseFloat(liveCfgEl('live_min_expected_edge_bps').value),
    live_use_l2book:liveCfgEl('live_use_l2book').value==='true',
    live_l2_max_age_ms:parseFloat(liveCfgEl('live_l2_max_age_ms').value),
    live_strategy_entry_grace_ms:parseFloat(liveCfgEl('live_strategy_entry_grace_ms').value),
    live_l2_max_spread_bps:parseFloat(liveCfgEl('live_l2_max_spread_bps').value),
    live_use_realtime_z:liveCfgEl('live_use_realtime_z').value==='true',
    live_realtime_strategy_interval_ms:parseFloat(liveCfgEl('live_realtime_strategy_interval_ms').value),
    live_strategy_enabled:liveCfgEl('live_strategy_enabled').value==='true',
  };
}
function livePerformanceRows(snapshot){
  const closed=(snapshot.closed||[])
    .filter(r=>r.status==='closed' && !isNaN(Number(r.net_pnl_usdc??r.pnl_usdc)) && !isNaN(Number(r.net_pnl_bps??r.pnl_bps)))
    .sort((a,b)=>Number(a.exit_ts||a.entry_ts||0)-Number(b.exit_ts||b.entry_ts||0));
  let equity=0, peak=0, wins=0;
  return closed.map((r,i)=>{
    const pnl=Number(r.net_pnl_usdc??r.pnl_usdc??0), bps=Number(r.net_pnl_bps??r.pnl_bps??0);
    equity+=pnl; peak=Math.max(peak,equity); if(pnl>0) wins+=1;
    const window=closed.slice(Math.max(0,i-9),i+1);
    const rollingWins=window.filter(x=>Number(x.net_pnl_usdc??x.pnl_usdc??0)>0).length;
    const avgBps=closed.slice(0,i+1).reduce((s,x)=>s+Number(x.net_pnl_bps??x.pnl_bps??0),0)/(i+1);
    return {
      ts:Number(r.exit_ts||r.entry_ts||0),
      trade_no:i+1,
      asset:r.asset,
      leader:r.leader,
      equity,
      drawdown:equity-peak,
      pnl_usdc:pnl,
      pnl_bps:bps,
      avg_bps:avgBps,
      rolling_win:rollingWins/window.length*100,
      total_win:wins/(i+1)*100
    };
  });
}
function drawLivePerformance(snapshot){
  const rows=livePerformanceRows(snapshot||{});
  const summary=document.getElementById('liveChartSummary');
  if(!rows.length){
    if(summary) summary.textContent='还没有可画图的真实已平仓记录。';
    ['liveEquityChart','liveWinChart','liveBpsChart'].forEach(id=>{
      const c=document.getElementById(id); if(c){const {ctx,w,h}=canvasMetrics(c);ctx.clearRect(0,0,w,h);ctx.fillStyle='#64748b';ctx.fillText('暂无真实已平仓数据',20,40);}
    });
    return;
  }
  const last=rows[rows.length-1];
  const best=rows.reduce((m,r)=>Math.max(m,r.equity),-Infinity);
  const worstDd=rows.reduce((m,r)=>Math.min(m,r.drawdown),0);
  const bestTrade=rows.reduce((m,r)=>Math.max(m,r.pnl_bps),-Infinity);
  const worstTrade=rows.reduce((m,r)=>Math.min(m,r.pnl_bps),Infinity);
  if(summary) summary.textContent=`真实曲线：${rows.length} 笔；累计 ${fmt(last.equity,4)}U；历史峰值 ${fmt(best,4)}U；最大回撤 ${fmt(worstDd,4)}U；单笔最好 ${fmt(bestTrade,1)}bps，最差 ${fmt(worstTrade,1)}bps。`;
  makeChart('liveEquityChart', rows, [
    {label:'累计U', color:'#2563eb', value:r=>r.equity, digits:4},
    {label:'回撤U', color:'#ef4444', value:r=>r.drawdown, digits:4}
  ], {marks:[0], footer:`真实累计收益 / 回撤：${rows.length} 笔`, reset:true});
  makeChart('liveWinChart', rows, [
    {label:'滚动10笔胜率%', color:'#16a34a', value:r=>r.rolling_win, digits:1},
    {label:'总胜率%', color:'#0f172a', value:r=>r.total_win, digits:1}
  ], {marks:[50], min:0, max:100, footer:'胜率走势：绿色=最近10笔，黑色=累计', reset:true});
  makeChart('liveBpsChart', rows, [
    {label:'单笔bps', color:'#7c3aed', value:r=>r.pnl_bps, digits:1},
    {label:'平均bps', color:'#f97316', value:r=>r.avg_bps, digits:1}
  ], {marks:[0], footer:'每笔真实收益 bps / 累计平均 bps', reset:true});
}
function renderL2Book(l2){
  const status=l2?.status||{}, books=l2?.books||[];
  const el=document.getElementById('liveL2Status');
  if(el){
    const age=status.last_message_age_ms===null||status.last_message_age_ms===undefined?'-':fmt(status.last_message_age_ms,0)+'ms';
    const scanText=status.scan_rows!==undefined?`；扫描 ${status.scan_rows}`:'';
    const midsAge=status.all_mids_age_ms===null||status.all_mids_age_ms===undefined?'-':fmt(status.all_mids_age_ms,0)+'ms';
    el.textContent=`${status.connected?'已连接':'未连接'}；订阅 ${status.subscribed||0}/${status.desired||0}${scanText}；盘口 ${status.books||0}；allMids ${status.all_mids||0} / ${midsAge}；最新 ${age}`;
    el.className=status.connected?'scoreGood':'scoreBad';
  }
  const tb=document.querySelector('#liveL2Tbl tbody'); if(!tb)return; tb.innerHTML='';
  const sorted=[...books].sort((a,b)=>Number(a.age_ms||999999)-Number(b.age_ms||999999)).slice(0,40);
  sorted.forEach(b=>{
    const tr=document.createElement('tr');
    const bidDepth=Number(b.bid||0)*Number(b.bid_size||0), askDepth=Number(b.ask||0)*Number(b.ask_size||0);
    const age=Number(b.age_ms||0), spread=Number(b.spread_bps||0);
    tr.innerHTML=`<td>${b.coin}</td><td>${fmt(b.bid,6)}</td><td>${fmt(b.ask,6)}</td><td class="${spread<=2.5?'scoreGood':'scoreBad'}">${fmt(spread,2)} bps</td><td>${fmt(bidDepth,2)}U / ${fmt(b.bid_size,4)}</td><td>${fmt(askDepth,2)}U / ${fmt(b.ask_size,4)}</td><td class="${age<=3000?'scoreGood':'scoreBad'}">${fmt(age,0)} ms</td>`;
    tb.appendChild(tr);
  });
  if(!sorted.length)tb.innerHTML='<tr><td colspan="7" class="muted">等待 l2Book WebSocket 盘口数据</td></tr>';
}
function renderLiveDiagnostics(diag){
  diag=diag||{};
  const counts=diag.counts||{}, total=Number(diag.total||0), pass=Number(diag.pass_count||0);
  const globalReasons=diag.global_reasons||[];
  const status=document.getElementById('liveOpportunityStatus');
  if(status){
    status.textContent=`${pass} 个可进入下单 / 扫描 ${total} 个`;
    status.className=pass>0&&!globalReasons.length?'scoreGood':'scoreMid';
  }
  const labels=[];
  if(Number(counts.z||0))labels.push(`Z不足 ${counts.z}`);
  if(Number(counts.corr||0))labels.push(`相关不足 ${counts.corr}`);
  if(Number(counts.spread||0))labels.push(`点差过大 ${counts.spread}`);
  if(Number(counts.edge||0))labels.push(`预期边际不足 ${counts.edge}`);
  if(Number(counts.l2||0))labels.push(`实时盘口未通过 ${counts.l2}`);
  const summary=document.getElementById('liveDiagnosticSummary');
  if(summary){
    const gate=globalReasons.length?`全局暂停原因：${globalReasons.join('；')}。`:'全局开关、资金与仓位状态允许检查新机会。';
    summary.textContent=`本轮扫描 ${total} 组，最终可进入下单阶段 ${pass} 组。${labels.length?' 首要过滤统计：'+labels.join('；')+'。':''}\n${gate}\n这里按“第一个拦截原因”计数；表格列出最接近开仓的组合。没有通过时不下单是正常风控，不是程序卡死。`;
  }
  const tb=document.querySelector('#liveDiagnosticTbl tbody');if(!tb)return;tb.innerHTML='';
  (diag.opportunities||[]).forEach(row=>{
    const tr=document.createElement('tr');
    const ok=row.status==='pass';
    tr.innerHTML=`<td class="${ok?'passChip':'blockChip'}">${ok?'可下单':'被过滤'}</td><td>${esc(row.asset)} vs ${esc(row.leader)}</td><td>${fmt(row.zscore,2)}</td><td>${fmt(row.kline_zscore,2)}</td><td>${fmt(row.corr,3)}</td><td>${fmt(row.spread_bps,2)} bps</td><td>${fmt(row.expected_edge_bps,1)} bps</td><td>${liveActionText(row.action)}</td><td class="reasonCell">${esc(row.reason||'-')}</td>`;
    tb.appendChild(tr);
  });
  if(!(diag.opportunities||[]).length)tb.innerHTML='<tr><td colspan="9" class="muted">当前还没有扫描数据</td></tr>';
}
async function refreshLiveL2Only(){
  if(!liveDlg.open || liveL2RefreshBusy) return;
  const auto=document.getElementById('liveAutoRefresh');
  if(auto && !auto.checked) return;
  liveL2RefreshBusy=true;
  const statusEl=document.getElementById('liveAutoRefreshStatus');
  try{
    const coins=liveL2Coins.slice(0,80).join(',');
    const r=await fetch('l2book'+(coins?`?coins=${encodeURIComponent(coins)}`:''));
    const data=await r.json();
    if(data.ok){
      renderL2Book(data.l2book||{});
      if(statusEl)statusEl.textContent='盘口自动刷新中：'+new Date().toLocaleTimeString();
    }else if(statusEl){
      statusEl.textContent='盘口刷新失败：'+(data.error||'未知错误');
    }
  }catch(e){
    if(statusEl)statusEl.textContent='盘口刷新失败：'+e.message;
  }finally{
    liveL2RefreshBusy=false;
  }
}
function renderLive(data){
  const cfg=data.config||{}, account=data.account||{}, positions=account.positions||[];
  const liveSnapshot=data.live_trades||{}, liveStats=liveSnapshot.stats||{};
  liveL2Coins=(data.l2book?.books||[]).map(b=>b.coin).filter(Boolean);
  const isUnified=account.account_mode==='unifiedAccount';
  fillLiveConfig(cfg);
  document.getElementById('liveStatus').textContent=cfg.live_enabled===true?(cfg.live_strategy_enabled===true?'已开启（真实策略运行中）':'已开启（策略开关关闭）'):'关闭（不会下真实单）';
  const engine=data.realtime_strategy||{},engineEl=document.getElementById('liveRealtimeEngine');
  if(engineEl){
    const evalAge=engine.last_eval_ts?Math.max(0,Date.now()-Number(engine.last_eval_ts)*1000):null;
    engineEl.textContent=`${engine.running?'运行中':'未运行'}；判断间隔 ${fmt(cfg.live_realtime_strategy_interval_ms,0)}ms；最近判断 ${evalAge===null?'-':fmt(evalAge,0)+'ms前'}；事件 ${engine.events||0}`+(engine.last_error?`；错误 ${engine.last_error}`:'');
    engineEl.className=engine.running&&!engine.last_error?'scoreGood':'scoreBad';
  }
  document.getElementById('liveBlocker').textContent=cfg.blocker||'';
  document.getElementById('liveBalance').textContent=account.account_value===undefined?'暂无账户快照':(isUnified?`统一账户；可用 ${fmt(account.spot_available_usdc??account.spot_usdc,2)} U`:`合约 ${fmt(account.account_value,2)} U；现货 ${fmt(account.spot_available_usdc??account.spot_usdc,2)} U`);
  document.getElementById('livePositionCount').textContent=account.ts?`${positions.length} 个；快照 ${new Date(account.ts*1000).toLocaleString()}`:'暂无；填写主钱包公开地址后等下一轮采集';
  const trades=Number(liveStats.trades||0), wins=Number(liveStats.wins||0), winRate=Number(liveStats.win_rate||0)*100;
  const realized=Number(liveStats.realized_usdc||0), avgBps=Number(liveStats.avg_bps||0), worstBps=Number(liveStats.worst_bps||0);
  const gross=Number(liveStats.gross_realized_usdc||0), fees=Number(liveStats.fee_usdc||0), funding=Number(liveStats.funding_usdc||0);
  const assetOnly=Number(liveStats.asset_only_net_usdc||0), hedgeLeg=Number(liveStats.hedge_leg_net_usdc||0);
  const totalEntryNotional=Number(liveStats.total_entry_notional_usdc||0);
  const avgFee=Number(liveStats.avg_fee_usdc||0);
  const feeRoundTripBps=totalEntryNotional>0?fees/totalEntryNotional*10000:0;
  const feeVsGross=Math.abs(gross)>1e-9?fees/Math.abs(gross)*100:null;
  const costDrag=fees-funding;
  const costShareOfLoss=realized<0?costDrag/Math.abs(realized)*100:null;
  const fundingVsFee=fees>1e-9?Math.abs(funding)/fees*100:null;
  document.getElementById('liveClosedWin').textContent=`${trades} 笔 / ${fmt(winRate,1)}%（赢 ${wins}）`;
  const realizedEl=document.getElementById('liveRealized');
  realizedEl.textContent=`${fmt(realized,4)} U`;
  realizedEl.className=realized>=0?'scoreGood':'scoreBad';
  const avgWorstEl=document.getElementById('liveAvgWorst');
  avgWorstEl.textContent=`${fmt(avgBps,1)} bps / ${fmt(worstBps,1)} bps`;
  avgWorstEl.className=avgBps>=0?'scoreGood':'scoreBad';
  document.getElementById('liveCosts').textContent=`手续费 ${fmt(fees,4)}U；资金费 ${fmt(funding,4)}U`;
  const feeBurdenEl=document.getElementById('liveFeeBurden');
  feeBurdenEl.textContent=`往返约 ${fmt(feeRoundTripBps,2)}bps；均每笔 ${fmt(avgFee,4)}U`+(costShareOfLoss===null?'':`；占净亏 ${fmt(costShareOfLoss,1)}%`);
  feeBurdenEl.className=feeRoundTripBps<=5?'scoreGood':feeRoundTripBps<=10?'scoreMid':'scoreBad';
  const fundingImpactEl=document.getElementById('liveFundingImpact');
  fundingImpactEl.textContent=`资金费/手续费 ${fundingVsFee===null?'-':fmt(fundingVsFee,2)+'%'}`+(feeVsGross===null?'':`；手续费/|毛收益| ${fmt(feeVsGross,1)}%`);
  fundingImpactEl.className=(fundingVsFee??0)<=10?'scoreGood':'scoreMid';
  const hedgeEl=document.getElementById('liveHedgeCompare');
  hedgeEl.textContent=`双腿 ${fmt(realized,4)}U；仅小币影子 ${fmt(assetOnly,4)}U；保护腿 ${fmt(hedgeLeg,4)}U`;
  hedgeEl.className=hedgeLeg>=0?'scoreGood':'scoreBad';
  const note=document.getElementById('liveStatsNote');
  if(note) note.textContent=`官方成本后统计：${trades} 笔，净胜率 ${fmt(winRate,1)}%，毛收益 ${fmt(gross,4)}U - 手续费 ${fmt(fees,4)}U + 资金费 ${fmt(funding,4)}U = 净收益 ${fmt(realized,4)}U；手续费往返约 ${fmt(feeRoundTripBps,2)}bps，平均 ${fmt(avgBps,1)}bps，最差 ${fmt(worstBps,1)}bps。单腿影子只用于比较，不会自动下单。`;
  if(account.ts && isUnified){
    document.getElementById('liveBlocker').textContent=`当前是 Hyperliquid 统一账户：可用 ${fmt(account.spot_available_usdc??account.spot_usdc,2)} USDC 会直接作为合约保证金来源，不需要也不能手动转入 Perps。`;
  }else if(account.ts && Number(account.account_value||0)<=0 && Number(account.spot_available_usdc ?? account.spot_usdc ?? 0)>0){
    document.getElementById('liveBlocker').textContent=`合约可用资金为 0；现货有 ${fmt(account.spot_available_usdc??account.spot_usdc,2)} USDC。若以后要交易合约，需要你在 Hyperliquid 官方页面自行将现货 USDC 转入 Perps。`;
  }
  const tb=document.querySelector('#livePosTbl tbody');tb.innerHTML='';
  positions.forEach(p=>{
    const tr=document.createElement('tr');
    tr.innerHTML=`<td>${p.coin||'-'}</td><td class="${Number(p.unrealized_pnl)>=0?'scoreGood':'scoreBad'}">${fmt(p.unrealized_pnl,2)} U</td><td class="${Number(p.size)>=0?'scoreGood':'scoreBad'}">${Number(p.size)>=0?'多 ':'空 '}${fmt(Math.abs(p.size),5)}</td><td>${fmt(p.entry_px,5)}</td><td>${fmt(p.position_value,2)} U</td><td>${p.leverage??'-'}x</td><td>${p.liquidation_px??'-'}</td>`;
    tb.appendChild(tr);
  });
  if(!positions.length)tb.innerHTML='<tr><td colspan="7" class="muted">当前没有读取到真实持仓</td></tr>';
  renderL2Book(data.l2book||{});
  renderLiveDiagnostics(data.diagnostics||{});
  drawLivePerformance(liveSnapshot);
  renderLiveTrades(liveSnapshot);
}
function liveActionText(action){
  if(action==='short_asset_long_hedge')return '空小币 / 多保护';
  if(action==='long_asset_short_hedge')return '多小币 / 空保护';
  return '-';
}
function renderLiveTrades(snapshot){
  const tb=document.querySelector('#liveTradeTbl tbody');if(!tb)return;tb.innerHTML='';
  const rows=[...(snapshot.open||[]).map(x=>({...x,_status:'持仓'})),...(snapshot.closed||[]).map(x=>({...x,_status:x.status==='closed'?'已平':x.status}))];
  rows.forEach(row=>{
    const tr=document.createElement('tr');
    const zNow=row.status==='open'?(row.current_z??row.entry_z):(row.exit_z??row.entry_z);
    const pnl=row.status==='open'?(row.signal_pnl_usdc??0):(row.net_pnl_usdc??row.pnl_usdc);
    const pnlBps=row.status==='open'?(row.signal_pnl_bps??0):(row.net_pnl_bps??row.pnl_bps);
    const corr=row.status==='open'?(row.current_corr??row.entry_corr):row.exit_corr??row.entry_corr;
    const beta=row.status==='open'?(row.current_beta??row.beta):row.beta;
    const spread=row.status==='open'?(row.current_spread_bps??row.entry_spread_bps):row.exit_spread_bps??row.entry_spread_bps;
    const fundingBps=Number(row.current_funding_hourly||0)*10000;
    const entryTime=fmtBeijingDateTime(row.entry_ts);
    const exitTime=row.status==='open'?'持仓中':fmtBeijingDateTime(row.exit_ts);
    const holdTime=fmtDuration(row.entry_ts,row.status==='open'?null:row.exit_ts);
    const feeText=row.status==='open'?'-':fmt(row.fee_usdc,4)+'U';
    const fundingText=row.status==='open'?'-':fmt(row.funding_usdc,4)+'U';
    tr.innerHTML=`<td>${row._status}</td><td>${row.asset} vs ${row.leader}</td><td class="${Number(pnl)>=0?'scoreGood':'scoreBad'}">${row.status==='open'?'盘口估算 ':'官方净收益 '}${fmt(pnlBps,1)} bps / ${fmt(pnl,4)}U</td><td>${feeText}</td><td class="${Number(row.funding_usdc||0)>=0?'scoreGood':'scoreBad'}">${fundingText}</td><td>${liveActionText(row.action)}</td><td>${entryTime}</td><td>${exitTime}</td><td>${holdTime}</td><td>${fmt(corr,3)}</td><td>${fmt(beta,2)}</td><td>${fmt(row.entry_z,2)}</td><td>${fmt(zNow,2)}</td><td>${fmt(row.current_asset_15m_bps,1)} bps</td><td>${fmt(row.current_hedge_15m_bps,1)} bps</td><td>${fmt(spread,2)}</td><td>${fmt(fundingBps,3)} bps</td><td>${fmt(row.asset_notional_usdc,1)}U / ${fmt(row.hedge_notional_usdc,1)}U</td><td style="text-align:left">${row.close_reason||row.current_plan||row.note||''}</td>`;
    tr.onclick=()=>showLiveTradeDetail(row);
    tb.appendChild(tr);
  });
  if(!rows.length)tb.innerHTML='<tr><td colspan="19" class="muted">还没有真实策略持仓或平仓记录</td></tr>';
}
function safeJson(value){try{return typeof value==='string'?JSON.parse(value):(value||{});}catch(e){return {parse_error:String(value)}}}
function esc(value){return String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function showLiveTradeDetail(row){
  const entry=safeJson(row.entry_json),exit=safeJson(row.exit_json),fills=entry.fills||{};
  document.getElementById('detailTitle').textContent=`真实策略交易：${row.asset} vs ${row.leader}`;
  const legs=[row.asset,row.leader].map(coin=>{
    const f=fills[coin]||{},raw=f.raw||{};
    const notional=Number(f.size||0)*Number(f.price||0);
    return `<tr><td>${esc(coin)}</td><td>${f.filled?'入场已成交':'入场未成交'}</td><td>${fmt(f.size,8)} ${esc(coin)}</td><td>${fmt(f.price,6)} U</td><td>${fmt(notional,4)} U</td><td style="text-align:left">${esc(raw.error||raw.filled?.oid||'-')}</td></tr>`;
  }).join('');
  const currentFundingBps=Number(row.current_funding_hourly||0)*10000;
  document.getElementById('detailBody').innerHTML=`
    <div class="detailGrid">
      <div class="detailBox"><div class="muted">结果</div><div>${esc(row.status)}</div></div>
      <div class="detailBox"><div class="muted">开仓时间（北京时间）</div><div>${fmtBeijingDateTime(row.entry_ts)}</div></div>
      <div class="detailBox"><div class="muted">平仓时间（北京时间）</div><div>${row.status==='open'?'持仓中':fmtBeijingDateTime(row.exit_ts)}</div></div>
      <div class="detailBox"><div class="muted">持仓时长</div><div>${fmtDuration(row.entry_ts,row.status==='open'?null:row.exit_ts)}</div></div>
      <div class="detailBox"><div class="muted">小币/保护腿名义金额</div><div>${fmt(row.asset_notional_usdc,2)}U / ${fmt(row.hedge_notional_usdc,2)}U</div></div>
      <div class="detailBox"><div class="muted">价格盈亏（未扣费）</div><div>${row.status==='open'?'持仓中':fmt(row.pnl_usdc,5)+' U'}</div></div>
      <div class="detailBox"><div class="muted">官方手续费 / 资金费</div><div>${row.status==='open'?'-':fmt(row.fee_usdc,5)+'U / '+fmt(row.funding_usdc,5)+'U'}</div></div>
      <div class="detailBox"><div class="muted">官方净盈亏</div><div class="${Number(row.net_pnl_usdc)>=0?'scoreGood':'scoreBad'}">${row.status==='open'?'-':fmt(row.net_pnl_bps,1)+' bps / '+fmt(row.net_pnl_usdc,5)+'U'}</div></div>
      <div class="detailBox"><div class="muted">小币腿 / 保护腿净贡献</div><div>${row.status==='open'?'-':fmt(row.asset_net_pnl_usdc,5)+'U / '+fmt(row.hedge_net_pnl_usdc,5)+'U'}</div></div>
      <div class="detailBox"><div class="muted">相关 / Beta</div><div>${fmt(row.current_corr??row.entry_corr,3)} / ${fmt(row.current_beta??row.beta,2)}</div></div>
      <div class="detailBox"><div class="muted">Z：入场 → 当前/出场</div><div>${fmt(row.entry_z,2)} → ${fmt(row.current_z??row.exit_z??row.entry_z,2)}</div></div>
      <div class="detailBox"><div class="muted">小币/保护腿 15m</div><div>${fmt(row.current_asset_15m_bps,1)} bps / ${fmt(row.current_hedge_15m_bps,1)} bps</div></div>
      <div class="detailBox"><div class="muted">点差 / 资金费</div><div>${fmt(row.current_spread_bps??row.entry_spread_bps,2)} bps / ${fmt(currentFundingBps,3)} bps每小时</div></div>
      <div class="detailBox"><div class="muted">当前计划</div><div>${esc(row.current_plan||row.note||'-')}</div></div>
    </div>
    <h3>两条入场腿</h3><table><thead><tr><th>合约</th><th>状态</th><th>成交数量（币）</th><th>均价</th><th>约USDC</th><th>交易所回执</th></tr></thead><tbody>${legs}</tbody></table>
    <h3>说明</h3><div class="detailText">${esc(row.note||'')}</div>
    <h3>原始订单回执</h3><div class="detailText"><code>${esc(JSON.stringify({entry,exit},null,2))}</code></div>`;
  detailDlg.showModal();
}
async function loadLive(silent=false){
  const status=document.getElementById('liveSaveStatus'); if(status && !silent)status.textContent='读取中...';
  try{
    const r=await fetch('live?fresh=1');const data=await r.json();
    if(!data.ok)throw new Error(data.error||'读取失败');
    renderLive(data);if(status && !silent)status.textContent=data.account_error?`账户读取失败：${data.account_error}`:'已读取真实账户快照';
  }catch(e){if(status)status.textContent='读取失败：'+e.message;}
}
function openLiveDialog(){liveDlg.showModal();loadLive();loadPaperConfig();}
async function saveLiveConfig(){
  const status=document.getElementById('liveSaveStatus');
  const token=adminTokenValue();
  if(!token){status.textContent='请输入管理口令';return;}
  const config=readLiveConfig();
  const paperConfig=readPaperConfigForm();
  if(config.live_enabled && !confirm('确认开启真实下单授权？系统将只在 API 私钥已加密配置、额度限制有效时允许真实交易。')) return;
  status.textContent='正在保存...';
  try{
    const r=await fetch('live_config',{method:'POST',headers:{'Content-Type':'application/json','X-Admin-Token':token},body:JSON.stringify({config})});
    const data=await r.json();
    if(!data.ok){status.textContent=data.error||'保存失败';return;}
    const paperRes=await fetch('paper_config',{method:'POST',headers:{'Content-Type':'application/json','X-Admin-Token':token},body:JSON.stringify({config:paperConfig})});
    const paperData=await paperRes.json();
    if(!paperData.ok){status.textContent='真实执行参数已保存，但统一退出参数保存失败：'+(paperData.error||'未知错误');return;}
    fillLiveConfig(data.config||{});
    fillPaperConfig(paperData.config||{},true);
    const lev=data.leverage_result;
    status.textContent=lev&&Object.keys(lev).length?`统一策略和真实执行已保存；杠杆结果 ${JSON.stringify(lev)}`:'统一策略和真实执行已保存；下一轮信号立即生效';
    loadLive();loadPaper();
  }catch(e){status.textContent='保存失败：'+e.message;}
}
async function runEmergencyClose(){
  const status=document.getElementById('emergencyStatus');
  const token=adminTokenValue();
  const confirmText=(document.getElementById('emergencyConfirm')?.value||'').trim().toUpperCase();
  if(!token){status.textContent='请先在“全局设置”填写管理口令';return}
  if(confirmText!=='CLOSE'){status.textContent='请输入 CLOSE 确认紧急全部平仓';return}
  if(!confirm('确认对官方当前所有真实仓位发送 reduce-only 紧急平仓单？这会产生真实成交、手续费和滑点。')) return;
  status.textContent='正在发送紧急 reduce-only 平仓单...';
  try{
    const r=await fetch('live_emergency_close',{method:'POST',headers:{'Content-Type':'application/json','X-Admin-Token':token},body:JSON.stringify({confirm:'CLOSE',reason:'ui emergency close'})});
    const data=await r.json();
    if(!data.ok){status.textContent='紧急平仓失败：'+(data.error||'未知错误');loadLive();return}
    const result=data.result||{};
    status.textContent=`紧急平仓已发送：${result.status||'submitted'}；请以官方仓位为准`;
    document.getElementById('emergencyConfirm').value='';
    loadLive();
  }catch(e){
    status.textContent='请求失败：'+e.message;
    loadLive();
  }
}
const leadlagConfigKeys=['leadlag_enabled','leadlag_notional_usdc','leadlag_max_open','leadlag_leaders','leadlag_leader_3s_bps','leadlag_leader_15s_bps','leadlag_min_lag_bps','leadlag_min_corr','leadlag_max_spread_bps','leadlag_max_data_age_ms','leadlag_min_imbalance','leadlag_min_depth_multiple','leadlag_fee_bps','leadlag_min_edge_bps','leadlag_take_profit_bps','leadlag_stop_bps','leadlag_trail_start_bps','leadlag_trail_gap_bps','leadlag_max_hold_minutes','leadlag_cooldown_minutes'];
const leadlagPresets={
  sampling:{leadlag_leader_3s_bps:2,leadlag_leader_15s_bps:4,leadlag_min_lag_bps:6,leadlag_min_corr:.60,leadlag_max_spread_bps:2.5,leadlag_max_data_age_ms:10000,leadlag_min_imbalance:-1,leadlag_min_depth_multiple:2,leadlag_min_edge_bps:6,leadlag_take_profit_bps:18,leadlag_stop_bps:14,leadlag_trail_start_bps:8,leadlag_trail_gap_bps:6,leadlag_max_hold_minutes:8,leadlag_cooldown_minutes:8},
  standard:{leadlag_leader_3s_bps:4,leadlag_leader_15s_bps:8,leadlag_min_lag_bps:8,leadlag_min_corr:.65,leadlag_max_spread_bps:2.5,leadlag_max_data_age_ms:8000,leadlag_min_imbalance:0,leadlag_min_depth_multiple:2,leadlag_min_edge_bps:8,leadlag_take_profit_bps:20,leadlag_stop_bps:15,leadlag_trail_start_bps:10,leadlag_trail_gap_bps:7,leadlag_max_hold_minutes:8,leadlag_cooldown_minutes:10},
  strict:{leadlag_leader_3s_bps:12,leadlag_leader_15s_bps:25,leadlag_min_lag_bps:15,leadlag_min_corr:.75,leadlag_max_spread_bps:1.5,leadlag_max_data_age_ms:5000,leadlag_min_imbalance:.05,leadlag_min_depth_multiple:5,leadlag_min_edge_bps:18,leadlag_take_profit_bps:30,leadlag_stop_bps:18,leadlag_trail_start_bps:15,leadlag_trail_gap_bps:10,leadlag_max_hold_minutes:10,leadlag_cooldown_minutes:20}
};
function updateLeadlagPreset(cfg){
  const el=document.getElementById('llPreset');if(!el)return;let match='custom';
  for(const [name,preset] of Object.entries(leadlagPresets)){if(Object.entries(preset).every(([k,v])=>Math.abs(Number(cfg[k])-Number(v))<1e-9)){match=name;break;}}
  el.value=match;
}
function applyLeadlagPreset(name){
  const preset=leadlagPresets[name];if(!preset)return;Object.entries(preset).forEach(([k,v])=>{const el=document.getElementById('cfg_'+k);if(el)el.value=v;});
  const status=document.getElementById('llSaveStatus');if(status)status.textContent='已套用档位，点击保存后生效';
}
function fillLeadlagConfig(cfg){
  leadlagConfigKeys.forEach(key=>{const el=document.getElementById('cfg_'+key);if(!el||cfg[key]===undefined)return;el.value=key==='leadlag_enabled'?(cfg[key]===true?'true':'false'):cfg[key];});
  updateLeadlagPreset(cfg);
}
function readLeadlagConfig(){
  const out={};
  leadlagConfigKeys.forEach(key=>{const el=document.getElementById('cfg_'+key);if(!el)return;if(key==='leadlag_enabled')out[key]=el.value==='true';else if(key==='leadlag_leaders')out[key]=el.value.trim();else if(['leadlag_max_open','leadlag_max_hold_minutes','leadlag_cooldown_minutes'].includes(key))out[key]=parseInt(el.value,10);else out[key]=parseFloat(el.value);});
  return out;
}
function leadlagSideText(side){return side==='long'?'做多小币':'做空小币'}
function renderLeadlag(data){
  const cfg=data.config||{},stats=data.stats||{},status=data.status||{},open=data.open||[],closed=data.closed||[];
  fillLeadlagConfig(cfg);
  document.getElementById('llStatus').textContent=cfg.leadlag_enabled===false?'已关闭':'模拟运行中';
  document.getElementById('llOpen').textContent=`${open.length} / 最多 ${cfg.leadlag_max_open??'-'}`;
  const pnl=Number(stats.realized_usdc||0),pnlEl=document.getElementById('llPnl');pnlEl.textContent=`${fmt(pnl,4)} U`;pnlEl.className=pnl>=0?'scoreGood':'scoreBad';
  document.getElementById('llWin').textContent=`${fmt(Number(stats.win_rate||0)*100,1)}% / ${stats.trades||0} 笔`;
  document.getElementById('llAvgWorst').textContent=`${fmt(stats.avg_bps,1)} / ${fmt(stats.worst_bps,1)} bps`;
  document.getElementById('llFees').textContent=`${fmt(stats.estimated_fees_usdc,4)} U（按配置估算）`;
  document.getElementById('llEligible').textContent=`${status.eligible||0} 个；累计开 ${status.opened||0} / 平 ${status.closed||0}`+(status.last_error?`；错误 ${status.last_error}`:'');
  const signalBody=document.querySelector('#llSignalTbl tbody');signalBody.innerHTML='';
  (status.signals||[]).slice(0,30).forEach(s=>{
    const tr=document.createElement('tr');
    tr.innerHTML=`<td class="${s.eligible?'passChip':'blockChip'}">${s.eligible?'可模拟':'过滤'}</td><td>${esc(s.asset)} / ${esc(s.leader)}</td><td>${leadlagSideText(s.side)}</td><td>${fmt(s.expected_edge_bps,1)}bps</td><td>${fmt(s.lag_bps,1)}bps</td><td>${fmt(s.leader_3s_bps,1)} / ${fmt(s.leader_15s_bps,1)}</td><td>${fmt(s.asset_1s_bps,1)} / ${fmt(s.asset_3s_bps,1)}</td><td>${fmt(s.corr,3)}</td><td>${fmt(s.spread_bps,2)}bps</td><td>${fmt(s.imbalance,2)}</td><td class="reasonCell">${esc(s.reason||'-')}</td>`;
    signalBody.appendChild(tr);
  });
  if(!(status.signals||[]).length)signalBody.innerHTML='<tr><td colspan="11" class="muted">等待至少 15 秒 WS 盘口历史</td></tr>';
  const tradeBody=document.querySelector('#llTradeTbl tbody');tradeBody.innerHTML='';
  const tradeRows=[...open.map(x=>({...x,_status:'持仓'})),...closed.slice(0,100).map(x=>({...x,_status:'已平'}))];
  tradeRows.forEach(t=>{
    const tr=document.createElement('tr');const isOpen=t.status==='open';
    tr.innerHTML=`<td>${t._status}</td><td>${esc(t.asset)} / ${esc(t.leader)}</td><td class="${Number(t.pnl_usdc)>=0?'scoreGood':'scoreBad'}">${fmt(t.net_bps,1)}bps / ${fmt(t.pnl_usdc,4)}U</td><td>${leadlagSideText(t.side)}</td><td>${fmtBeijingDateTime(t.entry_ts)}</td><td>${isOpen?'持仓中':fmtBeijingDateTime(t.exit_ts)}</td><td>${fmtDuration(t.entry_ts,isOpen?null:t.exit_ts)}</td><td>${fmt(t.entry_px,7)}</td><td>${fmt(t.lag_bps,1)}bps</td><td>${fmt(t.expected_edge_bps,1)}bps</td><td>${fmt(t.max_net_bps,1)}bps</td><td class="reasonCell">${esc(t.close_reason||'等待补涨/补跌')}</td>`;
    tradeBody.appendChild(tr);
  });
  if(!tradeRows.length)tradeBody.innerHTML='<tr><td colspan="12" class="muted">尚无 V2 模拟交易</td></tr>';
  let equity=0;const eq=[...closed].sort((a,b)=>Number(a.exit_ts||0)-Number(b.exit_ts||0)).map((t,i)=>{equity+=Number(t.pnl_usdc||0);return {ts:Number(t.exit_ts||0),equity,trade:i+1}});
  makeChart('llEquityChart',eq,[{label:'累计U',color:'#2563eb',value:r=>r.equity,digits:4}],{marks:[0],footer:`V2 单腿模拟累计收益：${eq.length} 笔`,reset:true});
}
async function loadLeadlag(){
  const status=document.getElementById('llSaveStatus');if(status)status.textContent='读取中...';
  try{const r=await fetch('leadlag?limit=200');const data=await r.json();if(!data.ok)throw new Error(data.error||'读取失败');renderLeadlag(data);if(status)status.textContent='已刷新：'+new Date().toLocaleTimeString();}
  catch(e){if(status)status.textContent='读取失败：'+e.message;}
}
function strategySignalText(value){
  const signal=Number(value||0);
  return signal>0?'做多':(signal<0?'做空':'空仓');
}
function strategyFamilyText(value){
  return ({
    ema_cross:'EMA趋势',macd:'MACD动量',rsi_reversion:'RSI均值回归',
    bollinger_reversion:'布林均值回归',donchian_breakout:'唐奇安突破',
    supertrend:'Supertrend趋势',momentum:'动量趋势'
  })[value]||value||'-';
}
function showStrategyLabDetail(row){
  const train=row.train||{},test=row.test||{};
  document.getElementById('detailTitle').textContent=`策略回测：${row.coin} / ${row.strategy}`;
  document.getElementById('detailBody').innerHTML=`
    <div class="detailGrid">
      <div class="detailBox"><div class="muted">研究门槛</div><div class="${row.promotable?'scoreGood':'scoreBad'}">${row.promotable?'达到，仍需继续验证':'未达到'}</div></div>
      <div class="detailBox"><div class="muted">当前收盘信号</div><div>${strategySignalText(row.current_signal)}</div></div>
      <div class="detailBox"><div class="muted">币种 / 周期</div><div>${esc(row.coin)} / ${esc(row.interval)}</div></div>
      <div class="detailBox"><div class="muted">实际历史覆盖</div><div>${fmt(row.actual_days,1)} 天 / ${row.samples||0} 根</div></div>
      <div class="detailBox"><div class="muted">参数</div><div>${esc(JSON.stringify(row.params||{}))}</div></div>
      <div class="detailBox"><div class="muted">训练净收益 / 回撤</div><div>${fmt(train.net_bps,1)} / ${fmt(train.max_drawdown_bps,1)} bps</div></div>
      <div class="detailBox"><div class="muted">样本外净收益 / 回撤</div><div>${fmt(test.net_bps,1)} / ${fmt(test.max_drawdown_bps,1)} bps</div></div>
      <div class="detailBox"><div class="muted">训练交易 / 胜率 / 盈亏比</div><div>${train.trades||0} / ${fmt(Number(train.win_rate||0)*100,1)}% / ${fmt(train.profit_factor,2)}</div></div>
      <div class="detailBox"><div class="muted">样本外交易 / 胜率 / 盈亏比</div><div>${test.trades||0} / ${fmt(Number(test.win_rate||0)*100,1)}% / ${fmt(test.profit_factor,2)}</div></div>
      <div class="detailBox"><div class="muted">样本外平均 / 最差单笔</div><div>${fmt(test.avg_trade_bps,1)} / ${fmt(test.worst_trade_bps,1)} bps</div></div>
      <div class="detailBox"><div class="muted">样本外暴露时间</div><div>${fmt(Number(test.exposure||0)*100,1)}%</div></div>
    </div>
    <h3>怎么理解</h3>
    <div class="detailText">信号在一根 K 线收盘后才确定，并按下一根 K 线开盘成交。收益已经扣除策略实验室设置的估算往返成本，但没有逐根模拟动态盘口深度、资金费和强平风险。“达到门槛”只允许进入更长周期回测和实时模拟，不代表可以直接真实下单。</div>`;
  detailDlg.showModal();
}
function renderStrategyLab(data){
  const rows=data.rows||[],body=document.querySelector('#strategyLabTbl tbody');body.innerHTML='';
  if((data.coins||[]).length)document.getElementById('labCoins').value=data.coins.join(',');
  if(data.interval)document.getElementById('labInterval').value=data.interval;
  if(data.days)document.getElementById('labDays').value=data.days;
  if(data.round_trip_cost_bps!==undefined)document.getElementById('labCost').value=data.round_trip_cost_bps;
  document.getElementById('labEvaluations').textContent=data.evaluations??rows.length;
  document.getElementById('labPromotable').textContent=data.promotable??rows.filter(row=>row.promotable).length;
  document.getElementById('labSource').textContent=({hyperliquid_candles:'Hyperliquid K线',sqlite_latest:'数据库上次结果',memory_cache:'内存缓存',empty:'尚未运行'})[data.source]||data.source||'Hyperliquid K线';
  document.getElementById('labTime').textContent=data.ts?fmtBeijingDateTime(data.ts):'-';
  rows.forEach(row=>{
    const test=row.test||{},train=row.train||{},tr=document.createElement('tr');
    tr.innerHTML=`<td class="${row.promotable?'passChip':'blockChip'}">${row.promotable?'研究候选':'未通过'}</td><td>${esc(row.coin)}</td><td>${fmt(row.actual_days,1)}天 / ${row.samples||0}根</td><td>${esc(row.strategy)}</td><td>${esc(strategyFamilyText(row.family))}</td><td>${strategySignalText(row.current_signal)}</td><td class="${Number(train.net_bps)>=0?'scoreGood':'scoreBad'}">${fmt(train.net_bps,1)} bps</td><td class="${Number(test.net_bps)>=0?'scoreGood':'scoreBad'}">${fmt(test.net_bps,1)} bps</td><td class="${Number(test.max_drawdown_bps)>=-500?'scoreGood':'scoreBad'}">${fmt(test.max_drawdown_bps,1)} bps</td><td>${test.trades||0}</td><td>${fmt(Number(test.win_rate||0)*100,1)}%</td><td>${fmt(test.profit_factor,2)}</td><td>${fmt(test.avg_trade_bps,1)} bps</td><td style="text-align:left">${esc(JSON.stringify(row.params||{}))}</td>`;
    tr.title='点击查看训练段、样本外和风险详情';tr.onclick=()=>showStrategyLabDetail(row);body.appendChild(tr);
  });
  if(!rows.length)body.innerHTML='<tr><td colspan="14" class="muted">还没有回测结果，点击“运行新回测”</td></tr>';
  const failures=(data.failures||[]).map(item=>`${item.coin}: ${item.error}`).join('；');
  document.getElementById('labNote').textContent=[data.note||'',failures?`未能读取：${failures}`:''].filter(Boolean).join(' ');
}
async function runStrategyLab(refresh){
  const status=document.getElementById('labStatus');
  const coins=(document.getElementById('labCoins').value||'').trim();
  const interval=document.getElementById('labInterval').value;
  const days=document.getElementById('labDays').value;
  const cost=document.getElementById('labCost').value;
  if(!coins){status.textContent='至少填写一个币种';return;}
  status.textContent=refresh?'正在下载K线并回测，可能需要几十秒...':'正在读取上次结果...';
  try{
    const query=new URLSearchParams({coins,interval,days,cost_bps:cost,limit:'80'});if(refresh)query.set('refresh','1');
    const response=await fetch('strategy_lab?'+query.toString());const data=await response.json();
    if(!data.ok){renderStrategyLab(data);const failed=(data.failures||[]).map(item=>`${item.coin}: ${item.error}`).join('；');throw new Error(data.error||failed||'回测失败');}renderStrategyLab(data);
    status.textContent=`完成：${data.evaluations||0} 组，${data.promotable||0} 组达到继续研究门槛；未授权真实下单。`;
  }catch(e){status.textContent='策略实验室失败：'+e.message;}
}
function openStrategyLabDialog(){strategyLabDlg.showModal();runStrategyLab(false);}
function openLeadlagDialog(){leadlagDlg.showModal();loadLeadlag();}
async function saveLeadlagConfig(){
  const status=document.getElementById('llSaveStatus'),token=adminTokenValue();if(!token){status.textContent='请先在全局设置填写管理口令';return;}
  status.textContent='保存中...';
  try{const r=await fetch('leadlag_config',{method:'POST',headers:{'Content-Type':'application/json','X-Admin-Token':token},body:JSON.stringify({config:readLeadlagConfig()})});const data=await r.json();if(!data.ok)throw new Error(data.error||'保存失败');fillLeadlagConfig(data.config||{});status.textContent='已保存，下一次 WS 行情立即生效';loadLeadlag();}
  catch(e){status.textContent='保存失败：'+e.message;}
}
const notifyGroups=[
  ['候选信号提醒',[
    ['notify_candidate_open','候选首次出现：首次达到入场门槛时提醒'],
    ['notify_candidate_repeat','候选持续提醒：同一候选仍存在时按冷却时间重复提醒'],
    ['notify_candidate_resolved','候选解除：候选消失、回归或质量变差时提醒'],
    ['notify_caution','谨慎风险：偏离很大但点差/质量不合格时提醒']
  ]],
  ['模拟盘提醒',[
    ['notify_paper_open','模拟开仓：纸面交易建立仓位时提醒'],
    ['notify_paper_close','模拟平仓：纸面交易出场时提醒'],
    ['notify_leadlag_open','V2模拟开仓：联动传播单腿建立仓位时提醒'],
    ['notify_leadlag_close','V2模拟平仓：联动传播单腿退出时提醒']
  ]],
  ['真实交易提醒',[
    ['notify_live_open','真实开仓：真实策略建立仓位时提醒'],
    ['notify_live_close','真实平仓：真实策略退出仓位时提醒'],
    ['notify_live_error','真实异常：真实开仓失败、平仓异常、单腿风险时提醒']
  ]]
];
const notifyLabels=Object.fromEntries(notifyGroups.flatMap(group=>group[1]));
const notifyTextKeys=['dingtalk_paper_webhook','dingtalk_paper_keyword','dingtalk_live_webhook','dingtalk_live_keyword','public_url'];
function renderNotifyConfig(cfg){
  notifyTextKeys.forEach(key=>{
    const el=document.getElementById('cfg_'+key);
    if(el) el.value=cfg[key]??(key.endsWith('_keyword')?'小测试':'');
  });
  const box=document.getElementById('notifyChecks');box.innerHTML='';
  notifyGroups.forEach(([title,items])=>{
    const group=document.createElement('div');group.className='metric';group.style.margin='10px 0';
    group.innerHTML=`<h3 style="margin:0 0 8px">${title}</h3>`;
    items.forEach(([key,label])=>{
      const line=document.createElement('label');line.style.display='block';line.style.margin='8px 0';
      line.innerHTML=`<input type="checkbox" id="cfg_${key}"> ${label}`;
      group.appendChild(line);
    });
    box.appendChild(group);
    items.forEach(([key])=>document.getElementById('cfg_'+key).checked=cfg[key]===true);
  });
  document.getElementById('cfg_notify_candidate_max_per_scan').value=cfg.notify_candidate_max_per_scan??1;
  document.getElementById('cfg_notify_candidate_min_z').value=cfg.notify_candidate_min_z??3;
  document.getElementById('cfg_notify_cooldown').value=cfg.notify_cooldown??1800;
}
async function loadNotifyConfig(){
  const status=document.getElementById('notifySaveStatus');status.textContent='读取中...';
  try{const r=await fetch('notify_config');const data=await r.json();if(!data.ok)throw new Error(data.error||'读取失败');renderNotifyConfig(data.config||{});status.textContent='已读取；修改后下一轮立即生效';}
  catch(e){status.textContent='读取失败：'+e.message;}
}
function openNotifyDialog(){notifyDlg.showModal();loadNotifyConfig();}
async function saveNotifyConfig(){
  const status=document.getElementById('notifySaveStatus');
  const token=adminTokenValue();
  if(!token){status.textContent='请输入管理口令';return;}
  const config={};Object.keys(notifyLabels).forEach(key=>config[key]=document.getElementById('cfg_'+key).checked);
  notifyTextKeys.forEach(key=>config[key]=(document.getElementById('cfg_'+key)?.value||'').trim());
  config.notify_candidate_max_per_scan=parseInt(document.getElementById('cfg_notify_candidate_max_per_scan').value,10);
  config.notify_candidate_min_z=parseFloat(document.getElementById('cfg_notify_candidate_min_z').value);
  config.notify_cooldown=parseInt(document.getElementById('cfg_notify_cooldown').value,10);
  status.textContent='正在保存...';
  try{const r=await fetch('notify_config',{method:'POST',headers:{'Content-Type':'application/json','X-Admin-Token':token},body:JSON.stringify({config})});const data=await r.json();if(!data.ok){status.textContent=data.error||'保存失败';return;}renderNotifyConfig(data.config||{});status.textContent='已保存，下一轮采集立即使用';}
  catch(e){status.textContent='保存失败：'+e.message;}
}
async function changeAdminToken(){
  const oldToken=document.getElementById('oldAdminToken').value.trim();
  const newToken=document.getElementById('newAdminToken').value.trim();
  const status=document.getElementById('tokenStatus');
  if(!oldToken || !newToken){status.textContent='请输入旧口令和新口令';return}
  if(newToken.length<8){status.textContent='新口令至少 8 位';return}
  status.textContent='正在修改...';
  try{
    const r=await fetch('admin_token',{
      method:'POST',
      headers:{'Content-Type':'application/json','X-Admin-Token':oldToken},
      body:JSON.stringify({old_token:oldToken,new_token:newToken})
    });
    const data=await r.json();
    if(!data.ok){status.textContent=data.error||'修改失败';return}
    localStorage.setItem('hlm_admin_token', newToken);
    document.getElementById('globalAdminToken').value=newToken;
    document.getElementById('oldAdminToken').value='';
    document.getElementById('newAdminToken').value='';
    status.textContent='管理口令已修改，新口令已填入保存参数框';
  }catch(e){
    status.textContent='修改失败：'+e;
  }
}
const savedToken=localStorage.getItem('hlm_admin_token');
if(savedToken && document.getElementById('globalAdminToken')) document.getElementById('globalAdminToken').value=savedToken;
['live_min_entry_z','live_min_corr','live_min_expected_edge_bps','live_l2_max_spread_bps'].forEach(key=>{
  const el=liveCfgEl(key);
  if(el)el.addEventListener('input', markLiveRiskCustom);
});
loadPaperConfig();
loadLatest(); setInterval(loadLatest,60000);
setInterval(()=>{if(liveDlg.open) refreshLiveL2Only();},1000);
setInterval(()=>{if(liveDlg.open) loadLive(true);},15000);
setInterval(()=>{if(leadlagDlg.open) loadLeadlag();},3000);
</script>
</body></html>"""


def make_alt_scan_payload(config):
    rows, failures = altcoin_scan_report(
        config["leaders"], config["assets"], hours=config["hours"], min_corr=config["min_corr"],
        min_z=config["min_z"], max_spread_bps=config.get("max_spread_bps"),
        min_volume=config.get("min_volume", 0), max_assets=config.get("max_assets", 0),
    )
    return {
        "ts": time.time(), "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "config": config, "rows": altcoin_payload_rows(rows, min_z=config["min_z"]),
        "failures": failures,
        "text": format_altcoin_scan(rows, failures, hours=config["hours"], min_z=config["min_z"], title="服务器采集"),
    }


def json_response(handler, data, status=200):
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class L2BookCache:
    def __init__(self, url=HL_WS):
        self.url = url
        self.books = {}
        self.market_mids = {}
        self.history = {}
        self.desired = set()
        self.subscribed = set()
        self.lock = threading.Lock()
        self.running = False
        self.ws = None
        self.thread = None
        self.connected = False
        self.error = ""
        self.last_message_at = 0.0
        self.revision = 0
        self.connected_at = 0.0

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    def set_coins(self, coins):
        clean = {str(coin).upper() for coin in coins if coin}
        if not clean:
            return
        with self.lock:
            self.desired.update(clean)
            ws = self.ws if self.connected else None
            pending = sorted(self.desired - self.subscribed)[:1000]
        if ws:
            for coin in pending:
                self._send_subscribe(ws, coin)

    def _send_subscribe(self, ws, coin):
        try:
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}))
            with self.lock:
                self.subscribed.add(coin)
        except Exception as exc:
            with self.lock:
                self.error = f"l2Book 订阅失败 {coin}: {exc}"

    def _on_open(self, ws):
        with self.lock:
            self.ws = ws
            self.connected = True
            self.connected_at = time.time()
            self.error = ""
            self.subscribed = set()
            coins = sorted(self.desired)[:1000]
        for coin in coins:
            self._send_subscribe(ws, coin)
        try:
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
        except Exception as exc:
            with self.lock:
                self.error = f"allMids 订阅失败：{exc}"

    def _on_close(self, _ws, *_args):
        with self.lock:
            self.connected = False
            self.ws = None
            self.subscribed = set()

    def _on_error(self, _ws, error):
        with self.lock:
            self.error = str(error)

    def _on_message(self, _ws, raw_message):
        try:
            msg = json.loads(raw_message)
            if msg.get("channel") == "allMids":
                data = msg.get("data") or {}
                mids = data.get("mids") or {}
                now = time.time()
                with self.lock:
                    for name, value in mids.items():
                        coin = str(name or "").upper()
                        try:
                            mid = float(value)
                        except (TypeError, ValueError):
                            continue
                        if not coin or mid <= 0:
                            continue
                        self.market_mids[coin] = (now, mid)
                        history = self.history.setdefault(coin, deque(maxlen=600))
                        if not history or now - history[-1][0] >= 0.10:
                            history.append((now, mid))
                    self.last_message_at = now
                    self.revision += 1
                    self.error = ""
                return
            if msg.get("channel") != "l2Book":
                return
            data = msg.get("data") or {}
            coin = str(data.get("coin") or "").upper()
            levels = data.get("levels") or []
            if not coin or len(levels) < 2 or not levels[0] or not levels[1]:
                return
            bids, asks = levels[0], levels[1]
            bid, ask = float(bids[0]["px"]), float(asks[0]["px"])
            if bid <= 0 or ask <= 0:
                return
            now = time.time()
            book = {
                "coin": coin,
                "bid": bid,
                "ask": ask,
                "mid": (bid + ask) / 2,
                "spread_bps": (ask / bid - 1) * 10_000,
                "server_time": float(data.get("time") or 0) / 1000,
                "received_at": now,
                "bid_size": float(bids[0].get("sz") or 0),
                "ask_size": float(asks[0].get("sz") or 0),
                "bid_orders": int(bids[0].get("n") or 0),
                "ask_orders": int(asks[0].get("n") or 0),
            }
            with self.lock:
                self.books[coin] = book
                history = self.history.setdefault(coin, deque(maxlen=600))
                if not history or now - history[-1][0] >= 0.10:
                    history.append((now, book["mid"]))
                self.last_message_at = now
                self.revision += 1
                self.error = ""
        except (ValueError, KeyError, TypeError) as exc:
            with self.lock:
                self.error = f"l2Book 解析失败：{exc}"

    def _loop(self):
        while self.running:
            try:
                ws = websocket.WebSocketApp(
                    self.url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as exc:
                with self.lock:
                    self.connected = False
                    self.ws = None
                    self.error = str(exc)
            if self.running:
                time.sleep(3)

    def get_book(self, coin):
        coin = str(coin or "").upper()
        with self.lock:
            book = dict(self.books.get(coin) or {})
        if book:
            now = time.time()
            book["age_ms"] = max(0.0, (now - float(book.get("received_at") or 0)) * 1000)
            book["server_lag_ms"] = max(0.0, (now - float(book.get("server_time") or now)) * 1000) if book.get("server_time") else None
        return book

    def motion(self, coin, windows=(1, 3, 15)):
        """Return short-horizon mid-price changes and current top-book imbalance."""
        coin = str(coin or "").upper()
        with self.lock:
            book = dict(self.books.get(coin) or {})
            history = list(self.history.get(coin) or ())
            market_mid = self.market_mids.get(coin)
        if not book or not history:
            return None
        if market_mid and time.time() - float(market_mid[0]) <= 5:
            current_ts, current_mid = float(market_mid[0]), float(market_mid[1])
        else:
            current_ts = float(book.get("received_at") or history[-1][0])
            current_mid = float(book.get("mid") or 0)
        if current_mid <= 0:
            return None
        out = {"coin": coin, "mid": current_mid, "age_ms": max(0.0, (time.time() - current_ts) * 1000)}
        for seconds in windows:
            target = current_ts - float(seconds)
            reference = next((mid for ts, mid in reversed(history) if ts <= target), None)
            out[f"ret_{seconds}s_bps"] = ((current_mid / reference - 1) * 10_000
                                                if reference and reference > 0 else None)
        bid_size = float(book.get("bid_size") or 0)
        ask_size = float(book.get("ask_size") or 0)
        depth = bid_size + ask_size
        out["imbalance"] = (bid_size - ask_size) / depth if depth > 0 else 0.0
        out["spread_bps"] = float(book.get("spread_bps") or 0)
        out["bid"] = float(book.get("bid") or 0)
        out["ask"] = float(book.get("ask") or 0)
        out["bid_size"] = bid_size
        out["ask_size"] = ask_size
        return out

    def recent_relationship(self, asset, leader, lookback_seconds=180, step_seconds=5):
        """Estimate short-horizon corr/beta from synchronized allMids history."""
        asset = str(asset or "").upper()
        leader = str(leader or "").upper()
        with self.lock:
            asset_history = list(self.history.get(asset) or ())
            leader_history = list(self.history.get(leader) or ())
        if len(asset_history) < 8 or len(leader_history) < 8:
            return None
        end_ts = min(asset_history[-1][0], leader_history[-1][0])
        start_ts = max(end_ts - float(lookback_seconds), asset_history[0][0], leader_history[0][0])
        if end_ts - start_ts < 30:
            return None
        targets = []
        ts = start_ts
        while ts <= end_ts:
            targets.append(ts)
            ts += float(step_seconds)

        def sampled(history):
            values, index, last = [], 0, None
            for target in targets:
                while index < len(history) and history[index][0] <= target:
                    last = history[index][1]
                    index += 1
                values.append(last)
            return values

        asset_prices = sampled(asset_history)
        leader_prices = sampled(leader_history)
        asset_returns, leader_returns = [], []
        previous = None
        for asset_px, leader_px in zip(asset_prices, leader_prices):
            if not asset_px or not leader_px:
                previous = None
                continue
            if previous:
                previous_asset, previous_leader = previous
                if previous_asset > 0 and previous_leader > 0:
                    asset_returns.append(math.log(asset_px / previous_asset))
                    leader_returns.append(math.log(leader_px / previous_leader))
            previous = (asset_px, leader_px)
        if len(asset_returns) < 6:
            return None
        corr = pearson(asset_returns, leader_returns)
        beta = beta_against(asset_returns, leader_returns)
        if corr is None or beta is None:
            return None
        return {"corr": corr, "beta": beta, "samples": len(asset_returns)}

    def snapshot(self, coins=None):
        with self.lock:
            status = {
                "connected": self.connected,
                "error": self.error,
                "desired": len(self.desired),
                "subscribed": len(self.subscribed),
                "books": len(self.books),
                "revision": self.revision,
                "all_mids": len(self.market_mids),
                "all_mids_age_ms": max(0.0, (time.time() - max((item[0] for item in self.market_mids.values()), default=0)) * 1000) if self.market_mids else None,
                "last_message_age_ms": max(0.0, (time.time() - self.last_message_at) * 1000) if self.last_message_at else None,
            }
            selected = {str(c).upper() for c in coins if c} if coins else set(self.desired)
            books = [dict(self.books[c]) for c in sorted(selected) if c in self.books]
        now = time.time()
        for book in books:
            book["age_ms"] = max(0.0, (now - float(book.get("received_at") or 0)) * 1000)
            book["server_lag_ms"] = max(0.0, (now - float(book.get("server_time") or now)) * 1000) if book.get("server_time") else None
        return {"status": status, "books": books}


class AltServerState:
    def __init__(self, config, db_path=ALT_DB_FILE):
        self.config = config
        self.db_path = db_path
        self.latest = None
        self.error = None
        self.lock = threading.Lock()
        self.running = True
        self.last_notify = {}
        self.active_candidates = set()
        self.live_account = None
        self.live_error = None
        self.live_leverage_cache = {}
        self.last_live_account_db_save = 0.0
        self.strategy_cycle_lock = threading.Lock()
        self.leadlag_lock = threading.Lock()
        self.strategy_lab_lock = threading.Lock()
        self.strategy_lab_cache = None
        self.realtime_strategy_status = {
            "running": False, "last_eval_ts": None, "last_event_ts": None,
            "last_error": None, "events": 0, "last_revision": 0,
        }
        self.leadlag_status = {
            "running": False, "last_eval_ts": None, "last_error": None,
            "eligible": 0, "opened": 0, "closed": 0, "signals": [],
        }
        self.l2book = L2BookCache()


def run_unified_strategy_cycle(state, payload, scan_id, *, skip_if_idle=False, source="scan"):
    """Run one serialized strategy decision cycle for scan or WS-triggered data."""
    with state.strategy_cycle_lock:
        if state.config.get("paper_sync_live", True):
            cycle = prepare_shared_strategy_cycle(state, payload, scan_id)
            has_events = bool(payload.get("strategy_pending_entries") or payload.get("strategy_closed_events"))
            if skip_if_idle and not has_events:
                return None
            steps = live_execution_steps(state.config)
            real_first = steps.index("submit_real") < steps.index("record_paper")
            if real_first:
                try:
                    live_snapshot = update_live_trading(state, payload, scan_id)
                finally:
                    paper_snapshot = finalize_shared_strategy_cycle(state, payload, scan_id, cycle)
            else:
                paper_snapshot = finalize_shared_strategy_cycle(state, payload, scan_id, cycle)
                live_snapshot = update_live_trading(state, payload, scan_id)
        else:
            if skip_if_idle:
                return None
            paper_snapshot = update_paper_trading(state, payload, scan_id)
            live_snapshot = update_live_trading(state, payload, scan_id)
        return {"paper": paper_snapshot, "live_trades": live_snapshot, "source": source}


def realtime_strategy_loop(state):
    """Evaluate the latest statistical model against WS books between full scans."""
    with state.lock:
        state.realtime_strategy_status["running"] = True
    last_revision = -1
    try:
        while state.running:
            loop_started = time.time()
            try:
                l2_status = state.l2book.snapshot(["__STATUS_ONLY__"]).get("status", {})
                revision = int(l2_status.get("revision") or 0)
                with state.lock:
                    latest = state.latest
                    raw_rows = list((latest or {}).get("rows", []))
                    scan_id = int((latest or {}).get("scan_id") or 0)
                # A WebSocket may send many books in a burst.  Coalesce them
                # into one decision per configured interval, but do not query
                # SQLite again when no new book has arrived.
                should_evaluate = revision != last_revision and revision > 0
                if should_evaluate and raw_rows and scan_id and state.config.get("paper_sync_live", True):
                    strategy_rows = prepare_live_rows(raw_rows, state.config, state.l2book)
                    payload = {
                        "ts": time.time(), "rows": raw_rows, "strategy_rows": strategy_rows,
                        "scan_id": scan_id,
                    }
                    result = run_unified_strategy_cycle(
                        state, payload, scan_id, skip_if_idle=True, source="ws_realtime",
                    )
                    event_count = len(payload.get("strategy_open_rows") or []) + len(payload.get("strategy_closed_keys") or [])
                    with state.lock:
                        state.realtime_strategy_status["last_eval_ts"] = time.time()
                        state.realtime_strategy_status["last_error"] = None
                        state.realtime_strategy_status["last_revision"] = revision
                        if result:
                            state.realtime_strategy_status["last_event_ts"] = time.time()
                            state.realtime_strategy_status["events"] += event_count
                            if state.latest:
                                state.latest["paper"] = result["paper"]
                                state.latest["live_trades"] = result["live_trades"]
                    if result:
                        print(
                            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] WS realtime strategy event "
                            f"scan={scan_id} opens={len(payload.get('strategy_open_rows') or [])} "
                            f"closes={len(payload.get('strategy_closed_keys') or [])}",
                            flush=True,
                        )
                if should_evaluate and raw_rows:
                    try:
                        leadlag_snapshot = update_leadlag_strategy(state, raw_rows, time.time())
                        with state.lock:
                            if state.latest:
                                state.latest["leadlag"] = leadlag_snapshot
                    except Exception as exc:
                        with state.leadlag_lock:
                            state.leadlag_status["last_error"] = str(exc)
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] leadlag strategy failed: {exc}", flush=True)
                if should_evaluate:
                    last_revision = revision
            except Exception as exc:
                with state.lock:
                    state.realtime_strategy_status["last_error"] = str(exc)
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] realtime strategy failed: {exc}", flush=True)
            interval = max(0.1, float(state.config.get("live_realtime_strategy_interval_ms", 500) or 500) / 1000)
            while state.running and time.time() - loop_started < interval:
                time.sleep(min(0.05, interval))
    finally:
        with state.lock:
            state.realtime_strategy_status["running"] = False


def collector_loop(state):
    init_alt_db(state.db_path)
    disk = load_latest_scan(state.db_path)
    if disk:
        state.l2book.set_coins(l2book_subscription_coins(
            disk["rows"], leaders=configured_ws_leaders(state.config),
        ))
        with state.lock:
            state.latest = {
                "ts": disk["scan"]["ts"], "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(disk["scan"]["ts"])),
                "config": state.config, "rows": disk["rows"], "failures": [], "text": "loaded from sqlite",
                "scan_id": int(disk["scan"]["id"]),
            }
            state.error = None
    while state.running:
        started = time.time()
        try:
            payload = make_alt_scan_payload(state.config)
            state.l2book.set_coins(l2book_subscription_coins(
                payload.get("rows", []), leaders=configured_ws_leaders(state.config),
            ))
            scan_id = save_alt_scan(payload, state.config, db_path=state.db_path)
            payload["scan_id"] = scan_id
            # Freeze one strategy snapshot for both destinations.  Simulation
            # decides continuously; the real executor later consumes only the
            # new-entry events produced from this same snapshot.
            payload["strategy_rows"] = prepare_live_rows(payload.get("rows", []), state.config, state.l2book)
            result = run_unified_strategy_cycle(state, payload, scan_id, source="full_scan")
            payload["paper"] = result["paper"]
            payload["live_trades"] = result["live_trades"]
            payload.pop("strategy_rows", None)
            payload.pop("strategy_open_rows", None)
            payload.pop("strategy_closed_keys", None)
            payload.pop("strategy_pending_entries", None)
            payload.pop("strategy_closed_events", None)
            payload.pop("strategy_live_entries", None)
            with state.lock:
                state.latest, state.error = payload, None
            notify_dingtalk_candidates(state, payload)
            print(f"[{payload['time']}] saved scan {scan_id}, rows={len(payload['rows'])}, failures={len(payload['failures'])}", flush=True)
        except Exception as exc:  # Server mode should keep running after transient API failures.
            with state.lock:
                state.error = str(exc)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] collect failed: {exc}", flush=True)
        while state.running and time.time() - started < state.config["interval"]:
            time.sleep(0.5)


class AltRequestHandler(BaseHTTPRequestHandler):
    server_version = "AltcoinMonitor/1.0"

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        state = self.server.state
        if parsed.path in ("/", "/dashboard"):
            html_response(self, dashboard_html())
            return
        if parsed.path == "/health":
            with state.lock:
                latest_ts = state.latest["ts"] if state.latest else None
                error = state.error
            json_response(self, {"ok": error is None or latest_ts is not None, "latest_ts": latest_ts, "error": error,
                                 "endpoints": ["/dashboard", "/latest", "/series?asset=SOL&leader=ETH&limit=240", "/history?asset=SOL&limit=200"]})
            return
        if parsed.path == "/latest":
            with state.lock:
                latest, error = state.latest, state.error
            if latest is None:
                disk = load_latest_scan(state.db_path)
                if disk:
                    json_response(self, {"ok": True, "source": "disk", **disk})
                else:
                    json_response(self, {"ok": False, "error": error or "还没有采集结果"}, status=503)
            else:
                payload = dict(latest)
                payload["rows"] = prepare_live_rows(payload.get("rows", []), state.config, state.l2book)
                json_response(self, {"ok": True, "source": "memory", **payload})
            return
        if parsed.path == "/paper":
            limit = int((query.get("limit") or ["200"])[0])
            try:
                with state.lock:
                    latest_rows = list((state.latest or {}).get("rows", []))
                if state.config.get("paper_sync_live", True):
                    latest_rows = prepare_live_rows(latest_rows, state.config, state.l2book)
                snap = load_paper_snapshot(state.db_path, limit, current_rows=latest_rows, config=state.config)
                snap["config"] = paper_config_public(state.config)
                json_response(self, {"ok": True, **snap})
            except (sqlite3.Error, OSError, ValueError) as exc:
                json_response(self, {"ok": False, "error": str(exc)}, status=500)
            return
        if parsed.path == "/paper_config":
            json_response(self, {
                "ok": True,
                "config": paper_config_public(state.config),
                "fields": PAPER_CONFIG_FIELDS,
                "admin_enabled": bool(state.config.get("admin_token")),
            })
            return
        if parsed.path == "/leadlag":
            limit = int((query.get("limit") or ["200"])[0])
            with state.leadlag_lock:
                leadlag_status = dict(state.leadlag_status)
                leadlag_status["signals"] = list(state.leadlag_status.get("signals") or [])
            snap = load_leadlag_snapshot(state.db_path, limit, status=leadlag_status)
            snap["config"] = leadlag_config_public(state.config)
            json_response(self, {"ok": True, **snap})
            return
        if parsed.path == "/leadlag_config":
            json_response(self, {
                "ok": True, "config": leadlag_config_public(state.config),
                "fields": LEADLAG_CONFIG_FIELDS,
                "admin_enabled": bool(state.config.get("admin_token")),
            })
            return
        if parsed.path == "/notify_config":
            json_response(self, {"ok": True, "config": notify_config_public(state.config),
                                 "fields": NOTIFY_CONFIG_FIELDS,
                                 "admin_enabled": bool(state.config.get("admin_token"))})
            return
        if parsed.path == "/l2book":
            coins_raw = (query.get("coins") or [""])[0]
            coins = [item.strip().upper() for item in coins_raw.split(",") if item.strip()] if coins_raw else None
            json_response(self, {"ok": True, "l2book": state.l2book.snapshot(coins)})
            return
        if parsed.path == "/live":
            with state.lock:
                account = state.live_account
                live_error = state.live_error
                latest_rows = list((state.latest or {}).get("rows", []))
                realtime_status = dict(state.realtime_strategy_status)
            latest_rows = prepare_live_rows(latest_rows, state.config, state.l2book)
            if (query.get("fresh") or [""])[0] in ("1", "true", "yes") and valid_evm_address(state.config.get("live_account_address")):
                try:
                    account = fetch_live_account_snapshot(state.config["live_account_address"])
                    save_live_account_snapshot(account, state.db_path)
                    with state.lock:
                        state.live_account, state.live_error = account, None
                    live_error = None
                except Exception as exc:
                    live_error = str(exc)
            account = account or load_latest_live_account_snapshot(state.db_path)
            l2_coins = l2book_subscription_coins(
                latest_rows, leaders=configured_ws_leaders(state.config),
            )
            l2_snapshot = state.l2book.snapshot(l2_coins)
            l2_snapshot.setdefault("status", {})["scan_rows"] = len(latest_rows)
            live_trades = load_live_trades_snapshot(state.db_path, 200, current_rows=latest_rows, config=state.config)
            diagnostics = live_opportunity_diagnostics(
                state, latest_rows, account=account, open_trades=live_trades.get("open", []), limit=20,
            )
            json_response(self, {
                "ok": True,
                "config": live_config_public(state.config),
                "account": account,
                "account_error": live_error,
                "live_trades": live_trades,
                "diagnostics": diagnostics,
                "realtime_strategy": realtime_status,
                "l2book": l2_snapshot,
                "admin_enabled": bool(state.config.get("admin_token")),
            })
            return
        if parsed.path == "/strategy_lab":
            coins = split_symbols((query.get("coins") or ["BTC,ETH,SOL"])[0])[:12]
            interval = str((query.get("interval") or ["15m"])[0]).lower()
            days = max(7, min(180, int((query.get("days") or ["30"])[0])))
            cost_bps = max(0.0, min(500.0, float((query.get("cost_bps") or ["12"])[0])))
            limit = max(1, min(200, int((query.get("limit") or ["80"])[0])))
            refresh = (query.get("refresh") or [""])[0].lower() in ("1", "true", "yes")
            if not coins:
                json_response(self, {"ok": False, "error": "至少填写一个币种"}, status=400)
                return
            if interval not in ("5m", "15m", "1h", "4h"):
                json_response(self, {"ok": False, "error": "周期只允许 5m/15m/1h/4h"}, status=400)
                return
            cache_key = (tuple(coins), interval, days, round(cost_bps, 6), limit)
            with state.strategy_lab_lock:
                cached = state.strategy_lab_cache
            if not refresh and cached and cached.get("key") == cache_key and time.time() - cached.get("ts", 0) < 300:
                json_response(self, {**cached["payload"], "source": "memory_cache"})
                return
            if not refresh:
                stored = load_latest_strategy_lab_run(db_path=state.db_path)
                if stored:
                    json_response(self, {**stored, "source": "sqlite_latest"})
                    return
                json_response(self, {"ok": True, "rows": [], "source": "empty", "note": "点击运行回测生成第一批结果"})
                return
            try:
                payload = run_strategy_lab(
                    tuple(coins), days=days, interval=interval,
                    round_trip_cost_bps=cost_bps, limit=limit,
                )
                payload["run_id"] = save_strategy_lab_run(payload, db_path=state.db_path)
                with state.strategy_lab_lock:
                    state.strategy_lab_cache = {"key": cache_key, "ts": time.time(), "payload": payload}
                json_response(self, {**payload, "source": "hyperliquid_candles"})
            except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                json_response(self, {"ok": False, "error": str(exc)}, status=500)
            return
        if parsed.path == "/history":
            asset = (query.get("asset") or [""])[0].upper()
            limit = int((query.get("limit") or ["200"])[0])
            if not asset:
                json_response(self, {"ok": False, "error": "missing asset"}, status=400)
                return
            json_response(self, {"ok": True, "asset": asset, "rows": load_asset_history(asset, limit, state.db_path)})
            return
        if parsed.path == "/series":
            asset = (query.get("asset") or [""])[0].upper()
            leader = (query.get("leader") or [""])[0].upper()
            limit = int((query.get("limit") or ["240"])[0])
            if not asset:
                json_response(self, {"ok": False, "error": "missing asset"}, status=400)
                return
            rows = load_asset_pair_series(asset, leader or None, limit, state.db_path)
            json_response(self, {"ok": True, "asset": asset, "leader": leader, "rows": rows})
            return
        if parsed.path == "/stats":
            asset = (query.get("asset") or [""])[0].upper()
            leader = (query.get("leader") or [""])[0].upper()
            limit = int((query.get("limit") or ["240"])[0])
            if not asset:
                json_response(self, {"ok": False, "error": "missing asset"}, status=400)
                return
            rows = load_asset_pair_series(asset, leader or None, limit, state.db_path)
            json_response(self, {"ok": True, "asset": asset, "leader": leader, "stats": summarize_pair_history(rows)})
            return
        if parsed.path == "/candles":
            asset = (query.get("asset") or [""])[0].upper()
            leader = (query.get("leader") or [""])[0].upper()
            hours = int((query.get("hours") or ["24"])[0])
            if not asset or not leader:
                json_response(self, {"ok": False, "error": "missing asset or leader"}, status=400)
                return
            try:
                json_response(self, {"ok": True, "asset": asset, "leader": leader, "rows": pair_candle_payload(asset, leader, hours)})
            except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                json_response(self, {"ok": False, "error": str(exc)}, status=500)
            return
        if parsed.path == "/backtest":
            asset = (query.get("asset") or [""])[0].upper()
            leader = (query.get("leader") or [""])[0].upper()
            hours = int((query.get("hours") or ["168"])[0])
            entry_z = float((query.get("entry_z") or ["2.0"])[0])
            exit_z = float((query.get("exit_z") or ["0.5"])[0])
            rolling = int((query.get("rolling") or ["72"])[0])
            max_hold = int((query.get("max_hold") or ["36"])[0])
            fee_bps = float((query.get("fee_bps") or ["4"])[0])
            source = (query.get("source") or ["db"])[0].lower()
            z_value_bps = float((query.get("z_value_bps") or [str(state.config.get("paper_z_value_bps", 18))])[0])
            if not asset or not leader:
                json_response(self, {"ok": False, "error": "missing asset or leader"}, status=400)
                return
            try:
                if source == "api":
                    result = backtest_pair(hl_candles(asset, hours=hours), hl_candles(leader, hours=hours),
                                           entry_z=entry_z, exit_z=exit_z, rolling=rolling,
                                           max_hold=max_hold, fee_bps=fee_bps)
                    result["source"] = "hyperliquid_candles"
                else:
                    rows = load_asset_pair_series_since(asset, leader, hours=hours, db_path=state.db_path)
                    result = replay_backtest_from_scan_rows(rows, entry_z=entry_z, exit_z=exit_z,
                                                            max_hold=max_hold, fee_bps=fee_bps,
                                                            z_value_bps=z_value_bps)
                json_response(self, {"ok": True, "asset": asset, "leader": leader, "hours": hours, "result": result})
            except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                json_response(self, {"ok": False, "error": str(exc)}, status=500)
            return
        json_response(self, {"ok": False, "error": "not found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        state = self.server.state
        if parsed.path == "/admin_token":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length).decode("utf-8") if length else "{}"
                body = json.loads(raw or "{}")
                old_token = body.get("old_token") or self.headers.get("X-Admin-Token")
                new_token = str(body.get("new_token") or "").strip()
                if not valid_admin_token(state.config, old_token):
                    json_response(self, {"ok": False, "error": "旧管理口令错误"}, status=403)
                    return
                if len(new_token) < 8:
                    json_response(self, {"ok": False, "error": "新管理口令至少 8 位"}, status=400)
                    return
                if any(ch.isspace() for ch in new_token):
                    json_response(self, {"ok": False, "error": "新管理口令不能包含空格或换行"}, status=400)
                    return
                with state.lock:
                    state.config["admin_token"] = new_token
                update_env_vars({"HLM_ADMIN_TOKEN": new_token})
                json_response(self, {"ok": True})
            except (json.JSONDecodeError, ValueError, OSError) as exc:
                json_response(self, {"ok": False, "error": str(exc)}, status=400)
            return
        if parsed.path == "/live_test":
            json_response(self, {"ok": False, "error": "真实回环测试已停用；现在使用真实策略持仓逻辑"}, status=410)
            return
        if parsed.path == "/live_emergency_close":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length).decode("utf-8") if length else "{}"
                body = json.loads(raw or "{}")
                token = self.headers.get("X-Admin-Token") or body.get("token")
                if not valid_admin_token(state.config, token):
                    json_response(self, {"ok": False, "error": "管理口令错误"}, status=403)
                    return
                if str(body.get("confirm") or "").strip().upper() != "CLOSE":
                    json_response(self, {"ok": False, "error": "请输入 CLOSE 确认紧急全部平仓"}, status=400)
                    return
                result = execute_live_emergency_flatten(state, str(body.get("reason") or "manual emergency close"))
                try:
                    snapshot = fetch_live_account_snapshot(state.config["live_account_address"])
                    save_live_account_snapshot(snapshot, state.db_path)
                    with state.lock:
                        state.live_account, state.live_error = snapshot, None
                except Exception:
                    pass
                json_response(self, {"ok": True, "result": result})
            except (json.JSONDecodeError, ValueError, RuntimeError, OSError, KeyError, IndexError, TypeError) as exc:
                json_response(self, {"ok": False, "error": str(exc)}, status=400)
            return
        if parsed.path not in ("/paper_config", "/live_config", "/notify_config", "/leadlag_config"):
            json_response(self, {"ok": False, "error": "not found"}, status=404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            body = json.loads(raw or "{}")
            token = self.headers.get("X-Admin-Token") or body.get("token")
            if not valid_admin_token(state.config, token):
                json_response(self, {"ok": False, "error": "管理口令错误或服务器未配置 HLM_ADMIN_TOKEN"}, status=403)
                return
            raw_config = body.get("config") or body
            if parsed.path == "/live_config":
                updates = coerce_live_config(raw_config)
            elif parsed.path == "/leadlag_config":
                updates = coerce_leadlag_config(raw_config)
            elif parsed.path == "/notify_config":
                updates = coerce_notify_config(raw_config)
            else:
                updates = coerce_paper_config(raw_config)
            if parsed.path == "/live_config" and updates.get("live_enabled"):
                proposed = {**state.config, **updates}
                status = live_config_public(proposed)
                if not status["execution_ready"]:
                    raise ValueError("不能开启真实下单：" + status["blocker"])
            with state.lock:
                state.config.update(updates)
                if state.latest:
                    state.latest["config"] = state.config
            if parsed.path == "/live_config":
                update_live_env_file(updates)
                try:
                    with state.lock:
                        latest_rows = list((state.latest or {}).get("rows", []))
                    state.l2book.set_coins(l2book_subscription_coins(
                        latest_rows, leaders=configured_ws_leaders(state.config),
                    ))
                except Exception as exc:
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] l2Book resubscribe failed after config update: {exc}", flush=True)
                leverage_result = None
                if "live_leverage" in updates and state.config.get("live_enabled"):
                    try:
                        leverage_result = apply_live_leverage_to_current_positions(state.config)
                    except Exception as exc:
                        leverage_result = {"error": str(exc)}
                json_response(self, {"ok": True, "config": live_config_public(state.config), "leverage_result": leverage_result})
            elif parsed.path == "/notify_config":
                update_notify_env_file(updates)
                json_response(self, {"ok": True, "config": notify_config_public(state.config)})
            elif parsed.path == "/leadlag_config":
                update_leadlag_env_file(updates)
                with state.lock:
                    latest_rows = list((state.latest or {}).get("rows", []))
                state.l2book.set_coins(l2book_subscription_coins(
                    latest_rows, leaders=configured_ws_leaders(state.config),
                ))
                json_response(self, {"ok": True, "config": leadlag_config_public(state.config)})
            else:
                update_env_file(updates)
                json_response(self, {"ok": True, "config": paper_config_public(state.config)})
        except (json.JSONDecodeError, ValueError, OSError) as exc:
            json_response(self, {"ok": False, "error": str(exc)}, status=400)

    def log_message(self, _format, *args):
        return


def backtest_pair(asset_series, hedge_series, *, entry_z=2.0, exit_z=0.5, rolling=72, max_hold=36, fee_bps=9.0):
    times, asset_returns, hedge_returns = aligned_returns(asset_series, hedge_series)
    beta = beta_against(asset_returns, hedge_returns)
    corr = pearson(asset_returns, hedge_returns)
    if beta is None or corr is None or len(asset_returns) < rolling + 10:
        raise ValueError("样本不足，无法回测")
    residuals = []
    mean_asset, mean_hedge = statistics.fmean(asset_returns), statistics.fmean(hedge_returns)
    for a_ret, h_ret in zip(asset_returns, hedge_returns):
        residuals.append((a_ret - mean_asset) - beta * (h_ret - mean_hedge))
    zscores = [None] * len(residuals)
    for i in range(rolling, len(residuals)):
        window = residuals[i - rolling:i]
        sigma = statistics.pstdev(window)
        zscores[i] = None if sigma == 0 else (residuals[i] - statistics.fmean(window)) / sigma
    trades, position = [], None
    for i, zscore in enumerate(zscores):
        if zscore is None:
            continue
        if position is None:
            if zscore >= entry_z:
                position = {"side": "short_asset", "entry": i, "entry_z": zscore, "pnl": -fee_bps}
            elif zscore <= -entry_z:
                position = {"side": "long_asset", "entry": i, "entry_z": zscore, "pnl": -fee_bps}
            continue
        if i > position["entry"]:
            pair_return = asset_returns[i] - beta * hedge_returns[i]
            position["pnl"] += (-pair_return if position["side"] == "short_asset" else pair_return) * 10_000
        hold = i - position["entry"]
        if abs(zscore) <= exit_z or hold >= max_hold or i == len(zscores) - 1:
            position["exit"] = i
            position["exit_z"] = zscore
            position["hold_bars"] = hold
            trades.append(position)
            position = None
    if not trades:
        return {"corr": corr, "beta": beta, "trades": [], "total_bps": 0, "win_rate": 0, "avg_bps": 0, "worst_bps": 0}
    total = sum(trade["pnl"] for trade in trades)
    wins = sum(trade["pnl"] > 0 for trade in trades)
    return {"corr": corr, "beta": beta, "trades": trades, "total_bps": total,
            "win_rate": wins / len(trades), "avg_bps": total / len(trades),
            "worst_bps": min(trade["pnl"] for trade in trades)}


class Monitor:
    def __init__(self, config):
        self.config = config
        self.samples = deque(maxlen=3000)
        self.running = False
        self.latest = None
        self.error = None
        self.lock = threading.Lock()

        # 下列值完全由 WebSocket 推送更新；不再每秒主动查询 Hyperliquid HTTP 接口。
        self.latest_book = None
        self.contexts = {}
        self.custom_reference = None
        self.custom_reference_updated = 0.0
        self.last_sample_at = 0.0
        self.ws = None

    @staticmethod
    def _context(ctx):
        return {
            "oracle": float(ctx["oraclePx"]),
            "mid": float(ctx.get("midPx") or ctx["markPx"]),
            "funding_hourly": float(ctx.get("funding") or 0),
            "premium": float(ctx.get("premium") or 0),
        }

    def _on_open(self, ws):
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": self.config["coin"]}}))
        # 订阅合约状态（预言机价格、资金费率、标记溢价）。若基准也是 HL，额外订阅它。
        coins = {self.config["coin"]}
        if self.config["reference_provider"] == "hyperliquid_oracle":
            coins.add(self.config["reference_coin"])
        for coin in coins:
            ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": coin}}))
        with self.lock:
            self.error = None

    def _on_message(self, _ws, raw_message):
        try:
            message = json.loads(raw_message)
            channel, data = message.get("channel"), message.get("data", {})
            now = time.time()
            if channel == "l2Book" and data.get("coin") == self.config["coin"]:
                bids, asks = data["levels"]
                bid, ask = float(bids[0]["px"]), float(asks[0]["px"])
                with self.lock:
                    self.latest_book = {
                        "bid": bid, "ask": ask, "mid": (bid + ask) / 2,
                        "spread_bps": (ask / bid - 1) * 10_000,
                        "server_time": float(data.get("time", 0)) / 1000,
                        "received_at": now,
                    }
            elif channel == "activeAssetCtx":
                coin, ctx = data.get("coin"), data.get("ctx")
                if coin and ctx:
                    with self.lock:
                        self.contexts[coin] = self._context(ctx)
            self._append_sample_if_ready(now)
        except (ValueError, KeyError, TypeError) as exc:
            with self.lock:
                self.error = f"WebSocket 数据解析失败：{exc}"

    def _on_error(self, _ws, error):
        if self.running:
            with self.lock:
                self.error = f"WebSocket：{error}"

    def _append_sample_if_ready(self, now):
        """按“多久记录一次”保存推送最新值；数据接收本身是实时的。"""
        with self.lock:
            if now - self.last_sample_at < max(1, int(self.config["interval_seconds"])):
                return
            book = self.latest_book
            contract = self.contexts.get(self.config["coin"])
            provider = self.config["reference_provider"]
            if provider == "hyperliquid_oracle":
                reference = self.contexts.get(self.config["reference_coin"])
                if not reference:
                    return
                ref, label = reference["oracle"], f"HL oracle（参考中间价 {reference['mid']:.4f}）"
            elif provider in ("custom_json", "ffd_crypto_snapshot"):
                if self.custom_reference is None:
                    return
                if provider == "ffd_crypto_snapshot" and base_asset(self.config["coin"]) != self.config["ffd_crypto_id"].strip().upper():
                    self.error = "FFD 加密快照只能与同名 HL 合约比较：例如 DEX 合约 BTC、FFD 标的 BTC"
                    return
                reference = self.custom_reference
                label = "自定义 JSON" if provider == "custom_json" else "FFD 加密快照（研究用）"
                ref = reference["mid"]
            else:
                self.error = "未知基准报价模式"
                return
            if not book or not contract:
                return
            self.last_sample_at = now
            server_lag = max(0, (now - book["server_time"]) * 1000) if book["server_time"] else None
            sample = {
                "t": now, "dex": book["mid"], "bid": book["bid"], "ask": book["ask"],
                "ref": ref, "ref_label": label, "spread_bps": (book["mid"] / ref - 1) * 10_000,
                "ref_bid": reference.get("bid") if provider in ("custom_json", "ffd_crypto_snapshot") else None,
                "ref_ask": reference.get("ask") if provider in ("custom_json", "ffd_crypto_snapshot") else None,
                "ref_source_time": reference.get("source_time") if provider in ("custom_json", "ffd_crypto_snapshot") else None,
                "book_bps": book["spread_bps"], "request_ms": server_lag or 0,
                "server_lag_ms": server_lag, "funding_hourly": contract["funding_hourly"],
                "premium_bps": contract["premium"] * 10_000,
            }
            self.samples.append(sample)
            self.latest = sample
            self.error = None

    def _reference_worker(self):
        """外部 REST 源刷新。FFD 只提供研究快照，强制低频避免无效消耗配额。"""
        while self.running:
            try:
                provider = self.config["reference_provider"]
                if provider == "ffd_crypto_snapshot":
                    with self.lock:
                        self.error = "正在读取 FFD 快照；首次读取可能要等几十秒"
                quote = fetch_custom_quote(self.config) if provider == "custom_json" else fetch_ffd_crypto_quote(self.config)
                with self.lock:
                    self.custom_reference, self.custom_reference_updated = quote, time.time()
                self._append_sample_if_ready(time.time())
            except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                with self.lock:
                    self.error = f"外部基准：{exc}"
            interval = max(1, int(self.config["interval_seconds"]))
            if self.config["reference_provider"] == "ffd_crypto_snapshot":
                interval = max(60, interval)
            time.sleep(interval)

    def _ws_worker(self):
        while self.running:
            try:
                ws = websocket.WebSocketApp(HL_WS, on_open=self._on_open, on_message=self._on_message, on_error=self._on_error)
                self.ws = ws
                ws.run_forever(ping_interval=20, ping_timeout=8)
            except (OSError, websocket.WebSocketException) as exc:
                with self.lock:
                    self.error = f"WebSocket 连接失败：{exc}"
            if self.running:
                time.sleep(2)

    def start(self):
        if not self.running:
            self.running = True
            threading.Thread(target=self._ws_worker, daemon=True).start()
            if self.config["reference_provider"] in ("custom_json", "ffd_crypto_snapshot"):
                threading.Thread(target=self._reference_worker, daemon=True).start()

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()

    def metrics(self):
        with self.lock:
            samples, latest, error = list(self.samples), self.latest, self.error
        horizon = time.time() - int(self.config["window_minutes"]) * 60
        samples = [s for s in samples if s["t"] >= horizon]
        duration = samples[-1]["t"] - samples[0]["t"] if len(samples) > 1 else 0
        funding_positive_share = (sum(s["funding_hourly"] > 0 for s in samples) / len(samples)) if samples else 0
        if len(samples) < 9:
            return latest, error, {"correlation": None, "lag": None, "zscore": None, "n": len(samples),
                                   "duration": duration, "funding_positive_share": funding_positive_share,
                                   "reason": "采样点不足"}
        dex_returns = [math.log(samples[i]["dex"] / samples[i - 1]["dex"]) for i in range(1, len(samples))]
        ref_returns = [math.log(samples[i]["ref"] / samples[i - 1]["ref"]) for i in range(1, len(samples))]
        correlation = pearson(dex_returns, ref_returns)
        max_shift = min(int(self.config["max_lag_seconds"]) // max(1, int(self.config["interval_seconds"])), len(dex_returns) // 3)
        best = (correlation if correlation is not None else -2, 0)
        for shift in range(-max_shift, max_shift + 1):
            if shift < 0:
                score = pearson(dex_returns[-shift:], ref_returns[:shift])
            elif shift > 0:
                score = pearson(dex_returns[:-shift], ref_returns[shift:])
            else:
                score = correlation
            if score is not None and score > best[0]:
                best = (score, shift)
        spreads = [s["spread_bps"] for s in samples]
        std = statistics.stdev(spreads) if len(spreads) > 1 else 0
        zscore = (spreads[-1] - statistics.fmean(spreads)) / std if std else 0
        return latest, error, {"correlation": correlation, "lag": best[1] * int(self.config["interval_seconds"]), "best_corr": best[0], "zscore": zscore, "n": len(samples), "duration": duration, "funding_positive_share": funding_positive_share, "reason": "" if correlation is not None else "参考价格在观察期内几乎没有变化"}


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("机会研究台：Hyperliquid / 资金费 / Positive EV（只读）")
        self.geometry("1280x950")
        self.config_data = read_config()
        self.monitor = Monitor(self.config_data)
        self.alt_monitor_running = False
        self.vars = {key: tk.StringVar(value=str(value)) for key, value in self.config_data.items()}
        self.status = tk.StringVar(value="停止。点击“开始监控”。")
        self._build()
        self.protocol("WM_DELETE_WINDOW", self.close)
        self.after(500, self.refresh)

    def _build(self):
        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=8, pady=8)
        market_tab = ttk.Frame(notebook)
        funding_tab = ttk.Frame(notebook)
        ev_tab = ttk.Frame(notebook)
        relation_tab = ttk.Frame(notebook)
        alt_monitor_tab = ttk.Frame(notebook)
        backtest_tab = ttk.Frame(notebook)
        api_tab = ttk.Frame(notebook)
        notebook.add(market_tab, text="1. 价差 / 对冲")
        notebook.add(funding_tab, text="2. 资金费率")
        notebook.add(ev_tab, text="3. 体育 Positive EV")
        notebook.add(relation_tab, text="4. 跨资产联动")
        notebook.add(alt_monitor_tab, text="5. 小币联动监控")
        notebook.add(backtest_tab, text="6. 历史回测")
        notebook.add(api_tab, text="7. 全局 API")
        self._build_market_tab(market_tab)
        self._build_funding_tab(funding_tab)
        self._build_ev_tab(ev_tab)
        self._build_relation_tab(relation_tab)
        self._build_alt_monitor_tab(alt_monitor_tab)
        self._build_backtest_tab(backtest_tab)
        self._build_api_tab(api_tab)

    def _build_market_tab(self, parent):
        form = ttk.LabelFrame(parent, text="数据源与信号参数", padding=10)
        form.pack(fill="x", padx=10, pady=8)
        fields = [("DEX 合约", "coin"), ("基准模式", "reference_provider"), ("基准合约(HL模式)", "reference_coin"),
                  ("记录间隔(秒)", "interval_seconds"), ("观察窗口(分钟)", "window_minutes"), ("触发阈值(bps)", "alert_bps"),
                  ("往返成本(bps)", "round_trip_cost_bps"), ("额外安全垫(bps)", "extra_buffer_bps"), ("最大时延扫描(秒)", "max_lag_seconds")]
        for i, (label, key) in enumerate(fields):
            ttk.Label(form, text=label).grid(row=i // 3, column=(i % 3) * 2, sticky="w", padx=(0, 4), pady=4)
            if key == "reference_provider":
                box = ttk.Combobox(form, textvariable=self.vars[key], values=("hyperliquid_oracle", "custom_json", "ffd_crypto_snapshot"), state="readonly", width=19)
                box.grid(row=i // 3, column=(i % 3) * 2 + 1, sticky="ew", padx=(0, 14), pady=4)
            else:
                ttk.Entry(form, textvariable=self.vars[key], width=22).grid(row=i // 3, column=(i % 3) * 2 + 1, sticky="ew", padx=(0, 14), pady=4)
        ttk.Label(form, text="自定义 JSON URL").grid(row=3, column=0, sticky="w", pady=4)
        ttk.Entry(form, textvariable=self.vars["custom_url"], width=55).grid(row=3, column=1, columnspan=3, sticky="ew", padx=(0, 14), pady=4)
        ttk.Label(form, text="最新价字段路径").grid(row=3, column=4, sticky="w", pady=4)
        ttk.Entry(form, textvariable=self.vars["custom_json_path"], width=22).grid(row=3, column=5, sticky="ew", pady=4)
        ttk.Label(form, text="买一字段（可选）").grid(row=4, column=0, sticky="w", pady=4)
        ttk.Entry(form, textvariable=self.vars["custom_bid_path"], width=22).grid(row=4, column=1, sticky="ew", padx=(0, 14), pady=4)
        ttk.Label(form, text="卖一字段（可选）").grid(row=4, column=2, sticky="w", pady=4)
        ttk.Entry(form, textvariable=self.vars["custom_ask_path"], width=22).grid(row=4, column=3, sticky="ew", padx=(0, 14), pady=4)
        ttk.Label(form, text="源时间字段（可选）").grid(row=4, column=4, sticky="w", pady=4)
        ttk.Entry(form, textvariable=self.vars["custom_timestamp_path"], width=22).grid(row=4, column=5, sticky="ew", pady=4)
        ttk.Label(form, text="请求头 JSON（可选）").grid(row=5, column=0, sticky="w", pady=4)
        ttk.Entry(form, textvariable=self.vars["custom_headers_json"], width=55).grid(row=5, column=1, columnspan=3, sticky="ew", padx=(0, 14), pady=4)
        ttk.Button(form, text="测试自定义源", command=self.test_custom_source).grid(row=5, column=4, sticky="ew", pady=4, padx=(0, 4))
        ttk.Button(form, text="测试 FFD", command=self.test_ffd_source).grid(row=5, column=5, sticky="ew", pady=4)
        ttk.Label(form, text="请求方法").grid(row=6, column=0, sticky="w", pady=4)
        ttk.Combobox(form, textvariable=self.vars["custom_http_method"], values=("GET", "POST"), state="readonly", width=10).grid(row=6, column=1, sticky="w", pady=4)
        ttk.Label(form, text="POST 请求体 JSON（可选）").grid(row=6, column=2, sticky="w", pady=4)
        ttk.Entry(form, textvariable=self.vars["custom_body_json"], width=50).grid(row=6, column=3, columnspan=3, sticky="ew", pady=4)
        ttk.Label(form, text="FFD 加密标的（研究用）").grid(row=7, column=0, sticky="w", pady=4)
        ttk.Entry(form, textvariable=self.vars["ffd_crypto_id"], width=22).grid(row=7, column=1, sticky="ew", padx=(0, 14), pady=4)
        ttk.Label(form, text="价差对冲要用可成交 bid/ask。FFD 只有快照，不是执行腿；接口配置集中在第 7 页。", foreground="#555555").grid(row=7, column=2, columnspan=4, sticky="w", pady=4)
        ttk.Button(form, text="一键 BTC 对 BTC", command=self.use_ffd_btc).grid(row=8, column=1, sticky="ew", padx=(0, 14), pady=(2, 0))
        ttk.Button(form, text="GOLD 用 HL 预言机", command=self.use_gold_oracle).grid(row=8, column=2, sticky="ew", padx=(0, 14), pady=(2, 0))
        controls = ttk.Frame(parent)
        controls.pack(fill="x", padx=10)
        ttk.Button(controls, text="开始监控", command=self.start).pack(side="left")
        ttk.Button(controls, text="停止", command=self.monitor.stop).pack(side="left", padx=6)
        ttk.Button(controls, text="保存配置", command=self.save).pack(side="left")
        ttk.Label(controls, textvariable=self.status).pack(side="left", padx=16)
        chart_box = ttk.LabelFrame(parent, text="实时走势（本次启动后采集的数据）", padding=(5, 3))
        chart_box.pack(fill="x", padx=10, pady=(8, 0))
        self.chart = tk.Canvas(chart_box, height=300, background="#ffffff", highlightthickness=0)
        self.chart.pack(fill="x")
        self.output = tk.Text(parent, height=15, wrap="word", font=("Consolas", 11), state="disabled")
        self.output.pack(fill="both", expand=True, padx=10, pady=(8, 10))

    def _build_funding_tab(self, parent):
        box = ttk.LabelFrame(parent, text="跨合约资金费率筛选（公开只读数据）", padding=12)
        box.pack(fill="x", padx=12, pady=12)
        self.funding_assets = tk.StringVar(value="BTC, ETH, SOL, HYPE, xyz:GOLD, flx:GOLD")
        ttk.Label(box, text="合约（逗号分隔）").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Entry(box, textvariable=self.funding_assets, width=70).grid(row=0, column=1, sticky="ew", padx=8)
        ttk.Button(box, text="立即扫描", command=self.scan_funding).grid(row=0, column=2, padx=4)
        box.columnconfigure(1, weight=1)
        ttk.Label(box, text="正费率=多头付空头；负费率=空头付多头。它只是持仓现金流，不是直接开仓信号。", foreground="#555555").grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 0))
        self.funding_output = tk.Text(parent, height=28, wrap="word", font=("Consolas", 11), state="disabled")
        self.funding_output.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    def _build_ev_tab(self, parent):
        box = ttk.LabelFrame(parent, text="体育 Positive EV 记录器（手动填写你的概率判断；不下单）", padding=12)
        box.pack(fill="x", padx=12, pady=12)
        self.ev_event = tk.StringVar()
        self.ev_model = tk.StringVar()
        self.ev_market = tk.StringVar()
        self.ev_stake = tk.StringVar(value="10")
        self.ev_outcome = tk.StringVar(value="待结算")
        fields = [("比赛 / 市场", self.ev_event, 28), ("你估计概率%", self.ev_model, 12), ("市场概率%", self.ev_market, 12), ("模拟投入 USDC", self.ev_stake, 12)]
        for i, (label, var, width) in enumerate(fields):
            ttk.Label(box, text=label).grid(row=0, column=i * 2, sticky="w", padx=(0, 3))
            ttk.Entry(box, textvariable=var, width=width).grid(row=0, column=i * 2 + 1, sticky="w", padx=(0, 9))
        ttk.Label(box, text="结果").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Combobox(box, textvariable=self.ev_outcome, values=("待结算", "赢", "输"), state="readonly", width=10).grid(row=1, column=1, sticky="w", pady=(8, 0))
        ttk.Button(box, text="记录这次判断", command=self.add_ev_record).grid(row=1, column=3, sticky="w", pady=(8, 0))
        ttk.Button(box, text="刷新统计", command=self.refresh_ev_summary).grid(row=1, column=5, sticky="w", pady=(8, 0))
        ttk.Label(box, text="市场概率可由预测市场 YES 合约价格近似（例如 42%）；只有“你的概率 − 市场概率”长期为正且结果验证后，才可能有正期望。", foreground="#555555").grid(row=2, column=0, columnspan=8, sticky="w", pady=(10, 0))
        self.ev_output = tk.Text(parent, height=25, wrap="word", font=("Consolas", 11), state="disabled")
        self.ev_output.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.refresh_ev_summary()

    def _build_relation_tab(self, parent):
        box = ttk.LabelFrame(parent, text="跨资产相关性与联动研究（Hyperliquid 历史 5 分钟 K 线）", padding=12)
        box.pack(fill="x", padx=12, pady=12)
        self.relation_assets = tk.StringVar(value="BTC, ETH, SOL, HYPE")
        self.relation_hours = tk.StringVar(value="24")
        ttk.Label(box, text="合约（逗号分隔）").grid(row=0, column=0, sticky="w")
        ttk.Entry(box, textvariable=self.relation_assets, width=52).grid(row=0, column=1, padx=8, sticky="w")
        ttk.Label(box, text="回看小时").grid(row=0, column=2, sticky="w")
        ttk.Entry(box, textvariable=self.relation_hours, width=8).grid(row=0, column=3, padx=8, sticky="w")
        ttk.Button(box, text="分析联动", command=self.analyze_relations).grid(row=0, column=4, padx=4)
        ttk.Label(box, text="相关性显示“过去是否一起涨跌”，不是预测。小币与大币短时联动可能在消息、清算或流动性变化时突然失效。", foreground="#555555").grid(row=1, column=0, columnspan=5, sticky="w", pady=(10, 0))

        scan_box = ttk.LabelFrame(parent, text="小币联动机会扫描（直接腿 + 保护腿研究）", padding=12)
        scan_box.pack(fill="x", padx=12, pady=(0, 12))
        self.scan_leaders = tk.StringVar(value="BTC, ETH")
        self.scan_assets = tk.StringVar(value=DEFAULT_ALT_ASSETS)
        self.scan_hours = tk.StringVar(value="24")
        self.scan_min_corr = tk.StringVar(value="0.55")
        self.scan_min_z = tk.StringVar(value="1.2")
        ttk.Label(scan_box, text="保护腿").grid(row=0, column=0, sticky="w")
        ttk.Entry(scan_box, textvariable=self.scan_leaders, width=18).grid(row=0, column=1, padx=8, sticky="w")
        ttk.Label(scan_box, text="候选小币").grid(row=0, column=2, sticky="w")
        ttk.Entry(scan_box, textvariable=self.scan_assets, width=70).grid(row=0, column=3, padx=8, sticky="ew")
        ttk.Button(scan_box, text="扫描小币机会", command=self.scan_altcoin_links).grid(row=0, column=4, padx=4)
        ttk.Label(scan_box, text="回看小时").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(scan_box, textvariable=self.scan_hours, width=8).grid(row=1, column=1, padx=8, sticky="w", pady=(8, 0))
        ttk.Label(scan_box, text="最低相关").grid(row=1, column=2, sticky="w", pady=(8, 0))
        ttk.Entry(scan_box, textvariable=self.scan_min_corr, width=8).grid(row=1, column=3, sticky="w", padx=8, pady=(8, 0))
        ttk.Label(scan_box, text="最低偏离Z").grid(row=1, column=4, sticky="w", padx=(8, 0), pady=(8, 0))
        ttk.Entry(scan_box, textvariable=self.scan_min_z, width=8).grid(row=1, column=5, sticky="w", padx=8, pady=(8, 0))
        ttk.Label(scan_box, text="输出里的“直接腿”是假设回归；“保护腿”用于降低大盘方向暴露，不保证盈利。先看盘口点差和资金费。", foreground="#555555").grid(row=2, column=0, columnspan=5, sticky="w", pady=(10, 0))
        scan_box.columnconfigure(3, weight=1)

        self.relation_output = tk.Text(parent, height=28, wrap="word", font=("Consolas", 11), state="disabled")
        self.relation_output.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    def _build_alt_monitor_tab(self, parent):
        box = ttk.LabelFrame(parent, text="持续监控：小币偏离 + 保护腿（只读，不下单）", padding=12)
        box.pack(fill="x", padx=12, pady=12)
        self.live_leaders = tk.StringVar(value="BTC, ETH")
        self.live_assets = tk.StringVar(value=DEFAULT_ALT_ASSETS)
        self.live_hours = tk.StringVar(value="24")
        self.live_min_corr = tk.StringVar(value="0.65")
        self.live_min_z = tk.StringVar(value="2.0")
        self.live_max_spread = tk.StringVar(value="8")
        self.live_interval = tk.StringVar(value="60")
        ttk.Label(box, text="保护腿").grid(row=0, column=0, sticky="w")
        ttk.Entry(box, textvariable=self.live_leaders, width=18).grid(row=0, column=1, sticky="w", padx=8)
        ttk.Label(box, text="候选小币").grid(row=0, column=2, sticky="w")
        ttk.Entry(box, textvariable=self.live_assets, width=75).grid(row=0, column=3, sticky="ew", padx=8)
        ttk.Button(box, text="开始持续监控", command=self.start_alt_monitor).grid(row=0, column=4, padx=4)
        ttk.Button(box, text="停止", command=self.stop_alt_monitor).grid(row=0, column=5, padx=4)
        ttk.Button(box, text="读服务器最新", command=self.read_server_latest).grid(row=0, column=6, padx=4)
        ttk.Button(box, text="? 指标说明", command=self.show_indicator_help).grid(row=0, column=7, padx=4)
        fields = [("回看小时", self.live_hours), ("最低相关", self.live_min_corr), ("入场Z", self.live_min_z), ("最大点差bps", self.live_max_spread), ("刷新秒", self.live_interval)]
        for i, (label, var) in enumerate(fields):
            base = i * 2
            ttk.Label(box, text=label).grid(row=1, column=base, sticky="w", pady=(8, 0))
            ttk.Entry(box, textvariable=var, width=10).grid(row=1, column=base + 1, sticky="w", padx=(6, 18), pady=(8, 0))
        ttk.Label(box, text="这个页盯你朋友可能做的方向：小币偏离时给出直接腿和保护腿；先纸面跟踪，不自动下单。", foreground="#555555").grid(row=2, column=0, columnspan=10, sticky="w", pady=(10, 0))
        box.columnconfigure(3, weight=1)
        self.live_output = tk.Text(parent, height=34, wrap="word", font=("Consolas", 11), state="disabled")
        self.live_output.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self._write_text(self.live_output, "未启动。\n\n说明：观察 = 没过门槛；候选 = 相关性、Z、盘口点差都过门槛，可加入纸面跟踪。")

    def _build_backtest_tab(self, parent):
        box = ttk.LabelFrame(parent, text="历史回测：单个小币 vs 保护腿（粗略模拟）", padding=12)
        box.pack(fill="x", padx=12, pady=12)
        self.bt_asset = tk.StringVar(value="SOL")
        self.bt_hedge = tk.StringVar(value="ETH")
        self.bt_hours = tk.StringVar(value="168")
        self.bt_entry_z = tk.StringVar(value="2.0")
        self.bt_exit_z = tk.StringVar(value="0.5")
        self.bt_rolling = tk.StringVar(value="72")
        self.bt_max_hold = tk.StringVar(value="36")
        self.bt_fee_bps = tk.StringVar(value="9")
        fields = [("小币", self.bt_asset, 10), ("保护腿", self.bt_hedge, 10), ("回看小时", self.bt_hours, 8),
                  ("入场Z", self.bt_entry_z, 8), ("出场Z", self.bt_exit_z, 8), ("滚动窗口", self.bt_rolling, 8),
                  ("最长持有K", self.bt_max_hold, 8), ("双边成本bps", self.bt_fee_bps, 8)]
        for i, (label, var, width) in enumerate(fields):
            ttk.Label(box, text=label).grid(row=i // 4, column=(i % 4) * 2, sticky="w", pady=4)
            ttk.Entry(box, textvariable=var, width=width).grid(row=i // 4, column=(i % 4) * 2 + 1, sticky="w", padx=(6, 16), pady=4)
        ttk.Button(box, text="回测这对", command=self.run_pair_backtest).grid(row=2, column=1, sticky="w", pady=(8, 0))
        ttk.Label(box, text="回测使用 5 分钟收盘价和残差回归；没有真实盘口成交和滑点，只能当筛选参考。", foreground="#555555").grid(row=2, column=2, columnspan=6, sticky="w", pady=(8, 0))
        self.bt_output = tk.Text(parent, height=34, wrap="word", font=("Consolas", 11), state="disabled")
        self.bt_output.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    def _build_api_tab(self, parent):
        box = ttk.LabelFrame(parent, text="全局 API / 数据源配置", padding=12)
        box.pack(fill="x", padx=12, pady=12)
        ttk.Label(box, text="当前逻辑：Hyperliquid 行情走公开 API；FFD 只能低频研究；真正价差对冲需要外部源提供 bid/ask 和时间戳。", foreground="#555555").grid(row=0, column=0, columnspan=6, sticky="w", pady=(0, 10))
        fields = [("服务器 URL", "server_url", 70), ("自定义 JSON URL", "custom_url", 70), ("最新价字段", "custom_json_path", 18),
                  ("买一字段", "custom_bid_path", 18), ("卖一字段", "custom_ask_path", 18),
                  ("源时间字段", "custom_timestamp_path", 18), ("请求头 JSON", "custom_headers_json", 70),
                  ("请求方法", "custom_http_method", 10), ("POST JSON", "custom_body_json", 70),
                  ("FFD 加密标的", "ffd_crypto_id", 18)]
        for i, (label, key, width) in enumerate(fields):
            row = i + 1
            ttk.Label(box, text=label).grid(row=row, column=0, sticky="w", pady=4)
            if key == "custom_http_method":
                ttk.Combobox(box, textvariable=self.vars[key], values=("GET", "POST"), state="readonly", width=width).grid(row=row, column=1, sticky="w", padx=8, pady=4)
            else:
                ttk.Entry(box, textvariable=self.vars[key], width=width).grid(row=row, column=1, columnspan=4, sticky="ew", padx=8, pady=4)
        ttk.Button(box, text="保存全局配置", command=self.save).grid(row=11, column=1, sticky="w", padx=8, pady=(10, 0))
        ttk.Button(box, text="测试服务器", command=self.test_server_source).grid(row=11, column=2, sticky="w", padx=8, pady=(10, 0))
        ttk.Button(box, text="测试自定义源", command=self.test_custom_source).grid(row=11, column=3, sticky="w", padx=8, pady=(10, 0))
        ttk.Button(box, text="测试 FFD", command=self.test_ffd_source).grid(row=11, column=4, sticky="w", padx=8, pady=(10, 0))
        box.columnconfigure(1, weight=1)
        self.api_output = tk.Text(parent, height=20, wrap="word", font=("Consolas", 11), state="disabled")
        self.api_output.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self._write_text(self.api_output, "数据源判断：\n\nFFD/你买的网站快照源：适合做宏观参考、跨源走势和事件验证；不适合直接套利，因为没有可成交买一/卖一。\n\n更适合交易执行的数据源：交易所 WebSocket 盘口、IBKR 这类有授权的实时报价、或你自己的可成交聚合报价。")

    def values(self):
        numeric = ("interval_seconds", "window_minutes", "alert_bps", "round_trip_cost_bps", "extra_buffer_bps", "max_lag_seconds", "min_observation_minutes")
        result = {key: var.get().strip() for key, var in self.vars.items()}
        for key in numeric:
            result[key] = int(float(result[key]))
        return result

    def save(self):
        try:
            self.config_data = self.values()
            write_config(self.config_data)
            self.status.set(f"已保存到 {CONFIG_FILE.name}")
        except ValueError:
            messagebox.showerror("参数错误", "数值字段必须为数字。")

    def use_ffd_btc(self):
        self.vars["coin"].set("BTC")
        self.vars["reference_provider"].set("ffd_crypto_snapshot")
        self.vars["reference_coin"].set("BTC")
        self.vars["ffd_crypto_id"].set("BTC")
        self.vars["interval_seconds"].set("60")
        self.status.set("已切到 FFD 示例：BTC 对 BTC。点击“开始监控”。")

    def use_gold_oracle(self):
        self.vars["coin"].set("xyz:GOLD")
        self.vars["reference_provider"].set("hyperliquid_oracle")
        self.vars["reference_coin"].set("xyz:GOLD")
        self.status.set("已切到 GOLD 示例：使用 Hyperliquid 自己的预言机作参考。")

    def test_custom_source(self):
        try:
            config = self.values()
        except ValueError:
            messagebox.showerror("参数错误", "数值字段必须为数字。")
            return
        self.status.set("正在测试外部行情接口…")

        def work():
            started = time.time()
            try:
                quote = fetch_custom_quote(config)
                elapsed = (time.time() - started) * 1000
                source_age = "未提供源时间戳"
                if quote["source_time"]:
                    source_age = f"源时间距现在约 {max(0, time.time() - quote['source_time']) * 1000:.0f} ms"
                bid_ask = "未提供买一/卖一"
                if quote["bid"] is not None and quote["ask"] is not None:
                    bid_ask = f"买一/卖一：{quote['bid']:.8g} / {quote['ask']:.8g}"
                message = f"读取成功\n中间价：{quote['mid']:.8g}\n{bid_ask}\n{source_age}\nHTTP 耗时：{elapsed:.0f} ms"
                self.after(0, lambda: (self.status.set("外部行情接口测试成功"), messagebox.showinfo("测试成功", message)))
            except (URLError, HTTPError, TimeoutError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                error_message = str(exc)
                self.after(0, lambda: (self.status.set("外部行情接口测试失败"), messagebox.showerror("测试失败", error_message)))
        threading.Thread(target=work, daemon=True).start()

    def test_ffd_source(self):
        try:
            config = self.values()
        except ValueError:
            messagebox.showerror("参数错误", "数值字段必须为数字。")
            return
        self.status.set("正在测试 FFD 快照，可能要等几秒…")

        def work():
            try:
                quote = fetch_ffd_crypto_quote(config)
                source_age = "未提供源时间"
                if quote["source_time"]:
                    source_age = f"源时间距现在约 {max(0, time.time() - quote['source_time']):.0f} 秒"
                message = f"FFD 读取成功\n标的：{config['ffd_crypto_id']}\n价格：{quote['mid']:.8g} USD\n{source_age}\n\n注意：FFD 没有买一/卖一，只能研究，不能做实时套利执行价格。"
                self.after(0, lambda: (self.status.set("FFD 测试成功"), messagebox.showinfo("FFD 测试成功", message)))
            except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                error_message = str(exc)
                self.after(0, lambda: (self.status.set("FFD 测试失败"), messagebox.showerror("FFD 测试失败", error_message)))
        threading.Thread(target=work, daemon=True).start()

    @staticmethod
    def _write_text(widget, content):
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", content)
        widget.configure(state="disabled")

    def scan_funding(self):
        assets = [item.strip() for item in self.funding_assets.get().split(",") if item.strip()]
        if not assets:
            messagebox.showerror("没有合约", "请输入至少一个 Hyperliquid 合约，例如 BTC, ETH, xyz:GOLD。")
            return
        self._write_text(self.funding_output, "正在读取公开资金费率…")

        def work():
            rows, failures = [], []
            for asset in assets:
                try:
                    ctx = hl_context(asset)
                    hourly = ctx["funding_hourly"]
                    rows.append((hourly, asset, ctx))
                except (URLError, HTTPError, TimeoutError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                    failures.append(f"{asset}: {exc}")
            rows.sort(reverse=True)
            lines = ["资金费率扫描结果（按每小时费率从高到低；仅公开行情，不是交易建议）", ""]
            for hourly, asset, ctx in rows:
                payer = "多头付空头" if hourly > 0 else "空头付多头" if hourly < 0 else "当前接近 0"
                lines.append(f"{asset:<12} {hourly * 10_000:+8.4f} bps/小时  |  简单年化 {hourly * 24 * 365 * 100:+7.2f}%  |  {payer}")
                lines.append(f"{'':12} oracle {ctx['oracle']:.6g}  mark {ctx['mid']:.6g}  标记溢价 {ctx['premium'] * 10_000:+.3f} bps")
            if failures:
                lines += ["", "未能读取："] + failures
            lines += ["", "使用方式：只把“持续多周期、两边可成交、扣费后为正”的费率差列为研究对象。单个高费率常常意味着高波动、拥挤或流动性风险。"]
            self.after(0, lambda: self._write_text(self.funding_output, "\n".join(lines)))
        threading.Thread(target=work, daemon=True).start()

    def add_ev_record(self):
        try:
            event = self.ev_event.get().strip()
            model = float(self.ev_model.get()) / 100
            market = float(self.ev_market.get()) / 100
            stake = float(self.ev_stake.get())
            outcome = self.ev_outcome.get()
            if not event or not 0 < model < 1 or not 0 < market < 1 or stake <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("输入错误", "请填写比赛名称、0 到 100 之间的两种概率，以及正数模拟投入。")
            return
        # 买入价格为 market 的 YES 合约：每投入 1 USDC 的理论期望收益。
        expected = stake * (model / market - 1)
        realized = ""
        if outcome == "赢":
            realized = f"{stake * (1 / market - 1):.8f}"
        elif outcome == "输":
            realized = f"{-stake:.8f}"
        new_file = not EV_JOURNAL_FILE.exists()
        with EV_JOURNAL_FILE.open("a", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=("time", "event", "model_probability", "market_probability", "edge", "stake", "outcome", "expected_pnl", "realized_pnl"))
            if new_file:
                writer.writeheader()
            writer.writerow({"time": time.strftime("%Y-%m-%d %H:%M:%S"), "event": event,
                             "model_probability": model, "market_probability": market, "edge": model - market,
                             "stake": stake, "outcome": outcome, "expected_pnl": expected, "realized_pnl": realized})
        self.ev_event.set("")
        self.refresh_ev_summary()

    def refresh_ev_summary(self):
        if not EV_JOURNAL_FILE.exists():
            self._write_text(self.ev_output, "还没有记录。\n\n示例：你认为某队胜率 55%，市场 YES 价格是 45%，模拟投入 10 USDC。\n只有长期记录显示实际结果和模型优势一致，才说明可能存在正期望。")
            return
        try:
            with EV_JOURNAL_FILE.open("r", newline="", encoding="utf-8-sig") as handle:
                records = list(csv.DictReader(handle))
            settled = [row for row in records if row["outcome"] in ("赢", "输") and row["realized_pnl"]]
            expected = sum(float(row["expected_pnl"]) for row in records)
            realized = sum(float(row["realized_pnl"]) for row in settled)
            wins = sum(row["outcome"] == "赢" for row in settled)
            lines = [f"记录数：{len(records)}；已结算：{len(settled)}；待结算：{len(records) - len(settled)}",
                     f"全部记录的模型预期收益：${expected:+.2f}（不代表实际收益）",
                     f"已结算实际收益：${realized:+.2f}" + (f"；实际胜率：{wins / len(settled) * 100:.1f}%" if settled else ""), "",
                     "最近记录："]
            for row in records[-12:][::-1]:
                lines.append(f"{row['time']} | {row['event']} | 你估 {float(row['model_probability']) * 100:.1f}% / 市场 {float(row['market_probability']) * 100:.1f}% | 差 {float(row['edge']) * 100:+.1f}% | {row['outcome']} | 实际 {row['realized_pnl'] or '待结算'}")
            lines += ["", "提示：样本很少时，正收益可能只是运气。体育市场还受规则、流动性、结算争议和地区合规限制影响。"]
            self._write_text(self.ev_output, "\n".join(lines))
        except (OSError, KeyError, ValueError, csv.Error) as exc:
            self._write_text(self.ev_output, f"读取 Positive EV 日志失败：{exc}")

    def analyze_relations(self):
        assets = [item.strip() for item in self.relation_assets.get().split(",") if item.strip()]
        try:
            hours = int(self.relation_hours.get())
            if len(assets) < 2 or hours < 1:
                raise ValueError
        except ValueError:
            messagebox.showerror("输入错误", "至少输入两个合约，回看时间为正整数。")
            return
        self._write_text(self.relation_output, "正在读取历史 5 分钟 K 线并计算相关性…")

        def work():
            series, failures = {}, []
            for asset in assets:
                try:
                    series[asset] = hl_candles(asset, hours=hours)
                except (URLError, HTTPError, TimeoutError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                    failures.append(f"{asset}: {exc}")
            lines = [f"最近 {hours} 小时、5 分钟收盘价收益率相关性", ""]
            for left, right in itertools.combinations(series, 2):
                common = sorted(set(series[left]) & set(series[right]))
                left_prices = [series[left][t] for t in common]
                right_prices = [series[right][t] for t in common]
                left_returns = [math.log(left_prices[i] / left_prices[i - 1]) for i in range(1, len(left_prices))]
                right_returns = [math.log(right_prices[i] / right_prices[i - 1]) for i in range(1, len(right_prices))]
                corr = pearson(left_returns, right_returns)
                label = "样本不足 / 无有效波动" if corr is None else f"r = {corr:+.3f}"
                lines.append(f"{left:<12} ↔ {right:<12}  {label}  （共同 K 线 {len(common)} 根）")
            if failures:
                lines += ["", "未能读取："] + failures
            lines += ["", "如何使用：相关性高仅表示过去一起涨跌，不能证明谁领先谁。只有在大量滚动窗口都稳定、且加入可成交价与成本后仍成立，才值得做模拟交易。"]
            self.after(0, lambda: self._write_text(self.relation_output, "\n".join(lines)))
        threading.Thread(target=work, daemon=True).start()

    def scan_altcoin_links(self):
        leaders = [item.strip().upper() for item in self.scan_leaders.get().split(",") if item.strip()]
        assets = [item.strip().upper() for item in self.scan_assets.get().split(",") if item.strip()]
        try:
            hours = int(self.scan_hours.get())
            min_corr = float(self.scan_min_corr.get())
            min_z = float(self.scan_min_z.get())
            if not leaders or not assets or hours < 1 or not 0 <= min_corr <= 1 or min_z <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("输入错误", "请填写保护腿、小币列表；回看小时为正数，最低相关 0~1，最低偏离Z 为正数。")
            return
        self._write_text(self.relation_output, "正在扫描小币联动：读取 K 线、盘口、资金费…")

        def work():
            rows, failures = altcoin_scan_report(leaders, assets, hours=hours, min_corr=min_corr, min_z=min_z)
            content = format_altcoin_scan(rows, failures, hours=hours, min_z=min_z)
            self.after(0, lambda: self._write_text(self.relation_output, content))

        threading.Thread(target=work, daemon=True).start()

    def start_alt_monitor(self):
        if self.alt_monitor_running:
            return
        try:
            leaders = [item.strip().upper() for item in self.live_leaders.get().split(",") if item.strip()]
            assets = [item.strip().upper() for item in self.live_assets.get().split(",") if item.strip()]
            hours = int(self.live_hours.get())
            min_corr = float(self.live_min_corr.get())
            min_z = float(self.live_min_z.get())
            max_spread = float(self.live_max_spread.get())
            interval = max(30, int(float(self.live_interval.get())))
            if not leaders or not assets or hours < 1 or not 0 <= min_corr <= 1 or min_z <= 0 or max_spread <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("输入错误", "持续监控参数不正确。刷新秒最少按 30 秒处理。")
            return
        self.alt_monitor_running = True
        self._write_text(self.live_output, "已启动，正在读取第一轮小币联动数据…")

        def loop():
            while self.alt_monitor_running:
                started = time.time()
                try:
                    rows, failures = altcoin_scan_report(leaders, assets, hours=hours, min_corr=min_corr, min_z=min_z, max_spread_bps=max_spread)
                    content = format_altcoin_scan(rows, failures, hours=hours, min_z=min_z, title="小币持续监控")
                    content = f"更新时间：{time.strftime('%Y-%m-%d %H:%M:%S')}\n刷新间隔：{interval} 秒；最大点差：{max_spread:g} bps\n\n{content}"
                    self.after(0, lambda text=content: self._write_text(self.live_output, text))
                except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                    self.after(0, lambda err=str(exc): self._write_text(self.live_output, f"持续监控读取失败：{err}"))
                while self.alt_monitor_running and time.time() - started < interval:
                    time.sleep(0.5)

        threading.Thread(target=loop, daemon=True).start()

    def stop_alt_monitor(self):
        self.alt_monitor_running = False
        if hasattr(self, "live_output"):
            self._write_text(self.live_output, "已停止持续监控。")

    def read_server_latest(self):
        try:
            config = self.values()
            url = server_endpoint(config.get("server_url", ""), "/latest")
        except ValueError:
            messagebox.showerror("参数错误", "数值字段必须为数字。")
            return
        self._write_text(self.live_output, f"正在读取服务器：{url}")

        def work():
            try:
                data = get_json(url, timeout=10)
                text = self.format_server_latest(data)
                self.after(0, lambda: self._write_text(self.live_output, text))
            except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                self.after(0, lambda err=str(exc): self._write_text(self.live_output, f"读取服务器失败：{err}"))

        threading.Thread(target=work, daemon=True).start()

    def test_server_source(self):
        try:
            config = self.values()
            url = server_endpoint(config.get("server_url", ""), "/health")
        except ValueError:
            messagebox.showerror("参数错误", "数值字段必须为数字。")
            return
        self._write_text(self.api_output, f"正在测试服务器：{url}")

        def work():
            try:
                data = get_json(url, timeout=8)
                status = "正常" if data.get("ok") else "异常"
                latest = data.get("latest_ts")
                latest_text = "暂无采集结果" if not latest else time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(latest)))
                text = f"服务器连接{status}\nURL：{url}\n最新采集：{latest_text}\n错误：{data.get('error') or '无'}\n接口：{', '.join(data.get('endpoints', []))}"
                self.after(0, lambda: self._write_text(self.api_output, text))
            except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                self.after(0, lambda err=str(exc): self._write_text(self.api_output, f"服务器测试失败：{err}"))

        threading.Thread(target=work, daemon=True).start()

    def show_indicator_help(self):
        text = """这些指标的人话解释

偏离：
小币平时跟 BTC/ETH 一起走，但现在跑歪了。不是比价格高低，而是比涨跌关系。

Z：
跑歪程度。Z=0 基本正常；Z=+2 表示小币相对保护腿明显偏强；Z=-2 表示明显偏弱。绝对值越大，偏离越极端，但不代表一定马上回归。

Z 为正：
小币偏强。若赌回归，通常观察“做空小币 + 做多保护腿”。

Z 为负：
小币偏弱。若赌回归，通常观察“做多小币 + 做空保护腿”。

保护腿：
BTC 或 ETH。作用是抵消大盘方向影响，主赌的是“小币相对 BTC/ETH 的偏离回归”。

beta：
大概对冲比例。beta=0.82 表示做 1000 USDC 小币，保护腿大概做 820 USDC 反向。beta 会变，不是固定真理。

相关性 corr：
过去两者是否一起涨跌。越接近 1 越同步；太低说明关系弱，Z 再大也可能没意义。

观察 / 候选 / 谨慎：
观察 = 没过门槛，只记录。
候选 = 相关性、偏离等过门槛，可以纸面跟踪。
谨慎 = 有偏离但点差或数据质量不舒服，容易被滑点吃掉。

盘口点差：
买一和卖一之间的差。点差越大，交易成本越高。

资金费：
永续合约多空之间的定期付费。持仓久了会明显影响收益。

正确用法：
先记录候选，观察 5 分钟、30 分钟、2 小时后有没有回归；统计几十次，再看胜率、平均收益和最大反向亏损。"""
        messagebox.showinfo("指标说明", text)

    @staticmethod
    def format_server_latest(data):
        if not data.get("ok"):
            return f"服务器暂不可用：{data.get('error')}"
        rows = data.get("rows", [])
        if "scan" in data:
            scan = data["scan"]
            ts = scan.get("ts")
            cfg = {"hours": scan.get("hours"), "min_corr": scan.get("min_corr"), "min_z": scan.get("min_z")}
        else:
            ts = data.get("ts")
            cfg = data.get("config", {})
        time_text = "未知" if not ts else time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))
        lines = [f"服务器最新采集：{time_text}", f"来源：{data.get('source', 'server')}",
                 f"参数：回看 {cfg.get('hours')} 小时；最低相关 {cfg.get('min_corr')}；入场Z {cfg.get('min_z')}", ""]
        if not rows:
            lines.append("服务器还没有候选/观察记录。")
            return "\n".join(lines)
        for row in rows[:30]:
            tag_map = {"candidate": "候选", "watch": "观察", "caution": "谨慎"}
            tag = tag_map.get(row.get("tag"), row.get("tag", "观察"))
            funding = row.get("funding_hourly")
            funding_text = "资金费未知" if funding is None else f"资金费 {float(funding) * 10_000:+.3f} bps/小时"
            spread = row.get("spread_bps")
            spread_text = "点差未知" if spread is None else f"点差 {float(spread):.2f} bps"
            lines.append(f"[{tag}] {row.get('asset')} vs {row.get('leader')}  corr {float(row.get('corr')):+.3f}  beta {float(row.get('beta')):+.2f}  Z {float(row.get('zscore')):+.2f}")
            lines.append(f"     {spread_text}；{funding_text}；{row.get('plan')}")
        failures = data.get("failures") or []
        if failures:
            lines += ["", "服务器采集失败项："] + failures[:10]
        return "\n".join(lines)

    def run_pair_backtest(self):
        try:
            asset = self.bt_asset.get().strip().upper()
            hedge = self.bt_hedge.get().strip().upper()
            hours = int(self.bt_hours.get())
            entry_z = float(self.bt_entry_z.get())
            exit_z = float(self.bt_exit_z.get())
            rolling = int(self.bt_rolling.get())
            max_hold = int(self.bt_max_hold.get())
            fee_bps = float(self.bt_fee_bps.get())
            if not asset or not hedge or asset == hedge or hours < 2 or entry_z <= 0 or exit_z < 0 or rolling < 12 or max_hold < 1:
                raise ValueError
        except ValueError:
            messagebox.showerror("输入错误", "回测参数不正确。")
            return
        self._write_text(self.bt_output, "正在读取历史 K 线并回测…")

        def work():
            try:
                asset_series = hl_candles(asset, hours=hours)
                hedge_series = hl_candles(hedge, hours=hours)
                result = backtest_pair(asset_series, hedge_series, entry_z=entry_z, exit_z=exit_z,
                                       rolling=rolling, max_hold=max_hold, fee_bps=fee_bps)
                trades = result["trades"]
                lines = [f"回测：{asset} vs {hedge}，最近 {hours} 小时，5分钟K线", "",
                         f"相关性：{result['corr']:+.3f}；beta：{result['beta']:+.2f}",
                         f"交易次数：{len(trades)}；胜率：{result['win_rate'] * 100:.1f}%",
                         f"总收益：{result['total_bps']:+.1f} bps；平均每次：{result['avg_bps']:+.1f} bps；最差单次：{result['worst_bps']:+.1f} bps",
                         "",
                         "最近交易："]
                for trade in trades[-12:][::-1]:
                    side = "做空小币/做多保护腿" if trade["side"] == "short_asset" else "做多小币/做空保护腿"
                    lines.append(f"{side:<16} 入场Z {trade['entry_z']:+.2f} -> 出场Z {trade['exit_z']:+.2f}  持有 {trade['hold_bars']} 根K  收益 {trade['pnl']:+.1f} bps")
                if not trades:
                    lines.append("没有触发交易。可以降低入场Z或拉长回看时间，但不要为了凑结果而过拟合。")
                lines += ["", "注意：这是粗回测。它没有逐笔盘口、滑点、资金费变化、爆仓约束，也存在参数过拟合风险。"]
                self.after(0, lambda: self._write_text(self.bt_output, "\n".join(lines)))
            except (URLError, HTTPError, TimeoutError, RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
                self.after(0, lambda err=str(exc): self._write_text(self.bt_output, f"回测失败：{err}"))

        threading.Thread(target=work, daemon=True).start()

    def start(self):
        try:
            config = self.values()
            if config["reference_provider"] == "ffd_crypto_snapshot":
                left, right = base_asset(config["coin"]), config["ffd_crypto_id"].strip().upper()
                if left != right:
                    message = (
                        f"当前配置无效：DEX 合约是 {config['coin']}，但 FFD 标的是 {config['ffd_crypto_id']}。\n\n"
                        "FFD 加密快照只能同名比较，例如 BTC 对 BTC、ETH 对 ETH。\n"
                        "如果你要看黄金 xyz:GOLD，请把“基准模式”改成 hyperliquid_oracle，或接入真正的黄金外部报价 JSON。"
                    )
                    self.status.set("配置无效：FFD 必须同名比较")
                    self._write_text(self.output, message)
                    messagebox.showerror("配置无效", message)
                    return
                config["interval_seconds"] = max(60, int(config["interval_seconds"]))
            if self.monitor.running:
                self.monitor.stop()
            self.monitor = Monitor(config)
            self.monitor.start()
            if config["reference_provider"] == "ffd_crypto_snapshot":
                self.status.set("正在接收 HL 推送，并等待 FFD 快照…")
                self._write_text(self.output, "已启动。\n\n正在等待两类数据：\n1. Hyperliquid WebSocket 盘口推送\n2. FFD 低频价格快照\n\nFFD 首次读取可能要等几十秒；它没有买一/卖一，只能做研究，不是可执行套利价格。")
            else:
                self.status.set("正在接收 Hyperliquid WebSocket 实时推送…")
                self._write_text(self.output, "已启动。\n\n正在等待 Hyperliquid WebSocket 第一笔盘口和合约状态数据。")
        except ValueError:
            messagebox.showerror("参数错误", "数值字段必须为数字。")

    def refresh(self):
        latest, error, metrics = self.monitor.metrics()
        with self.monitor.lock:
            horizon = time.time() - int(self.monitor.config["window_minutes"]) * 60
            chart_samples = [s for s in self.monitor.samples if s["t"] >= horizon]
        self.draw_chart(chart_samples)
        if error:
            waiting = error.startswith("正在读取 FFD 快照")
            self.status.set(("等待外部源：" if waiting else "采集失败：") + error)
            if not latest:
                self._write_text(self.output, self.explain_waiting_error(error))
        elif self.monitor.running and not latest:
            self.status.set("已启动，正在等待第一笔可用行情…")
            self._write_text(self.output, self.explain_waiting_error("还没有收到完整的盘口和参考价格"))
        if latest:
            costs = int(self.monitor.config["round_trip_cost_bps"]) + int(self.monitor.config["extra_buffer_bps"])
            threshold = max(int(self.monitor.config["alert_bps"]), costs)
            alert = abs(latest["spread_bps"]) >= threshold
            lag = "—" if latest["server_lag_ms"] is None else f"{latest['server_lag_ms']:.0f} ms"
            observation_minutes = metrics["duration"] / 60
            min_minutes = self.monitor.config["min_observation_minutes"]
            gap_usd = latest["dex"] - latest["ref"]
            threshold_usd = latest["ref"] * threshold / 10_000
            provider = self.monitor.config["reference_provider"]
            if provider == "hyperliquid_oracle":
                verdict = "仅作行情体检：参考价不能直接买卖，因此现在绝不是可执行套利。"
            elif provider == "ffd_crypto_snapshot":
                verdict = "FFD 仅作低频研究快照：没有买一/卖一，不能用于价差套利或延迟交易。"
            elif observation_minutes < min_minutes:
                verdict = f"继续观察：已记录 {observation_minutes:.0f}/{min_minutes} 分钟，不能凭短期数据判断。"
            elif metrics["correlation"] is None or metrics["correlation"] < 0.80:
                verdict = "暂不考虑：两边的价格变化还没有证明能稳定同步。"
            elif not alert:
                verdict = "正常：价差没有覆盖你填入的成本和安全垫。"
            else:
                verdict = "值得人工核对：确认外部价格可交易、两边买卖价和数量后再决定；程序不会下单。"
            if metrics["correlation"] is None:
                sync = f"暂不判断同步性：{metrics['reason']}（样本 {metrics['n']} 个）。"
            else:
                sync = f"同步程度 {metrics['correlation']:.2f}（接近 1 表示最近走势更接近；不代表必赚）。"
            fund_cash = 1000 * latest["funding_hourly"]
            fund_text = (f"正资金费：每持有 1,000 USDC 的多头，每小时约付 ${fund_cash:.4f} 给空头。"
                         if fund_cash >= 0 else f"负资金费：每持有 1,000 USDC 的空头，每小时约付 ${abs(fund_cash):.4f} 给多头。")
            ref_quote = "外部基准未提供买一/卖一；只能研究，不能计算真实可成交价差。"
            if latest.get("ref_bid") is not None and latest.get("ref_ask") is not None:
                ref_quote = f"外部基准买一/卖一：{latest['ref_bid']:.6g} / {latest['ref_ask']:.6g}；可进入可成交价差核对。"
            text = f"""现在的结论：{verdict}

市场交易价：{latest['dex']:.2f} USDC      参考价格：{latest['ref']:.2f} USDC
现在相差：${gap_usd:+.2f}（{latest['spread_bps'] / 100:+.4f}%）
你的成本线：至少相差 ${threshold_usd:.2f}（{threshold / 100:.2f}%）才值得继续检查。

数据是否可靠：{sync}
已连续观察：{observation_minutes:.1f} 分钟；保守筛选最少需要 {min_minutes} 分钟。

资金费说明：{fund_text}
资金费会改变，不能因为它为正或为负就直接开仓。

图表怎么读：蓝线=Hyperliquid 市场价，橙线=参考价；两线接近是正常。
下方紫线=两者差价；碰到红色虚线，才表示价差超过你设置的成本线。

外部数据质量：{ref_quote}

技术信息：买一/卖一 {latest['bid']:.2f} / {latest['ask']:.2f}；盘口点差 {latest['book_bps']:.2f} bps；最新盘口数据年龄 {lag}；HTTP 轮询已关闭，正在使用 WebSocket 推送。
"""
            self.output.configure(state="normal")
            self.output.delete("1.0", "end")
            self.output.insert("1.0", text)
            self.output.configure(state="disabled")
        self.after(1000, self.refresh)

    def explain_waiting_error(self, error):
        provider = self.monitor.config.get("reference_provider")
        coin = self.monitor.config.get("coin")
        ffd_id = self.monitor.config.get("ffd_crypto_id", "")
        if provider == "ffd_crypto_snapshot" and "同名" in error:
            return (
                f"现在没有图，是因为配置无效。\n\nDEX 合约：{coin}\nFFD 标的：{ffd_id}\n\n"
                "FFD 加密快照只能同名比较：BTC 对 BTC、ETH 对 ETH。\n"
                "黄金 xyz:GOLD 不是 FFD 加密标的 BTC。请点“一键 BTC 对 BTC”，或点“GOLD 用 HL 预言机”。"
            )
        if provider == "ffd_crypto_snapshot":
            return (
                f"正在等待 FFD 返回第一笔快照。\n\n当前标的：{ffd_id}\n状态：{error}\n\n"
                "这个源是低频研究源，可能几十秒才更新一次；它没有买一/卖一，所以不能用于延迟套利。"
            )
        return f"正在等待第一笔可用行情。\n\n状态：{error}"

    def draw_chart(self, samples):
        """不依赖第三方库，用 Canvas 画两条归一化价格曲线和价差曲线。"""
        canvas = self.chart
        canvas.delete("all")
        width = max(canvas.winfo_width(), 800)
        left, right = 68, width - 18
        top1, bottom1, top2, bottom2 = 28, 158, 198, 278
        canvas.create_text(left, 12, text="走势：蓝线=市场价，橙线=参考价；两线接近属于正常", anchor="w", fill="#333333", font=("Microsoft YaHei UI", 10))
        canvas.create_text(left, 178, text="价差：紫线接近 0 = 正常；红色虚线 = 你设定的成本线", anchor="w", fill="#333333", font=("Microsoft YaHei UI", 10))
        if not samples:
            canvas.create_text(width / 2, 145, text="等待第一笔行情…", fill="#666666", font=("Microsoft YaHei UI", 13))
            return
        base_dex, base_ref = samples[0]["dex"], samples[0]["ref"]
        dex = [100 * s["dex"] / base_dex for s in samples]
        reference = [100 * s["ref"] / base_ref for s in samples]
        spreads = [s["spread_bps"] for s in samples]
        pmin, pmax = min(dex + reference), max(dex + reference)
        pad = max((pmax - pmin) * 0.15, 0.003)
        pmin, pmax = pmin - pad, pmax + pad
        threshold = max(int(self.monitor.config["alert_bps"]), int(self.monitor.config["round_trip_cost_bps"]) + int(self.monitor.config["extra_buffer_bps"]))
        smin, smax = min(spreads + [-threshold, 0]), max(spreads + [threshold, 0])
        spad = max((smax - smin) * 0.15, 0.5)
        smin, smax = smin - spad, smax + spad

        def x(i):
            return left if len(samples) == 1 else left + (right - left) * i / (len(samples) - 1)
        def y(value, low, high, top, bottom):
            return bottom - (value - low) * (bottom - top) / (high - low)
        def line(values, low, high, top, bottom, colour, width=2, dash=None):
            if len(values) == 1:
                xx, yy = x(0), y(values[0], low, high, top, bottom)
                canvas.create_oval(xx - 3, yy - 3, xx + 3, yy + 3, fill=colour, outline=colour)
            else:
                points = []
                for i, value in enumerate(values):
                    points += [x(i), y(value, low, high, top, bottom)]
                canvas.create_line(*points, fill=colour, width=width, smooth=True, dash=dash)

        for yy in (top1, (top1 + bottom1) / 2, bottom1):
            canvas.create_line(left, yy, right, yy, fill="#e5e7eb")
        canvas.create_text(left - 5, top1, text=f"{pmax:.3f}", anchor="e", fill="#666")
        canvas.create_text(left - 5, bottom1, text=f"{pmin:.3f}", anchor="e", fill="#666")
        line(dex, pmin, pmax, top1, bottom1, "#2563eb")
        line(reference, pmin, pmax, top1, bottom1, "#ea580c")
        canvas.create_text(right, top1 - 5, text=f"最新：DEX {dex[-1]:.4f} / 基准 {reference[-1]:.4f}", anchor="e", fill="#333")

        for value, colour, dash in ((0, "#9ca3af", (3, 3)), (threshold, "#dc2626", (4, 3)), (-threshold, "#dc2626", (4, 3))):
            yy = y(value, smin, smax, top2, bottom2)
            canvas.create_line(left, yy, right, yy, fill=colour, dash=dash)
        canvas.create_text(left - 5, y(threshold, smin, smax, top2, bottom2), text=f"+{threshold}", anchor="e", fill="#dc2626")
        canvas.create_text(left - 5, y(0, smin, smax, top2, bottom2), text="0", anchor="e", fill="#666")
        canvas.create_text(left - 5, y(-threshold, smin, smax, top2, bottom2), text=f"-{threshold}", anchor="e", fill="#dc2626")
        line(spreads, smin, smax, top2, bottom2, "#7c3aed", width=2)
        canvas.create_text(right, bottom2 + 12, text=f"{len(samples)} 个样本  |  当前 {spreads[-1]:+.2f} bps", anchor="e", fill="#333")

    def close(self):
        self.alt_monitor_running = False
        self.monitor.stop()
        self.destroy()


def build_server_config(args):
    return {
        "leaders": split_symbols(args.leaders),
        "assets": split_symbols(args.assets),
        "hours": int(args.hours),
        "min_corr": float(args.min_corr),
        "min_z": float(args.min_z),
        "max_spread_bps": None if args.max_spread <= 0 else float(args.max_spread),
        "interval": max(30, int(args.interval)),
        "dingtalk_webhook": env_text("DINGTALK_WEBHOOK", ""),
        "dingtalk_keyword": env_text("DINGTALK_KEYWORD", args.dingtalk_keyword),
        "dingtalk_paper_webhook": env_text("DINGTALK_PAPER_WEBHOOK", env_text("DINGTALK_WEBHOOK", "")),
        "dingtalk_paper_keyword": env_text("DINGTALK_PAPER_KEYWORD", env_text("DINGTALK_KEYWORD", args.dingtalk_keyword)),
        "dingtalk_live_webhook": env_text("DINGTALK_LIVE_WEBHOOK", ""),
        "dingtalk_live_keyword": env_text("DINGTALK_LIVE_KEYWORD", "小测试"),
        "notify_cooldown": env_int("NOTIFY_COOLDOWN", int(args.notify_cooldown)),
        "notify_candidate_open": env_bool("NOTIFY_CANDIDATE_OPEN", True),
        "notify_candidate_repeat": env_bool("NOTIFY_CANDIDATE_REPEAT", False),
        "notify_candidate_resolved": env_bool("NOTIFY_CANDIDATE_RESOLVED", False),
        "notify_caution": env_bool("NOTIFY_CAUTION", False),
        "notify_paper_open": env_bool("NOTIFY_PAPER_OPEN", True),
        "notify_paper_close": env_bool("NOTIFY_PAPER_CLOSE", True),
        "notify_leadlag_open": env_bool("NOTIFY_LEADLAG_OPEN", True),
        "notify_leadlag_close": env_bool("NOTIFY_LEADLAG_CLOSE", True),
        "notify_live_test": env_bool("NOTIFY_LIVE_TEST", True),
        "notify_live_open": env_bool("NOTIFY_LIVE_OPEN", True),
        "notify_live_close": env_bool("NOTIFY_LIVE_CLOSE", True),
        "notify_live_error": env_bool("NOTIFY_LIVE_ERROR", True),
        "notify_candidate_max_per_scan": env_int("NOTIFY_CANDIDATE_MAX_PER_SCAN", 1),
        "notify_candidate_min_z": env_float("NOTIFY_CANDIDATE_MIN_Z", 3.0),
        "public_url": env_text("HLM_PUBLIC_URL", args.public_url),
        "admin_token": os.environ.get("HLM_ADMIN_TOKEN") or os.environ.get("ADMIN_TOKEN") or args.admin_token,
        "min_volume": float(args.min_volume),
        "max_assets": int(args.max_assets),
        "paper_enabled": env_bool("PAPER_ENABLED", bool(args.paper_enabled)),
        "paper_sync_live": env_bool("PAPER_SYNC_LIVE", True),
        "paper_notional_usdc": env_float("PAPER_NOTIONAL_USDC", float(args.paper_notional)),
        "paper_exit_z": env_float("PAPER_EXIT_Z", float(args.paper_exit_z)),
        "paper_take_profit_bps": env_float("PAPER_TAKE_PROFIT_BPS", float(args.paper_take_profit_bps)),
        "paper_stop_bps": env_float("PAPER_STOP_BPS", float(args.paper_stop_bps)),
        "paper_max_hold_minutes": env_int("PAPER_MAX_HOLD_MINUTES", int(args.paper_max_hold)),
        "paper_max_open": env_int("PAPER_MAX_OPEN", int(args.paper_max_open)),
        "paper_fee_bps": env_float("PAPER_FEE_BPS", float(args.paper_fee_bps)),
        "paper_z_value_bps": env_float("PAPER_Z_VALUE_BPS", float(args.paper_z_value_bps)),
        "paper_min_corr": env_float("PAPER_MIN_CORR", float(args.paper_min_corr)),
        "live_enabled": env_bool("LIVE_ENABLED", False),
        "live_account_poll_seconds": env_float("LIVE_ACCOUNT_POLL_SECONDS", 3.0),
        "live_account_cache_max_age_ms": env_float("LIVE_ACCOUNT_CACHE_MAX_AGE_MS", 60_000.0),
        "live_leverage_cache_seconds": env_float("LIVE_LEVERAGE_CACHE_SECONDS", 86_400.0),
        "live_account_address": os.environ.get("HLM_ACCOUNT_ADDRESS") or env_file_value("HLM_ACCOUNT_ADDRESS"),
        "live_notional_usdc": env_float("LIVE_NOTIONAL_USDC", 10.0),
        "live_max_open": env_int("LIVE_MAX_OPEN", 1),
        "live_reentry_cooldown_minutes": env_int("LIVE_REENTRY_COOLDOWN_MINUTES", 15),
        "live_max_slippage_bps": env_float("LIVE_MAX_SLIPPAGE_BPS", 15.0),
        "live_leverage": env_int("LIVE_LEVERAGE", 1),
        "live_auto_min_notional": env_bool("LIVE_AUTO_MIN_NOTIONAL", False),
        "live_min_entry_z": env_float("LIVE_MIN_ENTRY_Z", 3.0),
        "live_min_corr": env_float("LIVE_MIN_CORR", 0.75),
        "live_min_expected_edge_bps": env_float("LIVE_MIN_EXPECTED_EDGE_BPS", 25.0),
        "live_use_l2book": env_bool("LIVE_USE_L2BOOK", True),
        "live_l2_max_age_ms": env_float("LIVE_L2_MAX_AGE_MS", 3000.0),
        "live_strategy_entry_grace_ms": env_float("LIVE_STRATEGY_ENTRY_GRACE_MS", 10_000.0),
        "live_l2_max_spread_bps": env_float(
            "LIVE_L2_MAX_SPREAD_BPS", env_float("LIVE_MAX_ENTRY_SPREAD_BPS", 2.5),
        ),
        "live_use_realtime_z": env_bool("LIVE_USE_REALTIME_Z", True),
        "live_realtime_strategy_interval_ms": env_float("LIVE_REALTIME_STRATEGY_INTERVAL_MS", 500.0),
        "live_require_leverage_ok": env_bool("LIVE_REQUIRE_LEVERAGE_OK", True),
        "live_strategy_enabled": env_bool("LIVE_STRATEGY_ENABLED", False),
        "leadlag_enabled": env_bool("LEADLAG_ENABLED", True),
        "leadlag_notional_usdc": env_float("LEADLAG_NOTIONAL_USDC", 20.0),
        "leadlag_max_open": env_int("LEADLAG_MAX_OPEN", 3),
        "leadlag_leaders": env_text("LEADLAG_LEADERS", "BTC,ETH,SOL,HYPE,DOGE,BNB"),
        "leadlag_leader_3s_bps": env_float("LEADLAG_LEADER_3S_BPS", 2.0),
        "leadlag_leader_15s_bps": env_float("LEADLAG_LEADER_15S_BPS", 4.0),
        "leadlag_min_lag_bps": env_float("LEADLAG_MIN_LAG_BPS", 6.0),
        "leadlag_min_corr": env_float("LEADLAG_MIN_CORR", 0.60),
        "leadlag_max_spread_bps": env_float("LEADLAG_MAX_SPREAD_BPS", 2.5),
        "leadlag_max_data_age_ms": env_float("LEADLAG_MAX_DATA_AGE_MS", 10_000.0),
        "leadlag_min_imbalance": env_float("LEADLAG_MIN_IMBALANCE", -1.0),
        "leadlag_min_depth_multiple": env_float("LEADLAG_MIN_DEPTH_MULTIPLE", 2.0),
        "leadlag_fee_bps": env_float("LEADLAG_FEE_BPS", 5.0),
        "leadlag_min_edge_bps": env_float("LEADLAG_MIN_EDGE_BPS", 6.0),
        "leadlag_take_profit_bps": env_float("LEADLAG_TAKE_PROFIT_BPS", 18.0),
        "leadlag_stop_bps": env_float("LEADLAG_STOP_BPS", 14.0),
        "leadlag_trail_start_bps": env_float("LEADLAG_TRAIL_START_BPS", 8.0),
        "leadlag_trail_gap_bps": env_float("LEADLAG_TRAIL_GAP_BPS", 6.0),
        "leadlag_max_hold_minutes": env_int("LEADLAG_MAX_HOLD_MINUTES", 8),
        "leadlag_cooldown_minutes": env_int("LEADLAG_COOLDOWN_MINUTES", 8),
    }


def run_server(args):
    config = build_server_config(args)
    db_path = Path(args.db).resolve()
    init_alt_db(db_path)
    reconcile_shared_paper_from_live(db_path, config.get("paper_fee_bps", 9.0))
    state = AltServerState(config, db_path=db_path)
    state.live_account = load_latest_live_account_snapshot(db_path)
    state.l2book.set_coins(configured_ws_leaders(config))
    state.l2book.start()
    threading.Thread(target=live_account_cache_loop, args=(state,), daemon=True).start()
    threading.Thread(target=reconcile_live_trade_costs, args=(state,), daemon=True).start()
    threading.Thread(target=collector_loop, args=(state,), daemon=True).start()
    threading.Thread(target=realtime_strategy_loop, args=(state,), daemon=True).start()
    server = ThreadingHTTPServer((args.host, int(args.port)), AltRequestHandler)
    server.state = state
    print("Hyperliquid 小币联动采集服务器已启动", flush=True)
    print(f"HTTP: http://{args.host}:{args.port}", flush=True)
    print(f"DB: {db_path}", flush=True)
    print(f"leaders={','.join(config['leaders'])} assets={','.join(config['assets'])}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("正在停止服务器...", flush=True)
    finally:
        state.running = False
        state.l2book.stop()
        server.server_close()


def parse_args():
    parser = argparse.ArgumentParser(description="Hyperliquid 机会研究台 / 小币联动采集服务器")
    parser.add_argument("--server", action="store_true", help="启动只读采集服务器；不打开 GUI")
    parser.add_argument("--host", default="0.0.0.0", help="服务器监听地址，默认 0.0.0.0")
    parser.add_argument("--port", type=int, default=8787, help="服务器端口，默认 8787")
    parser.add_argument("--interval", type=int, default=60, help="采集间隔秒，默认 60，最低 30")
    parser.add_argument("--hours", type=int, default=24, help="每轮扫描回看小时，默认 24")
    parser.add_argument("--leaders", default=DEFAULT_ALT_LEADERS, help="保护腿，逗号分隔")
    parser.add_argument("--assets", default=DEFAULT_ALT_ASSETS, help="候选小币，逗号分隔")
    parser.add_argument("--min-corr", type=float, default=0.65, help="最低相关性，默认 0.65")
    parser.add_argument("--min-z", type=float, default=2.0, help="候选入场 Z，默认 2.0")
    parser.add_argument("--max-spread", type=float, default=8.0, help="最大盘口点差 bps；<=0 表示不过滤")
    parser.add_argument("--min-volume", type=float, default=0.0, help="最低 24h 名义成交额；默认 0 表示全部")
    parser.add_argument("--max-assets", type=int, default=0, help="最多扫描多少个合约；默认 0 表示不限制")
    parser.add_argument("--db", default=str(ALT_DB_FILE), help="SQLite 保存路径")
    parser.add_argument("--dingtalk-keyword", default="小测试", help="钉钉机器人安全关键词，默认 小测试")
    parser.add_argument("--notify-cooldown", type=int, default=1800, help="同一币对同一方向推送冷却秒数，默认 1800")
    parser.add_argument("--public-url", default="", help="公网访问地址，例如 http://服务器/hl；用于钉钉消息里的看图链接")
    parser.add_argument("--admin-token", default="", help="管理口令；用于网页修改模拟盘参数，建议用 HLM_ADMIN_TOKEN 环境变量")
    parser.add_argument("--paper-enabled", action=argparse.BooleanOptionalAction, default=True, help="开启模拟盘，默认开启")
    parser.add_argument("--paper-notional", type=float, default=DEFAULT_PAPER_NOTIONAL, help="每笔模拟名义本金 USDC，默认 1000")
    parser.add_argument("--paper-exit-z", type=float, default=0.5, help="模拟平仓回归阈值，默认 |Z|<=0.5")
    parser.add_argument("--paper-take-profit-bps", type=float, default=50.0, help="模拟固定止盈 bps，默认 50；0 表示关闭")
    parser.add_argument("--paper-stop-bps", type=float, default=80.0, help="模拟止损 bps，默认 80")
    parser.add_argument("--paper-max-hold", type=int, default=360, help="模拟最长持仓分钟，默认 360")
    parser.add_argument("--paper-max-open", type=int, default=12, help="最多同时模拟持仓数量，默认 12")
    parser.add_argument("--paper-fee-bps", type=float, default=9.0, help="模拟双腿往返手续费/额外滑点成本 bps，默认 9")
    parser.add_argument("--paper-z-value-bps", type=float, default=18.0, help="每 1 个 Z 回归折算多少 bps，默认 18")
    parser.add_argument("--paper-min-corr", type=float, default=0.65, help="模拟盘最低相关性风控基准，默认 0.65")
    parser.add_argument("--set-live-api-key", action="store_true", help="通过终端交互加密保存 API 钱包私钥；不会显示或写入 shell 历史")
    parser.add_argument("--change-live-api-key", action="store_true", help="替换加密 API 私钥；必须输入旧私钥")
    parser.add_argument("--generate-live-api-key", action="store_true", help="在服务器生成专用 API 钱包，仅输出需要授权的公开地址")
    return parser.parse_args()


def run_gui():
    if ttk is None or messagebox is None:
        raise SystemExit("当前 Python 环境没有 tkinter，不能打开 GUI；服务器模式请使用 --server。")
    App().mainloop()


if __name__ == "__main__":
    cli_args = parse_args()
    if cli_args.generate_live_api_key:
        generate_live_api_key_cli()
    elif cli_args.set_live_api_key or cli_args.change_live_api_key:
        configure_live_api_key_cli(change=cli_args.change_live_api_key)
    elif cli_args.server:
        run_server(cli_args)
    else:
        run_gui()
