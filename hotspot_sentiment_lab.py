#!/usr/bin/env python3
"""Historical news-heat research with causal next-bar execution.

The module downloads public trend/news and price data, stores research runs,
and never submits real or paper orders.
"""

from __future__ import annotations

import json
import math
import sqlite3
import statistics
import threading
import time
import xml.etree.ElementTree as ET
from contextlib import closing
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from urllib.parse import quote
from urllib.request import ProxyHandler
from urllib.request import Request
from urllib.request import build_opener
from urllib.request import urlopen

from multi_asset_portfolio_lab import fetch_yahoo_ohlcv


GOOGLE_TRENDS_RSS = "https://trends.google.com/trending/rss?geo={geo}"
GDELT_TIMELINE = "https://api.gdeltproject.org/api/v2/doc/doc"
LOCAL_PROXY = "http://127.0.0.1:7891"
PROXY_OPENER = build_opener(ProxyHandler({"http": LOCAL_PROXY, "https": LOCAL_PROXY}))
GDELT_LOCK = threading.Lock()
GDELT_LAST_REQUEST = 0.0

DEFAULT_PARAMS = {
    "baseline_days": 20,
    "heat_z": 2.0,
    "heat_acceleration": 1.8,
    "volume_ratio": 1.2,
    "max_5d_pump_pct": 20.0,
    "entry_slices": 3,
    "scale_days": 3,
    "stop_pct": 8.0,
    "take_profit_pct": 18.0,
    "trail_pct": 7.0,
    "max_hold_days": 12,
    "exit_heat_z": 0.5,
    "round_trip_cost_bps": 30.0,
    "capital": 10_000.0,
}

ASSET_RULES = (
    (("qwen", "通义千问"), ("BABA", "9988.HK")),
    (("deepseek",), ("BABA", "0700.HK")),
    (("nvidia", "英伟达", "gpu"), ("NVDA",)),
    (("bitcoin", "比特币"), ("BTC-USD",)),
    (("ethereum", "以太坊"), ("ETH-USD",)),
    (("solana",), ("SOL-USD",)),
    (("dogecoin", "狗狗币"), ("DOGE-USD",)),
    (("xrp", "ripple"), ("XRP-USD",)),
    (("telegram", "toncoin"), ("TON11419-USD",)),
    (("trump coin", "trump meme"), ("TRUMP-OFFICIAL-USD",)),
    (("marvel",), ("DIS",)),
    (("starz",), ("STRZ",)),
    (("hk express", "cathay pacific", "国泰航空", "國泰航空"), ("0293.HK",)),
    (("tesla", "特斯拉"), ("TSLA",)),
    (("apple", "iphone", "苹果公司", "蘋果公司"), ("AAPL",)),
    (("microsoft", "微软", "微軟", "xbox"), ("MSFT",)),
    (("openai", "chatgpt"), ("MSFT",)),
    (("google", "youtube", "gemini"), ("GOOGL",)),
    (("amazon", "亚马逊", "亞馬遜"), ("AMZN",)),
    (("facebook", "instagram", "meta"), ("META",)),
    (("coinbase",), ("COIN",)),
    (("binance", "bnb"), ("BNB-USD",)),
)


def _download(url, *, timeout=45):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 HLM-Hotspot-Lab/1.0"})
    direct_error = None
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read()
    except OSError as exc:
        direct_error = exc
    try:
        with PROXY_OPENER.open(request, timeout=timeout) as response:
            return response.read()
    except OSError as proxy_error:
        raise RuntimeError(f"直连失败：{direct_error}；7891代理失败：{proxy_error}") from proxy_error


def _number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def normalize_params(params=None):
    source = {**DEFAULT_PARAMS, **(params or {})}

    def clamp(name, low, high):
        return max(float(low), min(float(high), float(source[name])))

    return {
        "baseline_days": int(clamp("baseline_days", 7, 120)),
        "heat_z": clamp("heat_z", 0, 10),
        "heat_acceleration": clamp("heat_acceleration", 0, 20),
        "volume_ratio": clamp("volume_ratio", 0, 20),
        "max_5d_pump_pct": clamp("max_5d_pump_pct", 0, 1000),
        "entry_slices": int(clamp("entry_slices", 1, 10)),
        "scale_days": int(clamp("scale_days", 1, 20)),
        "stop_pct": clamp("stop_pct", 0.1, 100),
        "take_profit_pct": clamp("take_profit_pct", 0.1, 1000),
        "trail_pct": clamp("trail_pct", 0.1, 100),
        "max_hold_days": int(clamp("max_hold_days", 1, 365)),
        "exit_heat_z": clamp("exit_heat_z", -10, 10),
        "round_trip_cost_bps": clamp("round_trip_cost_bps", 0, 1000),
        "capital": clamp("capital", 100, 100_000_000),
    }


def suggest_assets(term):
    lowered = str(term).lower()
    for needles, assets in ASSET_RULES:
        if any(needle in lowered for needle in needles):
            return list(assets)
    return []


def parse_mappings(text):
    mappings = []
    for block in str(text or "").replace("\n", ";").split(";"):
        block = block.strip()
        if not block:
            continue
        separator = "=" if "=" in block else ":" if ":" in block else None
        if separator is None:
            raise ValueError(f"映射缺少等号：{block}")
        keyword, assets_text = block.split(separator, 1)
        keyword = keyword.strip()[:80]
        assets = list(dict.fromkeys(
            item.strip().upper() for item in assets_text.split(",") if item.strip()
        ))
        if not keyword or not assets:
            raise ValueError(f"映射不完整：{block}")
        for asset in assets:
            mappings.append({"keyword": keyword, "asset": asset})
    if not mappings:
        raise ValueError("至少填写一组“热点词=资产代码”")
    return mappings[:20]


def fetch_google_hotspots(geos=("US", "HK"), *, limit=20):
    namespace = "https://trends.google.com/trending/rss"
    rows, failures, seen = [], [], set()
    for geo in list(dict.fromkeys(str(item).strip().upper() for item in geos if str(item).strip()))[:5]:
        try:
            root = ET.fromstring(_download(GOOGLE_TRENDS_RSS.format(geo=quote(geo))).decode("utf-8"))
            for item in root.findall("./channel/item"):
                title = (item.findtext("title") or "").strip()
                key = (geo, title.lower())
                if not title or key in seen:
                    continue
                seen.add(key)
                news = [
                    (node.findtext(f"{{{namespace}}}news_item_title") or "").strip()
                    for node in item.findall(f"{{{namespace}}}news_item")
                ]
                rows.append({
                    "geo": geo,
                    "title": title,
                    "traffic": (item.findtext(f"{{{namespace}}}approx_traffic") or "-").strip(),
                    "published": (item.findtext("pubDate") or "").strip(),
                    "news_titles": [value for value in news if value][:3],
                    "suggested_assets": suggest_assets(title),
                })
        except (ET.ParseError, RuntimeError, OSError, UnicodeDecodeError) as exc:
            failures.append(f"Google Trends {geo}：{exc}")
    return rows[:max(1, int(limit))], failures


def fetch_gdelt_timeline(keyword, start_date, end_date):
    global GDELT_LAST_REQUEST
    query = quote(str(keyword).strip(), safe='"()')
    start_text = str(start_date).replace("-", "") + "000000"
    end_text = str(end_date).replace("-", "") + "235959"
    url = (
        f"{GDELT_TIMELINE}?query={query}&mode=timelinevolraw&format=json"
        f"&startdatetime={start_text}&enddatetime={end_text}&maxrecords=250"
    )
    payload = None
    last_error = None
    for backoff in (0, 6, 12, 24):
        if backoff:
            time.sleep(backoff)
        with GDELT_LOCK:
            wait = 5.2 - (time.monotonic() - GDELT_LAST_REQUEST)
            if wait > 0:
                time.sleep(wait)
            try:
                payload = json.loads(_download(url, timeout=90).decode("utf-8"))
                last_error = None
            except (RuntimeError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                last_error = exc
            finally:
                GDELT_LAST_REQUEST = time.monotonic()
        if payload is not None:
            break
        if "429" not in str(last_error) and "limit requests" not in str(last_error).lower():
            break
    if payload is None:
        raise RuntimeError(f"GDELT新闻时间线失败：{last_error}")
    series = (payload.get("timeline") or [{}])[0].get("data") or []
    rows = []
    for item in series:
        try:
            day = datetime.strptime(item["date"][:8], "%Y%m%d").date().isoformat()
            count = _number(item.get("value"))
            universe = _number(item.get("norm"))
            rows.append({
                "date": day,
                "article_count": count,
                "news_per_million": count / universe * 1_000_000 if universe > 0 else 0.0,
            })
        except (KeyError, TypeError, ValueError):
            continue
    return rows


def build_heat_series(news_rows, baseline_days=20):
    baseline_days = max(7, int(baseline_days))
    result = []
    for index, row in enumerate(news_rows):
        history = [item["news_per_million"] for item in news_rows[max(0, index - baseline_days):index]]
        mean = statistics.fmean(history) if history else 0.0
        std = statistics.pstdev(history) if len(history) >= 2 else 0.0
        current = row["news_per_million"]
        zscore = (current - mean) / std if std > 1e-12 else (5.0 if current > mean and current > 0 else 0.0)
        acceleration = current / mean if mean > 1e-12 else (10.0 if current > 0 else 0.0)
        result.append({**row, "heat_z": zscore, "heat_acceleration": acceleration, "baseline": mean})
    return result


def _ema(values, period):
    alpha = 2 / (period + 1)
    output = []
    current = None
    for value in values:
        current = value if current is None else value * alpha + current * (1 - alpha)
        output.append(current)
    return output


def backtest_hotspot(price_rows, heat_rows, params=None):
    params = normalize_params(params)
    bars = sorted(price_rows, key=lambda row: row["ts"])
    if len(bars) < params["baseline_days"] + 10:
        raise ValueError(f"价格日线不足：{len(bars)}根")
    heat = {row["date"]: row for row in heat_rows}
    closes = [_number(row["close"]) for row in bars]
    volumes = [_number(row.get("volume")) for row in bars]
    ema20 = _ema(closes, 20)
    position = None
    realized = 0.0
    trades, curve = [], []
    cooldown_until = -1
    pending_entry = False
    pending_exit = None
    slice_fraction = 1 / params["entry_slices"]
    half_cost = params["round_trip_cost_bps"] / 20_000

    def position_return(close):
        invested = sum(item["fraction"] for item in position["entries"])
        gross = sum(item["fraction"] * (close / item["price"] - 1) for item in position["entries"])
        return gross / invested if invested else 0.0

    for index, bar in enumerate(bars):
        day = datetime.fromtimestamp(bar["ts"] / 1000, timezone.utc).date().isoformat()
        open_price, close = _number(bar["open"]), closes[index]
        if position and pending_exit:
            invested = sum(item["fraction"] for item in position["entries"])
            gross = sum(item["fraction"] * (open_price / item["price"] - 1) for item in position["entries"])
            realized_delta = gross - invested * half_cost
            trade_net = gross - invested * half_cost * 2
            realized += realized_delta
            trades.append({
                "entry_date": position["entry_date"], "exit_date": day,
                "entry_price": position["avg_price"], "exit_price": open_price,
                "exposure_pct": invested * 100, "return_pct": trade_net * 100,
                "pnl": params["capital"] * trade_net, "slices": len(position["entries"]),
                "reason": pending_exit,
            })
            position, pending_exit = None, None
            cooldown_until = index + 3
        if pending_entry and index > cooldown_until:
            if position is None:
                position = {"entry_date": day, "entry_index": index, "entries": [], "peak_return": 0.0}
            if (
                len(position["entries"]) < params["entry_slices"]
                and index - position["entry_index"] <= params["scale_days"]
            ):
                position["entries"].append({"price": open_price, "fraction": slice_fraction})
                invested = sum(item["fraction"] for item in position["entries"])
                position["avg_price"] = sum(
                    item["price"] * item["fraction"] for item in position["entries"]
                ) / invested
                realized -= slice_fraction * half_cost
        pending_entry = False
        current_heat = heat.get(day, {"heat_z": 0.0, "heat_acceleration": 0.0, "article_count": 0})
        volume_history = volumes[max(0, index - 20):index]
        volume_mean = statistics.fmean(volume_history) if volume_history else 0.0
        volume_ratio = volumes[index] / volume_mean if volume_mean > 0 else 0.0
        five_day_return = (close / closes[index - 5] - 1) * 100 if index >= 5 and closes[index - 5] else 0.0
        hot = (
            current_heat["heat_z"] >= params["heat_z"]
            and current_heat["heat_acceleration"] >= params["heat_acceleration"]
            and close > ema20[index]
            and volume_ratio >= params["volume_ratio"]
            and five_day_return <= params["max_5d_pump_pct"]
        )
        if hot and (position is None or index - position["entry_index"] <= params["scale_days"]):
            pending_entry = True
        if position:
            net_return = position_return(close)
            position["peak_return"] = max(position["peak_return"], net_return)
            held = index - position["entry_index"]
            if net_return <= -params["stop_pct"] / 100:
                pending_exit = "止损"
            elif net_return >= params["take_profit_pct"] / 100:
                pending_exit = "止盈"
            elif (
                position["peak_return"] > params["trail_pct"] / 100
                and net_return <= position["peak_return"] - params["trail_pct"] / 100
            ):
                pending_exit = "移动止盈"
            elif held >= 2 and current_heat["heat_z"] < params["exit_heat_z"]:
                pending_exit = "热点降温"
            elif held >= params["max_hold_days"]:
                pending_exit = "最长持有"
        unrealized = 0.0
        if position:
            unrealized = sum(item["fraction"] * (close / item["price"] - 1) for item in position["entries"])
        curve.append({
            "ts": bar["ts"], "date": day, "equity": params["capital"] * (1 + realized + unrealized),
            "heat_z": current_heat["heat_z"], "price": close,
        })
    if position:
        final = bars[-1]
        final_price = closes[-1]
        invested = sum(item["fraction"] for item in position["entries"])
        gross = sum(item["fraction"] * (final_price / item["price"] - 1) for item in position["entries"])
        realized_delta = gross - invested * half_cost
        trade_net = gross - invested * half_cost * 2
        realized += realized_delta
        trades.append({
            "entry_date": position["entry_date"],
            "exit_date": datetime.fromtimestamp(final["ts"] / 1000, timezone.utc).date().isoformat(),
            "entry_price": position["avg_price"], "exit_price": final_price,
            "exposure_pct": invested * 100, "return_pct": trade_net * 100,
            "pnl": params["capital"] * trade_net, "slices": len(position["entries"]), "reason": "回测结束",
        })
        curve[-1]["equity"] = params["capital"] * (1 + realized)
    peak = params["capital"]
    max_drawdown = 0.0
    for point in curve:
        peak = max(peak, point["equity"])
        max_drawdown = min(max_drawdown, point["equity"] / peak - 1)
    wins = sum(item["pnl"] > 0 for item in trades)
    buy_hold = (closes[-1] / closes[0] - 1) * 100 if closes[0] else 0.0
    return {
        "trades": trades, "trade_count": len(trades),
        "win_rate": wins / len(trades) if trades else 0.0,
        "net_return_pct": realized * 100, "net_pnl": params["capital"] * realized,
        "max_drawdown_pct": max_drawdown * 100, "buy_hold_pct": buy_hold,
        "equity_curve": curve, "signals": sum(
            row["heat_z"] >= params["heat_z"] and row["heat_acceleration"] >= params["heat_acceleration"]
            for row in heat_rows
        ),
    }


def init_hotspot_db(db_path):
    with closing(sqlite3.connect(db_path)) as db:
        with db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS hotspot_backtest_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
                    request_json TEXT NOT NULL, payload_json TEXT NOT NULL
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_hotspot_runs_ts ON hotspot_backtest_runs(ts)")
            db.execute("""
                CREATE TABLE IF NOT EXISTS hotspot_topic_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
                    geo TEXT NOT NULL, title TEXT NOT NULL, payload_json TEXT NOT NULL
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS hotspot_news_cache (
                    keyword TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
                    fetched_ts REAL NOT NULL, payload_json TEXT NOT NULL,
                    PRIMARY KEY(keyword, start_date, end_date)
                )
            """)


def save_hotspot_topics(db_path, rows, *, now=None):
    init_hotspot_db(db_path)
    now = float(now if now is not None else time.time())
    with closing(sqlite3.connect(db_path)) as db:
        with db:
            db.executemany(
                "INSERT INTO hotspot_topic_snapshots(ts,geo,title,payload_json) VALUES(?,?,?,?)",
                [(now, row["geo"], row["title"], json.dumps(row, ensure_ascii=False)) for row in rows],
            )
            db.execute("DELETE FROM hotspot_topic_snapshots WHERE ts<?", (now - 30 * 86_400,))


def load_latest_hotspot_topics(db_path):
    init_hotspot_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        latest = db.execute("SELECT MAX(ts) FROM hotspot_topic_snapshots").fetchone()[0]
        if latest is None:
            return []
        records = db.execute(
            "SELECT payload_json FROM hotspot_topic_snapshots WHERE ts=? ORDER BY id", (latest,)
        ).fetchall()
    rows = []
    for (payload_json,) in records:
        try:
            value = json.loads(payload_json)
            if isinstance(value, dict):
                rows.append(value)
        except (json.JSONDecodeError, TypeError):
            continue
    return rows


def load_latest_hotspot_run(db_path):
    init_hotspot_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        row = db.execute(
            "SELECT payload_json FROM hotspot_backtest_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return json.loads(row[0]) if row else None


def load_hotspot_news_cache(db_path, keyword, start_date, end_date):
    init_hotspot_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        row = db.execute("""
            SELECT payload_json FROM hotspot_news_cache
            WHERE keyword=? AND start_date=? AND end_date=?
        """, (keyword, str(start_date), str(end_date))).fetchone()
    if not row:
        return None
    try:
        value = json.loads(row[0])
        return value if isinstance(value, list) else None
    except (json.JSONDecodeError, TypeError):
        return None


def save_hotspot_news_cache(db_path, keyword, start_date, end_date, rows):
    init_hotspot_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        with db:
            db.execute("""
                INSERT INTO hotspot_news_cache(keyword,start_date,end_date,fetched_ts,payload_json)
                VALUES(?,?,?,?,?)
                ON CONFLICT(keyword,start_date,end_date) DO UPDATE SET
                    fetched_ts=excluded.fetched_ts, payload_json=excluded.payload_json
            """, (
                keyword, str(start_date), str(end_date), time.time(),
                json.dumps(rows, ensure_ascii=False),
            ))


def run_hotspot_backtest(db_path, mappings_text, start_date, end_date, params=None):
    params = normalize_params(params)
    mappings = parse_mappings(mappings_text)
    start = date.fromisoformat(str(start_date))
    end = date.fromisoformat(str(end_date))
    if end <= start or (end - start).days > 730:
        raise ValueError("回测日期必须前后正确，且单次最多730天")
    news_cache, rows, failures = {}, [], []
    years = max(2.0, (date.today() - start).days / 365.25 + 0.2)
    for mapping in mappings:
        keyword, asset = mapping["keyword"], mapping["asset"]
        try:
            if keyword not in news_cache:
                raw_news = load_hotspot_news_cache(db_path, keyword, start, end)
                if raw_news is None:
                    raw_news = fetch_gdelt_timeline(keyword, start, end)
                    save_hotspot_news_cache(db_path, keyword, start, end, raw_news)
                news_cache[keyword] = build_heat_series(raw_news, params["baseline_days"])
            prices = [
                row for row in fetch_yahoo_ohlcv(asset, years=min(20, years))
                if start <= datetime.fromtimestamp(row["ts"] / 1000, timezone.utc).date() <= end
            ]
            result = backtest_hotspot(prices, news_cache[keyword], params)
            rows.append({"keyword": keyword, "asset": asset, **result})
        except (RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError) as exc:
            failures.append({"keyword": keyword, "asset": asset, "error": str(exc)})
    payload = {
        "ok": bool(rows), "ts": time.time(), "start_date": start.isoformat(),
        "end_date": end.isoformat(), "mappings": mappings, "params": params,
        "rows": rows, "failures": failures,
        "source": "GDELT全球新闻日线 + Yahoo Finance日线；信号收盘确认、次日开盘执行",
        "note": "这是相关性研究，不证明热点导致价格上涨；未接真实或模拟自动下单。",
    }
    init_hotspot_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        with db:
            cursor = db.execute(
                "INSERT INTO hotspot_backtest_runs(ts,request_json,payload_json) VALUES(?,?,?)",
                (payload["ts"], json.dumps({
                    "mappings": mappings, "start_date": start.isoformat(),
                    "end_date": end.isoformat(), "params": params,
                }, ensure_ascii=False), json.dumps(payload, ensure_ascii=False)),
            )
            payload["run_id"] = cursor.lastrowid
            db.execute(
                "UPDATE hotspot_backtest_runs SET payload_json=? WHERE id=?",
                (json.dumps(payload, ensure_ascii=False), cursor.lastrowid),
            )
    return payload


def default_dates():
    end = date.today()
    return (end - timedelta(days=365)).isoformat(), end.isoformat()
