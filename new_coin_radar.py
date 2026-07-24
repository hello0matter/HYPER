#!/usr/bin/env python3
"""Research newly created TON pools and selected Kraken spot pairs.

This module only records market observations and paper trades. It has no
wallet integration and cannot submit an order to either market.
"""

from __future__ import annotations

import json
import math
import sqlite3
import statistics
import time
from contextlib import closing
from datetime import datetime
from datetime import timezone
from urllib.parse import quote
from urllib.request import ProxyHandler
from urllib.request import Request
from urllib.request import build_opener
from urllib.request import urlopen


GECKO_NEW_POOLS = "https://api.geckoterminal.com/api/v2/networks/ton/new_pools?page={page}"
GECKO_POOL = "https://api.geckoterminal.com/api/v2/networks/ton/pools/{address}"
KRAKEN_OHLC = "https://api.kraken.com/0/public/OHLC?pair={pair}&interval=15"
LOCAL_PROXY = "http://127.0.0.1:7891"
PROXY_OPENER = build_opener(ProxyHandler({"http": LOCAL_PROXY, "https": LOCAL_PROXY}))
USE_LOCAL_PROXY = False

DEFAULT_CONFIG = {
    "pages": 3,
    "max_age_hours": 168.0,
    "min_liquidity_usd": 5_000.0,
    "min_volume_h1_usd": 500.0,
    "min_h1_buys": 5,
    "min_buy_share": 0.58,
    "min_score": 60.0,
    "max_h1_pump_pct": 35.0,
    "watchlist": "EVAAUSD",
    "paper_notional_usd": 20.0,
    "paper_max_open": 3,
    "paper_take_profit_pct": 25.0,
    "paper_stop_pct": 12.0,
    "paper_trail_start_pct": 15.0,
    "paper_trail_gap_pct": 8.0,
    "paper_max_hold_hours": 24.0,
    "paper_cost_bps": 300.0,
}


def _number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _clamp(value, low, high):
    return max(float(low), min(float(high), float(value)))


def _request_json(url):
    global USE_LOCAL_PROXY
    request = Request(url, headers={"User-Agent": "HYPER-NewCoin-Radar/1.0"})
    if USE_LOCAL_PROXY:
        with PROXY_OPENER.open(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    direct_error = None
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except OSError as exc:
        direct_error = exc
    try:
        with PROXY_OPENER.open(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        USE_LOCAL_PROXY = True
        return payload
    except OSError as proxy_error:
        raise RuntimeError(f"外部行情直连失败：{direct_error}；7891代理失败：{proxy_error}") from proxy_error


def normalize_config(config=None):
    source = {**DEFAULT_CONFIG, **(config or {})}
    return {
        "pages": int(_clamp(source["pages"], 1, 10)),
        "max_age_hours": _clamp(source["max_age_hours"], 1, 24 * 90),
        "min_liquidity_usd": _clamp(source["min_liquidity_usd"], 0, 100_000_000),
        "min_volume_h1_usd": _clamp(source["min_volume_h1_usd"], 0, 100_000_000),
        "min_h1_buys": int(_clamp(source["min_h1_buys"], 0, 100_000)),
        "min_buy_share": _clamp(source["min_buy_share"], 0, 1),
        "min_score": _clamp(source["min_score"], 0, 100),
        "max_h1_pump_pct": _clamp(source["max_h1_pump_pct"], 1, 10_000),
        "watchlist": ",".join(dict.fromkeys(
            item.strip().upper()
            for item in str(source["watchlist"]).split(",")
            if item.strip()
        )),
        "paper_notional_usd": _clamp(source["paper_notional_usd"], 1, 1_000_000),
        "paper_max_open": int(_clamp(source["paper_max_open"], 0, 100)),
        "paper_take_profit_pct": _clamp(source["paper_take_profit_pct"], 0.1, 10_000),
        "paper_stop_pct": _clamp(source["paper_stop_pct"], 0.1, 100),
        "paper_trail_start_pct": _clamp(source["paper_trail_start_pct"], 0, 10_000),
        "paper_trail_gap_pct": _clamp(source["paper_trail_gap_pct"], 0.1, 100),
        "paper_max_hold_hours": _clamp(source["paper_max_hold_hours"], 0.1, 24 * 30),
        "paper_cost_bps": _clamp(source["paper_cost_bps"], 0, 10_000),
    }


def _pool_score(row, config):
    age = row["age_hours"]
    liquidity = row["liquidity_usd"]
    volume_h1 = row["volume_h1_usd"]
    buys, sells = row["buys_h1"], row["sells_h1"]
    trades = buys + sells
    buy_share = buys / trades if trades else 0.0
    turnover = volume_h1 / liquidity if liquidity > 0 else 0.0
    change_h1 = row["price_change_h1_pct"]
    youth_score = 15 * (1 - _clamp(age / config["max_age_hours"], 0, 1))
    if liquidity > 0 and config["min_liquidity_usd"] > 0:
        liquidity_score = _clamp(8 + math.log10(liquidity / config["min_liquidity_usd"]) * 10, 0, 20)
    else:
        liquidity_score = 0.0
    turnover_score = _clamp(turnover * 100, 0, 25)
    pressure_score = _clamp((buy_share - 0.5) * 100, 0, 20)
    activity_score = _clamp(trades / max(1, config["min_h1_buys"] * 2) * 10, 0, 10)
    momentum_score = _clamp(change_h1 * 1.5, 0, 10)
    acceleration = row.get("volume_acceleration")
    acceleration_score = _clamp((float(acceleration) - 1) * 15, 0, 10) if acceleration is not None else 0.0
    score = (
        youth_score + liquidity_score + turnover_score + pressure_score
        + activity_score + momentum_score + acceleration_score
    )
    if change_h1 > config["max_h1_pump_pct"]:
        score -= min(35.0, (change_h1 - config["max_h1_pump_pct"]) * 0.8 + 15)
    return _clamp(score, 0, 100), turnover, buy_share


def _classify_ton_pool(row, config):
    failures = []
    if row["age_hours"] > config["max_age_hours"]:
        failures.append("池子超过年龄范围")
    if row["liquidity_usd"] < config["min_liquidity_usd"]:
        failures.append("流动性不足")
    if row["volume_h1_usd"] < config["min_volume_h1_usd"]:
        failures.append("1小时成交额不足")
    if row["buys_h1"] < config["min_h1_buys"]:
        failures.append("1小时买单太少")
    if row["buy_share_h1"] < config["min_buy_share"]:
        failures.append("买单占比不足")
    if row["price_change_h1_pct"] <= 0:
        failures.append("1小时价格尚未转强")
    if row["price_change_h1_pct"] > config["max_h1_pump_pct"]:
        failures.append("1小时涨幅过热，不追")
    if row["score"] < config["min_score"]:
        failures.append("综合分不足")
    if not failures:
        status = "candidate"
        reason = "年轻池子、成交放大、买盘占优且尚未过热"
    elif (
        row["age_hours"] <= config["max_age_hours"]
        and row["liquidity_usd"] >= config["min_liquidity_usd"]
        and row["buys_h1"] + row["sells_h1"] > 0
    ):
        status = "warming"
        reason = "；".join(failures)
    else:
        status = "filtered"
        reason = "；".join(failures)
    row.update({"status": status, "reason": reason})
    return row


def score_ton_pool(item, config=None, *, now=None):
    config = normalize_config(config)
    attributes = item.get("attributes") or {}
    transactions = attributes.get("transactions") or {}
    h1_transactions = transactions.get("h1") or {}
    volume = attributes.get("volume_usd") or {}
    changes = attributes.get("price_change_percentage") or {}
    created_text = attributes.get("pool_created_at")
    created = datetime.fromisoformat(str(created_text).replace("Z", "+00:00")).timestamp()
    now = float(now if now is not None else time.time())
    pair_id = str(item.get("id") or "")
    address = pair_id.removeprefix("ton_")
    row = {
        "source": "GeckoTerminal",
        "chain": "TON",
        "pair_id": pair_id,
        "pair_address": address,
        "symbol": str(attributes.get("name") or pair_id),
        "created_ts": created,
        "age_hours": max(0.0, (now - created) / 3600),
        "price_usd": _number(attributes.get("base_token_price_usd")),
        "liquidity_usd": _number(attributes.get("reserve_in_usd")),
        "volume_h1_usd": _number(volume.get("h1")),
        "volume_h24_usd": _number(volume.get("h24")),
        "price_change_h1_pct": _number(changes.get("h1")),
        "price_change_h24_pct": _number(changes.get("h24")),
        "buys_h1": int(_number(h1_transactions.get("buys"))),
        "sells_h1": int(_number(h1_transactions.get("sells"))),
        "url": f"https://www.geckoterminal.com/ton/pools/{quote(address, safe='-_')}",
    }
    score, turnover, buy_share = _pool_score(row, config)
    row.update({"score": score, "turnover_h1": turnover, "buy_share_h1": buy_share})
    return _classify_ton_pool(row, config)


def add_snapshot_acceleration(db_path, rows, config=None):
    config = normalize_config(config)
    init_new_coin_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        previous = {
            pair_id: json.loads(payload_json)
            for pair_id, payload_json in db.execute("""
                SELECT snapshots.pair_id, snapshots.payload_json
                FROM new_coin_snapshots AS snapshots
                JOIN (
                    SELECT pair_id, MAX(ts) AS latest_ts
                    FROM new_coin_snapshots GROUP BY pair_id
                ) AS latest
                ON snapshots.pair_id=latest.pair_id AND snapshots.ts=latest.latest_ts
            """)
        }
    for row in rows:
        old = previous.get(row["pair_id"])
        if old is None:
            row["volume_acceleration"] = None
        else:
            old_volume = _number(old.get("volume_h1_usd"))
            current_volume = row["volume_h1_usd"]
            if old_volume > 0:
                row["volume_acceleration"] = current_volume / old_volume
            elif current_volume > 0:
                row["volume_acceleration"] = 5.0
            else:
                row["volume_acceleration"] = 1.0
        score, turnover, buy_share = _pool_score(row, config)
        row.update({"score": score, "turnover_h1": turnover, "buy_share_h1": buy_share})
        _classify_ton_pool(row, config)
    return rows


def fetch_ton_new_pools(config=None, *, now=None):
    config = normalize_config(config)
    by_pair = {}
    failures = []
    for page in range(1, config["pages"] + 1):
        try:
            payload = _request_json(GECKO_NEW_POOLS.format(page=page))
            for index, item in enumerate(payload.get("data") or [], start=1):
                try:
                    row = score_ton_pool(item, config, now=now)
                    by_pair[row["pair_id"]] = row
                except Exception as exc:
                    pair_id = str((item or {}).get("id") or f"第{index}条")
                    failures.append(f"TON新池第{page}页 {pair_id}：{exc}")
        except Exception as exc:
            failures.append(f"TON新池第{page}页：{exc}")
    rows = list(by_pair.values())
    rank = {"candidate": 2, "warming": 1, "filtered": 0}
    rows.sort(key=lambda row: (rank[row["status"]], row["score"], row["liquidity_usd"]), reverse=True)
    return rows, failures


def fetch_open_trade_pools(db_path, known_pair_ids, config=None, *, now=None):
    config = normalize_config(config)
    init_new_coin_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        pair_ids = [row[0] for row in db.execute(
            "SELECT DISTINCT pair_id FROM new_coin_paper_trades WHERE status='open'"
        )]
    rows, failures = [], []
    for pair_id in pair_ids:
        if pair_id in known_pair_ids:
            continue
        address = str(pair_id).removeprefix("ton_")
        try:
            payload = _request_json(GECKO_POOL.format(address=quote(address, safe="-_")))
            item = payload.get("data") or {}
            row = score_ton_pool(item, config, now=now)
            row["open_trade_followup"] = True
            rows.append(row)
        except Exception as exc:
            failures.append(f"模拟持仓池 {address}：{exc}")
    return rows, failures


def _ema(values, period):
    alpha = 2 / (period + 1)
    value = float(values[0])
    for item in values[1:]:
        value = alpha * float(item) + (1 - alpha) * value
    return value


def analyze_kraken_ohlc(pair, payload):
    result = payload.get("result") or {}
    key = next((name for name in result if name != "last"), None)
    rows = result.get(key) or []
    if len(rows) < 100:
        raise ValueError(f"{pair} 只有 {len(rows)} 根15分钟K线")
    closes = [_number(row[4]) for row in rows]
    volumes = [_number(row[6]) for row in rows]
    current = closes[-1]

    def change(bars):
        return (current / closes[-1 - bars] - 1) * 100 if len(closes) > bars and closes[-1 - bars] > 0 else 0.0

    recent_h1 = sum(volumes[-4:])
    previous_hours = [sum(volumes[index:index + 4]) for index in range(max(0, len(volumes) - 100), len(volumes) - 4, 4)]
    normal_h1 = statistics.median(previous_hours) if previous_hours else 0.0
    volume_ratio = recent_h1 / normal_h1 if normal_h1 > 0 else 0.0
    ema20, ema60 = _ema(closes[-120:], 20), _ema(closes[-180:], 60)
    previous_high = max(closes[-97:-1])
    breakout_pct = (current / previous_high - 1) * 100 if previous_high > 0 else 0.0
    change_h1, change_h6, change_h24 = change(4), change(24), change(96)
    trend = current > ema20 > ema60
    candidate = trend and volume_ratio >= 2 and 1 <= change_h1 <= 20 and breakout_pct >= 0
    score = _clamp(
        (20 if trend else 0)
        + _clamp(volume_ratio * 8, 0, 25)
        + _clamp(change_h1 * 2, 0, 20)
        + _clamp(change_h6, 0, 15)
        + (20 if breakout_pct >= 0 else 0),
        0,
        100,
    )
    return {
        "pair": pair,
        "source": "Kraken现货",
        "price_usd": current,
        "change_h1_pct": change_h1,
        "change_h6_pct": change_h6,
        "change_h24_pct": change_h24,
        "volume_h1": recent_h1,
        "volume_ratio": volume_ratio,
        "ema20": ema20,
        "ema60": ema60,
        "breakout_pct": breakout_pct,
        "score": score,
        "status": "candidate" if candidate else "watch",
        "reason": "放量突破且短中期趋势向上" if candidate else "尚未同时满足放量、突破和趋势条件",
        "samples": len(rows),
        "data_start_ts": int(rows[0][0]),
        "data_end_ts": int(rows[-1][0]),
        "url": f"https://www.tradingview.com/chart/?symbol=KRAKEN%3A{quote(pair, safe='')}",
    }


def fetch_kraken_watchlist(config=None):
    config = normalize_config(config)
    rows, failures = [], []
    for pair in config["watchlist"].split(",") if config["watchlist"] else []:
        try:
            rows.append(analyze_kraken_ohlc(pair, _request_json(KRAKEN_OHLC.format(pair=quote(pair, safe="")))))
        except Exception as exc:
            failures.append(f"Kraken {pair}：{exc}")
    rows.sort(key=lambda row: row["score"], reverse=True)
    return rows, failures


def init_new_coin_db(db_path):
    with closing(sqlite3.connect(db_path)) as db:
        with db:
            db.execute("""
            CREATE TABLE IF NOT EXISTS new_coin_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                pair_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price_usd REAL,
                liquidity_usd REAL,
                volume_h1_usd REAL,
                volume_h24_usd REAL,
                buys_h1 INTEGER,
                sells_h1 INTEGER,
                age_hours REAL,
                score REAL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_new_coin_snapshot_pair_ts ON new_coin_snapshots(pair_id, ts)")
            db.execute("""
            CREATE TABLE IF NOT EXISTS new_coin_paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                notional_usd REAL NOT NULL,
                entry_ts REAL NOT NULL,
                exit_ts REAL,
                entry_price REAL NOT NULL,
                exit_price REAL,
                entry_liquidity_usd REAL,
                entry_score REAL,
                pnl_pct REAL NOT NULL DEFAULT 0,
                pnl_usd REAL NOT NULL DEFAULT 0,
                max_pnl_pct REAL NOT NULL DEFAULT 0,
                close_reason TEXT,
                entry_json TEXT
            )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_new_coin_trade_status ON new_coin_paper_trades(status, pair_id)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_new_coin_trade_ts ON new_coin_paper_trades(entry_ts, exit_ts)")
            db.execute("""
                CREATE TABLE IF NOT EXISTS new_coin_settings (
                    id INTEGER PRIMARY KEY CHECK (id=1),
                    payload_json TEXT NOT NULL,
                    updated_ts REAL NOT NULL
                )
            """)


def load_new_coin_config(db_path):
    init_new_coin_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        row = db.execute("SELECT payload_json FROM new_coin_settings WHERE id=1").fetchone()
    if not row:
        return normalize_config()
    try:
        return normalize_config(json.loads(row[0]))
    except (json.JSONDecodeError, TypeError, ValueError):
        return normalize_config()


def save_new_coin_config(db_path, config):
    config = normalize_config(config)
    init_new_coin_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        with db:
            db.execute("""
                INSERT INTO new_coin_settings (id, payload_json, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload_json=excluded.payload_json,
                    updated_ts=excluded.updated_ts
            """, (json.dumps(config, ensure_ascii=False), time.time()))
    return config


def _paper_snapshot(db_path, limit=100):
    init_new_coin_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        db.row_factory = sqlite3.Row
        trades = [dict(row) for row in db.execute(
            "SELECT * FROM new_coin_paper_trades ORDER BY entry_ts DESC LIMIT ?", (int(limit),)
        )]
        stats = dict(db.execute("""
            SELECT COALESCE(SUM(CASE WHEN status='closed' THEN 1 ELSE 0 END), 0) AS closed,
                   COALESCE(SUM(CASE WHEN status='closed' AND pnl_usd>0 THEN 1 ELSE 0 END), 0) AS wins,
                   COALESCE(SUM(CASE WHEN status='closed' THEN pnl_usd ELSE 0 END), 0) AS realized,
                   COALESCE(SUM(CASE WHEN status='open' THEN pnl_usd ELSE 0 END), 0) AS unrealized,
                   COALESCE(SUM(CASE WHEN status='open' THEN 1 ELSE 0 END), 0) AS open_count
            FROM new_coin_paper_trades
        """).fetchone())
    stats["win_rate"] = stats["wins"] / stats["closed"] if stats["closed"] else 0.0
    return {"trades": trades, "paper_stats": stats}


def update_new_coin_paper(db_path, rows, config=None, *, now=None):
    config = normalize_config(config)
    now = float(now if now is not None else time.time())
    init_new_coin_db(db_path)
    by_pair = {row["pair_id"]: row for row in rows if row.get("price_usd", 0) > 0}
    cost_pct = config["paper_cost_bps"] / 100
    with closing(sqlite3.connect(db_path)) as db:
        db.row_factory = sqlite3.Row
        with db:
            open_trades = [dict(row) for row in db.execute(
                "SELECT * FROM new_coin_paper_trades WHERE status='open' ORDER BY entry_ts"
            )]
            for trade in open_trades:
                row = by_pair.get(trade["pair_id"])
                if not row:
                    continue
                gross_pct = (row["price_usd"] / trade["entry_price"] - 1) * 100
                pnl_pct = gross_pct - cost_pct
                max_pnl = max(float(trade["max_pnl_pct"] or 0), pnl_pct)
                reason = None
                if pnl_pct >= config["paper_take_profit_pct"]:
                    reason = "固定止盈"
                elif pnl_pct <= -config["paper_stop_pct"]:
                    reason = "固定止损"
                elif max_pnl >= config["paper_trail_start_pct"] and pnl_pct <= max_pnl - config["paper_trail_gap_pct"]:
                    reason = "移动止盈"
                elif now - trade["entry_ts"] >= config["paper_max_hold_hours"] * 3600:
                    reason = "达到最长持有时间"
                elif row["liquidity_usd"] < float(trade["entry_liquidity_usd"] or 0) * 0.35:
                    reason = "流动性较入场时下降65%"
                pnl_usd = trade["notional_usd"] * pnl_pct / 100
                if reason:
                    db.execute("""
                        UPDATE new_coin_paper_trades
                        SET status='closed', exit_ts=?, exit_price=?, pnl_pct=?, pnl_usd=?,
                            max_pnl_pct=?, close_reason=? WHERE id=?
                    """, (now, row["price_usd"], pnl_pct, pnl_usd, max_pnl, reason, trade["id"]))
                else:
                    db.execute("""
                        UPDATE new_coin_paper_trades SET exit_price=?, pnl_pct=?, pnl_usd=?, max_pnl_pct=? WHERE id=?
                    """, (row["price_usd"], pnl_pct, pnl_usd, max_pnl, trade["id"]))
            open_pairs = {
                row[0] for row in db.execute("SELECT pair_id FROM new_coin_paper_trades WHERE status='open'")
            }
            slots = max(0, config["paper_max_open"] - len(open_pairs))
            candidates = [row for row in rows if row["status"] == "candidate" and row["pair_id"] not in open_pairs]
            for row in candidates:
                if slots <= 0:
                    break
                previous = db.execute(
                    "SELECT MAX(COALESCE(exit_ts, entry_ts)) FROM new_coin_paper_trades WHERE pair_id=?",
                    (row["pair_id"],),
                ).fetchone()[0]
                if previous and now - float(previous) < 24 * 3600:
                    continue
                db.execute("""
                    INSERT INTO new_coin_paper_trades (
                        pair_id, symbol, status, notional_usd, entry_ts, entry_price,
                        entry_liquidity_usd, entry_score, entry_json
                    ) VALUES (?, ?, 'open', ?, ?, ?, ?, ?, ?)
                """, (
                    row["pair_id"], row["symbol"], config["paper_notional_usd"], now,
                    row["price_usd"], row["liquidity_usd"], row["score"],
                    json.dumps(row, ensure_ascii=False),
                ))
                slots -= 1
    return _paper_snapshot(db_path)


def save_new_coin_snapshots(db_path, rows, *, now=None):
    now = float(now if now is not None else time.time())
    init_new_coin_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        with db:
            db.executemany("""
            INSERT INTO new_coin_snapshots (
                ts, pair_id, symbol, price_usd, liquidity_usd, volume_h1_usd,
                volume_h24_usd, buys_h1, sells_h1, age_hours, score, status, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [(
                now, row["pair_id"], row["symbol"], row["price_usd"], row["liquidity_usd"],
                row["volume_h1_usd"], row["volume_h24_usd"], row["buys_h1"], row["sells_h1"],
                row["age_hours"], row["score"], row["status"], json.dumps(row, ensure_ascii=False),
            ) for row in rows])
            db.execute("DELETE FROM new_coin_snapshots WHERE ts < ?", (now - 30 * 86_400,))


def load_latest_new_coin_rows(db_path):
    init_new_coin_db(db_path)
    with closing(sqlite3.connect(db_path)) as db:
        records = db.execute("""
            SELECT payload_json FROM new_coin_snapshots
            WHERE ts=(SELECT MAX(ts) FROM new_coin_snapshots)
            ORDER BY score DESC
        """).fetchall()
    rows = []
    for (payload_json,) in records:
        try:
            row = json.loads(payload_json)
            if isinstance(row, dict) and row.get("pair_id"):
                rows.append(row)
        except (json.JSONDecodeError, TypeError):
            continue
    return rows


def run_new_coin_radar(db_path, config=None, *, now=None):
    config = save_new_coin_config(db_path, config)
    now = float(now if now is not None else time.time())
    rows, ton_failures = fetch_ton_new_pools(config, now=now)
    ton_cache_stale = False
    followup_failures = []
    if not rows and ton_failures:
        rows = load_latest_new_coin_rows(db_path)
        ton_cache_stale = bool(rows)
    if not ton_cache_stale:
        followups, followup_failures = fetch_open_trade_pools(
            db_path, {row["pair_id"] for row in rows}, config, now=now,
        )
        rows.extend(followups)
        add_snapshot_acceleration(db_path, rows, config)
        rank = {"candidate": 2, "warming": 1, "filtered": 0}
        rows.sort(
            key=lambda row: (rank[row["status"]], row["score"], row["liquidity_usd"]),
            reverse=True,
        )
    watch_rows, watch_failures = fetch_kraken_watchlist(config)
    if ton_cache_stale:
        paper = _paper_snapshot(db_path)
    else:
        save_new_coin_snapshots(db_path, rows, now=now)
        paper = update_new_coin_paper(db_path, rows, config, now=now)
    return {
        "ok": bool(rows or watch_rows),
        "ts": now,
        "config": config,
        "rows": rows,
        "watch_rows": watch_rows,
        "candidates": sum(row["status"] == "candidate" for row in rows),
        "warming": sum(row["status"] == "warming" for row in rows),
        "ton_cache_stale": ton_cache_stale,
        "failures": ton_failures + followup_failures + watch_failures,
        **paper,
        "note": (
            "TON新池来自GeckoTerminal公开数据；Kraken自选来自15分钟OHLC。"
            "分数不能识别合约后门、持币集中、撤池或无法卖出，只允许模拟记录。"
            + (" TON接口本轮失败，池子表保留最后一次成功快照，未用旧价更新模拟交易。" if ton_cache_stale else "")
        ),
    }


def main():
    result = run_new_coin_radar("new_coin_radar.sqlite3")
    summary = {
        "ts": datetime.fromtimestamp(result["ts"], timezone.utc).isoformat(),
        "candidates": result["candidates"],
        "warming": result["warming"],
        "top": result["rows"][:10],
        "watch": result["watch_rows"],
        "failures": result["failures"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
