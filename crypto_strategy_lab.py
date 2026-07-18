#!/usr/bin/env python3
"""Hyperliquid K-line strategy research with walk-forward scoring.

The module deliberately stays independent from real order execution.  It is a
research gate: strategies must remain profitable after costs in the out-of-
sample segment before another component may consider them for paper/live use.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from dataclasses import dataclass
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener, urlopen


HL_INFO = "https://api.hyperliquid.xyz/info"
LOCAL_PROXY_OPENER = build_opener(ProxyHandler({
    "http": "http://127.0.0.1:7891",
    "https": "http://127.0.0.1:7891",
}))
USE_LOCAL_PROXY = False


@dataclass(frozen=True)
class StrategySpec:
    family: str
    name: str
    params: dict


def _request_json(payload):
    global USE_LOCAL_PROXY
    encoded = json.dumps(payload).encode("utf-8")
    req = Request(HL_INFO, data=encoded, headers={
        "Content-Type": "application/json", "User-Agent": "HLM-Strategy-Lab/1.0",
    })
    if USE_LOCAL_PROXY:
        last_error = None
        for _ in range(2):
            try:
                with LOCAL_PROXY_OPENER.open(req, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8"))
            except OSError as exc:
                last_error = exc
        raise RuntimeError(f"Hyperliquid K线 7891 代理连续失败：{last_error}") from last_error
    direct_error = None
    for attempt in range(4):
        try:
            with urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            direct_error = exc
            if exc.code == 429 and attempt < 3:
                time.sleep(1.5 * (2 ** attempt))
                continue
            break
        except OSError as exc:
            direct_error = exc
            break
    # Local Windows research machines in this project commonly expose a
    # proxy on 7891.  Production servers normally succeed on the direct
    # attempt and never enter this fallback.
    try:
        with LOCAL_PROXY_OPENER.open(req, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
        USE_LOCAL_PROXY = True
        return data
    except OSError as proxy_error:
        raise RuntimeError(f"Hyperliquid K线直连失败：{direct_error}；7891代理失败：{proxy_error}") from proxy_error


def fetch_hl_ohlcv(coin, *, days=30, interval="15m"):
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - int(float(days) * 86_400_000)
    cursor_end, by_ts = end_ms, {}
    for _ in range(32):
        data = _request_json({"type": "candleSnapshot", "req": {
            "coin": str(coin).upper(), "interval": interval,
            "startTime": start_ms, "endTime": cursor_end,
        }})
        if not isinstance(data, list) or not data:
            break
        earliest = cursor_end
        for item in data:
            ts = int(item["t"])
            earliest = min(earliest, ts)
            if start_ms <= ts <= end_ms:
                by_ts[ts] = {
                    "ts": ts, "open": float(item["o"]), "high": float(item["h"]),
                    "low": float(item["l"]), "close": float(item["c"]),
                    "volume": float(item.get("v") or 0),
                }
        if earliest <= start_ms or earliest >= cursor_end:
            break
        cursor_end = earliest - 1
    rows = []
    rows.extend(by_ts.values())
    rows.sort(key=lambda row: row["ts"])
    return rows


def _ema(values, period):
    out = [None] * len(values)
    if not values or period <= 0:
        return out
    alpha = 2.0 / (period + 1)
    value = values[0]
    for i, item in enumerate(values):
        value = item if i == 0 else alpha * item + (1 - alpha) * value
        if i >= period - 1:
            out[i] = value
    return out


def _sma(values, period):
    out, total = [None] * len(values), 0.0
    for i, item in enumerate(values):
        total += item
        if i >= period:
            total -= values[i - period]
        if i >= period - 1:
            out[i] = total / period
    return out


def _rolling_std(values, period):
    out = [None] * len(values)
    for i in range(period - 1, len(values)):
        out[i] = statistics.pstdev(values[i - period + 1:i + 1])
    return out


def _rsi(values, period=14):
    out = [None] * len(values)
    if len(values) <= period:
        return out
    gains, losses = [], []
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(values)):
        if i > period:
            avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
        rs = avg_gain / avg_loss if avg_loss > 0 else math.inf
        out[i] = 100 - 100 / (1 + rs)
    return out


def _atr(candles, period=14):
    tr = []
    for i, row in enumerate(candles):
        previous = candles[i - 1]["close"] if i else row["close"]
        tr.append(max(row["high"] - row["low"], abs(row["high"] - previous), abs(row["low"] - previous)))
    return _ema(tr, period)


def _hold_targets(raw):
    target, out = 0, []
    for item in raw:
        if item is not None:
            target = int(item)
        out.append(target)
    return out


def strategy_targets(candles, spec):
    close = [row["close"] for row in candles]
    high = [row["high"] for row in candles]
    low = [row["low"] for row in candles]
    p = spec.params
    family = spec.family
    raw = [0] * len(candles)

    if family == "ema_cross":
        fast, slow = _ema(close, p["fast"]), _ema(close, p["slow"])
        raw = [0 if a is None or b is None else (1 if a > b else -1) for a, b in zip(fast, slow)]
    elif family == "macd":
        fast, slow = _ema(close, p["fast"]), _ema(close, p["slow"])
        line = [0.0 if a is None or b is None else a - b for a, b in zip(fast, slow)]
        signal = _ema(line, p["signal"])
        raw = [0 if s is None else (1 if m > s else -1) for m, s in zip(line, signal)]
    elif family == "rsi_reversion":
        values = _rsi(close, p["period"])
        position = 0
        for i, value in enumerate(values):
            if value is None:
                raw[i] = position
            elif position == 0 and value <= p["lower"]:
                position = 1
            elif position == 0 and value >= p["upper"]:
                position = -1
            elif position == 1 and value >= p["exit"]:
                position = 0
            elif position == -1 and value <= p["exit"]:
                position = 0
            raw[i] = position
    elif family == "bollinger_reversion":
        mid, sd = _sma(close, p["period"]), _rolling_std(close, p["period"])
        position = 0
        for i, price in enumerate(close):
            if mid[i] is None or sd[i] is None:
                raw[i] = position
                continue
            upper, lower = mid[i] + p["dev"] * sd[i], mid[i] - p["dev"] * sd[i]
            if position == 0 and price < lower:
                position = 1
            elif position == 0 and price > upper:
                position = -1
            elif position == 1 and price >= mid[i]:
                position = 0
            elif position == -1 and price <= mid[i]:
                position = 0
            raw[i] = position
    elif family == "donchian_breakout":
        position = 0
        for i, price in enumerate(close):
            if i < p["entry"]:
                raw[i] = position
                continue
            upper = max(high[i - p["entry"]:i])
            lower = min(low[i - p["entry"]:i])
            exit_upper = max(high[max(0, i - p["exit"]):i])
            exit_lower = min(low[max(0, i - p["exit"]):i])
            if price > upper:
                position = 1
            elif price < lower:
                position = -1
            elif position == 1 and price < exit_lower:
                position = 0
            elif position == -1 and price > exit_upper:
                position = 0
            raw[i] = position
    elif family == "supertrend":
        atr = _atr(candles, p["period"])
        upper, lower = [None] * len(candles), [None] * len(candles)
        direction = 0
        for i, row in enumerate(candles):
            if atr[i] is None:
                raw[i] = direction
                continue
            mid = (row["high"] + row["low"]) / 2
            basic_upper, basic_lower = mid + p["mult"] * atr[i], mid - p["mult"] * atr[i]
            if i == 0 or upper[i - 1] is None:
                upper[i], lower[i] = basic_upper, basic_lower
            else:
                upper[i] = basic_upper if basic_upper < upper[i - 1] or candles[i - 1]["close"] > upper[i - 1] else upper[i - 1]
                lower[i] = basic_lower if basic_lower > lower[i - 1] or candles[i - 1]["close"] < lower[i - 1] else lower[i - 1]
            if direction == 0:
                direction = 1
            elif direction < 0 and row["close"] > upper[i]:
                direction = 1
            elif direction > 0 and row["close"] < lower[i]:
                direction = -1
            raw[i] = direction
    elif family == "momentum":
        trend = _ema(close, p["ema"])
        for i in range(p["lookback"], len(close)):
            momentum = close[i] / close[i - p["lookback"]] - 1
            if trend[i] is None:
                continue
            raw[i] = 1 if momentum > 0 and close[i] >= trend[i] else (-1 if momentum < 0 and close[i] <= trend[i] else 0)
    else:
        raise ValueError(f"unknown strategy family: {family}")
    return _hold_targets(raw)


def backtest_targets(candles, targets, *, start=1, end=None, round_trip_cost_bps=12.0):
    end = min(len(candles) - 1, int(end if end is not None else len(candles) - 1))
    start = max(1, int(start))
    one_way = float(round_trip_cost_bps) / 2
    position, entry_price, entry_cost = 0, None, 0.0
    equity_bps, peak_bps, max_drawdown_bps = 0.0, 0.0, 0.0
    returns, trades = [], []
    exposure = 0
    for i in range(start, end + 1):
        desired = int(targets[i - 1])
        price = float(candles[i]["open"])
        if desired != position:
            if position:
                gross = position * math.log(price / entry_price) * 10_000
                trades.append(gross - entry_cost - one_way)
            transition_cost = one_way * abs(desired - position)
            equity_bps -= transition_cost
            if desired:
                entry_price, entry_cost = price, one_way
            else:
                entry_price, entry_cost = None, 0.0
            position = desired
        if i < end:
            step = position * math.log(float(candles[i + 1]["open"]) / price) * 10_000
            equity_bps += step
            returns.append(step)
            exposure += int(position != 0)
            peak_bps = max(peak_bps, equity_bps)
            max_drawdown_bps = min(max_drawdown_bps, equity_bps - peak_bps)
    if position and entry_price:
        final_price = float(candles[end]["close"])
        gross = position * math.log(final_price / entry_price) * 10_000
        trades.append(gross - entry_cost - one_way)
        equity_bps -= one_way
    wins = [item for item in trades if item > 0]
    losses = [item for item in trades if item < 0]
    mean = statistics.fmean(returns) if returns else 0.0
    stdev = statistics.pstdev(returns) if len(returns) > 1 else 0.0
    sharpe = mean / stdev * math.sqrt(len(returns)) if stdev > 0 else 0.0
    profit_factor = sum(wins) / abs(sum(losses)) if losses else (999.0 if wins else 0.0)
    return {
        "net_bps": equity_bps, "max_drawdown_bps": max_drawdown_bps,
        "trades": len(trades), "wins": len(wins), "win_rate": len(wins) / len(trades) if trades else 0.0,
        "profit_factor": profit_factor, "avg_trade_bps": statistics.fmean(trades) if trades else 0.0,
        "worst_trade_bps": min(trades) if trades else 0.0, "sharpe_like": sharpe,
        "exposure": exposure / max(1, end - start), "last_target": int(targets[end - 1]),
    }


def strategy_specs():
    specs = []
    for fast, slow in ((9, 21), (12, 36), (20, 50), (30, 90), (50, 200)):
        specs.append(StrategySpec("ema_cross", f"EMA {fast}/{slow}", {"fast": fast, "slow": slow}))
    for fast, slow, signal in ((8, 21, 5), (12, 26, 9), (19, 39, 9)):
        specs.append(StrategySpec("macd", f"MACD {fast}/{slow}/{signal}", {"fast": fast, "slow": slow, "signal": signal}))
    for period, lower, upper in ((7, 25, 75), (14, 25, 75), (14, 30, 70), (21, 35, 65)):
        specs.append(StrategySpec("rsi_reversion", f"RSI回归 {period} {lower}/{upper}", {"period": period, "lower": lower, "upper": upper, "exit": 50}))
    for period, dev in ((20, 1.5), (20, 2.0), (30, 2.0), (50, 2.2)):
        specs.append(StrategySpec("bollinger_reversion", f"布林回归 {period} x{dev}", {"period": period, "dev": dev}))
    for entry, exit_ in ((10, 5), (20, 10), (40, 20), (55, 20)):
        specs.append(StrategySpec("donchian_breakout", f"唐奇安突破 {entry}/{exit_}", {"entry": entry, "exit": exit_}))
    for period, mult in ((7, 2.0), (10, 2.0), (10, 3.0), (14, 2.5), (14, 3.0), (21, 3.0)):
        specs.append(StrategySpec("supertrend", f"Supertrend {period} x{mult}", {"period": period, "mult": mult}))
    for lookback, ema in ((5, 20), (10, 30), (20, 50), (40, 100), (80, 200)):
        specs.append(StrategySpec("momentum", f"动量 {lookback} + EMA{ema}", {"lookback": lookback, "ema": ema}))
    return specs


def evaluate_coin(coin, candles, *, interval="15m", round_trip_cost_bps=12.0):
    if len(candles) < 240:
        raise ValueError(f"{coin} K线不足：{len(candles)}")
    split = max(180, int(len(candles) * 0.60))
    results = []
    for spec in strategy_specs():
        targets = strategy_targets(candles, spec)
        train = backtest_targets(candles, targets, start=1, end=split, round_trip_cost_bps=round_trip_cost_bps)
        test = backtest_targets(candles, targets, start=split + 1, end=len(candles) - 1, round_trip_cost_bps=round_trip_cost_bps)
        stable = train["net_bps"] > 0 and test["net_bps"] > 0
        promotable = bool(stable and test["trades"] >= 6 and test["profit_factor"] >= 1.10 and test["max_drawdown_bps"] >= -1200)
        score = test["net_bps"] + max(test["max_drawdown_bps"], -2000) * 0.35 + min(test["trades"], 20) * 2
        results.append({
            "coin": coin, "interval": interval, "family": spec.family, "strategy": spec.name,
            "params": spec.params, "samples": len(candles), "split_index": split,
            "train": train, "test": test, "stable": stable, "promotable": promotable,
            "score": score, "current_signal": int(targets[-1]),
        })
    results.sort(key=lambda item: item["score"], reverse=True)
    return results


def run_strategy_lab(coins=("BTC", "ETH", "SOL"), *, days=30, interval="15m", round_trip_cost_bps=12.0, limit=50):
    rows, failures, coverage = [], [], []
    pending = list(coins)
    for pass_index in range(2):
        retry = []
        for coin in pending:
            try:
                candles = fetch_hl_ohlcv(coin, days=days, interval=interval)
                actual_days = ((candles[-1]["ts"] - candles[0]["ts"]) / 86_400_000) if len(candles) > 1 else 0.0
                coverage.append({"coin": coin, "candles": len(candles), "actual_days": actual_days})
                evaluated = evaluate_coin(coin, candles, interval=interval, round_trip_cost_bps=round_trip_cost_bps)
                for row in evaluated:
                    row["actual_days"] = actual_days
                rows.extend(evaluated)
            except Exception as exc:  # Keep the other markets usable after one API failure.
                if pass_index == 0 and "429" in str(exc):
                    retry.append(coin)
                else:
                    failures.append({"coin": coin, "error": str(exc)})
            time.sleep(0.35)
        pending = retry
        if not pending:
            break
        time.sleep(5.0)
    rows.sort(key=lambda item: (bool(item["promotable"]), item["score"]), reverse=True)
    truncated = [item for item in coverage if item["actual_days"] + 1 < float(days)]
    coverage_note = ""
    if truncated:
        detail = "、".join(f'{item["coin"]}约{item["actual_days"]:.1f}天/{item["candles"]}根' for item in truncated)
        coverage_note = f" 请求{days}天，但 Hyperliquid 公开接口约有5000根K线上限；实际覆盖：{detail}。"
    return {
        "ok": bool(rows), "ts": time.time(), "coins": list(coins), "days": days, "interval": interval,
        "round_trip_cost_bps": round_trip_cost_bps, "strategy_count": len(strategy_specs()),
        "evaluations": len(rows), "promotable": sum(bool(row["promotable"]) for row in rows),
        "rows": rows[:max(1, int(limit))], "failures": failures, "coverage": coverage,
        "note": "排名只使用样本外结果并已扣配置的往返成本；promotable 仍不是自动实盘授权。" + coverage_note,
    }


def main():
    parser = argparse.ArgumentParser(description="Hyperliquid 多策略样本外回测")
    parser.add_argument("--coins", default="BTC,ETH,SOL")
    parser.add_argument("--interval", default="15m")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--cost", type=float, default=12.0, help="估算往返成本 bps")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    result = run_strategy_lab(
        tuple(item.strip().upper() for item in args.coins.split(",") if item.strip()),
        days=args.days, interval=args.interval, round_trip_cost_bps=args.cost, limit=args.limit,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
