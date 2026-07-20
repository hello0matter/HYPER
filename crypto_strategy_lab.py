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
    reference: str = "经典公开技术规则（项目独立实现）"


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


def _sma_optional(values, period):
    out = [None] * len(values)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        if all(value is not None for value in window):
            out[i] = sum(window) / period
    return out


def _stochastic_rsi(values, rsi_period=14, stoch_period=14, smooth=3):
    rsi = _rsi(values, rsi_period)
    raw = [None] * len(values)
    for i in range(stoch_period - 1, len(values)):
        window = rsi[i - stoch_period + 1:i + 1]
        if any(value is None for value in window):
            continue
        low, high = min(window), max(window)
        raw[i] = 50.0 if high == low else (rsi[i] - low) / (high - low) * 100
    k = _sma_optional(raw, smooth)
    return k, _sma_optional(k, smooth)


def _obv(candles):
    out, value = [], 0.0
    for i, row in enumerate(candles):
        if i:
            if row["close"] > candles[i - 1]["close"]:
                value += row["volume"]
            elif row["close"] < candles[i - 1]["close"]:
                value -= row["volume"]
        out.append(value)
    return out


def _adx(candles, period=14):
    plus_dm, minus_dm, true_range = [0.0], [0.0], [0.0]
    for i in range(1, len(candles)):
        up = candles[i]["high"] - candles[i - 1]["high"]
        down = candles[i - 1]["low"] - candles[i]["low"]
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        previous = candles[i - 1]["close"]
        true_range.append(max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - previous), abs(candles[i]["low"] - previous),
        ))
    atr, plus_avg, minus_avg = _ema(true_range, period), _ema(plus_dm, period), _ema(minus_dm, period)
    dx = []
    for tr, plus, minus in zip(atr, plus_avg, minus_avg):
        if tr in (None, 0) or plus is None or minus is None:
            dx.append(0.0)
            continue
        plus_di, minus_di = 100 * plus / tr, 100 * minus / tr
        total = plus_di + minus_di
        dx.append(0.0 if total == 0 else abs(plus_di - minus_di) / total * 100)
    return _ema(dx, period)


def _rolling_mid(high, low, period, index):
    if index < period - 1:
        return None
    return (max(high[index - period + 1:index + 1]) + min(low[index - period + 1:index + 1])) / 2


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
    elif family == "macd_sma_filter":
        fast, slow, trend = _ema(close, p["fast"]), _ema(close, p["slow"]), _sma(close, p["trend"])
        line = [0.0 if a is None or b is None else a - b for a, b in zip(fast, slow)]
        signal = _ema(line, p["signal"])
        raw = [
            0 if sig is None or filt is None else (
                1 if macd > sig and price > filt else (-1 if macd < sig and price < filt else 0)
            )
            for price, macd, sig, filt in zip(close, line, signal, trend)
        ]
    elif family == "bollinger_rsi":
        mid, sd, rsi = _sma(close, p["period"]), _rolling_std(close, p["period"]), _rsi(close, p["rsi"])
        position = 0
        for i, price in enumerate(close):
            if mid[i] is None or sd[i] is None or rsi[i] is None:
                raw[i] = position
                continue
            upper, lower = mid[i] + p["dev"] * sd[i], mid[i] - p["dev"] * sd[i]
            if position == 0 and price < lower and rsi[i] <= p["lower"]:
                position = 1
            elif position == 0 and price > upper and rsi[i] >= p["upper"]:
                position = -1
            elif position == 1 and price >= mid[i]:
                position = 0
            elif position == -1 and price <= mid[i]:
                position = 0
            raw[i] = position
    elif family == "stoch_rsi":
        k, d = _stochastic_rsi(close, p["rsi"], p["stoch"], p["smooth"])
        position = 0
        for i in range(1, len(close)):
            if None in (k[i - 1], d[i - 1], k[i], d[i]):
                raw[i] = position
                continue
            if position <= 0 and k[i - 1] <= d[i - 1] and k[i] > d[i] and k[i] <= p["lower"]:
                position = 1
            elif position >= 0 and k[i - 1] >= d[i - 1] and k[i] < d[i] and k[i] >= p["upper"]:
                position = -1
            elif position == 1 and k[i] >= 50:
                position = 0
            elif position == -1 and k[i] <= 50:
                position = 0
            raw[i] = position
    elif family == "ichimoku_ema":
        trend = _ema(close, p["ema"])
        for i, price in enumerate(close):
            conversion = _rolling_mid(high, low, p["conversion"], i)
            base = _rolling_mid(high, low, p["base"], i)
            if conversion is None or base is None or trend[i] is None:
                continue
            raw[i] = 1 if conversion > base and price > trend[i] else (
                -1 if conversion < base and price < trend[i] else 0
            )
    elif family == "triple_ema":
        fast, middle, slow = _ema(close, p["fast"]), _ema(close, p["middle"]), _ema(close, p["slow"])
        raw = [
            0 if None in (a, b, c) else (1 if a > b > c else (-1 if a < b < c else 0))
            for a, b, c in zip(fast, middle, slow)
        ]
    elif family == "squeeze_breakout":
        mid, sd, atr = _sma(close, p["period"]), _rolling_std(close, p["period"]), _atr(candles, p["period"])
        position, previous_squeeze = 0, False
        for i, price in enumerate(close):
            if mid[i] is None or sd[i] is None or atr[i] is None:
                raw[i] = position
                continue
            squeeze = (
                mid[i] + p["bb"] * sd[i] < mid[i] + p["kc"] * atr[i]
                and mid[i] - p["bb"] * sd[i] > mid[i] - p["kc"] * atr[i]
            )
            if previous_squeeze and not squeeze:
                position = 1 if price > mid[i] else -1
            elif position == 1 and price < mid[i]:
                position = 0
            elif position == -1 and price > mid[i]:
                position = 0
            raw[i] = position
            previous_squeeze = squeeze
    elif family == "obv_trend":
        obv = _obv(candles)
        obv_signal, price_trend = _ema(obv, p["obv_ema"]), _ema(close, p["price_ema"])
        raw = [
            0 if signal is None or trend is None else (
                1 if volume_line > signal and price > trend else (
                    -1 if volume_line < signal and price < trend else 0
                )
            )
            for volume_line, signal, price, trend in zip(obv, obv_signal, close, price_trend)
        ]
    elif family == "supertrend_adx":
        adx = _adx(candles, p["adx"])
        base = strategy_targets(candles, StrategySpec(
            "supertrend", "internal", {"period": p["period"], "mult": p["mult"]},
        ))
        raw = [direction if strength is not None and strength >= p["threshold"] else 0 for direction, strength in zip(base, adx)]
    elif family == "bollinger_breakout":
        mid, sd = _sma(close, p["period"]), _rolling_std(close, p["period"])
        position = 0
        for i, price in enumerate(close):
            if mid[i] is None or sd[i] is None:
                raw[i] = position
                continue
            upper, lower = mid[i] + p["dev"] * sd[i], mid[i] - p["dev"] * sd[i]
            if position == 0 and price > upper:
                position = 1
            elif position == 0 and price < lower:
                position = -1
            elif position == 1 and price < mid[i]:
                position = 0
            elif position == -1 and price > mid[i]:
                position = 0
            raw[i] = position
    elif family == "turtle_atr":
        atr = _atr(candles, p["atr"])
        position, peak, trough = 0, None, None
        for i, price in enumerate(close):
            if i < p["entry"] or atr[i] is None:
                raw[i] = position
                continue
            upper, lower = max(high[i - p["entry"]:i]), min(low[i - p["entry"]:i])
            if position == 0 and price > upper:
                position, peak = 1, high[i]
            elif position == 0 and price < lower:
                position, trough = -1, low[i]
            elif position == 1:
                peak = max(peak, high[i])
                if price < peak - p["mult"] * atr[i]:
                    position, peak = 0, None
            elif position == -1:
                trough = min(trough, low[i])
                if price > trough + p["mult"] * atr[i]:
                    position, trough = 0, None
            raw[i] = position
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
    community = "TradingView社区思路参考（按公开规则独立复刻，不是原作者源码）"
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
    for fast, slow, signal, trend in ((8, 21, 5, 100), (12, 26, 9, 200), (19, 39, 9, 200)):
        specs.append(StrategySpec(
            "macd_sma_filter", f"MACD趋势过滤 {fast}/{slow}/{signal} + SMA{trend}",
            {"fast": fast, "slow": slow, "signal": signal, "trend": trend},
            f"{community}：MACD + SMA 200 Strategy (by ChartArt)",
        ))
    for period, dev, rsi, lower, upper in (
        (20, 2.0, 14, 30, 70), (20, 2.0, 7, 25, 75),
        (30, 2.0, 14, 30, 70), (20, 1.5, 14, 25, 75),
    ):
        specs.append(StrategySpec(
            "bollinger_rsi", f"布林+RSI {period} x{dev} RSI{rsi}",
            {"period": period, "dev": dev, "rsi": rsi, "lower": lower, "upper": upper},
            f"{community}：Bollinger + RSI, Double Strategy (by ChartArt)",
        ))
    for rsi, stoch, smooth, lower, upper in ((14, 14, 3, 20, 80), (7, 14, 3, 20, 80), (21, 21, 3, 15, 85)):
        specs.append(StrategySpec(
            "stoch_rsi", f"StochRSI {rsi}/{stoch}/{smooth}",
            {"rsi": rsi, "stoch": stoch, "smooth": smooth, "lower": lower, "upper": upper},
            f"{community}：Stochastic RSI Strategy",
        ))
    for conversion, base, ema in ((9, 26, 200), (7, 22, 100), (12, 30, 200)):
        specs.append(StrategySpec(
            "ichimoku_ema", f"一目均衡交叉 {conversion}/{base} + EMA{ema}",
            {"conversion": conversion, "base": base, "ema": ema},
            f"{community}：Ichimoku TK Cross > EMA200 Crypto Strategy",
        ))
    for fast, middle, slow in ((5, 13, 34), (9, 21, 55), (20, 50, 100)):
        specs.append(StrategySpec(
            "triple_ema", f"三EMA排列 {fast}/{middle}/{slow}",
            {"fast": fast, "middle": middle, "slow": slow},
            f"{community}：CRYPTO 3EMA Strategy with TP/SL based on ATR（这里只复刻入场结构）",
        ))
    for period, bb, kc in ((20, 2.0, 1.5), (20, 2.0, 2.0), (30, 2.0, 1.5)):
        specs.append(StrategySpec(
            "squeeze_breakout", f"波动挤压突破 {period} BB{bb}/KC{kc}",
            {"period": period, "bb": bb, "kc": kc},
            f"{community}：Crypto Squeeze Strategy",
        ))
    for obv_ema, price_ema in ((10, 20), (20, 50), (30, 100)):
        specs.append(StrategySpec(
            "obv_trend", f"OBV趋势 {obv_ema} + EMA{price_ema}",
            {"obv_ema": obv_ema, "price_ema": price_ema},
            f"{community}：OBV Accumulation / Distribution Strategy Crypto",
        ))
    for period, mult, adx, threshold in ((10, 2.0, 14, 20), (10, 3.0, 14, 25), (14, 2.5, 14, 30)):
        specs.append(StrategySpec(
            "supertrend_adx", f"Supertrend+ADX {period} x{mult} ADX{threshold}",
            {"period": period, "mult": mult, "adx": adx, "threshold": threshold},
            f"{community}：ADX+DI+SUPERTREND Strategy",
        ))
    for period, dev in ((20, 1.5), (20, 2.0), (30, 2.0)):
        specs.append(StrategySpec(
            "bollinger_breakout", f"布林突破 {period} x{dev}",
            {"period": period, "dev": dev},
            f"{community}：Bollinger Bands Breakout Strategy",
        ))
    for entry, atr, mult in ((20, 14, 2.0), (40, 14, 2.5), (55, 20, 3.0)):
        specs.append(StrategySpec(
            "turtle_atr", f"海龟突破+ATR {entry}/{atr} x{mult}",
            {"entry": entry, "atr": atr, "mult": mult},
            f"{community}：Turtle trading strategy (Donchian/ATR)",
        ))
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
        gate_failures = []
        if train["net_bps"] <= 0:
            gate_failures.append("较早行情亏损")
        if test["net_bps"] <= 0:
            gate_failures.append("较新行情亏损")
        if test["trades"] < 6:
            gate_failures.append(f"较新行情只有{test['trades']}笔，少于6笔")
        if test["profit_factor"] < 1.10:
            gate_failures.append(f"盈亏比{test['profit_factor']:.2f}，低于1.10")
        if test["max_drawdown_bps"] < -1200:
            gate_failures.append(f"最大回落{test['max_drawdown_bps'] / 100:.2f}%，超过12%")
        promotable = not gate_failures
        score = test["net_bps"] + max(test["max_drawdown_bps"], -2000) * 0.35 + min(test["trades"], 20) * 2
        results.append({
            "coin": coin, "interval": interval, "family": spec.family, "strategy": spec.name,
            "params": spec.params, "reference": spec.reference, "samples": len(candles), "split_index": split,
            "train": train, "test": test, "stable": stable, "promotable": promotable,
            "gate_failures": gate_failures,
            "gate_summary": "本窗口全部最低条件满足；仍禁止实盘" if promotable else "；".join(gate_failures),
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
                    row["earlier_days"] = actual_days * row["split_index"] / max(1, row["samples"] - 1)
                    row["newer_days"] = max(0.0, actual_days - row["earlier_days"])
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
