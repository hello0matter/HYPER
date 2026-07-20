#!/usr/bin/env python3
"""Generate standalone Pine Script v6 versions of strategy-lab rules."""

from __future__ import annotations

import json
import re


def _pine_text(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def pine_filename(spec):
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", f"HYPER_{spec.family}_{spec.name}").strip("_")
    return f"{safe[:120]}.pine"


def _signal_body(spec):
    p, family = spec.params, spec.family
    if family == "ema_cross":
        return f"""fast = ta.ema(close, {p['fast']})
slow = ta.ema(close, {p['slow']})
int signal = fast > slow ? 1 : -1
plot(fast, "Fast EMA", color=color.aqua)
plot(slow, "Slow EMA", color=color.orange)"""
    if family == "macd":
        return f"""[macdLine, signalLine, _] = ta.macd(close, {p['fast']}, {p['slow']}, {p['signal']})
int signal = macdLine > signalLine ? 1 : -1"""
    if family == "rsi_reversion":
        return f"""rsiValue = ta.rsi(close, {p['period']})
var int signal = 0
if signal == 0 and rsiValue <= {p['lower']}
    signal := 1
else if signal == 0 and rsiValue >= {p['upper']}
    signal := -1
else if signal == 1 and rsiValue >= {p['exit']}
    signal := 0
else if signal == -1 and rsiValue <= {p['exit']}
    signal := 0"""
    if family in ("bollinger_reversion", "bollinger_breakout"):
        period, dev = p["period"], p["dev"]
        entry_long = "close < lower" if family == "bollinger_reversion" else "close > upper"
        entry_short = "close > upper" if family == "bollinger_reversion" else "close < lower"
        exit_long = "close >= basis" if family == "bollinger_reversion" else "close < basis"
        exit_short = "close <= basis" if family == "bollinger_reversion" else "close > basis"
        return f"""basis = ta.sma(close, {period})
deviation = ta.stdev(close, {period}) * {dev}
upper = basis + deviation
lower = basis - deviation
var int signal = 0
if signal == 0 and {entry_long}
    signal := 1
else if signal == 0 and {entry_short}
    signal := -1
else if signal == 1 and {exit_long}
    signal := 0
else if signal == -1 and {exit_short}
    signal := 0
plot(basis, "Basis", color=color.gray)
plot(upper, "Upper", color=color.blue)
plot(lower, "Lower", color=color.blue)"""
    if family == "donchian_breakout":
        return f"""entryUpper = ta.highest(high[1], {p['entry']})
entryLower = ta.lowest(low[1], {p['entry']})
exitUpper = ta.highest(high[1], {p['exit']})
exitLower = ta.lowest(low[1], {p['exit']})
var int signal = 0
if close > entryUpper
    signal := 1
else if close < entryLower
    signal := -1
else if signal == 1 and close < exitLower
    signal := 0
else if signal == -1 and close > exitUpper
    signal := 0"""
    if family == "supertrend":
        return f"""[supertrendLine, direction] = ta.supertrend({p['mult']}, {p['period']})
int signal = direction < 0 ? 1 : -1
plot(supertrendLine, "Supertrend", color=signal > 0 ? color.green : color.red)"""
    if family == "momentum":
        return f"""trend = ta.ema(close, {p['ema']})
momentum = close / close[{p['lookback']}] - 1
int signal = momentum > 0 and close >= trend ? 1 : momentum < 0 and close <= trend ? -1 : 0
plot(trend, "Trend EMA", color=color.orange)"""
    if family == "macd_sma_filter":
        return f"""[macdLine, signalLine, _] = ta.macd(close, {p['fast']}, {p['slow']}, {p['signal']})
trend = ta.sma(close, {p['trend']})
int signal = macdLine > signalLine and close > trend ? 1 : macdLine < signalLine and close < trend ? -1 : 0
plot(trend, "Trend SMA", color=color.orange)"""
    if family == "bollinger_rsi":
        return f"""basis = ta.sma(close, {p['period']})
deviation = ta.stdev(close, {p['period']}) * {p['dev']}
upper = basis + deviation
lower = basis - deviation
rsiValue = ta.rsi(close, {p['rsi']})
var int signal = 0
if signal == 0 and close < lower and rsiValue <= {p['lower']}
    signal := 1
else if signal == 0 and close > upper and rsiValue >= {p['upper']}
    signal := -1
else if signal == 1 and close >= basis
    signal := 0
else if signal == -1 and close <= basis
    signal := 0
plot(basis, "Basis", color=color.gray)
plot(upper, "Upper", color=color.blue)
plot(lower, "Lower", color=color.blue)"""
    if family == "stoch_rsi":
        return f"""rsiValue = ta.rsi(close, {p['rsi']})
rsiLow = ta.lowest(rsiValue, {p['stoch']})
rsiHigh = ta.highest(rsiValue, {p['stoch']})
rawStoch = rsiHigh == rsiLow ? 50.0 : (rsiValue - rsiLow) / (rsiHigh - rsiLow) * 100
k = ta.sma(rawStoch, {p['smooth']})
d = ta.sma(k, {p['smooth']})
var int signal = 0
if ta.crossover(k, d) and k <= {p['lower']}
    signal := 1
else if ta.crossunder(k, d) and k >= {p['upper']}
    signal := -1
else if signal == 1 and k >= 50
    signal := 0
else if signal == -1 and k <= 50
    signal := 0"""
    if family == "ichimoku_ema":
        return f"""conversion = (ta.highest(high, {p['conversion']}) + ta.lowest(low, {p['conversion']})) / 2
base = (ta.highest(high, {p['base']}) + ta.lowest(low, {p['base']})) / 2
trend = ta.ema(close, {p['ema']})
int signal = conversion > base and close > trend ? 1 : conversion < base and close < trend ? -1 : 0
plot(conversion, "Conversion", color=color.aqua)
plot(base, "Base", color=color.orange)
plot(trend, "Trend EMA", color=color.gray)"""
    if family == "triple_ema":
        return f"""fast = ta.ema(close, {p['fast']})
middle = ta.ema(close, {p['middle']})
slow = ta.ema(close, {p['slow']})
int signal = fast > middle and middle > slow ? 1 : fast < middle and middle < slow ? -1 : 0
plot(fast, "Fast EMA", color=color.aqua)
plot(middle, "Middle EMA", color=color.orange)
plot(slow, "Slow EMA", color=color.gray)"""
    if family == "squeeze_breakout":
        return f"""basis = ta.sma(close, {p['period']})
bbWidth = ta.stdev(close, {p['period']}) * {p['bb']}
kcWidth = ta.atr({p['period']}) * {p['kc']}
squeeze = basis + bbWidth < basis + kcWidth and basis - bbWidth > basis - kcWidth
release = squeeze[1] and not squeeze
var int signal = 0
if release
    signal := close > basis ? 1 : -1
else if signal == 1 and close < basis
    signal := 0
else if signal == -1 and close > basis
    signal := 0
plot(basis, "Basis", color=color.gray)"""
    if family == "obv_trend":
        return f"""signedVolume = close > close[1] ? volume : close < close[1] ? -volume : 0
obv = ta.cum(signedVolume)
obvSignal = ta.ema(obv, {p['obv_ema']})
priceTrend = ta.ema(close, {p['price_ema']})
int signal = obv > obvSignal and close > priceTrend ? 1 : obv < obvSignal and close < priceTrend ? -1 : 0
plot(priceTrend, "Price EMA", color=color.orange)"""
    if family == "supertrend_adx":
        return f"""[supertrendLine, direction] = ta.supertrend({p['mult']}, {p['period']})
[plusDI, minusDI, adxValue] = ta.dmi({p['adx']}, {p['adx']})
int signal = adxValue >= {p['threshold']} ? (direction < 0 ? 1 : -1) : 0
plot(supertrendLine, "Supertrend", color=signal > 0 ? color.green : color.red)"""
    if family == "turtle_atr":
        return f"""entryUpper = ta.highest(high[1], {p['entry']})
entryLower = ta.lowest(low[1], {p['entry']})
atrValue = ta.atr({p['atr']})
var int signal = 0
var float peak = na
var float trough = na
if signal == 0 and close > entryUpper
    signal := 1
    peak := high
else if signal == 0 and close < entryLower
    signal := -1
    trough := low
else if signal == 1
    peak := na(peak) ? high : math.max(peak, high)
    if close < peak - {p['mult']} * atrValue
        signal := 0
        peak := na
else if signal == -1
    trough := na(trough) ? low : math.min(trough, low)
    if close > trough + {p['mult']} * atrValue
        signal := 0
        trough := na"""
    raise ValueError(f"unsupported strategy family: {family}")


def generate_pine_strategy(spec, *, round_trip_cost_bps=12.0, initial_capital=1000.0):
    one_way_percent = float(round_trip_cost_bps) / 200.0
    title = _pine_text(f"HYPER Lab - {spec.name}")
    reference = str(spec.reference).replace("\n", " ")
    params = json.dumps(spec.params, ensure_ascii=False, separators=(",", ":"))
    body = _signal_body(spec)
    return f'''//@version=6
// Generated by HYPER strategy lab.
// Reference: {reference}
// Parameters: {params}
// This is an independent implementation of public rules, not copied protected source code.
strategy("{title}", overlay=true, initial_capital={float(initial_capital):.2f}, currency=currency.USD,
     default_qty_type=strategy.percent_of_equity, default_qty_value=100, pyramiding=0,
     commission_type=strategy.commission.percent, commission_value={one_way_percent:.6f},
     process_orders_on_close=false)

{body}

if signal == 1 and strategy.position_size <= 0
    strategy.entry("Long", strategy.long)
else if signal == -1 and strategy.position_size >= 0
    strategy.entry("Short", strategy.short)
else if signal == 0 and strategy.position_size != 0
    strategy.close_all(comment="Flat")
'''
