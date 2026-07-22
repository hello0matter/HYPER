#!/usr/bin/env python3
"""Walk-forward multi-market portfolio research.

The adaptive engine only uses information available before each rebalance,
keeps the final segment out of sample, and never places orders.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import statistics
import time
from datetime import datetime, timezone
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener, urlopen

from crypto_strategy_lab import backtest_targets, strategy_specs, strategy_targets


DEFAULT_SYMBOLS = (
    "SPY", "QQQ", "IWM", "EEM", "FXI", "EWJ", "EWG", "EWU", "TLT", "IEF",
    "GLD", "SLV", "USO", "DBA", "VNQ", "BTC-USD", "ETH-USD", "EURUSD=X",
    "JPY=X", "CL=F",
)
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
PROXY_OPENER = build_opener(ProxyHandler({
    "http": "http://127.0.0.1:7891",
    "https": "http://127.0.0.1:7891",
}))


def _download_json(url):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 HLM-Portfolio-Lab/1.0"})
    direct_error = None
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except OSError as exc:
        direct_error = exc
    try:
        with PROXY_OPENER.open(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except OSError as proxy_error:
        raise RuntimeError(f"Yahoo直连失败：{direct_error}；7891代理失败：{proxy_error}") from proxy_error


def fetch_yahoo_ohlcv(symbol, *, years=8):
    end = int(time.time()) + 86_400
    start = end - int(float(years) * 365.25 * 86_400)
    url = (
        YAHOO_CHART.format(symbol=quote(str(symbol), safe=""))
        + f"?period1={start}&period2={end}&interval=1d&events=history"
    )
    payload = _download_json(url)
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise RuntimeError(str(chart["error"]))
    results = chart.get("result") or []
    if not results:
        raise RuntimeError("Yahoo没有返回日线")
    result = results[0]
    timestamps = result.get("timestamp") or []
    quote_rows = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    rows = []
    for index, ts in enumerate(timestamps):
        try:
            values = {
                key: quote_rows.get(key, [])[index]
                for key in ("open", "high", "low", "close", "volume")
            }
            if any(values[key] is None for key in ("open", "high", "low", "close")):
                continue
            rows.append({
                "ts": int(ts) * 1000,
                "open": float(values["open"]),
                "high": float(values["high"]),
                "low": float(values["low"]),
                "close": float(values["close"]),
                "volume": float(values["volume"] or 0),
            })
        except (IndexError, TypeError, ValueError):
            continue
    rows.sort(key=lambda row: row["ts"])
    return rows


def _apply_direction(targets, allow_short):
    if allow_short:
        return targets
    return [max(0, int(target)) for target in targets]


def _selection_score(early, validation):
    return (
        min(early["net_bps"], validation["net_bps"])
        + 0.25 * max(early["max_drawdown_bps"], -5000)
        + 0.50 * max(validation["max_drawdown_bps"], -5000)
        + min(validation["trades"], 20) * 2
    )


def select_product_strategy(
    symbol,
    candles,
    *,
    round_trip_cost_bps=20.0,
    selection_ratio=0.70,
    min_validation_trades=3,
    max_selection_drawdown_pct=30.0,
    allow_short=False,
):
    if len(candles) < 500:
        raise ValueError(f"{symbol} 日线不足：{len(candles)}")
    selection_end = max(300, min(len(candles) - 120, int(len(candles) * selection_ratio)))
    early_end = max(200, int(selection_end * 0.70))
    candidates = []
    for spec in strategy_specs():
        targets = _apply_direction(strategy_targets(candles, spec), allow_short)
        early = backtest_targets(
            candles, targets, start=1, end=early_end,
            round_trip_cost_bps=round_trip_cost_bps,
        )
        validation = backtest_targets(
            candles, targets, start=early_end + 1, end=selection_end,
            round_trip_cost_bps=round_trip_cost_bps,
        )
        failures = []
        if early["net_return_pct"] <= 0:
            failures.append("较早选择段亏损")
        if validation["net_return_pct"] <= 0:
            failures.append("验证段亏损")
        if validation["trades"] < int(min_validation_trades):
            failures.append(f"验证段仅{validation['trades']}笔")
        if early["max_drawdown_pct"] < -abs(float(max_selection_drawdown_pct)):
            failures.append("较早选择段回撤超限")
        if validation["max_drawdown_pct"] < -abs(float(max_selection_drawdown_pct)):
            failures.append("验证段回撤超限")
        if early["bankrupt"] or validation["bankrupt"]:
            failures.append("选择阶段曾归零")
        candidates.append({
            "spec": spec,
            "targets": targets,
            "early": early,
            "validation": validation,
            "selection_pass": not failures,
            "selection_failures": failures,
            "selection_score": _selection_score(early, validation),
        })
    passing = [candidate for candidate in candidates if candidate["selection_pass"]]
    pool = passing or candidates
    selected = max(pool, key=lambda candidate: candidate["selection_score"])
    test = backtest_targets(
        candles, selected["targets"], start=selection_end + 1, end=len(candles) - 1,
        round_trip_cost_bps=round_trip_cost_bps,
    )
    spec = selected["spec"]
    return {
        "symbol": symbol,
        "strategy": spec.name,
        "family": spec.family,
        "params": spec.params,
        "reference": spec.reference,
        "samples": len(candles),
        "data_start": candles[0]["ts"],
        "data_end": candles[-1]["ts"],
        "selection_end": selection_end,
        "selection_pass": selected["selection_pass"],
        "selection_failures": selected["selection_failures"],
        "selection_score": selected["selection_score"],
        "early": selected["early"],
        "validation": selected["validation"],
        "test": test,
        "targets": selected["targets"],
    }


def _ema_values(values, period):
    output = [None] * len(values)
    alpha = 2.0 / (period + 1)
    value = None
    for index, item in enumerate(values):
        value = float(item) if value is None else alpha * float(item) + (1 - alpha) * value
        if index >= period - 1:
            output[index] = value
    return output


def _target_correlation(left, right, start, end):
    a = [float(value) for value in left[start:end]]
    b = [float(value) for value in right[start:end]]
    if len(a) < 3:
        return 1.0
    mean_a, mean_b = statistics.fmean(a), statistics.fmean(b)
    covariance = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
    variance_a = sum((x - mean_a) ** 2 for x in a)
    variance_b = sum((y - mean_b) ** 2 for y in b)
    denominator = math.sqrt(variance_a * variance_b)
    return covariance / denominator if denominator > 0 else 1.0


def _ensemble_targets(selected, candles, *, allow_short, start, end):
    output = [0] * len(candles)
    close = [row["close"] for row in candles]
    fast_trend, slow_trend = _ema_values(close, 50), _ema_values(close, 200)
    for index in range(max(1, start), min(len(candles), end)):
        votes = [int(candidate["targets"][index]) for candidate in selected]
        vote = sum(votes)
        bull = slow_trend[index] is not None and fast_trend[index] > slow_trend[index] and close[index] > slow_trend[index]
        bear = slow_trend[index] is not None and fast_trend[index] < slow_trend[index] and close[index] < slow_trend[index]
        if vote > 0 and bull:
            output[index] = 1
        elif vote < 0 and bear and allow_short:
            output[index] = -1
    return output


def select_adaptive_product_strategy(
    symbol,
    candles,
    *,
    round_trip_cost_bps=20.0,
    selection_ratio=0.70,
    min_validation_trades=3,
    max_selection_drawdown_pct=30.0,
    allow_short=False,
    training_bars=504,
    rebalance_bars=63,
    ensemble_size=3,
    max_signal_correlation=0.75,
    require_benchmark_excess=True,
):
    if len(candles) < 700:
        raise ValueError(f"{symbol} 日线不足：{len(candles)}")
    final_start = max(400, min(len(candles) - 126, int(len(candles) * selection_ratio)))
    specs = strategy_specs()
    targets_by_spec = [
        _apply_direction(strategy_targets(candles, spec), allow_short)
        for spec in specs
    ]
    dynamic_targets = [0] * len(candles)
    history = []
    active_scores = []
    last_early = backtest_targets(candles, [0] * len(candles), start=1, end=final_start, round_trip_cost_bps=0)
    last_validation = dict(last_early)
    for test_start in range(final_start, len(candles) - 1, max(21, int(rebalance_bars))):
        test_end = min(len(candles), test_start + max(21, int(rebalance_bars)))
        train_start = max(1, test_start - max(252, int(training_bars)))
        split = train_start + max(126, int((test_start - train_start) * 0.70))
        split = min(split, test_start - 32)
        benchmark_targets = [1] * len(candles)
        validation_benchmark = backtest_targets(
            candles, benchmark_targets, start=split + 1, end=test_start - 1,
            round_trip_cost_bps=round_trip_cost_bps,
        )
        candidates = []
        for spec, targets in zip(specs, targets_by_spec):
            early = backtest_targets(
                candles, targets, start=train_start, end=split,
                round_trip_cost_bps=round_trip_cost_bps,
            )
            validation = backtest_targets(
                candles, targets, start=split + 1, end=test_start - 1,
                round_trip_cost_bps=round_trip_cost_bps,
            )
            excess = validation["net_return_pct"] - validation_benchmark["net_return_pct"]
            valid = (
                early["net_return_pct"] > 0
                and validation["net_return_pct"] > 0
                and validation["trades"] >= int(min_validation_trades)
                and early["max_drawdown_pct"] >= -abs(float(max_selection_drawdown_pct))
                and validation["max_drawdown_pct"] >= -abs(float(max_selection_drawdown_pct))
                and not early["bankrupt"]
                and not validation["bankrupt"]
                and (not require_benchmark_excess or excess > 0)
            )
            score = (
                excess * 100
                + min(early["net_bps"], validation["net_bps"]) * 0.25
                + validation["max_drawdown_bps"] * 0.20
                + min(validation["trades"], 20) * 2
            )
            candidates.append({
                "spec": spec, "targets": targets, "early": early,
                "validation": validation, "excess_pct": excess,
                "score": score, "valid": valid,
            })
        candidates.sort(key=lambda candidate: candidate["score"], reverse=True)
        selected = []
        for candidate in candidates:
            if not candidate["valid"]:
                continue
            if all(
                abs(_target_correlation(
                    candidate["targets"], existing["targets"], split + 1, test_start,
                )) <= float(max_signal_correlation)
                for existing in selected
            ):
                selected.append(candidate)
            if len(selected) >= max(1, int(ensemble_size)):
                break
        if selected:
            quarter_targets = _ensemble_targets(
                selected, candles, allow_short=allow_short, start=test_start, end=test_end,
            )
            dynamic_targets[test_start:test_end] = quarter_targets[test_start:test_end]
            active_scores.append(statistics.fmean(candidate["score"] for candidate in selected))
            last_early = selected[0]["early"]
            last_validation = selected[0]["validation"]
        history.append({
            "start_ts": candles[test_start]["ts"],
            "end_ts": candles[test_end - 1]["ts"],
            "selected": [candidate["spec"].name for candidate in selected],
            "components": [{
                "family": candidate["spec"].family,
                "name": candidate["spec"].name,
                "params": candidate["spec"].params,
                "reference": candidate["spec"].reference,
                "validation_excess_pct": candidate["excess_pct"],
            } for candidate in selected],
            "validation_excess_pct": statistics.fmean(
                candidate["excess_pct"] for candidate in selected
            ) if selected else None,
            "cash": not selected,
        })
    test = backtest_targets(
        candles, dynamic_targets, start=final_start + 1, end=len(candles) - 1,
        round_trip_cost_bps=round_trip_cost_bps,
    )
    active_history = [item for item in history if not item["cash"]]
    latest_window = history[-1] if history else {"selected": [], "components": [], "cash": True}
    latest_names = latest_window["selected"]
    return {
        "symbol": symbol,
        "strategy": "滚动组合：" + (" + ".join(latest_names) if latest_names else "现金"),
        "family": "adaptive_ensemble",
        "params": {
            "training_bars": int(training_bars), "rebalance_bars": int(rebalance_bars),
            "ensemble_size": int(ensemble_size), "max_signal_correlation": float(max_signal_correlation),
            "require_benchmark_excess": bool(require_benchmark_excess),
        },
        "reference": "项目独立实现：滚动走样本外、低相关策略投票、趋势状态过滤",
        "samples": len(candles), "data_start": candles[0]["ts"], "data_end": candles[-1]["ts"],
        "selection_end": final_start,
        "selection_pass": bool(active_history),
        "selection_failures": [] if active_history else ["滚动选择期没有策略同时盈利并跑赢买入持有"],
        "selection_score": statistics.fmean(active_scores) if active_scores else -1_000_000.0,
        "early": last_early, "validation": last_validation, "test": test,
        "targets": dynamic_targets, "selection_history": history,
        "active_windows": len(active_history), "total_windows": len(history),
        "latest_cash": bool(latest_window["cash"]),
        "latest_components": latest_window["components"],
    }


def _test_return_series(candles, targets, *, start, round_trip_cost_bps):
    one_way_rate = float(round_trip_cost_bps) / 2 / 10_000
    position = 0
    equity = 1.0
    series = {}
    for index in range(max(1, int(start)), len(candles) - 1):
        desired = int(targets[index - 1])
        before = equity
        if desired != position:
            equity *= max(0.0, 1 - abs(desired - position) * one_way_rate)
            position = desired
        current_open = float(candles[index]["open"])
        next_open = float(candles[index + 1]["open"])
        equity *= 1 + position * (next_open / current_open - 1)
        day_ts = int(candles[index + 1]["ts"]) // 86_400_000 * 86_400_000
        if equity <= 0:
            equity = 0.0
            series[day_ts] = -1.0
            break
        daily_return = equity / before - 1 if before > 0 else -1.0
        series[day_ts] = (1 + series.get(day_ts, 0.0)) * (1 + daily_return) - 1
    return series


def _underlying_return_series(candles, *, start):
    series = {}
    for index in range(max(1, int(start)), len(candles)):
        previous = float(candles[index - 1]["close"])
        current = float(candles[index]["close"])
        if previous <= 0:
            continue
        day_ts = int(candles[index]["ts"]) // 86_400_000 * 86_400_000
        daily_return = current / previous - 1
        series[day_ts] = (1 + series.get(day_ts, 0.0)) * (1 + daily_return) - 1
    return series


def _cap_weights(raw_weights, cap):
    symbols = list(raw_weights)
    if not symbols:
        return {}
    cap = max(1 / len(symbols), min(1.0, float(cap)))
    remaining, weights, budget = set(symbols), {}, 1.0
    while remaining:
        total = sum(max(0.0, raw_weights[symbol]) for symbol in remaining)
        proposed = {
            symbol: budget * max(0.0, raw_weights[symbol]) / total
            if total > 0 else budget / len(remaining)
            for symbol in remaining
        }
        oversized = [symbol for symbol, weight in proposed.items() if weight > cap]
        if not oversized:
            weights.update(proposed)
            break
        for symbol in oversized:
            weights[symbol] = cap
            budget -= cap
            remaining.remove(symbol)
        if budget <= 0:
            break
    return weights


def _combine_product_returns(
    product_series,
    underlying_series,
    all_dates,
    *,
    weight_mode,
    exposure_multiplier,
    annual_financing_pct,
    volatility_window=60,
    max_weight_pct=20.0,
):
    symbols = list(product_series)
    history = {symbol: [] for symbol in symbols}
    output, latest_weights = [], {}
    financing_daily = (
        max(0.0, float(exposure_multiplier) - 1)
        * max(0.0, float(annual_financing_pct)) / 100 / 365
    )
    for ts in all_dates:
        if weight_mode == "risk_parity":
            raw = {}
            for symbol in symbols:
                values = history[symbol][-max(20, int(volatility_window)):]
                volatility = statistics.pstdev(values) if len(values) >= 20 else 0.0
                raw[symbol] = 1 / max(volatility, 0.0025)
            weights = _cap_weights(raw, float(max_weight_pct) / 100)
        else:
            weights = {symbol: 1 / len(symbols) for symbol in symbols} if symbols else {}
        base_return = sum(
            weights.get(symbol, 0.0) * product_series[symbol].get(ts, 0.0)
            for symbol in symbols
        )
        output.append((ts, float(exposure_multiplier) * base_return - financing_daily))
        for symbol in symbols:
            history[symbol].append(underlying_series[symbol].get(ts, 0.0))
        latest_weights = weights
    return output, latest_weights


def _portfolio_metrics(return_series, *, capital):
    equity, peak, max_drawdown = 1.0, 1.0, 0.0
    equity_curve, values = [], []
    for ts, daily_return in return_series:
        equity *= max(0.0, 1 + daily_return)
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1 if peak else -1.0)
        equity_curve.append({"ts": ts, "equity": equity * capital})
        values.append(daily_return)
        if equity <= 0:
            break
    mean = statistics.fmean(values) if values else 0.0
    stdev = statistics.pstdev(values) if len(values) > 1 else 0.0
    if len(return_series) > 1:
        years = max(
            1 / 365.25,
            (float(return_series[-1][0]) - float(return_series[0][0]))
            / 86_400_000 / 365.25,
        )
    else:
        years = len(values) / 365.25
    if not values:
        annualized = 0.0
    else:
        annualized = equity ** (1 / years) - 1 if equity > 0 and years > 0 else -1.0
    return {
        "initial_capital": capital,
        "final_value": equity * capital,
        "net_return_pct": (equity - 1) * 100,
        "annualized_return_pct": annualized * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "sharpe": mean / stdev * math.sqrt(365.25) if stdev > 0 else 0.0,
        "days": len(values),
        "bankrupt": equity <= 0,
        "equity_curve": equity_curve,
    }


def run_multi_asset_portfolio_lab(
    symbols=DEFAULT_SYMBOLS,
    *,
    years=8,
    capital=20_000.0,
    round_trip_cost_bps=20.0,
    selection_ratio=0.70,
    min_validation_trades=3,
    max_selection_drawdown_pct=30.0,
    allow_short=False,
    require_selection_pass=True,
    max_products=20,
    exposure_multiplier=1.0,
    annual_financing_pct=8.0,
    engine="adaptive",
    training_bars=504,
    rebalance_bars=63,
    ensemble_size=3,
    max_signal_correlation=0.75,
    require_benchmark_excess=True,
    weight_mode="risk_parity",
    volatility_window=60,
    max_weight_pct=20.0,
):
    engine = str(engine).strip().lower()
    if engine not in ("adaptive", "fixed"):
        raise ValueError("engine 只允许 adaptive 或 fixed")
    weight_mode = str(weight_mode).strip().lower()
    if weight_mode not in ("risk_parity", "equal"):
        raise ValueError("weight_mode 只允许 risk_parity 或 equal")
    clean_symbols = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
    candles_by_symbol, failures = {}, []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, max(1, len(clean_symbols)))) as pool:
        futures = {
            pool.submit(fetch_yahoo_ohlcv, symbol, years=years): symbol
            for symbol in clean_symbols
        }
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                candles_by_symbol[symbol] = future.result()
            except Exception as exc:
                failures.append({"symbol": symbol, "error": str(exc)})
    rows = []
    for symbol in clean_symbols:
        candles = candles_by_symbol.get(symbol)
        if not candles:
            continue
        try:
            common = {
                "round_trip_cost_bps": round_trip_cost_bps,
                "selection_ratio": selection_ratio,
                "min_validation_trades": min_validation_trades,
                "max_selection_drawdown_pct": max_selection_drawdown_pct,
                "allow_short": allow_short,
            }
            if engine == "adaptive":
                row = select_adaptive_product_strategy(
                    symbol, candles, **common,
                    training_bars=training_bars,
                    rebalance_bars=rebalance_bars,
                    ensemble_size=ensemble_size,
                    max_signal_correlation=max_signal_correlation,
                    require_benchmark_excess=require_benchmark_excess,
                )
            else:
                row = select_product_strategy(symbol, candles, **common)
            rows.append(row)
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc)})
    if engine == "adaptive":
        # Keep input order: ranking products on their completed OOS result would
        # use future information. Each rolling window already moves to cash when
        # no strategy passes its then-available validation gate.
        eligible = list(rows)
    else:
        eligible = [row for row in rows if row["selection_pass"] or not require_selection_pass]
        eligible.sort(key=lambda row: row["selection_score"], reverse=True)
    included = eligible[:max(1, int(max_products))]
    product_series = {
        row["symbol"]: _test_return_series(
            candles_by_symbol[row["symbol"]], row["targets"],
            start=row["selection_end"] + 1,
            round_trip_cost_bps=round_trip_cost_bps,
        )
        for row in included
    }
    underlying_series = {
        row["symbol"]: _underlying_return_series(
            candles_by_symbol[row["symbol"]], start=row["selection_end"] + 1,
        )
        for row in included
    }
    all_dates = sorted({ts for series in product_series.values() for ts in series})
    multiplier = max(0.0, float(exposure_multiplier))
    portfolio_returns, latest_weights = _combine_product_returns(
        product_series, underlying_series, all_dates,
        weight_mode=weight_mode,
        exposure_multiplier=multiplier,
        annual_financing_pct=annual_financing_pct,
        volatility_window=volatility_window,
        max_weight_pct=max_weight_pct,
    ) if product_series else ([], {})
    metrics = _portfolio_metrics(portfolio_returns, capital=float(capital))
    benchmark_series = {
        row["symbol"]: _test_return_series(
            candles_by_symbol[row["symbol"]], [1] * len(candles_by_symbol[row["symbol"]]),
            start=row["selection_end"] + 1,
            round_trip_cost_bps=round_trip_cost_bps,
        )
        for row in included
    }
    benchmark_returns = [
        (ts, statistics.fmean(series.get(ts, 0.0) for series in benchmark_series.values()))
        for ts in all_dates
    ] if benchmark_series else []
    benchmark = _portfolio_metrics(benchmark_returns, capital=float(capital))
    for row in rows:
        row.pop("targets", None)
        row["included"] = row in included
        row["weight_pct"] = latest_weights.get(row["symbol"], 0.0) * 100 if row in included else 0.0
    rows.sort(key=lambda row: (row["included"], row["test"]["net_return_pct"]), reverse=True)
    return {
        "ok": bool(rows),
        "ts": time.time(),
        "source": "Yahoo Finance日线（与Vibe的yfinance研究源同类）",
        "symbols": list(clean_symbols),
        "years": years,
        "capital": float(capital),
        "round_trip_cost_bps": float(round_trip_cost_bps),
        "selection_ratio": float(selection_ratio),
        "min_validation_trades": int(min_validation_trades),
        "max_selection_drawdown_pct": float(max_selection_drawdown_pct),
        "allow_short": bool(allow_short),
        "require_selection_pass": bool(require_selection_pass),
        "max_products": int(max_products),
        "exposure_multiplier": multiplier,
        "annual_financing_pct": float(annual_financing_pct),
        "engine": engine,
        "training_bars": int(training_bars),
        "rebalance_bars": int(rebalance_bars),
        "ensemble_size": int(ensemble_size),
        "max_signal_correlation": float(max_signal_correlation),
        "require_benchmark_excess": bool(require_benchmark_excess),
        "weight_mode": weight_mode,
        "volatility_window": int(volatility_window),
        "max_weight_pct": float(max_weight_pct),
        "strategy_count": len(strategy_specs()),
        "selected_products": len(rows),
        "included_products": len(included),
        "portfolio": metrics,
        "benchmark": benchmark,
        "excess_return_pct": metrics["net_return_pct"] - benchmark["net_return_pct"],
        "rows": rows,
        "failures": failures,
        "note": (
            "V2在最终约30%样本外中每隔一段时间重新选择，且每次只看当时以前的数据；"
            "无策略同时盈利并跑赢该产品买入持有时自动留现金。组合默认按历史波动分配风险。"
            "结果仍只是历史研究，禁止自动真实下单。"
            if engine == "adaptive" else
            "旧版在前段选定单一策略，最终约30%锁定样本外；组合不连接任何真实账户。"
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="多市场滚动组合研究")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", type=float, default=8)
    parser.add_argument("--capital", type=float, default=20_000)
    parser.add_argument("--cost", type=float, default=20)
    parser.add_argument("--max-products", type=int, default=20)
    parser.add_argument("--exposure", type=float, default=1)
    parser.add_argument("--financing", type=float, default=8)
    parser.add_argument("--engine", choices=("adaptive", "fixed"), default="adaptive")
    parser.add_argument("--weight-mode", choices=("risk_parity", "equal"), default="risk_parity")
    parser.add_argument("--allow-short", action="store_true")
    parser.add_argument("--include-failed", action="store_true")
    args = parser.parse_args()
    result = run_multi_asset_portfolio_lab(
        tuple(item.strip() for item in args.symbols.split(",") if item.strip()),
        years=args.years,
        capital=args.capital,
        round_trip_cost_bps=args.cost,
        allow_short=args.allow_short,
        require_selection_pass=not args.include_failed,
        max_products=args.max_products,
        exposure_multiplier=args.exposure,
        annual_financing_pct=args.financing,
        engine=args.engine,
        weight_mode=args.weight_mode,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
