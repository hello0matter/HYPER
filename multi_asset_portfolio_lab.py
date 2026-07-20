#!/usr/bin/env python3
"""Select one technical strategy per product and test an equal-weight portfolio.

The selector deliberately freezes each product's strategy before the final
out-of-sample segment.  It is a research tool and never places orders.
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
        "early": selected["early"],
        "validation": selected["validation"],
        "test": test,
        "targets": selected["targets"],
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
        if equity <= 0:
            equity = 0.0
            series[int(candles[index + 1]["ts"])] = -1.0
            break
        series[int(candles[index + 1]["ts"])] = equity / before - 1 if before > 0 else -1.0
    return series


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
    years = len(values) / 252
    annualized = equity ** (1 / years) - 1 if equity > 0 and years > 0 else -1.0
    return {
        "initial_capital": capital,
        "final_value": equity * capital,
        "net_return_pct": (equity - 1) * 100,
        "annualized_return_pct": annualized * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "sharpe": mean / stdev * math.sqrt(252) if stdev > 0 else 0.0,
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
):
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
            row = select_product_strategy(
                symbol, candles,
                round_trip_cost_bps=round_trip_cost_bps,
                selection_ratio=selection_ratio,
                min_validation_trades=min_validation_trades,
                max_selection_drawdown_pct=max_selection_drawdown_pct,
                allow_short=allow_short,
            )
            rows.append(row)
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc)})
    included = [row for row in rows if row["selection_pass"] or not require_selection_pass]
    all_dates = sorted({
        ts
        for row in included
        for ts in _test_return_series(
            candles_by_symbol[row["symbol"]], row["targets"],
            start=row["selection_end"] + 1,
            round_trip_cost_bps=round_trip_cost_bps,
        )
    })
    product_series = {
        row["symbol"]: _test_return_series(
            candles_by_symbol[row["symbol"]], row["targets"],
            start=row["selection_end"] + 1,
            round_trip_cost_bps=round_trip_cost_bps,
        )
        for row in included
    }
    portfolio_returns = [
        (ts, statistics.fmean(series.get(ts, 0.0) for series in product_series.values()))
        for ts in all_dates
    ] if product_series else []
    metrics = _portfolio_metrics(portfolio_returns, capital=float(capital))
    for row in rows:
        row.pop("targets", None)
        row["included"] = row in included
        row["weight_pct"] = 100 / len(included) if row in included and included else 0.0
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
        "strategy_count": len(strategy_specs()),
        "selected_products": len(rows),
        "included_products": len(included),
        "portfolio": metrics,
        "rows": rows,
        "failures": failures,
        "note": (
            "每个产品只用前段选择自己的策略，最后约30%为锁定样本外；组合按纳入产品等权。"
            "这是防止直接拿全历史冠军的第一道保护，仍需滚动复验，禁止自动真实下单。"
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="多产品一品一策等权组合研究")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", type=float, default=8)
    parser.add_argument("--capital", type=float, default=20_000)
    parser.add_argument("--cost", type=float, default=20)
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
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
