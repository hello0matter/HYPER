import math
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import multi_asset_portfolio_lab as lab


def candles(n=900):
    rows = []
    previous = 100.0
    for i in range(n):
        close = 100 * math.exp(0.0004 * i + 0.02 * math.sin(i / 17))
        rows.append({
            "ts": i * 86_400_000,
            "open": previous,
            "high": max(previous, close) * 1.002,
            "low": min(previous, close) * .998,
            "close": close,
            "volume": 1000 + i,
        })
        previous = close
    return rows


class MultiAssetPortfolioLabTests(unittest.TestCase):
    def test_selector_freezes_before_final_segment(self):
        result = lab.select_product_strategy("TEST", candles(), round_trip_cost_bps=2)
        self.assertTrue(result["strategy"])
        self.assertGreater(result["selection_end"], 300)
        self.assertLess(result["selection_end"], result["samples"])
        self.assertIn("test", result)

    def test_portfolio_equal_weights_and_failures_are_reported(self):
        with patch.object(lab, "fetch_yahoo_ohlcv", side_effect=lambda symbol, years: candles()):
            result = lab.run_multi_asset_portfolio_lab(
                ("AAA", "BBB"), years=2, capital=1000, round_trip_cost_bps=2,
                require_selection_pass=False, engine="fixed", weight_mode="equal",
            )
        self.assertEqual(result["included_products"], 2)
        self.assertAlmostEqual(result["rows"][0]["weight_pct"], 50.0)
        self.assertEqual(result["portfolio"]["initial_capital"], 1000.0)
        self.assertIn("equity_curve", result["portfolio"])
        self.assertIn("benchmark", result)

    def test_risk_multiplier_includes_financing_and_limits_products(self):
        with patch.object(lab, "fetch_yahoo_ohlcv", side_effect=lambda symbol, years: candles()):
            result = lab.run_multi_asset_portfolio_lab(
                ("AAA", "BBB", "CCC"), years=2, capital=1000,
                round_trip_cost_bps=2, require_selection_pass=False,
                max_products=2, exposure_multiplier=2, annual_financing_pct=10,
                engine="fixed",
            )
        self.assertEqual(result["included_products"], 2)
        self.assertEqual(result["exposure_multiplier"], 2.0)
        self.assertEqual(result["annual_financing_pct"], 10.0)

    def test_adaptive_stays_in_cash_when_nothing_beats_benchmark(self):
        spec = SimpleNamespace(family="cash", name="永远现金", params={}, reference="test")
        with (
            patch.object(lab, "strategy_specs", return_value=[spec]),
            patch.object(lab, "strategy_targets", return_value=[0] * 900),
        ):
            result = lab.select_adaptive_product_strategy(
                "TEST", candles(), round_trip_cost_bps=0,
                min_validation_trades=0, require_benchmark_excess=True,
            )
        self.assertEqual(result["active_windows"], 0)
        self.assertTrue(result["latest_cash"])
        self.assertEqual(result["test"]["trades"], 0)

    def test_adaptive_first_window_does_not_change_after_future_prices_change(self):
        spec = SimpleNamespace(family="parity", name="奇偶规则", params={}, reference="test")
        base = candles()
        changed = [dict(row) for row in base]
        for index in range(760, len(changed)):
            factor = 1 + (index - 759) * 0.01
            for key in ("open", "high", "low", "close"):
                changed[index][key] *= factor

        def causal_targets(rows, unused_spec):
            return [1 if index % 2 else 0 for index in range(len(rows))]

        with (
            patch.object(lab, "strategy_specs", return_value=[spec]),
            patch.object(lab, "strategy_targets", side_effect=causal_targets),
        ):
            first = lab.select_adaptive_product_strategy(
                "TEST", base, round_trip_cost_bps=0,
                min_validation_trades=0, require_benchmark_excess=False,
            )
            second = lab.select_adaptive_product_strategy(
                "TEST", changed, round_trip_cost_bps=0,
                min_validation_trades=0, require_benchmark_excess=False,
            )
        before_change_first = [item for item in first["selection_history"] if item["start_ts"] < base[760]["ts"]]
        before_change_second = [item for item in second["selection_history"] if item["start_ts"] < base[760]["ts"]]
        self.assertEqual(before_change_first, before_change_second)

    def test_risk_parity_weights_sum_to_one_and_respect_cap(self):
        dates = [index * 86_400_000 for index in range(100)]
        underlying = {
            "LOW": {ts: 0.001 * math.sin(index) for index, ts in enumerate(dates)},
            "MID": {ts: 0.01 * math.sin(index) for index, ts in enumerate(dates)},
            "HIGH": {ts: 0.05 * math.sin(index) for index, ts in enumerate(dates)},
        }
        products = {symbol: dict(series) for symbol, series in underlying.items()}
        _, weights = lab._combine_product_returns(
            products, underlying, dates, weight_mode="risk_parity",
            exposure_multiplier=1, annual_financing_pct=0,
            volatility_window=60, max_weight_pct=50,
        )
        self.assertAlmostEqual(sum(weights.values()), 1.0)
        self.assertLessEqual(max(weights.values()), 0.5 + 1e-12)
        self.assertGreater(weights["LOW"], weights["HIGH"])

    def test_intraday_rows_are_compounded_into_one_natural_day(self):
        rows = [
            {"ts": 0, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1},
            {"ts": 3_600_000, "open": 100, "high": 111, "low": 99, "close": 110, "volume": 1},
            {"ts": 7_200_000, "open": 110, "high": 122, "low": 109, "close": 121, "volume": 1},
            {"ts": 10_800_000, "open": 121, "high": 134, "low": 120, "close": 133.1, "volume": 1},
        ]
        series = lab._test_return_series(
            rows, [1, 1, 1, 1], start=1, round_trip_cost_bps=0,
        )
        self.assertEqual(len(series), 1)
        self.assertAlmostEqual(next(iter(series.values())), 0.21)

    def test_empty_portfolio_has_zero_annual_return(self):
        metrics = lab._portfolio_metrics([], capital=1000)
        self.assertEqual(metrics["net_return_pct"], 0.0)
        self.assertEqual(metrics["annualized_return_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
