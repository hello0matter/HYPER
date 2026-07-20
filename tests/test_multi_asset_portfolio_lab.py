import math
import unittest
from unittest.mock import patch

import multi_asset_portfolio_lab as lab


def candles(n=620):
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
                require_selection_pass=False,
            )
        self.assertEqual(result["included_products"], 2)
        self.assertAlmostEqual(result["rows"][0]["weight_pct"], 50.0)
        self.assertEqual(result["portfolio"]["initial_capital"], 1000.0)
        self.assertIn("equity_curve", result["portfolio"])


if __name__ == "__main__":
    unittest.main()
