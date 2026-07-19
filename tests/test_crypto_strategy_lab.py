import math
import unittest
from io import BytesIO
from urllib.error import HTTPError
from unittest.mock import patch

import crypto_strategy_lab as lab


def candles_from_closes(closes):
    rows = []
    previous = closes[0]
    for i, close in enumerate(closes):
        open_px = previous
        rows.append({
            "ts": i * 900_000, "open": open_px, "high": max(open_px, close) * 1.002,
            "low": min(open_px, close) * .998, "close": close, "volume": 1000 + i,
        })
        previous = close
    return rows


class CryptoStrategyLabTests(unittest.TestCase):
    def test_rate_limit_retries_before_proxy_fallback(self):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            @staticmethod
            def read():
                return b'{"ok": true}'

        rate_limit = HTTPError(lab.HL_INFO, 429, "Too Many Requests", {}, BytesIO())
        with patch.object(lab, "USE_LOCAL_PROXY", False), patch.object(
            lab, "urlopen", side_effect=[rate_limit, Response()],
        ) as request, patch.object(lab.time, "sleep") as sleep:
            result = lab._request_json({"type": "test"})
        self.assertEqual(result, {"ok": True})
        self.assertEqual(request.call_count, 2)
        sleep.assert_called_once_with(1.5)

    def test_lab_requeues_a_rate_limited_coin(self):
        candles = candles_from_closes([100 + i * .1 for i in range(260)])
        with patch.object(lab, "fetch_hl_ohlcv", side_effect=[RuntimeError("HTTP 429"), candles]) as fetch, patch.object(
            lab.time, "sleep",
        ):
            result = lab.run_strategy_lab(("BTC",), days=30, interval="15m")
        self.assertTrue(result["ok"])
        self.assertEqual(result["failures"], [])
        self.assertEqual(result["evaluations"], len(lab.strategy_specs()))
        self.assertEqual(fetch.call_count, 2)

    def test_history_fetch_paginates_beyond_exchange_page_limit(self):
        def item(ts):
            return {"t": ts, "o": "1", "h": "2", "l": ".5", "c": "1.5", "v": "10"}

        pages = [[item(8000), item(9000), item(10000)], [item(5000), item(6000), item(7000), item(8000)]]
        with patch.object(lab.time, "time", return_value=10), patch.object(lab, "_request_json", side_effect=pages) as request:
            rows = lab.fetch_hl_ohlcv("BTC", days=0.00005, interval="15m")
        self.assertEqual([row["ts"] for row in rows], [6000, 7000, 8000, 9000, 10000])
        self.assertEqual(request.call_count, 2)

    def test_strategy_catalog_covers_distinct_families(self):
        families = {item.family for item in lab.strategy_specs()}
        self.assertEqual(families, {
            "ema_cross", "macd", "rsi_reversion", "bollinger_reversion",
            "donchian_breakout", "supertrend", "momentum", "macd_sma_filter",
            "bollinger_rsi", "stoch_rsi", "ichimoku_ema", "triple_ema",
            "squeeze_breakout", "obv_trend", "supertrend_adx",
            "bollinger_breakout", "turtle_atr",
        })
        self.assertGreaterEqual(len(lab.strategy_specs()), 60)

    def test_every_catalog_strategy_produces_valid_targets(self):
        closes = [100 + math.sin(i / 7) * 4 + i * .01 for i in range(420)]
        candles = candles_from_closes(closes)
        for spec in lab.strategy_specs():
            with self.subTest(strategy=spec.name):
                targets = lab.strategy_targets(candles, spec)
                self.assertEqual(len(targets), len(candles))
                self.assertLessEqual(set(targets), {-1, 0, 1})

    def test_costs_can_turn_small_edge_negative(self):
        closes = [100 * math.exp(i * 0.00005) for i in range(300)]
        candles = candles_from_closes(closes)
        targets = [1] * len(candles)
        free = lab.backtest_targets(candles, targets, round_trip_cost_bps=0)
        costly = lab.backtest_targets(candles, targets, round_trip_cost_bps=200)
        self.assertGreater(free["net_bps"], costly["net_bps"])
        self.assertLess(costly["net_bps"], 0)

    def test_walk_forward_marks_clear_trend_candidate(self):
        closes = [100 * math.exp(i * 0.001) for i in range(420)]
        candles = candles_from_closes(closes)
        results = lab.evaluate_coin("TEST", candles, round_trip_cost_bps=2)
        ema = next(item for item in results if item["family"] == "ema_cross" and item["params"] == {"fast": 9, "slow": 21})
        self.assertGreater(ema["train"]["net_bps"], 0)
        self.assertGreater(ema["test"]["net_bps"], 0)
        self.assertEqual(ema["current_signal"], 1)

    def test_latest_signal_uses_latest_closed_bar(self):
        closes = [100 + i * .2 for i in range(260)]
        candles = candles_from_closes(closes)
        spec = lab.StrategySpec("momentum", "test", {"lookback": 5, "ema": 20})
        targets = lab.strategy_targets(candles, spec)
        self.assertEqual(targets[-1], 1)


if __name__ == "__main__":
    unittest.main()
