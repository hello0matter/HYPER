import unittest
import tempfile
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from unittest.mock import patch

import hotspot_sentiment_lab as lab


class HotspotSentimentLabTests(unittest.TestCase):
    def test_parse_mappings_expands_assets(self):
        self.assertEqual(
            lab.parse_mappings("Qwen=BABA,9988.HK; Bitcoin=BTC-USD"),
            [
                {"keyword": "Qwen", "asset": "BABA"},
                {"keyword": "Qwen", "asset": "9988.HK"},
                {"keyword": "Bitcoin", "asset": "BTC-USD"},
            ],
        )

    def test_heat_baseline_uses_only_earlier_days(self):
        rows = [
            {"date": f"2025-01-{day:02d}", "article_count": value, "news_per_million": value}
            for day, value in enumerate([1] * 9 + [10], start=1)
        ]
        heat = lab.build_heat_series(rows, 7)
        self.assertEqual(heat[0]["heat_z"], 5.0)
        self.assertGreater(heat[-1]["heat_z"], 2.0)
        changed_future = rows[:-1] + [{**rows[-1], "news_per_million": 1000}]
        changed = lab.build_heat_series(changed_future, 7)
        self.assertEqual(heat[-2], changed[-2])

    def test_signal_enters_on_next_bar_open(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        prices = []
        heat = []
        for index in range(50):
            close = 100 + index * 0.2
            prices.append({
                "ts": int((start + timedelta(days=index)).timestamp() * 1000),
                "open": close + 0.05,
                "high": close + 1,
                "low": close - 1,
                "close": close,
                "volume": 5000 if index == 25 else 1000,
            })
            heat.append({
                "date": (start + timedelta(days=index)).date().isoformat(),
                "article_count": 100 if index == 25 else 1,
                "news_per_million": 100 if index == 25 else 1,
                "heat_z": 4 if index == 25 else 0,
                "heat_acceleration": 5 if index == 25 else 1,
            })
        result = lab.backtest_hotspot(prices, heat, {
            "entry_slices": 1,
            "volume_ratio": 1.2,
            "heat_z": 2,
            "heat_acceleration": 2,
            "round_trip_cost_bps": 0,
        })
        self.assertEqual(result["trade_count"], 1)
        self.assertEqual(result["trades"][0]["entry_date"], "2025-01-27")
        self.assertEqual(result["trades"][0]["reason"], "热点降温")

    def test_google_topic_includes_asset_suggestion(self):
        xml = b"""<?xml version='1.0'?><rss xmlns:ht='https://trends.google.com/trending/rss'><channel><item><title>Bitcoin rally</title><ht:approx_traffic>100K+</ht:approx_traffic></item></channel></rss>"""
        with patch.object(lab, "_download", return_value=xml):
            rows, failures = lab.fetch_google_hotspots(("US",))
        self.assertEqual(failures, [])
        self.assertEqual(rows[0]["suggested_assets"], ["BTC-USD"])

    def test_public_product_maps_to_listed_parent(self):
        self.assertEqual(lab.suggest_assets("Marvel new game"), ["DIS"])
        self.assertEqual(lab.suggest_assets("HK Express flights"), ["0293.HK"])

    def test_news_cache_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "hotspot.sqlite3"
            rows = [{"date": "2025-01-01", "article_count": 2, "news_per_million": 3.5}]
            lab.save_hotspot_news_cache(db_path, "Qwen", "2025-01-01", "2025-03-01", rows)
            self.assertEqual(
                lab.load_hotspot_news_cache(db_path, "Qwen", "2025-01-01", "2025-03-01"),
                rows,
            )


if __name__ == "__main__":
    unittest.main()
