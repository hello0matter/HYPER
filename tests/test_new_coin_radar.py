import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import new_coin_radar as radar


def pool_item(*, now, price=1.0, liquidity=20_000, volume_h1=8_000, buys=30, sells=10, change_h1=8):
    return {
        "id": "ton_test-pair",
        "attributes": {
            "name": "TEST / TON",
            "pool_created_at": radar.datetime.fromtimestamp(
                now - 12 * 3600, radar.timezone.utc,
            ).isoformat().replace("+00:00", "Z"),
            "base_token_price_usd": str(price),
            "reserve_in_usd": str(liquidity),
            "volume_usd": {"h1": str(volume_h1), "h24": str(volume_h1 * 3)},
            "price_change_percentage": {"h1": str(change_h1), "h24": "12"},
            "transactions": {"h1": {"buys": buys, "sells": sells}},
        },
    }


class NewCoinRadarTests(unittest.TestCase):
    def test_balanced_young_pool_becomes_candidate(self):
        now = 1_800_000_000
        row = radar.score_ton_pool(pool_item(now=now), now=now)
        self.assertEqual(row["status"], "candidate")
        self.assertGreaterEqual(row["score"], radar.DEFAULT_CONFIG["min_score"])
        self.assertAlmostEqual(row["buy_share_h1"], 0.75)

    def test_already_vertical_pool_is_not_chased(self):
        now = 1_800_000_000
        row = radar.score_ton_pool(pool_item(now=now, change_h1=90), now=now)
        self.assertNotEqual(row["status"], "candidate")
        self.assertIn("过热", row["reason"])

    def test_low_liquidity_pool_is_filtered(self):
        now = 1_800_000_000
        row = radar.score_ton_pool(pool_item(now=now, liquidity=200), now=now)
        self.assertEqual(row["status"], "filtered")
        self.assertIn("流动性不足", row["reason"])

    def test_volume_acceleration_adds_but_does_not_dominate_score(self):
        now = 1_800_000_000
        row = radar.score_ton_pool(pool_item(now=now), now=now)
        base_score = row["score"]
        row["volume_acceleration"] = 2.0
        score, _, _ = radar._pool_score(row, radar.normalize_config())
        self.assertGreater(score, base_score)
        self.assertLessEqual(score - base_score, 10)

    def test_complete_ton_failure_reuses_snapshot_without_updating_paper(self):
        now = 1_800_000_000
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "radar.sqlite3"
            row = radar.score_ton_pool(pool_item(now=now), now=now)
            radar.save_new_coin_snapshots(db_path, [row], now=now)
            with (
                patch.object(radar, "fetch_ton_new_pools", return_value=([], ["HTTP 429"])),
                patch.object(radar, "fetch_kraken_watchlist", return_value=([], [])),
                patch.object(radar, "update_new_coin_paper") as update_paper,
            ):
                result = radar.run_new_coin_radar(db_path, now=now + 300)
            self.assertTrue(result["ton_cache_stale"])
            self.assertEqual(result["rows"], [row])
            update_paper.assert_not_called()

    def test_paper_trade_opens_then_takes_profit(self):
        now = 1_800_000_000
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "radar.sqlite3"
            entry = radar.score_ton_pool(pool_item(now=now, price=1.0), now=now)
            opened = radar.update_new_coin_paper(db_path, [entry], now=now)
            self.assertEqual(opened["paper_stats"]["open_count"], 1)
            exit_row = dict(entry)
            exit_row["price_usd"] = 1.30
            closed = radar.update_new_coin_paper(db_path, [exit_row], now=now + 3600)
            self.assertEqual(closed["paper_stats"]["open_count"], 0)
            self.assertEqual(closed["paper_stats"]["closed"], 1)
            self.assertEqual(closed["trades"][0]["close_reason"], "固定止盈")
            self.assertGreater(closed["trades"][0]["pnl_usd"], 0)


if __name__ == "__main__":
    unittest.main()
