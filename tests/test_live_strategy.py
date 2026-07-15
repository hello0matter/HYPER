import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

import hyperliquid_correlation_monitor as monitor


class LiveStrategySignalTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "live_use_realtime_z": False,
            "live_min_entry_z": 2.0,
            "live_min_corr": 0.65,
            "live_max_entry_spread_bps": 5.0,
            "live_min_expected_edge_bps": 10.0,
            "paper_exit_z": 0.5,
            "paper_z_value_bps": 18.0,
            "paper_fee_bps": 4.0,
        }

    def row(self, **updates):
        row = {
            "tag": "watch",
            "action": "watch",
            "asset": "TEST",
            "leader": "ETH",
            "corr": 0.80,
            "beta": 1.0,
            "zscore": 3.0,
            "spread_bps": 1.0,
        }
        row.update(updates)
        return row

    def test_live_signal_does_not_depend_on_old_scan_tag(self):
        prepared = monitor.prepare_live_rows([self.row(tag="watch")], self.config)
        self.assertEqual(prepared[0]["live_status"], "pass")
        self.assertEqual(prepared[0]["action"], "short_asset_long_hedge")

    def test_direction_is_recomputed_after_z_changes_sign(self):
        prepared = monitor.prepare_live_rows([self.row(zscore=-3.0, action="short_asset_long_hedge")], self.config)
        self.assertEqual(prepared[0]["action"], "long_asset_short_hedge")

    def test_all_filter_reasons_share_one_rule_set(self):
        reasons = monitor.live_candidate_reject_reasons(
            self.row(zscore=1.0, corr=0.4, spread_bps=8.0), self.config
        )
        self.assertEqual([key for key, _text in reasons], ["z", "corr", "spread", "edge"])
        self.assertIn("|Z|", monitor.live_candidate_reject_reason(
            self.row(zscore=1.0, corr=0.4, spread_bps=8.0), self.config
        ))

    def test_unified_simulation_runs_without_a_real_trade(self):
        config = {
            **self.config,
            "paper_enabled": True,
            "paper_sync_live": True,
            "paper_take_profit_bps": 0,
            "paper_stop_bps": 80,
            "paper_max_hold_minutes": 360,
            "paper_min_corr": 0.65,
            "max_spread_bps": 8.0,
            "live_use_l2book": False,
            "live_notional_usdc": 10.5,
            "live_max_open": 1,
            "dingtalk_paper_webhook": "",
        }
        # Some Windows SQLite builds briefly retain a file handle after the
        # final connection closes; ignore only that temporary cleanup race.
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "monitor.sqlite3"
            monitor.init_alt_db(db_path)
            state = SimpleNamespace(config=config, db_path=db_path, l2book=None)
            payload = {"ts": 1000.0, "rows": [self.row()]}
            opened = monitor.update_shared_strategy_paper(state, payload, 1)
            self.assertEqual(len(opened["open"]), 1)
            self.assertEqual(opened["open"][0]["mode"], "shared_strategy")
            self.assertEqual(len(payload["strategy_open_rows"]), 1)

            payload = {"ts": 1060.0, "rows": [self.row(zscore=0.2)]}
            closed = monitor.update_shared_strategy_paper(state, payload, 2)
            self.assertEqual(len(closed["open"]), 0)
            self.assertEqual(len(closed["closed"]), 1)
            self.assertEqual(closed["closed"][0]["close_reason"], "偏离回归")

    def test_recent_unchanged_strategy_book_gets_execution_grace(self):
        class FakeBook:
            def get_book(self, coin):
                prices = {"TEST": (10.0, 10.001), "ETH": (2000.0, 2000.1)}
                bid, ask = prices[coin]
                return {
                    "coin": coin, "bid": bid, "ask": ask, "mid": (bid + ask) / 2,
                    "spread_bps": (ask / bid - 1) * 10_000, "age_ms": 4500,
                    "bid_size": 1000, "ask_size": 1000,
                }

            def snapshot(self, _coins=None):
                return {"status": {"connected": True}, "books": []}

        config = {
            **self.config,
            "live_use_l2book": True,
            "live_l2_max_age_ms": 3000,
            "live_strategy_entry_grace_ms": 10000,
            "live_l2_max_spread_bps": 5,
        }
        state = SimpleNamespace(config=config, l2book=FakeBook())
        row = self.row()
        row["_strategy_l2_check"] = {
            "checked_at": time.time() - 4,
            "books": {
                "TEST": {"bid": 10.0, "ask": 10.001},
                "ETH": {"bid": 2000.0, "ask": 2000.1},
            },
        }
        blocked, _ = monitor.live_l2book_reject_reason(state, row, 10.5, 10.5)
        allowed, _ = monitor.live_l2book_reject_reason(
            state, row, 10.5, 10.5, allow_strategy_grace=True
        )
        self.assertIn("数据过旧", blocked)
        self.assertEqual(allowed, "")


if __name__ == "__main__":
    unittest.main()
