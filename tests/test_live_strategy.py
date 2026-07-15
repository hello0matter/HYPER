import tempfile
import threading
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
            "live_l2_max_spread_bps": 5.0,
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
            cycle = monitor.prepare_shared_strategy_cycle(state, payload, 1)
            before = monitor.load_paper_snapshot(db_path, config=config)
            self.assertEqual(len(before["open"]), 0)
            self.assertEqual(len(payload["strategy_open_rows"]), 1)
            opened = monitor.finalize_shared_strategy_cycle(state, payload, 1, cycle)
            self.assertEqual(len(opened["open"]), 1)
            self.assertEqual(opened["open"][0]["mode"], "shared_strategy")

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

    def test_execution_pipeline_is_fixed_and_safe(self):
        steps = monitor.live_execution_steps({"live_execution_steps": ["submit_real"]})
        self.assertEqual(steps, list(monitor.LIVE_EXECUTION_STEPS))
        self.assertLess(steps.index("final_l2"), steps.index("submit_real"))
        self.assertLess(steps.index("submit_real"), steps.index("record_paper"))

    def test_cached_account_is_read_without_network(self):
        state = SimpleNamespace(
            lock=threading.Lock(),
            live_account={"ts": time.time(), "spot_available_usdc": 50.0},
        )
        account, age_ms = monitor.cached_live_account(state, max_age_ms=1000)
        self.assertEqual(account["spot_available_usdc"], 50.0)
        self.assertLess(age_ms, 1000)

    def test_price_model_can_lose_even_when_z_reverts(self):
        trade = {
            "asset": "ADA", "leader": "ETH", "action": "short_asset_long_hedge",
            "entry_ts": 1000.0, "entry_z": 2.01, "beta": 0.5,
            "notional_usdc": 30.0, "asset_notional_usdc": 20.0,
            "hedge_notional_usdc": 10.0, "asset_entry_px": 10.0,
            "hedge_entry_px": 100.0,
        }
        row = self.row(
            asset="ADA", leader="ETH", zscore=-0.17,
            asset_l2_bid=10.01, asset_l2_ask=10.02,
            hedge_l2_bid=100.1, hedge_l2_ask=100.2,
            funding_hourly=0,
        )
        details = monitor.paper_trade_pnl_details(
            trade, row, {**self.config, "paper_fee_bps": 0}, now_ts=1001.0,
        )
        # Old Z math would report roughly +39bps here.  Executable two-leg
        # prices correctly show a loss: short ADA -0.04U, long ETH +0.01U.
        self.assertEqual(details["pnl_model"], "l2_executable")
        self.assertAlmostEqual(details["pnl_usdc"], -0.03, places=6)
        self.assertAlmostEqual(details["pnl_bps"], -10.0, places=6)

    def unified_state(self, db_path):
        config = {
            **self.config,
            "paper_enabled": True,
            "paper_sync_live": True,
            "paper_take_profit_bps": 0,
            "paper_stop_bps": 80,
            "paper_max_hold_minutes": 360,
            "paper_min_corr": 0.65,
            "live_use_l2book": False,
            "live_notional_usdc": 10.5,
            "live_max_open": 1,
            "live_enabled": False,
            "live_strategy_enabled": False,
            "dingtalk_paper_webhook": "",
        }
        return SimpleNamespace(
            config=config, db_path=db_path, l2book=None,
            strategy_cycle_lock=threading.Lock(),
        )

    def test_realtime_idle_cycle_does_not_create_a_trade(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "monitor.sqlite3"
            monitor.init_alt_db(db_path)
            state = self.unified_state(db_path)
            payload = {"ts": 1000.0, "rows": [self.row(zscore=0.5)]}
            result = monitor.run_unified_strategy_cycle(
                state, payload, 1, skip_if_idle=True, source="ws_realtime"
            )
            self.assertIsNone(result)
            self.assertEqual(len(monitor.load_paper_snapshot(db_path, config=state.config)["open"]), 0)

    def test_realtime_signal_opens_only_once_and_closes_on_reversion(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "monitor.sqlite3"
            monitor.init_alt_db(db_path)
            state = self.unified_state(db_path)

            first = monitor.run_unified_strategy_cycle(
                state, {"ts": 1000.0, "rows": [self.row()]}, 1,
                skip_if_idle=True, source="ws_realtime",
            )
            self.assertIsNotNone(first)
            self.assertEqual(len(first["paper"]["open"]), 1)

            duplicate = monitor.run_unified_strategy_cycle(
                state, {"ts": 1000.5, "rows": [self.row()]}, 1,
                skip_if_idle=True, source="ws_realtime",
            )
            self.assertIsNone(duplicate)
            self.assertEqual(len(monitor.load_paper_snapshot(db_path, config=state.config)["open"]), 1)

            closed = monitor.run_unified_strategy_cycle(
                state, {"ts": 1001.0, "rows": [self.row(zscore=0.2)]}, 1,
                skip_if_idle=True, source="ws_realtime",
            )
            self.assertIsNotNone(closed)
            self.assertEqual(len(closed["paper"]["open"]), 0)
            self.assertEqual(closed["paper"]["closed"][0]["close_reason"], "偏离回归")

    def test_unified_cycle_persists_executable_price_model(self):
        class MutableBook:
            def __init__(self):
                self.prices = {"TEST": (10.0, 10.01), "ETH": (99.99, 100.0)}

            def get_book(self, coin):
                bid, ask = self.prices[coin]
                return {
                    "coin": coin, "bid": bid, "ask": ask, "mid": (bid + ask) / 2,
                    "spread_bps": (ask / bid - 1) * 10_000, "age_ms": 10,
                    "bid_size": 1000, "ask_size": 1000,
                }

            def snapshot(self, _coins=None):
                return {"status": {"connected": True}, "books": []}

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "monitor.sqlite3"
            monitor.init_alt_db(db_path)
            state = self.unified_state(db_path)
            state.config.update({
                "live_use_l2book": True, "live_l2_max_age_ms": 3000,
                "live_l2_max_spread_bps": 20,
                "paper_fee_bps": 4,
            })
            state.l2book = MutableBook()
            opened = monitor.run_unified_strategy_cycle(
                state, {"ts": 1000.0, "rows": [self.row()]}, 1,
                skip_if_idle=True, source="ws_realtime",
            )
            self.assertEqual(opened["paper"]["open"][0]["pnl_model"], "l2_executable")

            state.l2book.prices = {"TEST": (10.01, 10.02), "ETH": (100.1, 100.2)}
            closed = monitor.run_unified_strategy_cycle(
                state, {"ts": 1001.0, "rows": [self.row(zscore=0.2)]}, 1,
                skip_if_idle=True, source="ws_realtime",
            )
            trade = closed["paper"]["closed"][0]
            self.assertEqual(trade["pnl_model"], "l2_executable")
            self.assertLess(trade["pnl_bps"], 0)
            self.assertEqual(closed["paper"]["stats"]["trades"], 1)


if __name__ == "__main__":
    unittest.main()
