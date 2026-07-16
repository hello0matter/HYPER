import json
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
            "asset_l2_bid": 10.0,
            "asset_l2_ask": 10.01,
            "hedge_l2_bid": 99.99,
            "hedge_l2_ask": 100.0,
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

    def test_official_costs_include_four_fills_and_funding(self):
        trade = {
            "asset": "APT", "leader": "BTC", "action": "short_asset_long_hedge",
            "entry_ts": 1000.0, "exit_ts": 1100.0, "total_notional_usdc": 28.0,
            "entry_json": json.dumps({"fills": {"APT": {"oid": 1}, "BTC": {"oid": 2}}}),
            "exit_json": json.dumps({"fills": {"APT": {"oid": 3}, "BTC": {"oid": 4}}}),
        }
        fills = {
            "1": {"fee": "0.002"}, "2": {"fee": "0.004"},
            "3": {"fee": "0.003", "closedPnl": "0.100"},
            "4": {"fee": "0.006", "closedPnl": "-0.030"},
        }
        funding = [
            {"time": 1_050_000, "delta": {"coin": "APT", "usdc": "0.001"}},
            {"time": 1_060_000, "delta": {"coin": "BTC", "usdc": "-0.002"}},
            {"time": 2_000_000, "delta": {"coin": "APT", "usdc": "99"}},
        ]
        result = monitor.official_trade_costs(trade, fills, funding)
        self.assertAlmostEqual(result["pnl_usdc"], 0.070, places=9)
        self.assertAlmostEqual(result["fee_usdc"], 0.015, places=9)
        self.assertAlmostEqual(result["funding_usdc"], -0.001, places=9)
        self.assertAlmostEqual(result["net_pnl_usdc"], 0.054, places=9)
        self.assertAlmostEqual(result["asset_net_pnl_usdc"], 0.096, places=9)
        self.assertAlmostEqual(result["hedge_net_pnl_usdc"], -0.042, places=9)

    def leadlag_config(self):
        return {
            "leadlag_enabled": True, "leadlag_notional_usdc": 20.0, "leadlag_max_open": 1,
            "leadlag_leader_3s_bps": 12.0, "leadlag_leader_15s_bps": 25.0,
            "leadlag_min_lag_bps": 15.0, "leadlag_min_corr": 0.75,
            "leadlag_max_spread_bps": 1.5, "leadlag_min_imbalance": 0.05,
            "leadlag_min_depth_multiple": 5.0, "leadlag_fee_bps": 5.0,
            "leadlag_min_edge_bps": 18.0, "leadlag_take_profit_bps": 30.0,
            "leadlag_stop_bps": 18.0, "leadlag_trail_start_bps": 15.0,
            "leadlag_trail_gap_bps": 10.0, "leadlag_max_hold_minutes": 10,
            "leadlag_cooldown_minutes": 20, "dingtalk_paper_webhook": "",
        }

    def test_leadlag_signal_requires_impulse_lag_and_book_confirmation(self):
        class FakeMotion:
            def motion(self, coin, windows=(1, 3, 15)):
                if coin == "BTC":
                    return {"ret_1s_bps": 5, "ret_3s_bps": 20, "ret_15s_bps": 30,
                            "age_ms": 10, "mid": 60000, "bid": 59999, "ask": 60001,
                            "spread_bps": .3, "imbalance": .1, "bid_size": 10, "ask_size": 10}
                return {"ret_1s_bps": 1, "ret_3s_bps": 2, "ret_15s_bps": 5,
                        "age_ms": 10, "mid": 10, "bid": 9.9995, "ask": 10.0005,
                        "spread_bps": 1.0, "imbalance": .2, "bid_size": 100, "ask_size": 100}

        signal = monitor.leadlag_pair_state(
            self.row(asset="TEST", leader="BTC", corr=.82, beta=1.5), FakeMotion(), self.leadlag_config()
        )
        self.assertTrue(signal["eligible"])
        self.assertEqual(signal["side"], "long")
        self.assertGreaterEqual(signal["lag_bps"], 15)
        self.assertGreaterEqual(signal["expected_edge_bps"], 18)

    def test_sampling_preset_can_disable_imbalance_gate(self):
        class FakeMotion:
            def motion(self, coin, windows=(1, 3, 15)):
                if coin == "BTC":
                    return {"ret_1s_bps": 5, "ret_3s_bps": 20, "ret_15s_bps": 30,
                            "age_ms": 10, "mid": 60000, "bid": 59999, "ask": 60001,
                            "spread_bps": .3, "imbalance": .1, "bid_size": 10, "ask_size": 10}
                return {"ret_1s_bps": 1, "ret_3s_bps": 2, "ret_15s_bps": 5,
                        "age_ms": 10, "mid": 10, "bid": 9.9995, "ask": 10.0005,
                        "spread_bps": 1.0, "imbalance": -.53, "bid_size": 100, "ask_size": 100}

        config = self.leadlag_config()
        config.update({
            "leadlag_leader_3s_bps": 2, "leadlag_leader_15s_bps": 4,
            "leadlag_min_lag_bps": 6, "leadlag_min_corr": .60,
            "leadlag_max_spread_bps": 2.5, "leadlag_min_imbalance": -1,
            "leadlag_min_depth_multiple": 2, "leadlag_min_edge_bps": 6,
        })
        signal = monitor.leadlag_pair_state(
            self.row(asset="TEST", leader="BTC", corr=.82, beta=1.5), FakeMotion(), config,
        )
        self.assertTrue(signal["eligible"])
        self.assertAlmostEqual(signal["imbalance"], -.53)

    def test_all_mids_updates_motion_price_history(self):
        cache = monitor.L2BookCache()
        cache._on_message(None, json.dumps({
            "channel": "l2Book", "data": {"coin": "BTC", "time": int(time.time() * 1000),
            "levels": [[{"px": "100", "sz": "10", "n": 1}], [{"px": "101", "sz": "8", "n": 1}]]},
        }))
        now = time.time()
        with cache.lock:
            cache.history["BTC"] = monitor.deque([(now - 16, 100.0), (now - 4, 100.5), (now - 2, 101.0)], maxlen=600)
        cache._on_message(None, json.dumps({"channel": "allMids", "data": {"mids": {"BTC": "102"}}}))
        motion = cache.motion("BTC")
        self.assertAlmostEqual(motion["mid"], 102.0)
        self.assertIsNotNone(motion["ret_3s_bps"])
        self.assertIsNotNone(motion["ret_15s_bps"])

    def test_recent_relationship_finds_dynamic_leader(self):
        cache = monitor.L2BookCache()
        now = time.time()
        leader, asset = [], []
        for i in range(40):
            ts = now - (39 - i) * 5
            leader_px = 100 + i * .1 + (i % 3) * .02
            asset_px = 10 + i * .02 + (i % 3) * .004
            leader.append((ts, leader_px))
            asset.append((ts, asset_px))
        with cache.lock:
            cache.history["SOL"] = monitor.deque(leader, maxlen=600)
            cache.history["TEST"] = monitor.deque(asset, maxlen=600)
        relationship = cache.recent_relationship("TEST", "SOL")
        self.assertIsNotNone(relationship)
        self.assertGreater(relationship["corr"], .9)
        self.assertGreater(relationship["samples"], 20)

    def test_leadlag_paper_trade_opens_and_takes_profit(self):
        class MutableMotion:
            def __init__(self):
                self.asset_mid = 10.0

            def motion(self, coin, windows=(1, 3, 15)):
                if coin == "BTC":
                    return {"ret_1s_bps": 5, "ret_3s_bps": 20, "ret_15s_bps": 30,
                            "age_ms": 10, "mid": 60000, "bid": 59999, "ask": 60001,
                            "spread_bps": .3, "imbalance": .1, "bid_size": 10, "ask_size": 10}
                mid = self.asset_mid
                return {"ret_1s_bps": 1, "ret_3s_bps": 2, "ret_15s_bps": 5,
                        "age_ms": 10, "mid": mid, "bid": mid * .99995, "ask": mid * 1.00005,
                        "spread_bps": 1.0, "imbalance": .2, "bid_size": 100, "ask_size": 100}

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "monitor.sqlite3"
            monitor.init_alt_db(db_path)
            book = MutableMotion()
            state = SimpleNamespace(
                config=self.leadlag_config(), db_path=db_path, l2book=book,
                leadlag_lock=threading.Lock(),
                leadlag_status={"running": False, "last_eval_ts": None, "last_error": None,
                                "eligible": 0, "opened": 0, "closed": 0, "signals": []},
            )
            row = self.row(asset="TEST", leader="BTC", corr=.82, beta=1.5, funding_hourly=0)
            opened = monitor.update_leadlag_strategy(state, [row], 1000.0)
            self.assertEqual(len(opened["open"]), 1)
            book.asset_mid = 10.05
            closed = monitor.update_leadlag_strategy(state, [row], 1001.0)
            self.assertEqual(len(closed["open"]), 0)
            self.assertEqual(len(closed["closed"]), 1)
            self.assertEqual(closed["closed"][0]["close_reason"], "达到单腿止盈")
            self.assertGreater(closed["closed"][0]["net_bps"], 30)

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

    def test_pair_cannot_reenter_during_cooldown(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "monitor.sqlite3"
            monitor.init_alt_db(db_path)
            state = self.unified_state(db_path)
            state.config["live_reentry_cooldown_minutes"] = 15
            monitor.run_unified_strategy_cycle(state, {"ts": 1000.0, "rows": [self.row()]}, 1)
            monitor.run_unified_strategy_cycle(state, {"ts": 1001.0, "rows": [self.row(zscore=0.2)]}, 2)
            blocked = monitor.run_unified_strategy_cycle(
                state, {"ts": 1002.0, "rows": [self.row()]}, 3, skip_if_idle=True,
            )
            self.assertIsNone(blocked)
            reopened = monitor.run_unified_strategy_cycle(
                state, {"ts": 1902.0, "rows": [self.row()]}, 4, skip_if_idle=True,
            )
            self.assertEqual(len(reopened["paper"]["open"]), 1)

    def test_real_fill_overrides_paper_entry_estimate(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "monitor.sqlite3"
            monitor.init_alt_db(db_path)
            state = self.unified_state(db_path)
            payload = {"ts": 1000.0, "rows": [self.row()]}
            cycle = monitor.prepare_shared_strategy_cycle(state, payload, 1)
            pending = payload["strategy_pending_entries"][0][0]
            payload["strategy_live_entries"] = {
                pending["trade_key"]: {
                    "trade_key": pending["trade_key"], "total_notional_usdc": 30.0,
                    "asset_notional_usdc": 20.0, "hedge_notional_usdc": 10.0,
                    "asset_entry_px": 10.123, "hedge_entry_px": 100.456, "entry_ts": 1000.25,
                }
            }
            snapshot = monitor.finalize_shared_strategy_cycle(state, payload, 1, cycle)
            trade = snapshot["open"][0]
            self.assertAlmostEqual(trade["asset_entry_px"], 10.123)
            self.assertAlmostEqual(trade["hedge_entry_px"], 100.456)
            self.assertAlmostEqual(trade["notional_usdc"], 30.0)

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
