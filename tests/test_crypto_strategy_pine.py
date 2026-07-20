import unittest

from crypto_strategy_lab import strategy_specs
from crypto_strategy_pine import generate_pine_strategy, pine_filename


class CryptoStrategyPineTests(unittest.TestCase):
    def test_every_catalog_strategy_has_standalone_pine(self):
        for spec in strategy_specs():
            with self.subTest(strategy=spec.name):
                code = generate_pine_strategy(spec, round_trip_cost_bps=12)
                self.assertTrue(code.startswith("//@version=6"))
                self.assertIn("strategy(\"HYPER Lab -", code)
                self.assertIn("commission_value=0.060000", code)
                self.assertIn("if signal == 1", code)
                self.assertIn(spec.reference, code)

    def test_filename_is_safe_and_has_pine_extension(self):
        filename = pine_filename(strategy_specs()[0])
        self.assertTrue(filename.endswith(".pine"))
        self.assertNotIn(" ", filename)
        self.assertLessEqual(len(filename), 125)


if __name__ == "__main__":
    unittest.main()
