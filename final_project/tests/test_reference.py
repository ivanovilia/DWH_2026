"""Run: python -m unittest discover -s tests -v (no Docker, Spark or Kafka required)."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from reference import compute  # noqa: E402


class ReferenceTests(unittest.TestCase):
    def test_fixture(self):
        report = compute(ROOT / "data" / "fixture")
        self.assertEqual(report["users"], 3)
        self.assertEqual(report["scd2_rows"], 7)
        self.assertEqual(report["events"], 26)
        self.assertEqual(sum(val[0] for val in report["minute"].values()), 20)
        self.assertEqual(sum(val[1] for val in report["minute"].values()), 3141)
        self.assertIn(("2026-01-01T00:01:00", 0), report["minute"])
        self.assertEqual(report["top5"][("2026-01-01", 1)], (0, 3, 317))
        self.assertEqual(report["words"][("2026-01-01", "космос")], 1)

    def test_demo(self):
        report = compute(ROOT / "data" / "demo")
        self.assertEqual(report["users"], 100)
        self.assertEqual(report["scd2_rows"], 187)
        self.assertEqual(report["events"], 6000)
        self.assertEqual(sum(val[0] for val in report["minute"].values()), 4503)
        self.assertEqual(sum(val[1] for val in report["minute"].values()), 11378116)
        self.assertEqual(len(report["top5"]), 15)


if __name__ == "__main__":
    unittest.main()
