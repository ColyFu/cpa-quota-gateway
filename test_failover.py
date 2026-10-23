import os
import tempfile
import unittest

import yaml

from failover import ProFailoverGate


CONFIG = """\
routing:
  strategy: fill-first
codex-api-key:
  - api-key: backup-secret
    base-url: https://backup.example/codex
    excluded-models:
      - '*'
"""


class ProFailoverGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "config.yaml")
        with open(self.path, "w", encoding="utf-8") as handle:
            handle.write(CONFIG)
        self.state = {}
        self.gate = ProFailoverGate(
            self.path,
            lambda key, default="": self.state.get(key, default),
            lambda key, value: self.state.__setitem__(key, value),
            window_seconds=600,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def event(self, failed, auth_type="oauth"):
        return {"provider": "codex", "auth_type": auth_type, "failed": failed}

    def backup_excluded(self):
        with open(self.path, encoding="utf-8") as handle:
            entry = yaml.safe_load(handle)["codex-api-key"][0]
        return entry.get("excluded-models") == ["*"]

    def test_single_failure_and_idle_time_never_enable_backup(self):
        self.assertEqual(self.gate.observe(self.event(True), 1000), "started")
        self.assertTrue(self.backup_excluded())
        self.assertFalse(self.gate.backup_enabled)

    def test_all_requests_must_fail_across_full_window(self):
        self.gate.observe(self.event(True), 1000)
        self.assertEqual(self.gate.observe(self.event(True), 1599), "failing")
        self.assertTrue(self.backup_excluded())
        self.assertEqual(self.gate.observe(self.event(True), 1600), "failed_over")
        self.assertFalse(self.backup_excluded())
        self.assertTrue(self.gate.backup_enabled)

    def test_any_pro_success_resets_window_and_disables_backup(self):
        self.gate.observe(self.event(True), 1000)
        self.gate.observe(self.event(True), 1600)
        self.assertEqual(self.gate.observe(self.event(False), 1601), "recovered")
        self.assertTrue(self.backup_excluded())
        self.assertEqual(self.gate.failure_started_at, 0)
        self.assertEqual(self.gate.failure_count, 0)

    def test_success_inside_window_requires_a_new_full_window(self):
        self.gate.observe(self.event(True), 1000)
        self.gate.observe(self.event(False), 1300)
        self.gate.observe(self.event(True), 1400)
        self.assertEqual(self.gate.observe(self.event(True), 1999), "failing")
        self.assertTrue(self.backup_excluded())
        self.assertEqual(self.gate.observe(self.event(True), 2000), "failed_over")

    def test_api_key_events_do_not_affect_pro_health(self):
        self.assertIsNone(self.gate.observe(self.event(True, "apikey"), 1000))
        self.assertEqual(self.gate.failure_count, 0)
        self.assertTrue(self.backup_excluded())


if __name__ == "__main__":
    unittest.main()
