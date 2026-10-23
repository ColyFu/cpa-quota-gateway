"""Delayed failover gate for a primary Codex OAuth credential."""

import os
import tempfile
import threading
import time

import yaml


class ProFailoverGate:
    def __init__(self, config_path, state_get, state_set, window_seconds=600, clock=time.time):
        self.config_path = config_path
        self.state_get = state_get
        self.state_set = state_set
        self.window_seconds = max(1, int(window_seconds))
        self.clock = clock
        self.lock = threading.Lock()
        self.failure_started_at = int(state_get("pro_failover_failure_started_at", "0") or 0)
        self.failure_count = int(state_get("pro_failover_failure_count", "0") or 0)
        self.backup_enabled = state_get("pro_failover_backup_enabled", "0") == "1"

    @staticmethod
    def is_pro_event(event):
        return (
            str(event.get("provider") or "").lower() == "codex"
            and str(event.get("auth_type") or "").lower() == "oauth"
        )

    def reconcile(self):
        """Make the config agree with the last durable gate state."""
        with self.lock:
            self._set_backup(self.backup_enabled)

    def observe(self, event, observed_at=None):
        if not self.is_pro_event(event):
            return None
        timestamp = int(self.clock() if observed_at is None else observed_at)
        failed = bool(event.get("failed"))
        with self.lock:
            if not failed:
                changed = self.backup_enabled or self.failure_started_at or self.failure_count
                self.failure_started_at = 0
                self.failure_count = 0
                self._persist()
                if self.backup_enabled:
                    self._set_backup(False)
                    print("Pro failover: OAuth recovered; backup API disabled", flush=True)
                return "recovered" if changed else None

            if not self.failure_started_at:
                self.failure_started_at = timestamp
                self.failure_count = 1
                self._persist()
                print(
                    f"Pro failover: failure window started; backup remains disabled for "
                    f"{self.window_seconds}s",
                    flush=True,
                )
                return "started"

            self.failure_count += 1
            self._persist()
            # Two failures spanning the full window prove that requests kept failing;
            # a single failure followed by an idle period never opens the paid fallback.
            if (
                not self.backup_enabled
                and self.failure_count >= 2
                and timestamp - self.failure_started_at >= self.window_seconds
            ):
                self._set_backup(True)
                print(
                    f"Pro failover: all observed OAuth requests failed for "
                    f"{timestamp - self.failure_started_at}s; backup API enabled",
                    flush=True,
                )
                return "failed_over"
            return "failing"

    def _persist(self):
        self.state_set("pro_failover_failure_started_at", str(self.failure_started_at))
        self.state_set("pro_failover_failure_count", str(self.failure_count))
        self.state_set("pro_failover_backup_enabled", "1" if self.backup_enabled else "0")

    def _set_backup(self, enabled):
        self._rewrite_backup_exclusion(not enabled)
        self.backup_enabled = enabled
        self._persist()

    def _rewrite_backup_exclusion(self, excluded):
        with open(self.config_path, "r", encoding="utf-8") as handle:
            original = handle.read()
        parsed = yaml.safe_load(original) or {}
        entries = parsed.get("codex-api-key") or []
        if len(entries) != 1:
            raise RuntimeError("Pro failover requires exactly one codex-api-key fallback")

        lines = original.splitlines(keepends=True)
        start = next((i for i, line in enumerate(lines) if line.startswith("codex-api-key:")), None)
        if start is None:
            raise RuntimeError("codex-api-key section is missing")
        end = len(lines)
        for i in range(start + 1, len(lines)):
            if lines[i] and not lines[i][0].isspace() and not lines[i].lstrip().startswith("#"):
                end = i
                break

        section = lines[start:end]
        exclusion = None
        for i, line in enumerate(section):
            if line.startswith("    excluded-models:"):
                exclusion = i
                break
        currently_excluded = exclusion is not None and any(
            line.strip() in ("- '*'", '- "*"', "- *")
            for line in section[exclusion + 1 :] if exclusion is not None
        )
        if currently_excluded == excluded:
            return

        if excluded:
            section.extend(["    excluded-models:\n", "      - '*'\n"])
        else:
            remove_end = exclusion + 1
            while remove_end < len(section) and section[remove_end].startswith("      "):
                remove_end += 1
            del section[exclusion:remove_end]

        updated = "".join(lines[:start] + section + lines[end:])
        candidate = yaml.safe_load(updated) or {}
        candidate_entries = candidate.get("codex-api-key") or []
        if len(candidate_entries) != 1:
            raise RuntimeError("refusing to write an invalid codex-api-key configuration")
        mode = os.stat(self.config_path).st_mode
        directory = os.path.dirname(self.config_path)
        fd, temporary = tempfile.mkstemp(prefix=".config.yaml.failover-", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(updated)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, mode)
            os.replace(temporary, self.config_path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
