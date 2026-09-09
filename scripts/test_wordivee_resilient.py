#!/usr/bin/env python3
"""Offline resilience tests. No Buffer/Cloudinary mutation is performed."""

from __future__ import annotations

import os
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from wordivee_resilient import (  # noqa: E402
    RequestFailure,
    dice_similarity,
    execute_entries,
    idempotency_key,
    iso_utc,
    normalize_quote,
    slot_due_wib,
)


class FakeBuffer:
    def __init__(self, fail_slot: int | None = None, fail_times: int = 0):
        self.posts = []
        self.created = []
        self.fail_slot = fail_slot
        self.fail_times = fail_times
        self.fail_counts = {}

    def scheduled_posts(self):
        return list(self.posts)

    def create_post(self, video_url, due_at):
        slot = int(video_url.rsplit("video", 1)[-1])
        count = self.fail_counts.get(slot, 0)
        if slot == self.fail_slot and count < self.fail_times:
            self.fail_counts[slot] = count + 1
            raise RequestFailure("buffer_timeout", "fault injection", True)
        post = {"id": f"post-{slot:02d}", "dueAt": iso_utc(due_at), "status": "scheduled"}
        self.posts.append(post)
        self.created.append(slot)
        return post, 1


def make_state(slots):
    target = date(2099, 1, 1)
    entries = {}
    for slot in slots:
        due = slot_due_wib(target, slot).astimezone(timezone.utc)
        normalized = f"quote unik untuk slot nomor {slot:02d}"
        entries[f"{slot:02d}"] = {
            "status": "planned",
            "attempts": 0,
            "idempotency_key": idempotency_key(target, slot, due),
            "scheduled_for": iso_utc(due),
            "video_public_id": f"wordavee/exports/video{slot:02d}",
            "video_url": f"https://example.invalid/video{slot}",
            "quote": {"text": normalized, "normalized": normalized, "hash": str(slot) * 64},
        }
    return {"version": 1, "target_date": target.isoformat(), "slots": entries}


class ResilienceTests(unittest.TestCase):
    def setUp(self):
        os.environ["WORDIVEE_RETRY_DELAYS"] = "0,0,0"
        self.now = lambda: datetime(2098, 12, 31, tzinfo=timezone.utc)

    def test_quote_normalization(self):
        self.assertEqual(normalize_quote("  Jangan—Menyerah!!! "), "jangan menyerah")
        self.assertEqual(dice_similarity("Tetaplah kuat!", "tetaplah kuat"), 1.0)

    def test_fault_isolation_continues_other_slots(self):
        state = make_state([1, 2, 3, 4, 5])
        buffer = FakeBuffer(fail_slot=3, fail_times=4)
        history = {"version": 1, "quotes": []}
        results = execute_entries(state, [1, 2, 3, 4, 5], buffer, history, dry_run=False, now_fn=self.now)
        self.assertEqual(buffer.created, [1, 2, 4, 5])
        self.assertEqual(state["slots"]["03"]["status"], "failed_retryable")
        self.assertEqual(sum(item.status == "failed" for item in results), 1)

    def test_idempotent_rerun_creates_no_duplicates(self):
        state = make_state([1, 2, 3])
        buffer = FakeBuffer()
        history = {"version": 1, "quotes": []}
        execute_entries(state, [1, 2, 3], buffer, history, dry_run=False, now_fn=self.now)
        execute_entries(state, [1, 2, 3], buffer, history, dry_run=False, now_fn=self.now)
        self.assertEqual(buffer.created, [1, 2, 3])
        self.assertEqual(len(history["quotes"]), 3)

    def test_recovery_processes_only_failed_slot(self):
        state = make_state([1, 2, 3, 4])
        buffer = FakeBuffer(fail_slot=3, fail_times=4)
        history = {"version": 1, "quotes": []}
        execute_entries(state, [1, 2, 3, 4], buffer, history, dry_run=False, now_fn=self.now)
        first_created = list(buffer.created)
        execute_entries(state, [1, 2, 3, 4], buffer, history, dry_run=False, now_fn=self.now)
        self.assertEqual(first_created, [1, 2, 4])
        self.assertEqual(buffer.created, [1, 2, 4, 3])
        self.assertEqual(state["slots"]["03"]["status"], "scheduled")
        self.assertEqual(len(history["quotes"]), 4)

    def test_retry_reconciles_ambiguous_timeout(self):
        state = make_state([1])

        class AmbiguousBuffer(FakeBuffer):
            def create_post(self, video_url, due_at):
                if not self.posts:
                    self.posts.append({"id": "accepted-before-timeout", "dueAt": iso_utc(due_at), "status": "scheduled"})
                    raise RequestFailure("buffer_timeout", "response hilang", True)
                raise AssertionError("mutation tidak boleh diulang setelah rekonsiliasi")

        buffer = AmbiguousBuffer()
        history = {"version": 1, "quotes": []}
        results = execute_entries(state, [1], buffer, history, dry_run=False, now_fn=self.now)
        self.assertEqual(state["slots"]["01"]["buffer_post_id"], "accepted-before-timeout")
        self.assertTrue(results[0].retry_success)

    def test_buffer_dueat_reconciliation(self):
        state = make_state([1])
        due = state["slots"]["01"]["scheduled_for"]
        buffer = FakeBuffer()
        buffer.posts.append({"id": "already-there", "dueAt": due, "status": "scheduled"})
        history = {"version": 1, "quotes": []}
        results = execute_entries(state, [1], buffer, history, dry_run=False, now_fn=self.now)
        self.assertEqual(buffer.created, [])
        self.assertEqual(state["slots"]["01"]["buffer_post_id"], "already-there")
        self.assertEqual(results[0].status, "skipped")


if __name__ == "__main__":
    unittest.main(verbosity=2)
