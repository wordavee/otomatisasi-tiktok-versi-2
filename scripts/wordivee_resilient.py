#!/usr/bin/env python3
"""Resilient Wordavee Cloudinary -> Buffer scheduler.

The workflow runs this program in two phases:
1. plan: validate credentials/assets and persist deterministic per-slot intent.
2. execute: reconcile Buffer by dueAt, create only missing posts, and persist results.

Only Python's standard library is required.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo


WIB = ZoneInfo("Asia/Jakarta")
UTC = timezone.utc
SESSION_SLOTS = {
    "1": list(range(1, 11)),
    "2": list(range(11, 21)),
    "3": list(range(21, 25)),
}
MAIN_SCHEDULES = {
    "30 0 * * *": "1",
    "30 10 * * *": "2",
    "30 20 * * *": "3",
}
RECOVERY_SCHEDULES = {
    "45 0 * * *": "1",
    "45 10 * * *": "2",
    "45 20 * * *": "3",
}
TERMINAL_STATUSES = {"scheduled", "skipped_existing", "skipped_late"}
RETRYABLE_HTTP = {408, 409, 425, 429, 500, 502, 503, 504}


class CriticalError(RuntimeError):
    """Configuration/auth failure that must fail the whole job."""


class RequestFailure(RuntimeError):
    def __init__(self, kind: str, message: str, retryable: bool, status: int | None = None):
        super().__init__(message)
        self.kind = kind
        self.retryable = retryable
        self.status = status


@dataclass
class SlotResult:
    slot: int
    status: str
    message: str
    retry_success: bool = False


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def normalize_quote(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = text.replace("_", " ")
    return re.sub(r"\s+", " ", text).strip()


def quote_hash(normalized: str) -> str:
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def dice_similarity(left: str, right: str) -> float:
    left = normalize_quote(left).replace(" ", "")
    right = normalize_quote(right).replace(" ", "")
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    if len(left) < 2 or len(right) < 2:
        return 0.0
    counts: dict[str, int] = {}
    for index in range(len(left) - 1):
        pair = left[index : index + 2]
        counts[pair] = counts.get(pair, 0) + 1
    overlap = 0
    for index in range(len(right) - 1):
        pair = right[index : index + 2]
        if counts.get(pair, 0) > 0:
            overlap += 1
            counts[pair] -= 1
    return 2.0 * overlap / ((len(left) - 1) + (len(right) - 1))


def idempotency_key(target_day: date, slot: int, due_at: datetime) -> str:
    raw = f"wordavee|{target_day.isoformat()}|{slot:02d}|{iso_utc(due_at)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def slot_due_wib(target_day: date, slot: int) -> datetime:
    if slot == 24:
        next_day = target_day + timedelta(days=1)
        return datetime(next_day.year, next_day.month, next_day.day, 0, 0, tzinfo=WIB)
    return datetime(target_day.year, target_day.month, target_day.day, slot, 0, tzinfo=WIB)


def retry_delays() -> list[float]:
    raw = os.environ.get("WORDIVEE_RETRY_DELAYS", "20,60,120")
    try:
        values = [max(0.0, float(item.strip())) for item in raw.split(",") if item.strip()]
    except ValueError as exc:
        raise CriticalError(f"WORDIVEE_RETRY_DELAYS tidak valid: {raw}") from exc
    return values or [20.0, 60.0, 120.0]


def classify_http(service: str, status: int, body: str) -> RequestFailure:
    lowered = body.lower()
    if status in {401, 403}:
        return RequestFailure(f"{service}_auth", f"{service} credential ditolak (HTTP {status})", False, status)
    if status == 404:
        return RequestFailure(f"{service}_not_found", f"{service} resource tidak ditemukan (HTTP 404)", False, status)
    if status == 400:
        return RequestFailure(f"{service}_invalid_input", f"{service} input ditolak: {body[:500]}", False, status)
    if status == 429 or "rate limit" in lowered:
        return RequestFailure(f"{service}_rate_limit", f"{service} rate limit (HTTP {status})", True, status)
    return RequestFailure(
        f"{service}_http",
        f"{service} HTTP {status}: {body[:500]}",
        status in RETRYABLE_HTTP,
        status,
    )


def request_json(
    service: str,
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    timeout: int = 45,
    before_retry: Callable[[int, RequestFailure], Any] | None = None,
    allow_retry: bool = True,
) -> tuple[dict[str, Any], int]:
    delays = retry_delays()
    attempts = len(delays) + 1 if allow_retry else 1
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(url, method=method, headers=headers or {}, data=data)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
                payload = json.loads(raw.decode("utf-8")) if raw else {}
                return payload, attempt
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            failure = classify_http(service, exc.code, body)
        except (urllib.error.URLError, TimeoutError) as exc:
            failure = RequestFailure(f"{service}_timeout", f"{service} timeout/network error: {exc}", True)
        except json.JSONDecodeError as exc:
            failure = RequestFailure(f"{service}_invalid_response", f"{service} JSON tidak valid: {exc}", True)

        if not failure.retryable or attempt >= attempts:
            raise failure
        if before_retry is not None:
            reconciled = before_retry(attempt, failure)
            if reconciled is not None:
                return {"_reconciled": reconciled}, attempt
        wait_seconds = delays[attempt - 1]
        print(f"⚠ {failure.kind}: {failure}. retry {attempt}/{len(delays)} dalam {wait_seconds:g} detik")
        time.sleep(wait_seconds)
    raise AssertionError("unreachable")


class CloudinaryClient:
    def __init__(self) -> None:
        self.cloud_name = required_env("CLOUDINARY_CLOUD_NAME")
        self.key = required_env("CLOUDINARY_API_KEY")
        self.secret = required_env("CLOUDINARY_API_SECRET")
        self.asset_folder = os.environ.get("EXPORT_ASSET_FOLDER", "wordavee/exports")

    def auth_headers(self) -> dict[str, str]:
        encoded = base64.b64encode(f"{self.key}:{self.secret}".encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    def list_exports(self) -> list[dict[str, Any]]:
        resources: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"asset_folder": self.asset_folder, "max_results": 500}
            if cursor:
                params["next_cursor"] = cursor
            url = (
                f"https://api.cloudinary.com/v1_1/{self.cloud_name}/resources/by_asset_folder?"
                + urllib.parse.urlencode(params)
            )
            page, _ = request_json("cloudinary", url, headers=self.auth_headers())
            resources.extend(page.get("resources") or [])
            cursor = page.get("next_cursor")
            if not cursor:
                return resources

    def fetch_details(self, assets: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        asset_ids = [str(asset.get("asset_id") or "") for asset in assets]
        if any(not item for item in asset_ids):
            raise CriticalError("Ada export tanpa asset_id; Quote Guard tidak dapat diverifikasi.")
        pairs = [("asset_ids[]", item) for item in asset_ids]
        pairs.append(("context", "true"))
        url = (
            f"https://api.cloudinary.com/v1_1/{self.cloud_name}/resources/by_asset_ids?"
            + urllib.parse.urlencode(pairs)
        )
        payload, _ = request_json("cloudinary", url, headers=self.auth_headers())
        return {str(item.get("asset_id")): item for item in (payload.get("resources") or [])}


class BufferClient:
    def __init__(self) -> None:
        self.key = required_env("BUFFER_API_KEY")
        self.channel_id = required_env("BUFFER_CHANNEL_ID")
        self.organization_id: str | None = None
        self.organization_name: str | None = None

    def graphql(self, query: str, *, allow_retry: bool = True) -> tuple[dict[str, Any], int]:
        delays = retry_delays()
        attempts = len(delays) + 1 if allow_retry else 1
        for attempt in range(1, attempts + 1):
            payload, http_attempt = request_json(
                "buffer",
                "https://api.buffer.com",
                method="POST",
                headers={"Authorization": f"Bearer {self.key}", "Content-Type": "application/json"},
                data=json.dumps({"query": query}).encode("utf-8"),
                allow_retry=allow_retry,
            )
            errors = payload.get("errors") or []
            if not errors:
                return payload.get("data") or {}, max(attempt, http_attempt)
            message = json.dumps(errors, ensure_ascii=False)
            lowered = message.lower()
            if any(word in lowered for word in ("unauthorized", "forbidden", "authentication", "invalid token")):
                raise RequestFailure("buffer_auth", f"Buffer credential ditolak: {message[:500]}", False)
            retryable = any(word in lowered for word in ("timeout", "temporar", "rate", "unavailable", "internal"))
            failure = RequestFailure("buffer_graphql", f"Buffer GraphQL: {message[:500]}", retryable)
            if not retryable or attempt >= attempts:
                raise failure
            wait_seconds = delays[attempt - 1]
            print(f"⚠ Buffer GraphQL sementara gagal; retry {attempt}/{len(delays)} dalam {wait_seconds:g} detik")
            time.sleep(wait_seconds)
        raise AssertionError("unreachable")

    def resolve_channel(self) -> None:
        account_data, _ = self.graphql("""
        query GetOrganizations {
          account { organizations { id name } }
        }
        """)
        organizations = ((account_data.get("account") or {}).get("organizations") or [])
        if not organizations:
            raise CriticalError("Buffer tidak mengembalikan organization; cek BUFFER_API_KEY.")
        for organization in organizations:
            org_id = organization.get("id")
            query = f"""
            query GetChannels {{
              channels(input: {{ organizationId: {json.dumps(org_id)} }}) {{
                id name displayName service
              }}
            }}
            """
            data, _ = self.graphql(query)
            for channel in data.get("channels") or []:
                if channel.get("id") == self.channel_id:
                    service = str(channel.get("service") or "").lower()
                    if service and service != "tiktok":
                        raise CriticalError(
                            f"BUFFER_CHANNEL_ID ditemukan tetapi service={service}, bukan TikTok wordavee."
                        )
                    self.organization_id = str(org_id)
                    self.organization_name = str(organization.get("name") or "")
                    return
        raise CriticalError("BUFFER_CHANNEL_ID tidak ditemukan; TikTok wordavee mungkin terputus.")

    def scheduled_posts(self) -> list[dict[str, Any]]:
        if not self.organization_id:
            self.resolve_channel()
        query = f"""
        query GetScheduledPosts {{
          posts(
            first: 100
            input: {{
              organizationId: {json.dumps(self.organization_id)}
              filter: {{ status: [scheduled], channelIds: [{json.dumps(self.channel_id)}] }}
              sort: [{{ field: dueAt, direction: asc }}]
            }}
          ) {{ edges {{ node {{ id dueAt channelId status }} }} }}
        }}
        """
        data, _ = self.graphql(query)
        return [edge.get("node") or {} for edge in ((data.get("posts") or {}).get("edges") or [])]

    def create_post(self, video_url: str, due_at: datetime) -> tuple[dict[str, Any], int]:
        due_iso = iso_utc(due_at)
        query = f"""
        mutation CreatePost {{
          createPost(
            input: {{
              text: ""
              channelId: {json.dumps(self.channel_id)}
              schedulingType: automatic
              mode: customScheduled
              dueAt: {json.dumps(due_iso)}
              assets: [{{ video: {{ url: {json.dumps(video_url)}, metadata: {{ thumbnailOffset: 1000 }} }} }}]
            }}
          ) {{
            ... on PostActionSuccess {{ post {{ id dueAt status }} }}
            ... on MutationError {{ message }}
          }}
        }}
        """
        # A create mutation is deliberately one-shot. The caller reconciles by dueAt
        # before deciding whether another attempt is safe.
        data, attempts = self.graphql(query, allow_retry=False)
        result = data.get("createPost") or {}
        if result.get("message"):
            message = str(result["message"])
            lowered = message.lower()
            retryable = any(word in lowered for word in ("timeout", "temporar", "rate", "unavailable", "try again"))
            raise RequestFailure("buffer_create", f"Buffer menolak post: {message}", retryable)
        post = result.get("post")
        if not post:
            raise RequestFailure("buffer_invalid_response", "Respons createPost tidak berisi post.", True)
        return post, attempts


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise CriticalError(f"Secret/env {name} kosong.")
    return value


def load_json(path: Path, fallback: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return fallback
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CriticalError(f"JSON tidak valid: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise CriticalError(f"Isi {path} harus object JSON.")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def decode_context(value: Any) -> str:
    return urllib.parse.unquote(str(value or ""))


def asset_label(asset: dict[str, Any]) -> str:
    return str(asset.get("display_name") or asset.get("filename") or asset.get("original_filename") or "")


def choose_assets(
    resources: list[dict[str, Any]], target_day: date, slots: list[int], max_age_hours: float, now: datetime
) -> tuple[dict[int, dict[str, Any]], dict[int, str]]:
    pattern = re.compile(r"^wordivee-video(\d{2})-(\d{4}-\d{2}-\d{2})(?:_|$)")
    candidates = {slot: [] for slot in slots}
    target_text = target_day.isoformat()
    for asset in resources:
        match = pattern.match(asset_label(asset).lower())
        if not match or match.group(2) != target_text:
            continue
        slot = int(match.group(1))
        if slot in candidates:
            candidates[slot].append(asset)
    chosen: dict[int, dict[str, Any]] = {}
    errors: dict[int, str] = {}
    for slot in slots:
        items = sorted(candidates[slot], key=lambda item: item.get("created_at", ""), reverse=True)
        if not items:
            errors[slot] = f"export tanggal {target_text} tidak ditemukan"
            continue
        asset = items[0]
        created = parse_iso(asset.get("created_at"))
        if not created:
            errors[slot] = "created_at export kosong"
            continue
        age = (now - created).total_seconds() / 3600
        if age > max_age_hours:
            errors[slot] = f"export berumur {age:.1f} jam (maks {max_age_hours:g})"
            continue
        if not asset.get("secure_url"):
            errors[slot] = "secure_url export kosong"
            continue
        chosen[slot] = asset
    return chosen, errors


def load_history(path: Path) -> dict[str, Any]:
    history = load_json(path, {"version": 1, "updated_at": None, "quotes": []})
    history.setdefault("version", 1)
    history.setdefault("updated_at", None)
    if not isinstance(history.get("quotes"), list):
        raise CriticalError("quotes-history.json: field quotes harus array.")
    return history


def reserved_quotes(state_dir: Path, exclude_path: Path) -> list[dict[str, Any]]:
    reserved: list[dict[str, Any]] = []
    if not state_dir.exists():
        return reserved
    for path in state_dir.glob("*.json"):
        if path == exclude_path or path.name.startswith("_"):
            continue
        payload = load_json(path, {})
        reserved.extend(reserved_from_state_payload(payload))
    return reserved


def reserved_from_state_payload(
    payload: dict[str, Any], exclude_slots: set[str] | None = None
) -> list[dict[str, Any]]:
    reserved: list[dict[str, Any]] = []
    excluded = exclude_slots or set()
    for key, entry in (payload.get("slots") or {}).items():
        if key in excluded or entry.get("status") not in {"planned", "failed_retryable"}:
            continue
        quote = entry.get("quote") or {}
        if quote.get("hash"):
            reserved.append(
                {**quote, "date": payload.get("target_date"), "slot": key, "reserved": True}
            )
    return reserved


def validate_quote(
    slot: int,
    asset: dict[str, Any],
    details: dict[str, dict[str, Any]],
    target_day: date,
    comparisons: list[dict[str, Any]],
    threshold: float,
) -> tuple[dict[str, str] | None, str | None]:
    detail = details.get(str(asset.get("asset_id"))) or {}
    custom = ((detail.get("context") or {}).get("custom") or {})
    text = decode_context(custom.get("quote"))
    normalized = decode_context(custom.get("quote_normalized")) or normalize_quote(text)
    supplied_hash = str(custom.get("quote_hash") or "").strip().lower()
    calculated_hash = quote_hash(normalized) if normalized else ""
    if supplied_hash and supplied_hash != calculated_hash:
        return None, "quote_hash metadata tidak cocok dengan quote_normalized"
    digest = supplied_hash or calculated_hash
    context_date = str(custom.get("posting_date") or "")
    context_slot = str(custom.get("video_slot") or "").zfill(2)
    if not text or not normalized or not digest:
        return None, "metadata quote tidak ada; export ulang dengan generator v38+"
    if context_date and context_date != target_day.isoformat():
        return None, f"posting_date={context_date}, seharusnya {target_day.isoformat()}"
    if context_slot and context_slot != f"{slot:02d}":
        return None, f"video_slot={context_slot}, seharusnya {slot:02d}"
    for old in comparisons:
        if old.get("hash") == digest:
            label = "reservasi" if old.get("reserved") else "history"
            return None, f"quote sudah ada pada {label} {old.get('date', '?')} slot {old.get('slot', '?')}"
        old_normalized = old.get("normalized") or normalize_quote(old.get("text"))
        if len(normalized) < 18 or len(old_normalized) < 18:
            continue
        ratio = min(len(normalized), len(old_normalized)) / max(len(normalized), len(old_normalized))
        if ratio < 0.72:
            continue
        score = dice_similarity(normalized, old_normalized)
        if score >= threshold:
            return None, f"quote terlalu mirip ({score * 100:.0f}%) dengan {old.get('date', '?')} slot {old.get('slot', '?')}"
    return {"text": text, "normalized": normalized, "hash": digest}, None


def resolve_run(args: argparse.Namespace, now: datetime) -> tuple[str, date, list[int], str]:
    session = args.session
    mode = args.mode
    schedule = os.environ.get("SCHEDULE_EXPR", "")
    if os.environ.get("EVENT_NAME") == "schedule":
        if schedule in MAIN_SCHEDULES:
            session, mode = MAIN_SCHEDULES[schedule], "main"
        elif schedule in RECOVERY_SCHEDULES:
            session, mode = RECOVERY_SCHEDULES[schedule], "recovery"
        else:
            hour = now.astimezone(WIB).hour
            if 1 <= hour <= 10:
                session = "1"
            elif 11 <= hour <= 20:
                session = "2"
            else:
                session = "3"
            mode = "recovery"
    elif session == "auto":
        hour = now.astimezone(WIB).hour
        session = "1" if hour < 10 else ("2" if hour < 20 else "3")
    if session not in SESSION_SLOTS:
        raise CriticalError(f"Session tidak valid: {session}")
    try:
        target_day = date.fromisoformat(args.target_date) if args.target_date else now.astimezone(WIB).date()
    except ValueError as exc:
        raise CriticalError("target_date harus YYYY-MM-DD.") from exc
    slots = SESSION_SLOTS[session]
    if args.slots.strip():
        try:
            requested = sorted({int(item.strip()) for item in args.slots.split(",") if item.strip()})
        except ValueError as exc:
            raise CriticalError("slots harus berupa angka dipisahkan koma, contoh 03,05.") from exc
        invalid = [slot for slot in requested if slot not in slots]
        if invalid:
            raise CriticalError(f"Slot {invalid} bukan bagian session {session}.")
        slots = requested
    return session, target_day, slots, mode


def error_record(kind: str, message: str, retryable: bool) -> dict[str, Any]:
    return {"kind": kind, "message": message, "retryable": retryable, "at": iso_utc(utc_now())}


def plan(args: argparse.Namespace) -> int:
    now = utc_now()
    session, target_day, slots, mode = resolve_run(args, now)
    state_dir = Path(args.state_dir)
    state_path = state_dir / f"{target_day.isoformat()}.json"
    history_path = Path(args.history_file)
    guard_start = date.fromisoformat(os.environ.get("QUOTE_GUARD_START_DATE", "2026-09-11"))
    guard_active = target_day >= guard_start
    threshold = float(os.environ.get("QUOTE_SIMILARITY_THRESHOLD", "0.94"))
    max_age = float(os.environ.get("MAX_EXPORT_AGE_HOURS", "30"))

    cloudinary = CloudinaryClient()
    buffer = BufferClient()
    resources = cloudinary.list_exports()
    buffer.resolve_channel()
    scheduled = buffer.scheduled_posts()
    chosen, asset_errors = choose_assets(resources, target_day, slots, max_age, now)
    details = cloudinary.fetch_details(chosen.values()) if guard_active and chosen else {}
    state = load_json(
        state_path,
        {"version": 1, "target_date": target_day.isoformat(), "timezone": "Asia/Jakarta", "slots": {}},
    )
    state.setdefault("slots", {})
    history = load_history(history_path)
    comparisons = (
        list(history.get("quotes") or [])
        + reserved_quotes(state_dir, state_path)
        + reserved_from_state_payload(state, {f"{slot:02d}" for slot in slots})
    )
    batch: list[dict[str, Any]] = []

    print(f"WIB sekarang       : {now.astimezone(WIB).isoformat()}")
    print(f"Session / mode     : {session} / {mode}")
    print(f"Target tanggal WIB : {target_day}")
    print(f"Slots              : {slots}")
    print(f"Cloudinary exports : {len(resources)} di {cloudinary.asset_folder}")
    print(f"Buffer scheduled   : {len(scheduled)} (queue tidak wajib kosong)")
    print(f"Quote Guard        : {'AKTIF' if guard_active else 'LEGACY'}")

    for slot in slots:
        key = f"{slot:02d}"
        existing = state["slots"].get(key) or {}
        if existing.get("status") in {"scheduled", "skipped_existing"}:
            print(f"Video{key} ✅ state sudah {existing.get('status')}, tidak diubah")
            continue
        due = slot_due_wib(target_day, slot).astimezone(UTC)
        if due <= now + timedelta(minutes=3):
            state["slots"][key] = {
                **existing,
                "status": "skipped_late",
                "scheduled_for": iso_utc(due),
                "last_error": error_record("deadline", "waktu posting lewat/terlalu dekat", False),
                "updated_at": iso_utc(now),
            }
            print(f"Video{key} ⏭ waktu sudah lewat/terlalu dekat")
            continue
        if slot in asset_errors:
            state["slots"][key] = {
                **existing,
                "status": "failed_missing_asset",
                "scheduled_for": iso_utc(due),
                "last_error": error_record("missing_asset", asset_errors[slot], False),
                "updated_at": iso_utc(now),
            }
            print(f"Video{key} ❌ {asset_errors[slot]}")
            continue
        asset = chosen[slot]
        quote: dict[str, str] = {}
        if guard_active:
            quote_value, quote_error = validate_quote(
                slot, asset, details, target_day, comparisons + batch, threshold
            )
            if quote_error:
                state["slots"][key] = {
                    **existing,
                    "status": "failed_quote",
                    "scheduled_for": iso_utc(due),
                    "video_public_id": asset.get("public_id"),
                    "last_error": error_record("quote_guard", quote_error, False),
                    "updated_at": iso_utc(now),
                }
                print(f"Video{key} ❌ Quote Guard: {quote_error}")
                continue
            quote = quote_value or {}
            batch.append({**quote, "date": target_day.isoformat(), "slot": slot, "reserved": True})
        state["slots"][key] = {
            "status": "planned",
            "attempts": int(existing.get("attempts") or 0),
            "idempotency_key": idempotency_key(target_day, slot, due),
            "scheduled_for": iso_utc(due),
            "video_public_id": asset.get("public_id"),
            "video_asset_id": asset.get("asset_id"),
            "video_url": asset.get("secure_url"),
            "quote": quote,
            "last_error": None,
            "updated_at": iso_utc(now),
        }
        print(f"Video{key} ✅ planned -> {slot_due_wib(target_day, slot).strftime('%Y-%m-%d %H:%M WIB')}")

    state["updated_at"] = iso_utc(now)
    context = {
        "session": session,
        "mode": mode,
        "target_date": target_day.isoformat(),
        "slots": slots,
        "dry_run": args.dry_run,
        "state_path": str(state_path),
        "planned_state": state,
        "buffer_organization_id": buffer.organization_id,
        "buffer_organization_name": buffer.organization_name,
    }
    write_json(Path(args.context_file), context)
    if args.dry_run:
        print("DRY RUN: state hanya disimpan pada context sementara; repository tidak diubah.")
    else:
        write_json(state_path, state)
        print(f"State intent tersimpan: {state_path}")
    return 0


def scheduled_by_due(posts: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for post in posts:
        due = parse_iso(post.get("dueAt"))
        if due:
            result[iso_utc(due)] = post
    return result


def append_history_once(history: dict[str, Any], entry: dict[str, Any], post_id: str) -> bool:
    digest = str((entry.get("quote") or {}).get("hash") or "")
    if not digest:
        return False
    for old in history.get("quotes") or []:
        if old.get("hash") == digest or (post_id and old.get("buffer_post_id") == post_id):
            return False
    quote = entry["quote"]
    history["quotes"].append(
        {
            "text": quote.get("text"),
            "normalized": quote.get("normalized"),
            "hash": digest,
            "date": entry.get("target_date"),
            "slot": entry.get("slot"),
            "buffer_post_id": post_id,
            "cloudinary_public_id": entry.get("video_public_id"),
            "saved_at": iso_utc(utc_now()),
        }
    )
    history["updated_at"] = iso_utc(utc_now())
    return True


def create_post_with_reconciliation(buffer: Any, video_url: str, due: datetime) -> tuple[dict[str, Any], int, bool]:
    """Retry a create only after proving the dueAt is still absent.

    This avoids the classic timeout ambiguity where Buffer accepted the first
    mutation but the response never reached the runner.
    """
    delays = retry_delays()
    total_attempts = len(delays) + 1
    last_failure: RequestFailure | None = None
    for attempt in range(1, total_attempts + 1):
        try:
            post, _ = buffer.create_post(video_url, due)
            return post, attempt, False
        except RequestFailure as exc:
            last_failure = exc
            try:
                reconciled = scheduled_by_due(buffer.scheduled_posts()).get(iso_utc(due))
            except Exception as reconcile_error:
                # We cannot prove absence, so another mutation would be unsafe.
                raise RequestFailure(
                    "buffer_reconcile_failed",
                    f"create gagal dan verifikasi dedupe juga gagal: {reconcile_error}",
                    True,
                ) from exc
            if reconciled:
                return reconciled, attempt, True
            if not exc.retryable or attempt >= total_attempts:
                raise
            wait_seconds = delays[attempt - 1]
            print(
                f"⚠ Buffer create belum berhasil dan dueAt masih kosong; "
                f"retry {attempt}/{len(delays)} dalam {wait_seconds:g} detik"
            )
            time.sleep(wait_seconds)
    assert last_failure is not None
    raise last_failure


def execute_entries(
    state: dict[str, Any],
    slots: list[int],
    buffer: Any,
    history: dict[str, Any],
    *,
    dry_run: bool,
    now_fn: Callable[[], datetime] = utc_now,
) -> list[SlotResult]:
    results: list[SlotResult] = []
    posts = buffer.scheduled_posts()
    due_map = scheduled_by_due(posts)
    queue_size = len(posts)
    for slot in slots:
        key = f"{slot:02d}"
        entry = state.get("slots", {}).get(key)
        if not entry:
            results.append(SlotResult(slot, "failed", "state slot tidak ditemukan"))
            continue
        entry["slot"] = slot
        entry["target_date"] = state.get("target_date")
        due_text = entry.get("scheduled_for")
        due = parse_iso(due_text)
        if not due:
            entry["status"] = "failed_permanent"
            entry["last_error"] = error_record("invalid_state", "scheduled_for tidak valid", False)
            results.append(SlotResult(slot, "failed", "scheduled_for tidak valid"))
            continue
        existing_post = due_map.get(iso_utc(due))
        if entry.get("status") in {"scheduled", "skipped_existing"}:
            results.append(SlotResult(slot, "skipped", f"state {entry.get('status')}"))
            continue
        if existing_post:
            post_id = str(existing_post.get("id") or "")
            entry.update(
                {
                    "status": "scheduled",
                    "buffer_post_id": post_id,
                    "reconciled": True,
                    "last_error": None,
                    "updated_at": iso_utc(now_fn()),
                }
            )
            append_history_once(history, entry, post_id)
            results.append(SlotResult(slot, "skipped", f"sudah ada di Buffer ({post_id}), direkonsiliasi"))
            continue
        if entry.get("status") not in {"planned", "failed_retryable"}:
            results.append(SlotResult(slot, "failed", f"state {entry.get('status')} tidak dapat diproses"))
            continue
        if due <= now_fn() + timedelta(minutes=3):
            entry["status"] = "skipped_late"
            entry["last_error"] = error_record("deadline", "waktu posting lewat/terlalu dekat", False)
            entry["updated_at"] = iso_utc(now_fn())
            results.append(SlotResult(slot, "skipped", "melewati cutoff; jadwal tidak digeser"))
            continue
        if dry_run:
            results.append(SlotResult(slot, "success", "DRY RUN: akan dibuat, tanpa mutasi"))
            continue
        if queue_size >= 10:
            entry["status"] = "failed_retryable"
            entry["last_error"] = error_record("buffer_queue_full", "queue Buffer mencapai batas 10", True)
            entry["updated_at"] = iso_utc(now_fn())
            results.append(SlotResult(slot, "failed", "queue Buffer penuh; menunggu recovery"))
            continue

        # Re-query immediately before every mutation. This is the dedupe barrier.
        refreshed = buffer.scheduled_posts()
        due_map = scheduled_by_due(refreshed)
        queue_size = len(refreshed)
        existing_post = due_map.get(iso_utc(due))
        if existing_post:
            post_id = str(existing_post.get("id") or "")
            entry.update(
                {"status": "scheduled", "buffer_post_id": post_id, "reconciled": True, "last_error": None}
            )
            append_history_once(history, entry, post_id)
            results.append(SlotResult(slot, "skipped", f"ditemukan saat recheck ({post_id})"))
            continue
        if queue_size >= 10:
            entry["status"] = "failed_retryable"
            entry["last_error"] = error_record("buffer_queue_full", "queue Buffer mencapai batas 10", True)
            results.append(SlotResult(slot, "failed", "queue Buffer penuh saat recheck"))
            continue
        try:
            entry["attempts"] = int(entry.get("attempts") or 0) + 1
            post, attempts, reconciled_after_error = create_post_with_reconciliation(
                buffer, str(entry.get("video_url") or ""), due
            )
            post_id = str(post.get("id") or "")
            entry.update(
                {
                    "status": "scheduled",
                    "buffer_post_id": post_id,
                    "reconciled": reconciled_after_error,
                    "last_error": None,
                    "updated_at": iso_utc(now_fn()),
                }
            )
            append_history_once(history, entry, post_id)
            queue_size += 1
            due_map[iso_utc(due)] = post
            detail = f"Buffer post {post_id}"
            if reconciled_after_error:
                detail += " (direkonsiliasi setelah error)"
            results.append(
                SlotResult(
                    slot,
                    "success",
                    detail,
                    retry_success=attempts > 1 or reconciled_after_error,
                )
            )
        except RequestFailure as exc:
            # A timeout may occur after Buffer accepted the mutation. Reconcile once before recording failure.
            try:
                after_error = scheduled_by_due(buffer.scheduled_posts()).get(iso_utc(due))
            except Exception:
                after_error = None
            if after_error:
                post_id = str(after_error.get("id") or "")
                entry.update(
                    {
                        "status": "scheduled",
                        "buffer_post_id": post_id,
                        "reconciled": True,
                        "last_error": None,
                        "updated_at": iso_utc(now_fn()),
                    }
                )
                append_history_once(history, entry, post_id)
                results.append(SlotResult(slot, "success", f"berhasil direkonsiliasi ({post_id})", True))
            else:
                entry["status"] = "failed_retryable" if exc.retryable else "failed_permanent"
                entry["last_error"] = error_record(exc.kind, str(exc), exc.retryable)
                entry["updated_at"] = iso_utc(now_fn())
                results.append(SlotResult(slot, "failed", f"{exc.kind}: {exc}"))
        except Exception as exc:  # isolate an unexpected per-slot failure
            entry["status"] = "failed_retryable"
            entry["last_error"] = error_record("unexpected_slot_error", str(exc), True)
            entry["updated_at"] = iso_utc(now_fn())
            results.append(SlotResult(slot, "failed", f"unexpected: {exc}"))
    return results


def write_summary(context: dict[str, Any], results: list[SlotResult], dry_run: bool) -> None:
    counts = {
        "success": sum(item.status == "success" for item in results),
        "skipped": sum(item.status == "skipped" for item in results),
        "failed": sum(item.status == "failed" for item in results),
        "retry_success": sum(item.retry_success for item in results),
    }
    print("\nSUMMARY")
    print(f"SUCCESS: {counts['success']}")
    print(f"SKIPPED: {counts['skipped']}")
    print(f"FAILED: {counts['failed']}")
    print(f"RETRY SUCCESS: {counts['retry_success']}")
    for item in results:
        icon = "✅" if item.status == "success" else ("⏭" if item.status == "skipped" else "❌")
        print(f"Video{item.slot:02d} {icon} {item.message}")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    lines = [
        f"## Wordavee resilient — session {context['session']}",
        "",
        f"- Target WIB: **{context['target_date']}**",
        f"- Mode: **{context['mode']}**",
        f"- Dry run: **{str(dry_run).lower()}**",
        f"- Success: **{counts['success']}**",
        f"- Skipped/dedupe: **{counts['skipped']}**",
        f"- Failed (slot only): **{counts['failed']}**",
        f"- Retry success: **{counts['retry_success']}**",
        "",
        "| Slot | Result | Detail |",
        "|---|---|---|",
    ]
    for item in results:
        safe_message = item.message.replace("|", "\\|")
        lines.append(f"| Video{item.slot:02d} | {item.status} | {safe_message} |")
    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def execute(args: argparse.Namespace) -> int:
    context = load_json(Path(args.context_file), {})
    if not context:
        raise CriticalError("Context plan tidak ditemukan.")
    state_path = Path(str(context["state_path"]))
    state = context.get("planned_state") if args.dry_run else load_json(state_path, {})
    if not state:
        raise CriticalError("State harian tidak ditemukan.")
    history_path = Path(args.history_file)
    history = load_history(history_path)
    buffer = BufferClient()
    buffer.resolve_channel()
    results = execute_entries(
        state,
        [int(item) for item in context["slots"]],
        buffer,
        history,
        dry_run=args.dry_run,
    )
    state["updated_at"] = iso_utc(utc_now())
    if not args.dry_run:
        write_json(state_path, state)
        write_json(history_path, history)
    write_summary(context, results, args.dry_run)
    # Slot failures are warnings and remain recoverable; critical errors raise and fail the job.
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("plan", "execute"), required=True)
    parser.add_argument("--session", default=os.environ.get("INPUT_SESSION") or "auto")
    parser.add_argument("--target-date", default=os.environ.get("INPUT_TARGET_DATE") or "")
    parser.add_argument("--slots", default=os.environ.get("INPUT_SLOTS") or "")
    parser.add_argument("--mode", choices=("main", "recovery"), default=os.environ.get("INPUT_MODE") or "main")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--state-dir", default="state")
    parser.add_argument("--history-file", default=os.environ.get("QUOTE_HISTORY_FILE", "quotes-history.json"))
    parser.add_argument("--context-file", default=".wordivee-run.json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        return plan(args) if args.phase == "plan" else execute(args)
    except (CriticalError, RequestFailure) as exc:
        kind = exc.kind if isinstance(exc, RequestFailure) else "critical_config"
        print(f"CRITICAL ERROR [{kind}]: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
