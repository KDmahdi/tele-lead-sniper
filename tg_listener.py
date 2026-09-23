#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tg_listener.py — Telegram Channel Listener (Telethon, modular)
==============================================================

A production-oriented Telethon *user-client* that monitors target channels,
extracts contact handles from message texts, applies a negative filter,
de-duplicates contacts through SQLite, sends ONE initial interaction
message per new contact and reports every action to Saved Messages (`me`).

Requirement mapping
-------------------
1. Architecture / base .......... CONFIGURATION section (API_ID, API_HASH,
                                   SESSION_NAME, TARGET_CHANNELS, NewMessage)
2. Negative filter .............. is_blacklisted() + BLACKLIST_WORDS
3. Handle extraction/validation . extract_usernames() + resolve_valid_user()
4. Local storage ................ ContactStore  (history.db / contacted_users)
5. Initial outreach ............. send_initial_message() + MESSAGE_TEMPLATE
                                   (randomized asyncio.sleep, full try/except)
6. Logging to Saved Messages .... build_report() + log_to_saved_messages()

Quick start (راهنمای سریع)
--------------------------
    pip install -r requirements.txt
    1) TARGET_CHANNELS و MESSAGE_TEMPLATE را در بالای همین فایل تکمیل کنید.
    2) python tg_listener.py        # بار اول: ورود تعاملی (شماره + کد)
    3) history.db و listener.log کنار اسکریپت ساخته می‌شوند.

NOTE: with MESSAGE_TEMPLATE left empty the script runs in DRY-RUN mode —
handles are extracted, validated and recorded, but nothing is ever sent.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import sqlite3
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

from telethon import TelegramClient, events, utils
from telethon.errors import (
    ChatWriteForbiddenError,
    FloodWaitError,
    UserIsBlockedError,
    UserNotMutualContactError,
    UserPrivacyRestrictedError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.custom import Message
from telethon.tl.types import User

# ============================================================================
# 1) CONFIGURATION
# ============================================================================
API_ID: int = 2040
API_HASH: str = "b18441a1ff607e10a989891a5462e627"

# Telethon keeps the login session in "<SESSION_NAME>.session".
SESSION_NAME: str = "client_session"

# -----------------------------------------------------------------------------
# آیدی کانال‌های هدف — @username / numeric id (e.g. -1001234567890) / t.me link
# The listener reacts ONLY to events.NewMessage coming from these chats.
# -----------------------------------------------------------------------------
TARGET_CHANNELS: list = [
    # "@example_channel",
    # -1001234567890,
    "https://t.me/Estkhdamadminchanel",
    "https://t.me/freelancer_job",
    "https://t.me/Hajifreelance",
    "https://t.me/Daneshjoo_Com",
    "https://t.me/doorkarijoo",
    "https://t.me/FreelancerH",
    "https://t.me/danshjo_bartar",
    "https://t.me/ProzheLancer",
    "https://t.me/cproje",
    "https://t.me/SevenProzhe",
    "https://t.me/idorkar",
    "https://t.me/Freelaancing",
    "https://t.me/Pinal_job",
    "https://t.me/Project_jobplus",
    "https://t.me/uprojeh",
    "https://t.me/Collegian_Projection",
    "https://t.me/ZFreelancer",
    "https://t.me/noottinngg"
]

# -----------------------------------------------------------------------------
# Negative filter / Blacklist: اگر متن پیام حاوی هر یک از این عبارت‌ها باشد،
# پردازش متوقف شده و ایونت نادیده گرفته می‌شود.
# -----------------------------------------------------------------------------
BLACKLIST_WORDS: list[str] = [
    "مجری هستم",
    "مجری",
    "انجام دهنده",
    "نمونه کار",
    "فروش",
    "تبلیغات",
    "استخدام حضوری",
    "حضوری",
    "استخدام",
    "تایپیست",
    
    "تبلیغات",
    "رزرومه",
    "admin",
    "ادمین",
    "وردپرس",
]

# -----------------------------------------------------------------------------
# Placeholder برای متن پیام اولیه — متن دلخواه خود را اینجا قرار دهید:
#   e.g. "سلام، پست شما را در کانال دیدم و مایل به همکاری هستم ..."
# NOTE: خالی بودن این متغیر = حالت DRY-RUN (هیچ پیامی ارسال نمی‌شود).
# -----------------------------------------------------------------------------
# MESSAGE_TEMPLATE: str = ""
MESSAGE_TEMPLATE: str = """
\n سلام. وقتتون بخیر.
برای انجام پروژه‌ای که ثبت کردید، ما آمادگی کامل داریم. اجرای دقیق و بدون‌نقص هر پروژه‌ای ظرافت‌های خاص خودش رو داره و باید با بالاترین کیفیت انجام بشه.
ما یک تیم تخصصی هستیم که صفر تا صد پروژه‌های مختلف رو پوشش می‌دیم. مزیت کار با ما اینه که برای هر حوزه، یک متخصص اختصاصی داریم؛ یعنی کار شما دقیقاً به دست شخصی سپرده می‌شه که تو همون زمینه مهارت و تجربه کامل داره.
این سبک کار باعث می‌شه خروجی نهایی با بهترین کیفیت، کاملاً حرفه‌ای و دقیقاً مطابق با نیاز شما تحویل داده بشه.
از اونجایی که هر کار جزئیات خاص خودش رو داره، ممنون می‌شیم توضیحات تکمیلی یا فایل‌های مربوط به پروژه‌تون رو ارسال کنید. این‌طوری متخصص مربوطه تو تیم ما، کار رو سریعاً بررسی می‌کنه و می‌تونیم سر یک هزینه منصفانه و زمان تحویل دقیق با هم توافق کنیم.
منتظر پیامتون در چت هستیم.
"""



# --- storage / logging -------------------------------------------------------
DB_PATH: str = "history.db"
LOG_FILE: str = "listener.log"

# --- behaviour tuning --------------------------------------------------------
INCOMING_ONLY: bool = True                   # ignore our own outgoing posts
REPLY_DELAY_RANGE: tuple[float, float] = (3.0, 8.0)   # human-like pause (sec)
SAVED_LOG_DELAY: float = 2.0                 # pause before report to 'me'
MESSAGE_SNIPPET_LIMIT: int = 300             # chars of source text in report

# Reserved Telegram paths / service handles that must never be treated as a
# contact candidate (@admin, t.me/joinchat, t.me/share, ...).
RESERVED_USERNAMES: set[str] = {
    "admin", "addlist", "addstickers", "addtheme", "contact", "iv", "joinchat",
    "login", "premium", "proxy", "settings", "setlanguage", "share", "socks",
    "spambot", "support", "tg", "telegram", "telegramtips",
}


# ============================================================================
# 2) LOGGING (console + rotating file)
# ============================================================================
def setup_logging() -> logging.Logger:
    """Configure a module logger with console + rotating file output."""
    logger = logging.getLogger("tg_listener")
    if logger.handlers:                      # guard against double-init
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    rotating = RotatingFileHandler(
        LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    rotating.setFormatter(formatter)
    logger.addHandler(rotating)

    return logger


log = setup_logging()


# ============================================================================
# 3) STORAGE — SQLite (history.db)
# ============================================================================
class ContactStore:
    """SQLite-backed registry of every handle the script has processed.

    Table ``contacted_users`` keeps the handle (case-insensitive unique),
    its Telegram id (when known), a status (``pending`` -> ``sent`` /
    ``failed`` / ``invalid`` / ``dry_run``), the source channel, the source
    message id and the UTC timestamp of registration. If the process dies
    mid-send, the row stays ``pending`` — conservative: never double-DM.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._create_schema()

    # -- internals --------------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _create_schema(self) -> None:
        conn = self._connect()
        try:
            with conn:  # commits on success, rolls back on error
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS contacted_users (
                        id             INTEGER PRIMARY KEY AUTOINCREMENT,
                        username       TEXT    NOT NULL UNIQUE COLLATE NOCASE,
                        telegram_id    INTEGER,
                        status         TEXT    NOT NULL DEFAULT 'pending',
                        source_channel TEXT,
                        source_msg_id  INTEGER,
                        raw_text       TEXT,
                        created_at     TEXT    NOT NULL,
                        updated_at     TEXT
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_contacted_username "
                    "ON contacted_users (username)"
                )
        finally:
            conn.close()

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    # -- public API ---------------------------------------------------------------
    def exists(self, username: str) -> bool:
        """True when the handle was processed before (any status)."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM contacted_users "
                "WHERE username = ? COLLATE NOCASE LIMIT 1",
                (username,),
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def add(
        self,
        username: str,
        source_channel: str | None = None,
        source_msg_id: int | None = None,
        raw_text: str | None = None,
        status: str = "pending",
        telegram_id: int | None = None,
    ) -> None:
        """Insert a new handle; INSERT OR IGNORE keeps it idempotent."""
        now = self._utc_now()
        conn = self._connect()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO contacted_users
                        (username, telegram_id, status, source_channel,
                         source_msg_id, raw_text, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (username, telegram_id, status, source_channel,
                     source_msg_id, raw_text, now, now),
                )
        finally:
            conn.close()

    def mark(self, username: str, status: str, telegram_id: int | None = None) -> None:
        """Update the status (and id, when known) of an existing handle."""
        now = self._utc_now()
        conn = self._connect()
        try:
            with conn:
                conn.execute(
                    """
                    UPDATE contacted_users
                       SET status      = ?,
                           telegram_id = COALESCE(telegram_id, ?),
                           updated_at  = ?
                     WHERE username = ? COLLATE NOCASE
                    """,
                    (status, telegram_id, now, username),
                )
        finally:
            conn.close()


# ============================================================================
# 4) TEXT FILTERING & HANDLE EXTRACTION
# ============================================================================
# Persian/Arabic glyph normalisation so the blacklist also matches messages
# typed with Arabic "ي/ك" instead of Persian "ی/ک", ZWNJ variants, etc.
_CHAR_NORMALIZE = str.maketrans({
    "ي": "ی", "ك": "ک", "ى": "ی",
    "أ": "ا", "إ": "ا", "ؤ": "و", "ة": "ه",
    "\u200c": " ",   # ZWNJ -> space
    "\u200f": "",    # RTL mark -> removed
    "\u200e": "",    # LTR mark -> removed
})


def normalize_text(text: str) -> str:
    """Normalize Persian/Arabic glyph variants + casefold for matching."""
    return text.translate(_CHAR_NORMALIZE).casefold()


_BLACKLIST_NORMALIZED = [normalize_text(word) for word in BLACKLIST_WORDS]


def is_blacklisted(text: str | None) -> bool:
    """Negative filter: True when the text contains ANY blacklist phrase."""
    if not text:
        return False
    haystack = normalize_text(text)
    return any(needle in haystack for needle in _BLACKLIST_NORMALIZED)


# A valid Telegram handle: 5-32 chars, starts with a letter, then letters,
# digits or underscores. Matches both `@username` and `t.me/username` forms.
# The lookbehind before "@" avoids false positives inside e-mail addresses.
USERNAME_REGEX = re.compile(
    r"""
    (?:
        (?<![A-Za-z0-9._%+-])@(?P<at>[A-Za-z][A-Za-z0-9_]{4,31})
      |
        (?:(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/)
        (?P<link>[A-Za-z][A-Za-z0-9_]{4,31})
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)

# trailing punctuation that must not stick to a captured handle
_TRIM_CHARS = ".,!?;:\"'“”«»…)]}"


def extract_usernames(text: str | None) -> list[str]:
    """Return unique candidate handles found as `@user` or `t.me/user`."""
    if not text:
        return []

    candidates: list[str] = []
    seen: set[str] = set()

    for match in USERNAME_REGEX.finditer(text):
        raw = match.group("at") or match.group("link") or ""
        username = raw.strip().rstrip(_TRIM_CHARS)
        key = username.casefold()

        if not username or key in seen or key in RESERVED_USERNAMES:
            continue

        seen.add(key)
        candidates.append(username)

    return candidates


# ============================================================================
# 5) VALIDATION — bots / channels / source / self must never be contacted
# ============================================================================
def _entity_usernames(entity) -> set[str]:
    """All usernames attached to an entity (main + collectible ones)."""
    names: set[str] = set()

    main = getattr(entity, "username", None)
    if main:
        names.add(main.casefold())

    for extra in getattr(entity, "usernames", None) or []:
        handle = getattr(extra, "username", None)
        if handle:
            names.add(handle.casefold())

    return names


async def resolve_valid_user(
    client: TelegramClient,
    username: str,
    source_chat,
    own_usernames: set[str],
) -> User | None:
    """Resolve ``username`` and return the entity ONLY if it is a real user.

    Rejects:
      * unresolvable / invalid usernames
      * channels, groups and broadcast objects   (not a User)
      * bot accounts                              (entity.bot)
      * the source channel itself                 (username overlap)
      * our own account                           (own_usernames)
    """
    key = username.casefold()
    if key in own_usernames:
        return None
    if source_chat is not None and key in _entity_usernames(source_chat):
        return None

    try:
        entity = await client.get_entity(f"@{username}")
    except (UsernameNotOccupiedError, UsernameInvalidError):
        return None
    except (ValueError, TypeError):
        # Telethon raises ValueError when a username cannot be resolved.
        return None
    except Exception as exc:          # noqa: BLE001 — never crash the listener
        log.warning("resolve '@%s' failed: %s", username, exc)
        return None

    if not isinstance(entity, User) or entity.bot:
        return None

    return entity


def _normalize_target(target):
    """Reduce @user / t.me/user / numeric-string to a Telethon-friendly form."""
    if isinstance(target, int):
        return target
    if isinstance(target, str):
        cleaned = target.strip()
        if not cleaned:
            return None
        if cleaned.lstrip("-").isdigit():
            return int(cleaned)
        match = re.search(
            r"(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{4,31})",
            cleaned, re.IGNORECASE,
        )
        if match:
            return f"@{match.group(1)}"
        return cleaned if cleaned.startswith("@") else f"@{cleaned.lstrip('@')}"
    return None


# ============================================================================
# 6) OUTREACH & SAVED-MESSAGES REPORTING
# ============================================================================
async def send_initial_message(client: TelegramClient, user: User, username: str) -> bool:
    """Send MESSAGE_TEMPLATE to a validated contact exactly once.

    Returns True when the message was delivered. Every Telegram-specific
    failure is caught and logged, so the listener loop never stops.
    """
    delay = random.uniform(*REPLY_DELAY_RANGE)
    log.info("waiting %.1fs before DM to @%s ...", delay, username)
    await asyncio.sleep(delay)

    try:
        await client.send_message(user, MESSAGE_TEMPLATE, parse_mode=None)
        return True
    except FloodWaitError as exc:
        wait = int(getattr(exc, "seconds", 0)) + 1
        log.warning("FloodWait: sleeping %ss then retrying @%s ...", wait, username)
        await asyncio.sleep(wait)
        try:
            await client.send_message(user, MESSAGE_TEMPLATE, parse_mode=None)
            return True
        except Exception as retry_exc:            # noqa: BLE001
            log.warning("retry to @%s failed: %s", username, retry_exc)
            return False
    except UserPrivacyRestrictedError:
        log.info("@%s restricts messages from strangers — skipped.", username)
    except UserNotMutualContactError:
        log.info("@%s only accepts mutual contacts — skipped.", username)
    except UserIsBlockedError:
        log.info("@%s has blocked this account — skipped.", username)
    except ChatWriteForbiddenError:
        log.info("writing to @%s is forbidden — skipped.", username)
    except Exception as exc:                      # noqa: BLE001 — any other RPC error
        log.warning("DM to @%s failed: %s", username, exc)

    return False


def build_report(
    username: str,
    telegram_id: int | None,
    source: str,
    msg_id: int,
    text: str | None,
) -> str:
    """Human-readable outreach report sent to Saved Messages ('me')."""
    sent_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")

    lines = [
        "گزارش مخاطب جدید",
        "─────────────────────",
        f"شناسه: @{username}",
        f"آیدی عددی: {telegram_id if telegram_id else 'نامشخص'}",
        f"مبدأ پیام: {source}",
        f"شناسه پیام: {msg_id}",
        f"زمان ارسال: {sent_at}",
    ]

    if text:
        snippet = text[:MESSAGE_SNIPPET_LIMIT]
        if len(text) > MESSAGE_SNIPPET_LIMIT:
            snippet += " ..."
        lines.append("متن پیام: " + snippet.replace("\n", " "))

    return "\n".join(lines)


async def log_to_saved_messages(client: TelegramClient, text: str) -> None:
    """Deliver a report to Saved Messages ('me'); never raises."""
    try:
        await asyncio.sleep(SAVED_LOG_DELAY)
        await client.send_message("me", text, parse_mode=None)
    except Exception as exc:                      # noqa: BLE001
        log.error("failed to log report to Saved Messages: %s", exc)


# ============================================================================
# 7) EVENT PIPELINE
# ============================================================================
class ChannelListener:
    """Full pipeline executed for every new message in the target channels.

        blacklist filter -> handle extraction -> validation -> SQLite dedupe
        -> randomized delay -> one initial DM -> report to Saved Messages
    """

    def __init__(
        self,
        client: TelegramClient,
        store: ContactStore,
        target_ids: set[int],
        own_usernames: set[str],
    ) -> None:
        self.client = client
        self.store = store
        self.target_ids = target_ids
        self.own_usernames = own_usernames

    # -- entry point -------------------------------------------------------------
    async def handle_new_message(self, event: events.NewMessage.Event) -> None:
        # Defensive re-check (Telethon already filters by chats=...).
        if self.target_ids and event.chat_id not in self.target_ids:
            return

        message: Message = event.message
        text: str = (event.raw_text or "").strip()
        if not text:
            return                                    # media-only message

        chat = await event.get_chat()
        source = self._source_label(chat, event.chat_id)
        log.info("new message #%s from «%s»", message.id, source)

        # (1) negative filter --------------------------------------------------
        if is_blacklisted(text):
            log.info("blacklist hit — event #%s ignored.", message.id)
            return

        # (2) handle extraction --------------------------------------------------
        candidates = extract_usernames(text)
        if not candidates:
            return
        log.info("candidates: %s", ", ".join(f"@{c}" for c in candidates))

        for username in candidates:
            await self._process_candidate(username, text, source, message.id, chat)

    # -- helpers -------------------------------------------------------------------
    async def _process_candidate(
        self,
        username: str,
        text: str,
        source: str,
        msg_id: int,
        source_chat,
    ) -> None:
        # (3) dedupe: skip handles registered before ---------------------------
        if self.store.exists(username):
            log.info("@%s already processed earlier — skipped.", username)
            return

        # Reserve the handle immediately so concurrent messages cannot
        # trigger a second outreach while this one is still in flight.
        self.store.add(
            username=username,
            source_channel=source,
            source_msg_id=msg_id,
            raw_text=text,
        )

        # (4) validation ---------------------------------------------------------
        user = await resolve_valid_user(
            self.client, username, source_chat, self.own_usernames
        )
        if user is None:
            self.store.mark(username, "invalid")
            log.info("@%s rejected (bot / channel / invalid) — skipped.", username)
            return

        # (5) outreach — only ONE initial interactive message --------------------
        if not MESSAGE_TEMPLATE.strip():
            self.store.mark(username, "dry_run", telegram_id=user.id)
            log.info("DRY-RUN: @%s validated & recorded — nothing sent.", username)
            return

        sent = await send_initial_message(self.client, user, username)

        # (6) report to Saved Messages --------------------------------------------
        if sent:
            self.store.mark(username, "sent", telegram_id=user.id)
            log.info("initial message delivered to @%s — reporting.", username)
            report = build_report(username, user.id, source, msg_id, text)
            await log_to_saved_messages(self.client, report)
        else:
            self.store.mark(username, "failed", telegram_id=user.id)

    @staticmethod
    def _source_label(chat, chat_id: int) -> str:
        title = getattr(chat, "title", None)
        if title:
            return title
        handle = getattr(chat, "username", None)
        if handle:
            return f"@{handle}"
        return str(chat_id)


# ============================================================================
# 8) STARTUP & RUNNER
# ============================================================================
async def resolve_targets(client: TelegramClient) -> tuple[set[int], list]:
    """Resolve TARGET_CHANNELS into marked ids + normalized target list."""
    ids: set[int] = set()
    ok_targets: list = []

    for target in TARGET_CHANNELS:
        normalized = _normalize_target(target)
        if normalized is None:
            log.error("invalid target format: %r", target)
            continue
        try:
            entity = await client.get_entity(normalized)
            marked = utils.get_peer_id(entity)
            ids.add(marked)
            ok_targets.append(normalized)
            log.info(
                "target resolved: %r -> id %s (%s)",
                target, marked, getattr(entity, "title", "") or "",
            )
        except Exception as exc:                  # noqa: BLE001
            log.error(
                "cannot resolve target %r: %s — join the channel first "
                "or fix the id.", target, exc,
            )
    return ids, ok_targets


async def main() -> None:
    if not TARGET_CHANNELS:
        log.error("TARGET_CHANNELS is empty — add at least one channel.")
        return

    if not MESSAGE_TEMPLATE.strip():
        log.warning("MESSAGE_TEMPLATE is empty — DRY-RUN mode, nothing will be sent.")

    store = ContactStore(DB_PATH)
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    # First run: interactive login (phone number + code). Afterwards the
    # session file `client_session.session` is reused automatically.
    await client.start()

    me = await client.get_me()
    own_usernames = _entity_usernames(me)
    log.info(
        "logged in as %s (id=%s)",
        getattr(me, "username", None) or getattr(me, "first_name", "?"), me.id,
    )

    target_ids, ok_targets = await resolve_targets(client)
    if not target_ids:
        log.error("no target channel could be resolved — aborting.")
        return

    listener = ChannelListener(client, store, target_ids, own_usernames)
    client.add_event_handler(
        listener.handle_new_message,
        events.NewMessage(chats=ok_targets, incoming=INCOMING_ONLY),
    )

    log.info("listening on %d channel(s) — press Ctrl+C to stop.", len(target_ids))
    try:
        await client.run_until_disconnected()
    finally:
        await client.disconnect()
        log.info("client disconnected — bye.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(0)
