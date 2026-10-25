#!/usr/bin/env python3
"""
Render 24/7 Zero-Sleep Keep-Alive & Auto-Healer for Telegram Bots.
Pings the Render web service URLs every 4-8 minutes to bypass Render's 15-minute inactivity spin-down.
If any service is down or unhealthy, automatically uses the Render REST API to restart it.
"""

import os
import sys
import json
import time
import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("requests library not found. Please install requests.")
    sys.exit(1)

BASE_DIR = Path(__file__).parent.parent.resolve()
CONFIG_FILE = BASE_DIR / "render_config.json"
LOG_FILE = BASE_DIR / "keep_alive_render.log"

DEFAULT_SERVICES = [
    {
        "name": "bb-checker-bot",
        "id": "srv-db5lqaflk1mc73aeqteg",
        "url": "https://bb-checker-bot.onrender.com"
    },
    {
        "name": "telegram-cloud-pc-docker",
        "id": "srv-dbe4n88m7kps73capeog",
        "url": "https://telegram-cloud-pc-docker.onrender.com/health"
    }
]


def log(msg):
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now_str}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_config():
    cfg = {}
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception as e:
            log(f"Error loading {CONFIG_FILE}: {e}")

    api_key = os.environ.get("RENDER_API_KEY") or cfg.get("api_key") or "rnd_3tagyUF2IqkgZyiRiBHD7CbZjxAg"
    owner_id = os.environ.get("RENDER_OWNER_ID") or cfg.get("owner_id") or "tea-db5454ijnfac73963k9g"

    services = cfg.get("services") or DEFAULT_SERVICES

    return {
        "api_key": api_key,
        "owner_id": owner_id,
        "services": services,
    }


def restart_service(api_key, service_id):
    """Triggers restart via Render REST API."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }
    url = f"https://api.render.com/v1/services/{service_id}/restart"
    try:
        r = requests.post(url, headers=headers, timeout=20)
        if r.status_code in (200, 201, 202):
            log(f"🔄 Successfully requested restart for service {service_id}")
            return True
        else:
            log(f"Restart failed with status {r.status_code}: {r.text}")
    except Exception as e:
        log(f"Restart request exception: {e}")
    return False


def ping_service(api_key, svc):
    svc_name = svc.get("name", "Unknown")
    svc_id = svc.get("id")
    svc_url = svc.get("url")

    if not svc_url:
        log(f"⚠️ No URL configured for service '{svc_name}'")
        return False

    ping_url = svc_url.rstrip("/") if svc_url.endswith("/health") else svc_url.rstrip("/") + "/"
    log(f"📡 Sending Keep-Alive Ping to [{svc_name}]: {ping_url}")
    try:
        r = requests.get(ping_url, timeout=25)
        if r.status_code in (200, 201, 204):
            log(f"✅ [{svc_name}] is 100% HEALTHY & ALIVE! (HTTP {r.status_code}) - Inactivity timer reset.")
            return True
        else:
            log(f"⚠️ [{svc_name}] returned unexpected status: {r.status_code}")
    except requests.exceptions.RequestException as e:
        log(f"❌ [{svc_name}] Ping failed: {e}")

    # Self-heal if ping fails
    if svc_id and api_key:
        log(f"Attempting automatic revival for [{svc_name}] via Render API restart...")
        restart_service(api_key, svc_id)
    return False


def ping_once():
    log("Running Render 24/7 Keep-Alive & Health Ping Check...")
    cfg = load_config()
    api_key = cfg.get("api_key")
    services = cfg.get("services", [])

    all_ok = True
    for svc in services:
        ok = ping_service(api_key, svc)
        if not ok:
            all_ok = False

    return all_ok


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Render 24/7 Keep-Alive & Auto-Healer")
    parser.add_argument("--loop", type=int, default=0, help="Run continuously for N minutes (pings every 4 mins)")
    parser.add_argument("--interval", type=int, default=240, help="Interval between pings in seconds (default 240)")
    args = parser.parse_args()

    if args.loop <= 0:
        ping_once()
        return

    end_time = time.time() + (args.loop * 60)
    log(f"🚀 Starting continuous Keep-Alive loop for {args.loop} minutes (ping interval: {args.interval}s)...")
    while time.time() < end_time:
        ping_once()
        remaining = end_time - time.time()
        if remaining <= 0:
            break
        sleep_dur = min(args.interval, remaining)
        time.sleep(sleep_dur)
    log("🏁 Continuous keep-alive session ended.")


if __name__ == "__main__":
    main()
