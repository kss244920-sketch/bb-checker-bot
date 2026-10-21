#!/usr/bin/env python3
import json
import os
import sys
import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("requests library not found. Please install requests.")
    sys.exit(1)

APP_NAME = "Telegram OTP Bot"
APP_ID = 62268

def get_mcp_config():
    # 1. Prioritize project-specific jrnm_identity.token
    token_candidates = [
        Path(__file__).parent.parent / "jrnm_identity.token",
        Path(__file__).parent / "jrnm_identity.token",
        Path.home() / "Jio-Automation/jrnm_identity.token",
        Path.home() / "JAJAJAJA/jrnm_identity.token",
    ]
    env_identity = os.environ.get("JRNM_IDENTITY")
    if not env_identity:
        for tp in token_candidates:
            if tp.exists() and tp.stat().st_size > 0:
                try:
                    env_identity = tp.read_text(encoding="utf-8").strip()
                    if env_identity:
                        break
                except Exception:
                    pass

    if not env_identity:
        # Fallback token for App 62268 to guarantee autonomous cloud execution
        env_identity = "CfDJ8OJj33e2MidEmv/zWgEBADPZZiMDzq9eY/SZVEAf+QwKTJnSngGCtrZDX/36mQImf7bCA7YrYoQKXi6ovZu9XKi/phu0sSVq2/kslJLs19yFVNAuwh1ypf+0N4rOLynQzv6TmIeGimshVjFefjWoEsPSbQD7v7Z15Vq5yJ4rB0XprZvC1g72/3+0pQGKlOrBF/jGS+P5eK1DRptmQApE+IyzVbGx9Lk8iQERzsLhhb+s5CZmpUm2qh5C/lpH7M23opc3Hsu6drSRJvBzNWzrU7iPeO3HEB1S5RtSF93Sbo7WiQgboxuGWHpTzsGQAnccMOpGi/bZyUzwCOjuEfX80gBKMhQr6DWEDd/ZxETW0OVGLOj2zUgGZQER/uxfJGDHgJ+afGn1SZh1pm00xyk4L4+owNtwDlhxcOdx0B1GaieQba8BcJBA0Fllh+6q3IYrP58o3vQSr1Rxo/HflT9/PLr4UCOdaTup9PcVzGxtJ7q98ClliXCSDUa23CE8+Ac5FTfw/yE6Xp488V9qZIbKMDt7WkJRoVNlWhWzwkc9xaoXRp8GOKcPUUSVG1COQz2FPsOITiMkcAX6kq+cGPYusdhBiDUTXO19kWcOudTWvLCH5fuNlmTIrb/XYshPr15jLzLxpbuGxI3z/BSwQYhUycwH9fI97lueB3/oyINmojXh0ZLRvGcFsEF9zHSORnuxGA18QNa2sdB/0dMXoZzTZGZi0WMGGpU3W181SsZctAQMw6a2sAb63l7VchTdzvCsPqt3JrLdF55caskq5GjbBff5+N1UFFHAeCS5279HeC3w2Pn8TDHrwLX7uFFcXv5OWxsvogZCMttWaz9C/TgTbFA="

    if env_identity:
        return {
            "url": "https://justrunmy.app/api/mcp",
            "headers": {"X-User-Identity": env_identity}
        }

    # 2. Fallback to generic config paths
    paths = [
        Path.home() / ".gemini/config/mcp_config.json",
        Path.home() / "Downloads/mcp.json",
        Path(__file__).parent / "mcp.json",
    ]
    for p in paths:
        if p.exists() and p.stat().st_size > 0:
            try:
                with open(p) as f:
                    data = json.load(f)
                    if "mcpServers" in data and "justrunmy.app" in data["mcpServers"]:
                        return data["mcpServers"]["justrunmy.app"]
            except Exception as e:
                print(f"Error reading {p}: {e}")

    return None

def call_mcp_tool(server_conf, tool_name, arguments=None):
    url = server_conf["url"]
    headers = server_conf.get("headers", {})
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments or {}
        }
    }
    import time
    last_err = None
    for attempt in range(5):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=25)
            r.raise_for_status()
            for line in r.text.splitlines():
                if line.startswith("data:"):
                    data = json.loads(line[5:].strip())
                    content = data.get("result", {}).get("content", [])
                    if content and "text" in content[0]:
                        raw_text = content[0]["text"]
                        try:
                            return json.loads(raw_text)
                        except Exception:
                            return raw_text
            return None
        except requests.exceptions.RequestException as e:
            last_err = e
            if attempt < 4:
                print(f"[Attempt {attempt+1}/5] Network/API connection error ({e}). Retrying in 10s...")
                time.sleep(10)
            else:
                raise last_err
    return None

def parse_ping_left_hours(ping_left_str):
    try:
        parts = ping_left_str.split(":")
        if len(parts) >= 3:
            first = parts[0]
            if "." in first:
                days_str, hours_str = first.split(".")
                days = int(days_str)
                hours = int(hours_str)
            else:
                days = 0
                hours = int(first)
            minutes = int(parts[1])
            return (days * 24) + hours + (minutes / 60.0)
    except Exception as e:
        print(f"Could not parse ping_left: {e}")
    return 999.0

def check_scan_in_progress(server_conf):
    try:
        res = call_mcp_tool(server_conf, "jrnm_execute_command_in_app", {
            "appId": APP_ID,
            "commandAndArguments": ["/bin/sh", "-c", "cat /app/scan_state.json /app/user_data/*/scan_state.json 2>/dev/null"]
        })
        if res and "stdOut" in res and res["stdOut"].strip():
            for block in res["stdOut"].split("}"):
                block = block.strip()
                if not block:
                    continue
                try:
                    state = json.loads(block + "}")
                    if state.get("was_running"):
                        return True, state
                except Exception:
                    pass
    except Exception:
        pass
    return False, None

def main():
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now_str}] Running JustRunMy.App Keep-Alive Check...")
    
    server_conf = get_mcp_config()
    if not server_conf:
        print("ERROR: Could not locate MCP credentials for JustRunMy.App.")
        sys.exit(1)
        
    apps_data = call_mcp_tool(server_conf, "jrnm_get_user_apps")
    if not apps_data or "result" not in apps_data:
        print("Failed to retrieve applications list.")
        sys.exit(1)
        
    items = apps_data.get("result", {}).get("items", [])
    target_app = None
    for app in items:
        if app.get("id") == APP_ID or app.get("name") == APP_NAME:
            target_app = app
            break
            
    if not target_app:
        print(f"Target app (ID {APP_ID} / '{APP_NAME}') not found.")
        sys.exit(1)
        
    is_started = target_app.get("isStarted", False)
    properties = {p["name"]: p["value"] for p in target_app.get("properties", []) if "name" in p and "value" in p}
    
    ping_left_str = properties.get("com.jrnm.computed.ping_left", "")
    hours_left = parse_ping_left_hours(ping_left_str)
    
    print(f"App: '{target_app.get('name')}' (ID: {target_app.get('id')})")
    print(f"Status: isStarted={is_started}, Hours Left={hours_left:.2f}h (Raw: '{ping_left_str}')")
    
    # First, always attempt HTTP ping to public endpoint if available
    status_data = call_mcp_tool(server_conf, "jrnm_get_app_status", {"appId": target_app["id"]})
    endpoint_url = None
    if status_data:
        eps = status_data.get("endpoints", [])
        if eps and "url" in eps[0]:
            endpoint_url = eps[0]["url"]
            try:
                ping_r = requests.get(endpoint_url, timeout=10)
                print(f"📡 Sent HTTP keepalive ping to {endpoint_url} (HTTP {ping_r.status_code})")
            except Exception as e:
                print(f"HTTP Ping error to {endpoint_url}: {e}")

    needs_action = False
    action_reason = ""
    
    if not is_started:
        needs_action = True
        action_reason = "App is currently STOPPED."
    elif hours_left < 6.0:
        needs_action = True
        action_reason = f"Free timer is low ({hours_left:.2f}h remaining < 6.0h threshold for 4h cron)."
        
    if needs_action and is_started:
        # Check if scan is actively running
        scan_active, scan_data = check_scan_in_progress(server_conf)
        if scan_active:
            cur_b = scan_data.get('last_batch_idx', 0)
            tot_p = scan_data.get('total_panels', '?')
            print(f"⚡ Scan is ACTIVE right now (Batch {cur_b}, Panels: {tot_p}).")
            if hours_left > 1.5:
                print(f"Timer still has {hours_left:.2f}h (> 1.5h threshold). Deferring restart so active scan runs uninterrupted!")
                needs_action = False
            else:
                print(f"⚠️ Timer critically low ({hours_left:.2f}h < 1.5h). Proceeding with lease renewal (scanner will auto-resume).")

    if needs_action:
        print(f"Triggering Start/Restart because: {action_reason}")
        if is_started:
            # Stop then start to reset lastStartedAt and lease
            call_mcp_tool(server_conf, "jrnm_stop_app", {"appId": target_app["id"]})
            import time
            time.sleep(3)
        res = call_mcp_tool(server_conf, "jrnm_start_or_restart_app", {"appId": target_app["id"]})
        print(f"Start Response: {res}")
        print("Keep-Alive action completed successfully. Lease renewed!")
    else:
        print(f"All good. Bot is healthy with {hours_left:.2f}h remaining buffer.")

if __name__ == "__main__":
    main()
