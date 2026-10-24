"""
Admin and Multi-User Management Module for Jio-Gemini Scanner Bot.
Supports Super Admin (8327850869), user registry, whitelisting, role-based access,
broadcast messaging, free mode, and dual-channel isolation (Master Channel hidden from members).
"""

import os
import json
import time
import threading
from pathlib import Path
from typing import Dict, List, Optional, Set, Any

BASE_DIR = Path(__file__).parent.resolve()
ADMIN_SETTINGS_FILE = BASE_DIR / "admin_settings.json"

DEFAULT_SUPER_ADMIN = 8327850869
DEFAULT_SUPER_ADMINS: Set[int] = {8327850869, 8219398815}
DEFAULT_MASTER_STORAGE_CHANNEL = "-1003907841421"
DEFAULT_MASTER_LOG_CHANNEL = "-1003946923981"


class AdminManager:
    def __init__(self, filepath: Path = ADMIN_SETTINGS_FILE):
        self.filepath = filepath
        self._lock = threading.RLock()
        self.super_admin_id: int = DEFAULT_SUPER_ADMIN
        self.super_admin_ids: Set[int] = set(DEFAULT_SUPER_ADMINS)
        self.admin_user_ids: Set[int] = set(DEFAULT_SUPER_ADMINS)
        self.whitelisted_user_ids: Set[int] = set()
        self.master_storage_channel: str = DEFAULT_MASTER_STORAGE_CHANNEL
        self.master_log_channel: str = DEFAULT_MASTER_LOG_CHANNEL
        self.free_mode: bool = False
        self.user_registry: Dict[str, dict] = {}
        self.bot_start_time: float = time.time()
        self.load()

    def is_free_mode(self) -> bool:
        return bool(self.free_mode)

    def set_free_mode(self, enabled: bool) -> bool:
        self.free_mode = bool(enabled)
        self.save()
        return self.free_mode

    def toggle_free_mode(self) -> bool:
        self.free_mode = not self.free_mode
        self.save()
        return self.free_mode

    def load(self):
        env_admin = os.getenv("ADMIN_USER_IDS", "").strip()
        if env_admin:
            for uid in env_admin.replace(",", " ").split():
                c = uid.strip().lstrip("-")
                if c.isdigit():
                    self.admin_user_ids.add(int(c))

        env_master_log = os.getenv("LOG_CHANNEL_ID", "").strip()
        if env_master_log:
            self.master_log_channel = env_master_log

        env_master_storage = os.getenv("STORAGE_CHANNEL_ID", "").strip()
        if env_master_storage:
            self.master_storage_channel = env_master_storage

        if not self.filepath.exists():
            self.save()
            return

        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.free_mode = bool(data.get("free_mode", False))
            self.master_log_channel = str(data.get("master_log_channel") or self.master_log_channel).strip()
            self.master_storage_channel = str(data.get("master_storage_channel") or self.master_storage_channel).strip()

            admins = data.get("admin_user_ids", [self.super_admin_id])
            self.admin_user_ids = {int(x) for x in admins if str(x).lstrip("-").isdigit()}
            self.admin_user_ids.update(self.super_admin_ids)
            self.admin_user_ids.add(self.super_admin_id)

            users = data.get("whitelisted_user_ids", [])
            self.whitelisted_user_ids = {int(x) for x in users if str(x).lstrip("-").isdigit()}

            self.user_registry = data.get("user_registry", {})
        except Exception as e:
            print(f"[AdminManager] Error loading settings: {e}")

    def save(self):
        with self._lock:
            data = {
                "free_mode": self.free_mode,
                "master_log_channel": self.master_log_channel,
                "master_storage_channel": self.master_storage_channel,
                "admin_user_ids": list(self.admin_user_ids),
                "whitelisted_user_ids": list(self.whitelisted_user_ids),
                "user_registry": self.user_registry,
            }
            tmp_file = self.filepath.with_suffix(f".tmp.{os.getpid()}.{time.time_ns()}")
            try:
                with open(tmp_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                tmp_file.replace(self.filepath)
            except Exception as e:
                print(f"[AdminManager] Error saving settings: {e}")
                try:
                    if tmp_file.exists():
                        tmp_file.unlink()
                except Exception:
                    pass

    def record_user(self, user_id: int, username: str = "", first_name: str = ""):
        if not user_id:
            return
        with self._lock:
            uid_str = str(user_id)
            now = time.time()
            role = self.get_user_role_label(user_id)
            if uid_str not in self.user_registry:
                self.user_registry[uid_str] = {
                    "user_id": user_id,
                    "username": username or "",
                    "first_name": first_name or "",
                    "role": role,
                    "joined_at": now,
                    "last_active": now,
                    "user_log_channel": None,
                    "user_storage_channel": None,
                }
            else:
                if username:
                    self.user_registry[uid_str]["username"] = username
                if first_name:
                    self.user_registry[uid_str]["first_name"] = first_name
                self.user_registry[uid_str]["last_active"] = now
                self.user_registry[uid_str]["role"] = role
            self.save()

    def _to_int(self, user_id: Any) -> Optional[int]:
        try:
            return int(user_id)
        except (ValueError, TypeError):
            return None

    def get_user_role_label(self, user_id: Any) -> str:
        uid = self._to_int(user_id)
        if not uid:
            return "⚪ Guest"
        if uid == self.super_admin_id or uid in self.super_admin_ids:
            return "👑 Super Admin"
        if uid in self.admin_user_ids:
            return "🛡️ Admin"
        if uid in self.whitelisted_user_ids:
            return "👤 Member"
        return "⚪ Guest"

    def is_super_admin(self, user_id: Any) -> bool:
        uid = self._to_int(user_id)
        return bool(uid and (uid == self.super_admin_id or uid in self.super_admin_ids))

    def is_admin(self, user_id: Any) -> bool:
        uid = self._to_int(user_id)
        if not uid:
            return False
        return uid == self.super_admin_id or uid in self.super_admin_ids or uid in self.admin_user_ids

    def is_whitelisted(self, user_id: Any) -> bool:
        uid = self._to_int(user_id)
        if not uid:
            return False
        return uid in self.whitelisted_user_ids

    def has_access(self, user_id: Any) -> bool:
        uid = self._to_int(user_id)
        if not uid:
            return False
        if self.free_mode:
            return True
        return self.is_admin(uid) or self.is_whitelisted(uid)

    def add_admin(self, user_id: Any, username: str = "", first_name: str = ""):
        uid = self._to_int(user_id)
        if not uid:
            return
        with self._lock:
            self.admin_user_ids.add(uid)
            if uid in self.whitelisted_user_ids:
                self.whitelisted_user_ids.remove(uid)
            self.record_user(uid, username, first_name)
            self.save()

    def remove_admin(self, user_id: Any):
        uid = self._to_int(user_id)
        if not uid or uid == self.super_admin_id:
            return
        with self._lock:
            if uid in self.admin_user_ids:
                self.admin_user_ids.remove(uid)
                uid_str = str(uid)
                if uid_str in self.user_registry:
                    self.user_registry[uid_str]["role"] = self.get_user_role_label(uid)
                self.save()

    def add_user(self, user_id: Any, username: str = "", first_name: str = ""):
        uid = self._to_int(user_id)
        if not uid:
            return
        with self._lock:
            self.whitelisted_user_ids.add(uid)
            self.record_user(uid, username, first_name)
            self.save()

    def remove_user(self, user_id: Any):
        uid = self._to_int(user_id)
        if not uid:
            return
        with self._lock:
            if uid in self.whitelisted_user_ids:
                self.whitelisted_user_ids.remove(uid)
                uid_str = str(uid)
                if uid_str in self.user_registry:
                    self.user_registry[uid_str]["role"] = self.get_user_role_label(uid)
                self.save()

    def get_master_log_channel(self) -> str:
        return self.master_log_channel

    def set_master_log_channel(self, ch_id: str):
        with self._lock:
            self.master_log_channel = str(ch_id).strip()
            self.save()

    def get_master_storage_channel(self) -> str:
        return self.master_storage_channel

    def set_master_storage_channel(self, ch_id: str):
        with self._lock:
            self.master_storage_channel = str(ch_id).strip()
            self.save()

    def get_user_log_channel(self, user_id: int) -> Optional[str]:
        uid_str = str(user_id)
        with self._lock:
            if uid_str in self.user_registry:
                return self.user_registry[uid_str].get("user_log_channel")
            return None

    def set_user_log_channel(self, user_id: int, ch_id: Optional[str]):
        uid_str = str(user_id)
        with self._lock:
            if uid_str not in self.user_registry:
                self.record_user(user_id)
            self.user_registry[uid_str]["user_log_channel"] = str(ch_id).strip() if ch_id else None
            self.save()

    def get_user_storage_channel(self, user_id: int) -> Optional[str]:
        uid_str = str(user_id)
        with self._lock:
            if uid_str in self.user_registry:
                return self.user_registry[uid_str].get("user_storage_channel")
            return None

    def set_user_storage_channel(self, user_id: int, ch_id: Optional[str]):
        uid_str = str(user_id)
        with self._lock:
            if uid_str not in self.user_registry:
                self.record_user(user_id)
            self.user_registry[uid_str]["user_storage_channel"] = str(ch_id).strip() if ch_id else None
            self.save()

    def get_display_log_channel(self, user_id: int) -> str:
        user_ch = self.get_user_log_channel(user_id)
        if user_ch:
            return user_ch
        if self.is_super_admin(user_id):
            return f"{self.master_log_channel} (Master)"
        return "Not Set"

    def get_display_storage_channel(self, user_id: int) -> str:
        user_ch = self.get_user_storage_channel(user_id)
        if user_ch:
            return user_ch
        if self.is_super_admin(user_id):
            return f"{self.master_storage_channel} (Master)"
        return "Not Set"

    def get_effective_log_channel(self, user_id: int) -> Optional[str]:
        user_ch = self.get_user_log_channel(user_id)
        if user_ch:
            return user_ch
        if self.master_log_channel:
            return self.master_log_channel
        return None

    def get_effective_storage_channel(self, user_id: int) -> Optional[str]:
        user_ch = self.get_user_storage_channel(user_id)
        if user_ch:
            return user_ch
        if self.is_super_admin(user_id) and self.master_storage_channel:
            return self.master_storage_channel
        return None

    def has_storage_channel(self, user_id: int) -> bool:
        if self.is_super_admin(user_id):
            return bool(self.master_storage_channel or self.get_user_storage_channel(user_id))
        user_ch = self.get_user_storage_channel(user_id)
        return bool(user_ch and user_ch.strip())

    def get_user_by_storage_channel(self, channel_id: Any) -> Optional[int]:
        if not channel_id:
            return None
        ch_str = str(channel_id).strip()
        with self._lock:
            for uid_str, data in list(self.user_registry.items()):
                if data.get("user_storage_channel") and str(data.get("user_storage_channel")).strip() == ch_str:
                    if uid_str.lstrip("-").isdigit():
                        return int(uid_str)
            if self.master_storage_channel and str(self.master_storage_channel).strip() == ch_str:
                return self.super_admin_id
        return None

    def get_all_target_chat_ids(self) -> Set[int]:
        with self._lock:
            targets = set(self.admin_user_ids).union(self.whitelisted_user_ids)
            for uid_str in list(self.user_registry.keys()):
                if uid_str.lstrip("-").isdigit():
                    targets.add(int(uid_str))
        return targets

    def get_all_accounts(self) -> List[dict]:
        accounts = []
        with self._lock:
            for uid_str, data in list(self.user_registry.items()):
                uid = int(uid_str) if uid_str.lstrip("-").isdigit() else uid_str
                accounts.append({
                    "user_id": uid,
                    "username": data.get("username") or "",
                    "first_name": data.get("first_name") or "User",
                    "role": self.get_user_role_label(uid if isinstance(uid, int) else 0),
                    "joined_at": data.get("joined_at", 0),
                    "last_active": data.get("last_active", 0)
                })
        return accounts

admin_manager = AdminManager()
