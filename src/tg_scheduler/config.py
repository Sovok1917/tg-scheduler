from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import timedelta, timezone, tzinfo
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv

load_dotenv()

SETTINGS_FILE = Path("settings.json")

TZ_ALIASES: dict[str, str] = {
    "minsk": "Europe/Minsk",
    "moscow": "Europe/Moscow",
    "msk": "Europe/Moscow",
    "amsterdam": "Europe/Amsterdam",
    "cet": "Europe/Amsterdam",
    "utc": "UTC",
}


def parse_timezone(raw: str) -> tzinfo:
    """
    Parses an IANA timezone name (e.g. 'Europe/Minsk'), an alias ('minsk', 'msk'),
    or a fixed UTC offset ('UTC+3', '+3', '-5:30').
    """
    cleaned = raw.strip()
    lower = cleaned.lower()

    if lower in TZ_ALIASES:
        return ZoneInfo(TZ_ALIASES[lower])

    offset_match = re.fullmatch(r"(?:utc|gmt)?\s*([+-]\d{1,2})(?::(\d{2}))?", lower)
    if offset_match:
        hours = int(offset_match.group(1))
        minutes = int(offset_match.group(2) or 0)
        if not (-12 <= hours <= 14) or not (0 <= minutes < 60):
            raise ValueError(f"UTC offset out of range: '{raw}'")
        sign = -1 if hours < 0 else 1
        delta = timedelta(hours=abs(hours), minutes=minutes) * sign
        return timezone(delta)

    try:
        return ZoneInfo(cleaned)
    except ZoneInfoNotFoundError as err:
        raise ValueError(
            f"Unknown timezone '{raw}'. Try 'Europe/Minsk', 'Europe/Moscow', or 'UTC+3'."
        ) from err


def tz_label(tz: tzinfo) -> str:
    if isinstance(tz, ZoneInfo):
        return tz.key
    return tz.tzname(None) or str(tz)


@dataclass(frozen=True)
class Config:
    api_id: int
    api_hash: str
    string_session: str
    default_timezone: str
    default_offset_ms: float
    default_lead_sec: float
    control_chat: str
    port: int

    @classmethod
    def from_env(cls, require_session: bool = True) -> Config:
        api_id_raw = os.getenv("TG_API_ID")
        api_hash = os.getenv("TG_API_HASH")
        string_session = os.getenv("TG_STRING_SESSION", "")
        default_timezone = os.getenv("TIMEZONE", "Europe/Minsk")
        default_offset_ms = float(os.getenv("OFFSET_MS", "0"))
        default_lead_sec = float(os.getenv("LEAD_SEC", "3.0"))
        control_chat = os.getenv("CONTROL_CHAT", "").strip()
        port = int(os.getenv("PORT", "8080"))

        if not api_id_raw or not api_hash:
            raise RuntimeError("TG_API_ID and TG_API_HASH must be set in environment / .env")
        if require_session and not string_session:
            raise RuntimeError(
                "TG_STRING_SESSION is empty. Run: uv run python -m tg_scheduler.generate_session"
            )

        # Validate default timezone early
        parse_timezone(default_timezone)

        return cls(
            api_id=int(api_id_raw),
            api_hash=api_hash,
            string_session=string_session,
            default_timezone=default_timezone,
            default_offset_ms=default_offset_ms,
            default_lead_sec=default_lead_sec,
            control_chat=control_chat,
            port=port,
        )


@dataclass
class RuntimeSettings:
    timezone_str: str
    tz: tzinfo
    offset_ms: float
    lead_sec: float

    @classmethod
    def load(cls, cfg: Config) -> RuntimeSettings:
        tz_str = cfg.default_timezone
        offset_ms = cfg.default_offset_ms
        lead_sec = cfg.default_lead_sec

        if SETTINGS_FILE.exists():
            try:
                data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
                tz_str = str(data.get("timezone_str", tz_str))
                offset_ms = float(data.get("offset_ms", offset_ms))
                lead_sec = float(data.get("lead_sec", lead_sec))
            except (OSError, ValueError, TypeError):
                pass

        return cls(
            timezone_str=tz_str,
            tz=parse_timezone(tz_str),
            offset_ms=offset_ms,
            lead_sec=lead_sec,
        )

    def save(self) -> None:
        payload = {
            "timezone_str": self.timezone_str,
            "offset_ms": self.offset_ms,
            "lead_sec": self.lead_sec,
        }
        SETTINGS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
