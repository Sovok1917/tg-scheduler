from __future__ import annotations

import os
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    api_id: int
    api_hash: str
    string_session: str
    timezone: ZoneInfo
    port: int

    @classmethod
    def from_env(cls, require_session: bool = True) -> Config:
        api_id_raw = os.getenv("TG_API_ID")
        api_hash = os.getenv("TG_API_HASH")
        string_session = os.getenv("TG_STRING_SESSION", "")
        tz_name = os.getenv("TIMEZONE", "Europe/Amsterdam")
        port = int(os.getenv("PORT", "8080"))

        if not api_id_raw or not api_hash:
            raise RuntimeError("TG_API_ID and TG_API_HASH must be set in environment / .env")
        if require_session and not string_session:
            raise RuntimeError(
                "TG_STRING_SESSION is empty. Run: uv run python -m tg_scheduler.generate_session"
            )

        return cls(
            api_id=int(api_id_raw),
            api_hash=api_hash,
            string_session=string_session,
            timezone=ZoneInfo(tz_name),
            port=port,
        )
