from __future__ import annotations

import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession

from tg_scheduler.config import Config


async def main() -> None:
    cfg = Config.from_env(require_session=False)
    async with TelegramClient(StringSession(), cfg.api_id, cfg.api_hash) as client:
        session_str = client.session.save()
        print("\n=== COPY THIS INTO YOUR .env AND RENDER ENV VARS ===")
        print(f"TG_STRING_SESSION={session_str}")
        print("=====================================================\n")


if __name__ == "__main__":
    asyncio.run(main())
