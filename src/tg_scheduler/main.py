from __future__ import annotations

import asyncio
import logging
import secrets
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from typing import Any

from aiohttp import web
from telethon import TelegramClient, events, utils
from telethon.errors import RPCError
from telethon.sessions import StringSession
from telethon.tl.functions import PingRequest
from telethon.tl.functions.channels import CreateChannelRequest
from telethon.tl.types import Channel

from tg_scheduler.config import Config, RuntimeSettings, parse_timezone, tz_label

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("tg_scheduler")

DEFAULT_CONTROL_TITLE = "TG Scheduler Control"


@dataclass
class ScheduledJob:
    job_id: str
    target_label: str
    target_peer: Any
    message: str
    target_dt: datetime
    task: asyncio.Task[None] | None = None


def parse_target_time(raw: str, tz: tzinfo) -> datetime:
    """
    Accepts:
      - HH:MM
      - HH:MM:SS
      - HH:MM:SS.fff
      - YYYY-MM-DDTHH:MM:SS(.fff) or YYYY-MM-DD_HH:MM:SS
    If only time is given and it already passed today, schedules for tomorrow.
    """
    now = datetime.now(tz)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d_%H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=tz)
            if dt <= now:
                raise ValueError("Target datetime is already in the past.")
            return dt
        except ValueError as err:
            if "already in the past" in str(err):
                raise

    for fmt in ("%H:%M:%S.%f", "%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(raw, fmt).time()
            dt = datetime.combine(now.date(), t, tzinfo=tz)
            if dt <= now:
                dt += timedelta(days=1)
            return dt
        except ValueError:
            continue

    raise ValueError(
        "Invalid time format. Use HH:MM, HH:MM:SS, HH:MM:SS.mmm, or YYYY-MM-DDTHH:MM:SS"
    )


async def measure_min_rtt(client: TelegramClient, samples: int = 3) -> float:
    """Measures minimum round-trip time (seconds) to the connected Telegram DC."""
    best = float("inf")
    for _ in range(samples):
        t0 = time.perf_counter()
        await client(PingRequest(ping_id=secrets.randbits(63)))
        elapsed = time.perf_counter() - t0
        best = min(best, elapsed)
    return best


def format_settings_message(settings: RuntimeSettings, control_id: int) -> str:
    now_local = datetime.now(settings.tz)
    return (
        "🟢 **TG Scheduler Online**\n"
        f"• Current time: `{now_local.strftime('%Y-%m-%d %H:%M:%S')}` "
        f"(`{tz_label(settings.tz)}`)\n"
        f"• Extra offset: `{settings.offset_ms:+.1f} ms`\n"
        f"• Wake-up lead: `{settings.lead_sec:.1f} s`\n"
        f"• Control chat ID: `{control_id}`\n\n"
        "**Quick config:**\n"
        "`/tz Europe/Minsk` (or `UTC+3`, `Europe/Moscow`)\n"
        "`/offset <ms>` — fine-tune timing (e.g. `/offset -30`)\n"
        "`/lead <sec>` — pre-warm lead time (default `3.0`)\n"
        "`/help` — all commands"
    )


async def resolve_control_channel(client: TelegramClient, cfg: Config) -> Any:
    """
    Uses CONTROL_CHAT from env if provided; otherwise finds an existing
    private 'TG Scheduler Control' channel owned by you, or creates one.
    """
    dialogs = await client.get_dialogs(limit=100)

    if cfg.control_chat:
        lookup: int | str = (
            int(cfg.control_chat)
            if cfg.control_chat.lstrip("-").isdigit()
            else cfg.control_chat
        )
        return await client.get_entity(lookup)

    for dialog in dialogs:
        entity = dialog.entity
        if (
            isinstance(entity, Channel)
            and entity.title == DEFAULT_CONTROL_TITLE
            and entity.creator
        ):
            return entity

    result = await client(
        CreateChannelRequest(
            title=DEFAULT_CONTROL_TITLE,
            about="Command center for TG Scheduler userbot",
            megagroup=False,
        )
    )
    return result.chats[0]


async def precision_send_job(
    client: TelegramClient,
    job: ScheduledJob,
    jobs: dict[str, ScheduledJob],
    settings: RuntimeSettings,
    control_entity: Any,
) -> None:
    tz = settings.tz
    try:
        # 1. Sleep until lead_sec before target time
        target_ts = job.target_dt.timestamp()
        wake_up_delay = (target_ts - settings.lead_sec) - time.time()
        if wake_up_delay > 0:
            await asyncio.sleep(wake_up_delay)

        # 2. Ensure connection is warm & measure current network RTT
        if not client.is_connected():
            await client.connect()

        rtt = await measure_min_rtt(client, samples=3)
        one_way_delay = min(rtt / 2.0, 0.250)
        fire_ts = target_ts - one_way_delay + (settings.offset_ms / 1000.0)

        # 3. Async sleep until ~2ms before fire_ts, then spin for sub-ms precision
        remaining = fire_ts - time.time()
        if remaining > 0.002:
            await asyncio.sleep(remaining - 0.002)

        deadline = time.perf_counter() + max(0.0, fire_ts - time.time())
        while time.perf_counter() < deadline:
            pass

        fired_local = datetime.now(tz)
        sent = await client.send_message(job.target_peer, job.message)
        ack_local = datetime.now(tz)

        server_time = sent.date.astimezone(tz)
        report = (
            f"✅ **Sent** (`{job.job_id}` → `{job.target_label}`)\n"
            f"• Target ({tz_label(tz)}): `{job.target_dt.strftime('%H:%M:%S.%f')[:-3]}`\n"
            f"• One-way RTT comp: `{one_way_delay * 1000:.1f} ms` "
            f"(offset `{settings.offset_ms:+.1f} ms`)\n"
            f"• Fired locally: `{fired_local.strftime('%H:%M:%S.%f')[:-3]}`\n"
            f"• Ack received: `{ack_local.strftime('%H:%M:%S.%f')[:-3]}`\n"
            f"• Telegram server timestamp: `{server_time.strftime('%H:%M:%S')}`"
        )
        await client.send_message(control_entity, report)
    except asyncio.CancelledError:
        logger.info("Job %s cancelled.", job.job_id)
        raise
    except Exception as exc:
        logger.exception("Failed job %s", job.job_id)
        await client.send_message(control_entity, f"❌ Job `{job.job_id}` failed: `{exc}`")
    finally:
        jobs.pop(job.job_id, None)


async def start_http_server(
    port: int,
    client: TelegramClient,
    jobs: dict[str, ScheduledJob],
) -> None:
    """Lightweight HTTP server for Render Web Services & external keep-alive pingers."""

    async def health(_request: web.Request) -> web.Response:
        status = {
            "ok": True,
            "telegram_connected": client.is_connected(),
            "pending_jobs": len(jobs),
        }
        return web.json_response(status)

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    logger.info("HTTP health server listening on 0.0.0.0:%d", port)


async def async_main() -> None:
    cfg = Config.from_env()
    settings = RuntimeSettings.load(cfg)
    jobs: dict[str, ScheduledJob] = {}

    client = TelegramClient(
        StringSession(cfg.string_session),
        cfg.api_id,
        cfg.api_hash,
    )
    await client.start()
    me = await client.get_me()
    logger.info("Logged in as %s (id=%s)", me.username or me.first_name, me.id)

    control_entity = await resolve_control_channel(client, cfg)
    control_peer_id = utils.get_peer_id(control_entity)
    logger.info("Using control chat: %s (id=%s)", DEFAULT_CONTROL_TITLE, control_peer_id)

    await client.send_message(
        control_entity,
        format_settings_message(settings, control_peer_id),
    )

    @client.on(events.NewMessage(chats=[control_peer_id]))
    async def on_control_message(event: events.NewMessage.Event) -> None:
        text = (event.raw_text or "").strip()
        if not text.startswith("/"):
            return

        parts = text.split(maxsplit=3)
        cmd = parts[0].lower()

        if cmd == "/help":
            await event.reply(
                "**Commands:**\n"
                "`/send <@user_or_id> <HH:MM[:SS[.mmm]]> <message>`\n"
                "`/list` — show pending jobs\n"
                "`/cancel [id]` — cancel job (or last scheduled if no ID)\n"
                "`/clear [count]` — delete chat history (or last N messages)\n"
                "`/settings` — show current settings & clock\n"
                "`/tz <zone>` — set timezone (e.g. `Europe/Minsk`, `UTC+3`)\n"
                "`/offset <ms>` — set timing offset in ms\n"
                "`/lead <sec>` — set pre-warm lead time in seconds"
            )
            return

        if cmd == "/settings":
            await event.reply(format_settings_message(settings, control_peer_id))
            return

        if cmd == "/tz":
            if len(parts) < 2:
                now_local = datetime.now(settings.tz)
                await event.reply(
                    f"Current timezone: `{tz_label(settings.tz)}` "
                    f"(`{now_local.strftime('%H:%M:%S')}`)\n"
                    "Usage: `/tz Europe/Minsk` or `/tz UTC+3`"
                )
                return
            raw_tz = text.split(maxsplit=1)[1].strip()
            try:
                new_tz = parse_timezone(raw_tz)
            except ValueError as err:
                await event.reply(f"❌ {err}")
                return
            settings.timezone_str = raw_tz
            settings.tz = new_tz
            settings.save()
            now_local = datetime.now(settings.tz)
            await event.reply(
                f"✅ Timezone set to `{tz_label(settings.tz)}`. "
                f"Current time: `{now_local.strftime('%Y-%m-%d %H:%M:%S')}`"
            )
            return

        if cmd == "/offset":
            if len(parts) < 2:
                await event.reply(f"Current offset: `{settings.offset_ms:+.1f} ms`")
                return
            try:
                val = float(parts[1])
            except ValueError:
                await event.reply("❌ Usage: `/offset <ms>` (e.g. `/offset -25`)")
                return
            settings.offset_ms = val
            settings.save()
            await event.reply(f"✅ Extra timing offset set to `{val:+.1f} ms`.")
            return

        if cmd == "/lead":
            if len(parts) < 2:
                await event.reply(f"Current lead time: `{settings.lead_sec:.1f} s`")
                return
            try:
                val = float(parts[1])
                if not (0.5 <= val <= 30.0):
                    raise ValueError
            except ValueError:
                await event.reply("❌ Usage: `/lead <seconds>` (between `0.5` and `30.0`)")
                return
            settings.lead_sec = val
            settings.save()
            await event.reply(f"✅ Wake-up lead time set to `{val:.1f} s`.")
            return

        if cmd == "/list":
            if not jobs:
                await event.reply("No pending jobs.")
                return
            lines = []
            for job in jobs.values():
                ts_str = job.target_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                lines.append(
                    f"• `{job.job_id}` → `{job.target_label}` at `{ts_str}`"
                )
            await event.reply("**Pending jobs:**\n" + "\n".join(lines))
            return

        if cmd == "/cancel":
            if not jobs:
                await event.reply("No pending jobs to cancel.")
                return
            job_id = parts[1].strip() if len(parts) >= 2 else next(reversed(jobs))
            job = jobs.get(job_id)
            if not job or not job.task:
                await event.reply(f"Job `{job_id}` not found.")
                return
            job.task.cancel()
            await event.reply(
                f"🛑 Cancelled `{job_id}` (`{job.target_label}` at "
                f"`{job.target_dt.strftime('%H:%M:%S')}`)."
            )
            return

        if cmd == "/clear":
            count_limit: int | None = None
            if len(parts) >= 2:
                try:
                    count_limit = int(parts[1])
                    if count_limit <= 0:
                        raise ValueError
                except ValueError:
                    await event.reply("❌ Usage: `/clear` or `/clear <count>`")
                    return

            ids: list[int] = []
            fetch_limit = (count_limit + 1) if count_limit is not None else 500
            async for msg in client.iter_messages(control_entity, limit=fetch_limit):
                if msg.id == 1 and getattr(msg, "action", None) is not None:
                    continue
                ids.append(msg.id)

            deleted = 0
            for i in range(0, len(ids), 100):
                chunk = ids[i : i + 100]
                try:
                    await client.delete_messages(control_entity, chunk)
                    deleted += len(chunk)
                except RPCError as err:
                    logger.warning("Failed deleting some messages: %s", err)

            notice = await client.send_message(
                control_entity,
                f"🧹 Cleared {deleted} message(s).",
            )
            await asyncio.sleep(3)
            try:
                await notice.delete()
            except RPCError:
                pass
            return

        if cmd == "/send":
            if len(parts) < 4:
                await event.reply(
                    "Usage: `/send <@user_or_id> <HH:MM[:SS[.mmm]]> <message>`"
                )
                return

            target_raw, time_raw, msg_body = parts[1], parts[2], parts[3]

            try:
                target_dt = parse_target_time(time_raw, settings.tz)
            except ValueError as err:
                await event.reply(f"❌ {err}")
                return

            try:
                target_lookup: int | str = (
                    int(target_raw) if target_raw.lstrip("-").isdigit() else target_raw
                )
                peer = await client.get_input_entity(target_lookup)
            except (ValueError, RPCError, OSError) as err:
                await event.reply(f"❌ Could not resolve chat `{target_raw}`: `{err}`")
                return

            job_id = uuid.uuid4().hex[:6]
            job = ScheduledJob(
                job_id=job_id,
                target_label=target_raw,
                target_peer=peer,
                message=msg_body,
                target_dt=target_dt,
            )
            job.task = asyncio.create_task(
                precision_send_job(client, job, jobs, settings, control_entity)
            )
            jobs[job_id] = job

            ts_str = target_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            await event.reply(
                f"⏱️ Scheduled `{job_id}` for `{ts_str} {tz_label(settings.tz)}`"
            )

    await start_http_server(cfg.port, client, jobs)
    await client.run_until_disconnected()


def run() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    run()
