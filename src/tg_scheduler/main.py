from __future__ import annotations

import asyncio
import logging
import secrets
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from aiohttp import web
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions import PingRequest

from tg_scheduler.config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("tg_scheduler")


@dataclass
class ScheduledJob:
    job_id: str
    target_label: str
    target_peer: Any
    message: str
    target_dt: datetime
    task: asyncio.Task[None] | None = None


def parse_target_time(raw: str, tz: Any) -> datetime:
    """
    Accepts:
      - HH:MM
      - HH:MM:SS
      - HH:MM:SS.fff
      - YYYY-MM-DD HH:MM:SS(.fff) (passed via ISO or space-joined)
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


async def precision_send_job(
    client: TelegramClient,
    job: ScheduledJob,
    jobs: dict[str, ScheduledJob],
    tz: Any,
) -> None:
    try:
        # 1. Sleep until 3 seconds before target time
        lead_time = 3.0
        now_ts = time.time()
        target_ts = job.target_dt.timestamp()
        wake_up_delay = (target_ts - lead_time) - now_ts
        if wake_up_delay > 0:
            await asyncio.sleep(wake_up_delay)

        # 2. Ensure connection is warm & measure current network RTT
        if not client.is_connected():
            await client.connect()

        rtt = await measure_min_rtt(client, samples=3)
        one_way_delay = rtt / 2.0

        # Cap compensation at 250ms in case of an anomalous ping spike
        one_way_delay = min(one_way_delay, 0.250)
        fire_ts = target_ts - one_way_delay

        # 3. Coarse async sleep until 15ms before fire_ts
        remaining = fire_ts - time.time()
        if remaining > 0.015:
            await asyncio.sleep(remaining - 0.015)

        # 4. Fine sleep (yield to event loop cleanly in small steps until exact fire_ts)
        while time.time() < fire_ts:
            if fire_ts - time.time() > 0.002:
                await asyncio.sleep(0.001)
            else:
                # Final <2ms spin for sub-millisecond OS scheduler precision
                pass

        fired_local = datetime.now(tz)
        sent = await client.send_message(job.target_peer, job.message)
        ack_local = datetime.now(tz)

        server_time = sent.date.astimezone(tz)
        await client.send_message(
            "me",
            (
                f"✅ **Sent** (`{job.job_id}`)\n"
                f"• Target: `{job.target_dt.strftime('%H:%M:%S.%f')[:-3]}`\n"
                f"• One-way RTT comp: `{one_way_delay * 1000:.1f} ms`\n"
                f"• Fired locally: `{fired_local.strftime('%H:%M:%S.%f')[:-3]}`\n"
                f"• Ack received: `{ack_local.strftime('%H:%M:%S.%f')[:-3]}`\n"
                f"• Telegram server timestamp: `{server_time.strftime('%H:%M:%S')}`"
            ),
        )
    except asyncio.CancelledError:
        logger.info("Job %s cancelled.", job.job_id)
        raise
    except Exception as exc:
        logger.exception("Failed job %s", job.job_id)
        await client.send_message("me", f"❌ Job `{job.job_id}` failed: `{exc}`")
    finally:
        jobs.pop(job.job_id, None)


async def start_http_server(port: int, client: TelegramClient, jobs: dict[str, ScheduledJob]) -> None:
    """Lightweight HTTP server required by Render Web Services & external keep-alive pingers."""

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
    jobs: dict[str, ScheduledJob] = {}

    client = TelegramClient(
        StringSession(cfg.string_session),
        cfg.api_id,
        cfg.api_hash,
    )
    await client.start()
    me = await client.get_me()
    logger.info("Logged in as %s (id=%s)", me.username or me.first_name, me.id)

    @client.on(events.NewMessage(chats="me", outgoing=True))
    async def on_saved_message(event: events.NewMessage.Event) -> None:
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
                "`/cancel <id>` — cancel a job"
            )
            return

        if cmd == "/list":
            if not jobs:
                await event.reply("No pending jobs.")
                return
            lines = [
                f"• `{j.job_id}` → `{j.target_label}` at `{j.target_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}`"
                for j in jobs.values()
            ]
            await event.reply("**Pending jobs:**\n" + "\n".join(lines))
            return

        if cmd == "/cancel":
            if len(parts) < 2:
                await event.reply("Usage: `/cancel <id>`")
                return
            job_id = parts[1].strip()
            job = jobs.get(job_id)
            if not job or not job.task:
                await event.reply(f"Job `{job_id}` not found.")
                return
            job.task.cancel()
            await event.reply(f"🛑 Cancelled job `{job_id}`.")
            return

        if cmd == "/send":
            if len(parts) < 4:
                await event.reply("Usage: `/send <@user_or_id> <HH:MM[:SS[.mmm]]> <message>`")
                return

            target_raw, time_raw, msg_body = parts[1], parts[2], parts[3]

            try:
                target_dt = parse_target_time(time_raw, cfg.timezone)
            except ValueError as err:
                await event.reply(f"❌ {err}")
                return

            # Pre-resolve entity now so there is zero lookup overhead at fire time
            try:
                target_lookup: int | str = int(target_raw) if target_raw.lstrip("-").isdigit() else target_raw
                peer = await client.get_input_entity(target_lookup)
            except Exception as err:
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
            job.task = asyncio.create_task(precision_send_job(client, job, jobs, cfg.timezone))
            jobs[job_id] = job

            await event.reply(
                f"⏱️ Scheduled `{job_id}` for `{target_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} {cfg.timezone.key}`"
            )

    await start_http_server(cfg.port, client, jobs)
    await client.run_until_disconnected()


def run() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    run()
