"""telegram_kafka_bot.py
A Telegram chatbot that:
1. Listens to a Kafka topic with aiokafka and receives JSON events.
2. Broadcasts every event to all users who have pressed /start (subscribers).
3. Leaves a placeholder for a second Kafka topic (to be added later).

Dependencies:
    aiogram>=3.5
    aiokafka>=0.10
    python-dotenv

Environment variables (see .env file):
    TELEGRAM_BOT_TOKEN=<your bot token>
Kafka config is hard‑coded below for brevity. Refactor to env if needed.
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Set

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart
from aiogram.types import Message, InputFile
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

###############################################################################
# Configuration
###############################################################################

load_dotenv()

TELEGRAM_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "processed-data"

# TODO: Add a second Kafka topic here once it becomes available, e.g.:
# SECOND_KAFKA_TOPIC = "<future_topic>"

KAFKA_GROUP_ID = "telegram-bot-consumer-debug"  # see README about scaling

SUBSCRIBERS_FILE = Path("subscribers.json")

###############################################################################
# Subscriber persistence helpers
###############################################################################

_subscribers: Set[int] = set()


def _load_subscribers() -> None:
    """Populate the in‑memory subscriber set from JSON on disk (if any)."""
    global _subscribers
    if SUBSCRIBERS_FILE.exists():
        try:
            data = json.loads(SUBSCRIBERS_FILE.read_text())
            # File can be either a list or a mapping {"subscribers": [...]}
            if isinstance(data, dict):
                data = data.get("subscribers", [])
            _subscribers.update(int(x) for x in data)
        except (json.JSONDecodeError, ValueError):
            logging.warning("Failed to parse %s – starting with empty list", SUBSCRIBERS_FILE)


def _save_subscribers() -> None:
    """Persist the current subscriber list atomically."""
    tmp = SUBSCRIBERS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(sorted(_subscribers), ensure_ascii=False, indent=2))
    tmp.replace(SUBSCRIBERS_FILE)

###############################################################################
# Bot setup
###############################################################################

router = Router()


@router.message(CommandStart())
async def on_start(msg: Message) -> None:
    """/start handler – registers a user and sends a confirmation."""
    user_id = msg.from_user.id
    if user_id not in _subscribers:
        _subscribers.add(user_id)
        _save_subscribers()
    await msg.answer("✅ Вы подписаны на рассылку!")


async def _broadcast(bot: Bot, text: str) -> None:
    """Send *text* to all known subscribers, removing those that block the bot.

    • JSON оборачивается в <pre> и HTML‑экранируется, чтобы Telegram
      не пытался интерпретировать символы «<»/«>» как теги.
    • Если сообщение превышает лимит 4096 символов, отправляем его как файл
      `event.json`.
    """
    from html import escape
    from io import BytesIO

    escaped = escape(text)

    for chat_id in list(_subscribers):
        try:
            if len(escaped) <= 4000:
                await bot.send_message(chat_id, f"<pre>{escaped}</pre>", parse_mode="HTML")
            else:
                # Более крупный payload отправляем документом
                infile = BytesIO(text.encode())
                infile.name = "event.json"
                await bot.send_document(chat_id, InputFile(infile))
        except Exception as exc:
            logging.exception("Failed to send to %s: %s", chat_id, exc)
            if any(err in str(exc) for err in ("bot was blocked", "chat not found")):
                _subscribers.discard(chat_id)
                _save_subscribers()

###############################################################################
# Kafka consumer
###############################################################################

async def _consume_kafka(bot: Bot) -> None:
    """Background task that forwards Kafka messages to Telegram users."""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        # Add SECOND_KAFKA_TOPIC here when ready
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="latest",  # start from the tail for new group_ids
    )

    await consumer.start()
    try:
        async for record in consumer:
            try:
                payload = json.loads(record.value)
                pretty = json.dumps(payload, ensure_ascii=False, indent=2)
            except json.JSONDecodeError:
                # Raw string if value is not valid JSON
                pretty = record.value
            await _broadcast(bot, pretty)
    finally:
        await consumer.stop()

###############################################################################
# Entry point
###############################################################################

async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    _load_subscribers()

    bot = Bot(TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
    dp = Dispatcher()
    dp.include_router(router)

    # Run both polling and Kafka consumer in parallel
    await asyncio.gather(
        dp.start_polling(bot, skip_updates=True),
        _consume_kafka(bot),
    )


if __name__ == "__main__":
    asyncio.run(main())
