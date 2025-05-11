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

load_dotenv()

TELEGRAM_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC_JSON_BATCH = "processed-data"
KAFKA_TOPIC_STREAM = "x-stream-data"

KAFKA_TOPICS = [KAFKA_TOPIC_JSON_BATCH, KAFKA_TOPIC_STREAM]

KAFKA_GROUP_ID = "telegram-bot-consumer-debug"

SUBSCRIBERS_FILE = Path("subscribers.json")

_subscribers: Set[int] = set()


def _load_subscribers() -> None:
    """Populate the in‑memory subscriber set from JSON on disk (if any)."""
    global _subscribers
    if SUBSCRIBERS_FILE.exists():
        try:
            data = json.loads(SUBSCRIBERS_FILE.read_text())
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

router = Router()


@router.message(CommandStart())
async def on_start(msg: Message) -> None:
    """/start handler – registers a user and sends a confirmation."""
    user_id = msg.from_user.id
    if user_id not in _subscribers:
        _subscribers.add(user_id)
        _save_subscribers()
    await msg.answer("Вы подписаны на рассылку!")


async def _broadcast(bot: Bot, text: str) -> None:
    """Send *text* to all subscribers, removing those that block the bot."""
    from html import escape
    from io import BytesIO

    escaped = escape(text)

    for chat_id in list(_subscribers):
        try:
            if len(escaped) <= 4000:
                await bot.send_message(chat_id, f"<pre>{escaped}</pre>")
            else:
                buf = BytesIO(text.encode())
                buf.name = "event.txt"
                await bot.send_document(chat_id, InputFile(buf))
        except Exception as exc:
            logging.exception("Failed to send to %s: %s", chat_id, exc)
            if any(err in str(exc) for err in ("bot was blocked", "chat not found")):
                _subscribers.discard(chat_id)
                _save_subscribers()

async def _consume_kafka(bot: Bot) -> None:
    """Background task that forwards Kafka messages to Telegram users."""
    consumer = AIOKafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="latest",
    )

    await consumer.start()
    try:
        async for record in consumer:
            if record.topic == KAFKA_TOPIC_STREAM:
                pretty = record.value  # plain string
            else:
                try:
                    payload = json.loads(record.value)
                    pretty = json.dumps(payload, ensure_ascii=False, indent=2)
                except json.JSONDecodeError:
                    pretty = record.value
            await _broadcast(bot, pretty)
    finally:
        await consumer.stop()

async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    _load_subscribers()

    bot = Bot(TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()
    dp.include_router(router)

    await asyncio.gather(
        dp.start_polling(bot, skip_updates=True),
        _consume_kafka(bot),
    )


if __name__ == "__main__":
    asyncio.run(main())
