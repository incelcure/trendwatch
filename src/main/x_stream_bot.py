import asyncio
import json
import os
from pathlib import Path

from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

# ────────────────────── конфиг ──────────────────────
load_dotenv()
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

KAFKA_TOPIC = "processed_data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"

SUBSCRIBERS_FILE = Path("subscribers.json")

# ────────────────────── telegram ─────────────────────
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher()

# загружаем/создаём набор подписчиков
if SUBSCRIBERS_FILE.exists():
    subscribers: set[int] = set(json.loads(SUBSCRIBERS_FILE.read_text()))
else:
    subscribers: set[int] = set()


def dump_subscribers() -> None:
    """Пишем set[int] в файл как JSON-массив."""
    SUBSCRIBERS_FILE.write_text(json.dumps(list(subscribers), ensure_ascii=False))


@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    """Подписать чат на рассылку Kafka-сообщений."""
    if message.chat.id in subscribers:
        await message.answer("✅ Вы уже подписаны на обновления.")
    else:
        subscribers.add(message.chat.id)
        dump_subscribers()
        await message.answer(
            "Теперь я буду присылать новые сообщения из апишек.\n"
            "Чтобы отписаться, введите /stop."
        )


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message) -> None:
    """Отписать чат от рассылки."""
    if subscribers.discard(message.chat.id):
        dump_subscribers()
        await message.answer("Вы отписались от уведомлений.")
    else:
        await message.answer("Вы и так не были подписаны.")


# ────────────────────── kafka ────────────────────────
async def kafka_loop() -> None:
    """Слушаем Kafka и рассылаем каждое сообщение всем подписчикам."""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="telegram-bot-consumer",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            text = json.dumps(msg.value, indent=2, ensure_ascii=False)
            if not subscribers:
                continue

            tasks = [
                bot.send_message(
                    chat_id=chat_id,
                    text=f"<b>Новое сообщение из Kafka</b>\n<pre>{text}</pre>",
                )
                for chat_id in subscribers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for chat_id, res in zip(subscribers.copy(), results):
                if isinstance(res, Exception):
                    subscribers.discard(chat_id)
                    dump_subscribers()
    finally:
        await consumer.stop()


async def main() -> None:
    asyncio.create_task(kafka_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
