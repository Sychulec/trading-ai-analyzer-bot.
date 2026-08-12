import os
import logging
import urllib.parse
import urllib.request
import json

from openai import AsyncOpenAI
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

logger.info("TELEGRAM_TOKEN dostępny: %s", bool(TELEGRAM_TOKEN))
logger.info("OPENAI_API_KEY dostępny: %s", bool(OPENAI_API_KEY))
logger.info("TWELVE_DATA_API_KEY dostępny: %s", bool(TWELVE_DATA_API_KEY))

if not TELEGRAM_TOKEN:
    raise RuntimeError(
        "Brak TELEGRAM_TOKEN w Environment Variables na Render."
    )

if not OPENAI_API_KEY:
    raise RuntimeError(
        "Brak OPENAI_API_KEY w Environment Variables na Render."
    )

if not TWELVE_DATA_API_KEY:
    raise RuntimeError(
        "Brak TWELVE_DATA_API_KEY w Environment Variables na Render."
    )

client = AsyncOpenAI(api_key=OPENAI_API_KEY)


def get_xauusd_price():
    params = urllib.parse.urlencode(
        {
            "symbol": "XAU/USD",
            "apikey": TWELVE_DATA_API_KEY,
        }
    )

    url = f"https://api.twelvedata.com/price?{params}"

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))

        if "price" in data:
            return data["price"]

        logger.error("Błąd Twelve Data: %s", data)
        return None

    except Exception as error:
        logger.exception("Błąd pobierania ceny XAU/USD: %s", error)
        return None


async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    await update.message.reply_text(
        "Cześć! 👋\n\n"
        "Jestem Trading AI Analyzer.\n"
        "Mogę analizować rynek przy pomocy AI "
        "oraz pobierać aktualną cenę XAU/USD."
    )


async def answer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message or not update.message.text:
        return

    text = update.message.text

    try:
        await update.message.chat.send_action(action="typing")

        xauusd_price = get_xauusd_price()

        if xauusd_price:
            market_data = (
                f"Aktualna cena XAU/USD z Twelve Data: "
                f"{xauusd_price} USD za uncję."
            )
        else:
            market_data = (
                "Nie udało się pobrać aktualnej ceny XAU/USD "
                "z Twelve Data."
            )

        prompt = (
            f"Pytanie użytkownika:\n{text}\n\n"
            f"Dane rynkowe:\n{market_data}"
        )

        response = await client.responses.create(
            model="gpt-5-mini",
            instructions=(
                "Odpowiadaj po polsku. "
                "Jesteś pomocnym asystentem AI o nazwie Trading AI Analyzer. "
                "Jeżeli użytkownik pyta o trading, forex, złoto, indeksy, "
                "kryptowaluty lub analizę rynku, odpowiadaj jasno i konkretnie. "
                "Jeżeli otrzymasz aktualne dane rynkowe, wykorzystaj je w analizie. "
                "Nie udawaj, że masz dostęp do danych, których nie otrzymałeś. "
                "Wyraźnie oddzielaj fakty od przypuszczeń. "
                "Nie gwarantuj zysków."
            ),
            input=prompt,
        )

        answer_text = response.output_text or (
            "OpenAI nie zwrócił odpowiedzi. Spróbuj ponownie."
        )

        for i in range(0, len(answer_text), 4000):
            await update.message.reply_text(
                answer_text[i:i + 4000]
            )

    except Exception as error:
        logger.exception("Błąd OpenAI lub danych rynkowych: %s", error)

        await update.message.reply_text(
            "Wystąpił błąd podczas analizy. "
            "Sprawdź logi Render."
        )


async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    logger.error(
        "Błąd Telegrama:",
        exc_info=context.error,
    )


def main():
    logger.info("Uruchamianie Trading AI Analyzer...")

    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .build()
    )

    application.add_handler(
        CommandHandler("start", start)
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            answer,
        )
    )

    application.add_error_handler(error_handler)

    logger.info("Bot działa i oczekuje na wiadomości.")

    application.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    main()
