import os
import logging

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

logger.info("TELEGRAM_TOKEN dostępny: %s", bool(TELEGRAM_TOKEN))
logger.info("OPENAI_API_KEY dostępny: %s", bool(OPENAI_API_KEY))

if not TELEGRAM_TOKEN:
    raise RuntimeError(
        "Brak TELEGRAM_TOKEN w Environment Variables na Render."
    )

if not OPENAI_API_KEY:
    raise RuntimeError(
        "Brak OPENAI_API_KEY w Environment Variables na Render."
    )

client = AsyncOpenAI(api_key=OPENAI_API_KEY)


async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    await update.message.reply_text(
        "Cześć! 👋\n\n"
        "Jestem Trading AI Analyzer.\n"
        "Wyślij mi pytanie, a odpowiem przy pomocy AI."
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

        response = await client.responses.create(
            model="gpt-5-mini",
            instructions=(
                "Odpowiadaj po polsku. "
                "Jesteś pomocnym asystentem AI o nazwie Trading AI Analyzer. "
                "Jeżeli użytkownik pyta o trading, forex, złoto, indeksy, "
                "kryptowaluty lub analizę rynku, odpowiadaj jasno i konkretnie. "
                "Wyraźnie oddzielaj fakty od przypuszczeń. "
                "Nie gwarantuj zysków."
            ),
            input=text,
        )

        answer_text = response.output_text or (
            "OpenAI nie zwrócił odpowiedzi. Spróbuj ponownie."
        )

        await update.message.reply_text(answer_text)

    except Exception as error:
        logger.exception("Błąd OpenAI: %s", error)

        await update.message.reply_text(
            "Wystąpił błąd podczas łączenia z OpenAI. "
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
