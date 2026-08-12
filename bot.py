import os
import asyncio

from openai import OpenAI
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

client = OpenAI(api_key=OPENAI_API_KEY)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Cześć! 👋 Jestem asystentem AI.\n\n"
        "Wyślij mi wiadomość, a odpowiem przy pomocy OpenAI."
    )


async def answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    try:
        response = await asyncio.to_thread(
            client.responses.create,
            model="gpt-5-mini",
            instructions=(
                "Odpowiadaj po polsku. "
                "Jesteś pomocnym asystentem AI. "
                "Jeśli użytkownik pyta o trading lub analizę rynku, "
                "wyraźnie oddzielaj fakty od przypuszczeń i nie gwarantuj zysków."
            ),
            input=text,
        )

        await update.message.reply_text(response.output_text)

    except Exception as e:
        print(f"OpenAI error: {e}")
        await update.message.reply_text(
            "Wystąpił błąd podczas łączenia z OpenAI. Spróbuj ponownie."
        )


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, answer)
    )

    print("Bot działa...")
    app.run_polling()


if __name__ == "__main__":
    main()
