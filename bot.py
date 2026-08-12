import os
import logging
import urllib.parse
import urllib.request
import json
import asyncio

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
logger.info(
    "TWELVE_DATA_API_KEY dostępny: %s",
    bool(TWELVE_DATA_API_KEY),
)

if not TELEGRAM_TOKEN:
    raise RuntimeError("Brak TELEGRAM_TOKEN w Render.")

if not OPENAI_API_KEY:
    raise RuntimeError("Brak OPENAI_API_KEY w Render.")

if not TWELVE_DATA_API_KEY:
    raise RuntimeError("Brak TWELVE_DATA_API_KEY w Render.")

client = AsyncOpenAI(api_key=OPENAI_API_KEY)


# --------------------------------------------------
# TWELVE DATA
# --------------------------------------------------

def fetch_candles(interval, outputsize=120):
    params = urllib.parse.urlencode(
        {
            "symbol": "XAU/USD",
            "interval": interval,
            "outputsize": outputsize,
            "apikey": TWELVE_DATA_API_KEY,
        }
    )

    url = f"https://api.twelvedata.com/time_series?{params}"

    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            data = json.loads(
                response.read().decode("utf-8")
            )

        if "values" not in data:
            logger.error(
                "Twelve Data %s: %s",
                interval,
                data,
            )
            return None

        candles = []

        # Twelve Data zwraca najnowsze świece jako pierwsze.
        # Odwracamy kolejność: najstarsza -> najnowsza.
        for item in reversed(data["values"]):
            candles.append(
                {
                    "datetime": item["datetime"],
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                }
            )

        return candles

    except Exception as error:
        logger.exception(
            "Błąd Twelve Data %s: %s",
            interval,
            error,
        )
        return None


# --------------------------------------------------
# WSKAŹNIKI TECHNICZNE
# --------------------------------------------------

def ema_series(values, period):
    if not values:
        return []

    multiplier = 2 / (period + 1)
    result = [values[0]]

    for value in values[1:]:
        new_ema = (
            value * multiplier
            + result[-1] * (1 - multiplier)
        )
        result.append(new_ema)

    return result


def calculate_rsi(closes, period=14):
    if len(closes) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]

        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (
            (avg_gain * (period - 1)) + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def calculate_macd(closes):
    if len(closes) < 35:
        return None, None, None

    ema12 = ema_series(closes, 12)
    ema26 = ema_series(closes, 26)

    macd_line = [
        a - b
        for a, b in zip(ema12, ema26)
    ]

    signal_line = ema_series(macd_line, 9)

    macd = macd_line[-1]
    signal = signal_line[-1]
    histogram = macd - signal

    return macd, signal, histogram


def calculate_support_resistance(candles, lookback=30):
    if not candles:
        return None, None

    recent = candles[-lookback:]

    support = min(
        candle["low"]
        for candle in recent
    )

    resistance = max(
        candle["high"]
        for candle in recent
    )

    return support, resistance


def analyze_timeframe(interval):
    candles = fetch_candles(interval)

    if not candles or len(candles) < 55:
        return {
            "interval": interval,
            "error": "Brak wystarczających danych",
        }

    closes = [
        candle["close"]
        for candle in candles
    ]

    latest = candles[-1]

    ema20_values = ema_series(closes, 20)
    ema50_values = ema_series(closes, 50)

    ema20 = ema20_values[-1]
    ema50 = ema50_values[-1]

    rsi = calculate_rsi(closes)

    macd, signal, histogram = calculate_macd(
        closes
    )

    support, resistance = (
        calculate_support_resistance(candles)
    )

    if ema20 > ema50:
        trend = "wzrostowy"
    elif ema20 < ema50:
        trend = "spadkowy"
    else:
        trend = "neutralny"

    return {
        "interval": interval,
        "datetime": latest["datetime"],
        "price": latest["close"],
        "open": latest["open"],
        "high": latest["high"],
        "low": latest["low"],
        "rsi": rsi,
        "ema20": ema20,
        "ema50": ema50,
        "macd": macd,
        "signal": signal,
        "histogram": histogram,
        "support": support,
        "resistance": resistance,
        "trend": trend,
    }


def build_market_analysis():
    intervals = [
        "1min",
        "5min",
        "15min",
        "1h",
    ]

    results = []

    for interval in intervals:
        results.append(
            analyze_timeframe(interval)
        )

    return results


def format_market_data(results):
    parts = []

    for data in results:
        interval = data["interval"]

        if "error" in data:
            parts.append(
                f"{interval}: {data['error']}"
            )
            continue

        parts.append(
            f"""
INTERWAŁ {interval}
Czas świecy: {data['datetime']}
Cena/close: {data['price']:.2f}
Open: {data['open']:.2f}
High: {data['high']:.2f}
Low: {data['low']:.2f}

Trend EMA20/EMA50: {data['trend']}
EMA20: {data['ema20']:.2f}
EMA50: {data['ema50']:.2f}

RSI(14): {data['rsi']:.2f}

MACD: {data['macd']:.4f}
Signal: {data['signal']:.4f}
Histogram: {data['histogram']:.4f}

Orientacyjne wsparcie:
{data['support']:.2f}

Orientacyjny opór:
{data['resistance']:.2f}
""".strip()
        )

    return "\n\n----------------\n\n".join(parts)


# --------------------------------------------------
# TELEGRAM
# --------------------------------------------------

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    await update.message.reply_text(
        "Cześć! 👋\n\n"
        "Jestem Trading AI Analyzer.\n"
        "Analizuję XAU/USD przy użyciu aktualnych "
        "danych rynkowych z Twelve Data.\n\n"
        "Uwzględniam interwały:\n"
        "1m • 5m • 15m • 1h\n\n"
        "oraz RSI, EMA, MACD i orientacyjne "
        "poziomy wsparcia/oporu."
    )


async def answer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message or not update.message.text:
        return

    text = update.message.text

    try:
        await update.message.chat.send_action(
            action="typing"
        )

        # Pobieranie danych wykonujemy poza główną
        # pętlą async Telegrama.
        market_results = await asyncio.to_thread(
            build_market_analysis
        )

        market_data = format_market_data(
            market_results
        )

        prompt = f"""
PYTANIE UŻYTKOWNIKA:
{text}

AKTUALNE DANE TECHNICZNE XAU/USD:
{market_data}

Przeanalizuj dane i odpowiedz użytkownikowi.
"""

        response = await client.responses.create(
            model="gpt-5-mini",
            instructions=(
                "Odpowiadaj po polsku. "
                "Jesteś Trading AI Analyzer. "
                "Analizujesz XAU/USD na podstawie "
                "dostarczonych danych rynkowych. "

                "Najpierw podaj aktualną cenę widoczną "
                "w najświeższych danych. "

                "Następnie przeanalizuj osobno interwały "
                "1m, 5m, 15m i 1h. "

                "Uwzględniaj RSI, EMA20, EMA50, MACD, "
                "histogram MACD oraz zakres ostatnich świec. "

                "Oceń, czy sygnały z różnych interwałów "
                "są zgodne czy sprzeczne. "

                "Wsparcie i opór z programu traktuj jako "
                "orientacyjne poziomy wynikające z zakresu "
                "ostatnich świec, a nie pewne poziomy techniczne. "

                "Na końcu przedstaw maksymalnie trzy scenariusze: "
                "wzrostowy, spadkowy i neutralny. "

                "Nie wymyślaj danych. "
                "Nie twierdź, że masz dane, których nie otrzymałeś. "
                "Wyraźnie oddzielaj fakty od interpretacji. "
                "Nie gwarantuj zysków ani kierunku rynku. "
                "Nie przedstawiaj analizy jako pewnego sygnału "
                "kupna lub sprzedaży."
            ),
            input=prompt,
        )

        answer_text = response.output_text or (
            "OpenAI nie zwrócił odpowiedzi."
        )

        # Telegram ma limit długości wiadomości.
        for i in range(
            0,
            len(answer_text),
            4000,
        ):
            await update.message.reply_text(
                answer_text[i:i + 4000]
            )

    except Exception as error:
        logger.exception(
            "Błąd analizy: %s",
            error,
        )

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
    logger.info(
        "Uruchamianie Trading AI Analyzer..."
    )

    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            answer,
        )
    )

    application.add_error_handler(
        error_handler
    )

    logger.info(
        "Bot działa i oczekuje na wiadomości."
    )

    application.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    main()
