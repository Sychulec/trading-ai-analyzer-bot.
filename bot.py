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


# =========================================================
# LOGOWANIE
# =========================================================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# =========================================================
# ENVIRONMENT VARIABLES
# =========================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")


if not TELEGRAM_TOKEN:
    raise RuntimeError("Brak TELEGRAM_TOKEN w Render.")

if not OPENAI_API_KEY:
    raise RuntimeError("Brak OPENAI_API_KEY w Render.")

if not TWELVE_DATA_API_KEY:
    raise RuntimeError("Brak TWELVE_DATA_API_KEY w Render.")


client = AsyncOpenAI(
    api_key=OPENAI_API_KEY
)


# =========================================================
# OBSŁUGIWANE INSTRUMENTY
# =========================================================

SYMBOLS = {
    "XAUUSD": {
        "api_symbol": "XAU/USD",
        "name": "XAUUSD",
    },

    "EURUSD": {
        "api_symbol": "EUR/USD",
        "name": "EURUSD",
    },
}


def detect_symbol(text):
    cleaned = (
        text.upper()
        .replace("/", "")
        .replace(" ", "")
    )

    for alias in SYMBOLS:
        if alias in cleaned:
            return alias

    return None


# =========================================================
# WYSZUKIWANIE SYMBOLI TWELVE DATA
# =========================================================

def search_twelve_data_symbol(query):
    params = urllib.parse.urlencode(
        {
            "symbol": query,
            "apikey": TWELVE_DATA_API_KEY,
        }
    )

    url = (
        "https://api.twelvedata.com/"
        f"symbol_search?{params}"
    )

    try:
        with urllib.request.urlopen(
            url,
            timeout=15,
        ) as response:
            data = json.loads(
                response.read().decode("utf-8")
            )

        results = data.get(
            "data",
            []
        )

        if not results:
            return "Brak wyników."

        lines = []

        for item in results[:10]:
            symbol = item.get(
                "symbol",
                "?"
            )

            instrument_name = (
                item.get("instrument_name")
                or item.get("name")
                or "?"
            )

            instrument_type = (
                item.get("instrument_type")
                or item.get("type")
                or "?"
            )

            exchange = item.get(
                "exchange",
                "?"
            )

            country = item.get(
                "country",
                "?"
            )

            currency = item.get(
                "currency",
                "?"
            )

            lines.append(
                f"Symbol: {symbol}\n"
                f"Nazwa: {instrument_name}\n"
                f"Typ: {instrument_type}\n"
                f"Giełda: {exchange}\n"
                f"Kraj: {country}\n"
                f"Waluta: {currency}"
            )

        return (
            "\n\n----------------\n\n"
        ).join(lines)

    except Exception as error:
        logger.exception(
            "Błąd symbol_search: %s",
            error,
        )

        return (
            f"Błąd wyszukiwania: {error}"
        )


# =========================================================
# TEST US100 / NASDAQ-100
# =========================================================

def test_us100_candidates():
    candidates = [
        "NDX",
        "NASDAQ100",
        "NASDAQ-100",
        "NQX",
        "US100",
    ]

    results = []

    for symbol in candidates:
        params = urllib.parse.urlencode(
            {
                "symbol": symbol,
                "interval": "1min",
                "outputsize": 5,
                "apikey": TWELVE_DATA_API_KEY,
            }
        )

        url = (
            "https://api.twelvedata.com/"
            f"time_series?{params}"
        )

        try:
            with urllib.request.urlopen(
                url,
                timeout=15,
            ) as response:
                data = json.loads(
                    response.read().decode("utf-8")
                )

            if (
                "values" in data
                and data["values"]
            ):
                latest = data[
                    "values"
                ][0]

                results.append(
                    f"✅ {symbol}\n"
                    f"Cena: {latest.get('close')}\n"
                    f"Czas: {latest.get('datetime')}"
                )

            else:
                message = data.get(
                    "message",
                    "brak danych",
                )

                results.append(
                    f"❌ {symbol}\n"
                    f"{message}"
                )

        except Exception as error:
            results.append(
                f"❌ {symbol}\n"
                f"Błąd: {error}"
            )

    return (
        "\n\n----------------\n\n"
    ).join(results)


# =========================================================
# TWELVE DATA - ŚWIECE
# =========================================================

def fetch_candles(
    symbol,
    interval,
    outputsize=120,
):
    api_symbol = SYMBOLS[
        symbol
    ]["api_symbol"]

    params = urllib.parse.urlencode(
        {
            "symbol": api_symbol,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": TWELVE_DATA_API_KEY,
        }
    )

    url = (
        "https://api.twelvedata.com/"
        f"time_series?{params}"
    )

    try:
        with urllib.request.urlopen(
            url,
            timeout=15,
        ) as response:
            data = json.loads(
                response.read().decode("utf-8")
            )

        if "values" not in data:
            logger.error(
                "Twelve Data %s %s: %s",
                symbol,
                interval,
                data,
            )

            return None

        candles = []

        for item in reversed(
            data["values"]
        ):
            candles.append(
                {
                    "datetime": item[
                        "datetime"
                    ],
                    "open": float(
                        item["open"]
                    ),
                    "high": float(
                        item["high"]
                    ),
                    "low": float(
                        item["low"]
                    ),
                    "close": float(
                        item["close"]
                    ),
                }
            )

        return candles

    except Exception as error:
        logger.exception(
            "Błąd Twelve Data %s %s: %s",
            symbol,
            interval,
            error,
        )

        return None


# =========================================================
# EMA
# =========================================================

def ema_series(
    values,
    period,
):
    if not values:
        return []

    multiplier = (
        2 / (period + 1)
    )

    result = [
        values[0]
    ]

    for value in values[1:]:
        new_ema = (
            value * multiplier
            + result[-1]
            * (1 - multiplier)
        )

        result.append(
            new_ema
        )

    return result


# =========================================================
# RSI
# =========================================================

def calculate_rsi(
    closes,
    period=14,
):
    if len(closes) <= period:
        return None

    gains = []
    losses = []

    for i in range(
        1,
        len(closes),
    ):
        change = (
            closes[i]
            - closes[i - 1]
        )

        gains.append(
            max(change, 0)
        )

        losses.append(
            max(-change, 0)
        )

    avg_gain = (
        sum(
            gains[:period]
        )
        / period
    )

    avg_loss = (
        sum(
            losses[:period]
        )
        / period
    )

    for i in range(
        period,
        len(gains),
    ):
        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = (
        avg_gain
        / avg_loss
    )

    return (
        100
        - (
            100
            / (1 + rs)
        )
    )


# =========================================================
# MACD
# =========================================================

def calculate_macd(
    closes,
):
    if len(closes) < 35:
        return (
            None,
            None,
            None,
        )

    ema12 = ema_series(
        closes,
        12,
    )

    ema26 = ema_series(
        closes,
        26,
    )

    macd_line = [
        a - b
        for a, b in zip(
            ema12,
            ema26,
        )
    ]

    signal_line = ema_series(
        macd_line,
        9,
    )

    macd = (
        macd_line[-1]
    )

    signal = (
        signal_line[-1]
    )

    histogram = (
        macd - signal
    )

    return (
        macd,
        signal,
        histogram,
    )


# =========================================================
# WSPARCIE / OPÓR
# =========================================================

def calculate_support_resistance(
    candles,
    lookback=30,
):
    if not candles:
        return (
            None,
            None,
        )

    recent = (
        candles[-lookback:]
    )

    support = min(
        candle["low"]
        for candle in recent
    )

    resistance = max(
        candle["high"]
        for candle in recent
    )

    return (
        support,
        resistance,
    )


# =========================================================
# ANALIZA INTERWAŁU
# =========================================================

def analyze_timeframe(
    symbol,
    interval,
):
    candles = fetch_candles(
        symbol,
        interval,
    )

    if (
        not candles
        or len(candles) < 55
    ):
        return {
            "interval": interval,
            "error": (
                "Brak wystarczających danych"
            ),
        }

    closes = [
        candle["close"]
        for candle in candles
    ]

    latest = (
        candles[-1]
    )

    ema20_values = ema_series(
        closes,
        20,
    )

    ema50_values = ema_series(
        closes,
        50,
    )

    ema20 = (
        ema20_values[-1]
    )

    ema50 = (
        ema50_values[-1]
    )

    rsi = calculate_rsi(
        closes
    )

    (
        macd,
        signal,
        histogram,
    ) = calculate_macd(
        closes
    )

    (
        support,
        resistance,
    ) = calculate_support_resistance(
        candles
    )

    if ema20 > ema50:
        trend = "wzrostowy"

    elif ema20 < ema50:
        trend = "spadkowy"

    else:
        trend = "neutralny"

    return {
        "interval": interval,
        "datetime": latest[
            "datetime"
        ],
        "price": latest[
            "close"
        ],
        "open": latest[
            "open"
        ],
        "high": latest[
            "high"
        ],
        "low": latest[
            "low"
        ],
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


# =========================================================
# ANALIZA WIELU INTERWAŁÓW
# =========================================================

def build_market_analysis(
    symbol="XAUUSD",
):
    intervals = [
        "1min",
        "5min",
        "15min",
        "1h",
    ]

    results = []

    for interval in intervals:
        results.append(
            analyze_timeframe(
                symbol,
                interval,
            )
        )

    return results


# =========================================================
# FORMAT DANYCH DLA AI
# =========================================================

def format_market_data(
    results,
):
    parts = []

    for data in results:
        interval = (
            data["interval"]
        )

        if "error" in data:
            parts.append(
                f"{interval}: "
                f"{data['error']}"
            )

            continue

        parts.append(
            f"""
INTERWAŁ {interval}

Czas:
{data['datetime']}

Cena:
{data['price']:.4f}

Open:
{data['open']:.4f}

High:
{data['high']:.4f}

Low:
{data['low']:.4f}

Trend EMA:
{data['trend']}

EMA20:
{data['ema20']:.4f}

EMA50:
{data['ema50']:.4f}

RSI:
{data['rsi']:.2f}

MACD:
{data['macd']:.4f}

Signal:
{data['signal']:.4f}

Histogram:
{data['histogram']:.4f}

Wsparcie:
{data['support']:.4f}

Opór:
{data['resistance']:.4f}
""".strip()
        )

    return (
        "\n\n"
        "----------------"
        "\n\n"
    ).join(parts)


# =========================================================
# /START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    await update.message.reply_text(
        "Cześć! 👋\n\n"
        "Jestem Trading AI Analyzer.\n\n"
        "Analiza:\n"
        "XAUUSD\n"
        "EURUSD\n\n"
        "Wyszukiwanie:\n"
        "SZUKAJ US100\n"
        "SZUKAJ NDX\n"
        "SZUKAJ Nasdaq 100\n\n"
        "Test Nasdaq/US100:\n"
        "TEST US100\n\n"
        "Analizuję 1m, 5m, 15m i 1h."
    )


# =========================================================
# /ID
# =========================================================

async def show_id(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if (
        not update.message
        or not update.effective_chat
    ):
        return

    await update.message.reply_text(
        "Twój TELEGRAM_CHAT_ID:\n"
        f"{update.effective_chat.id}"
    )


# =========================================================
# WIADOMOŚCI
# =========================================================

async def answer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if (
        not update.message
        or not update.message.text
    ):
        return

    text = (
        update.message.text.strip()
    )


    # =====================================================
    # TEST US100
    # =====================================================

    if text.upper() == "TEST US100":
        await update.message.chat.send_action(
            action="typing"
        )

        result = (
            await asyncio.to_thread(
                test_us100_candidates
            )
        )

        await update.message.reply_text(
            "🧪 TEST US100 / NASDAQ-100\n\n"
            + result
        )

        return


    # =====================================================
    # WYSZUKIWANIE SYMBOLU
    # =====================================================

    if text.upper().startswith(
        "SZUKAJ "
    ):
        query = (
            text[7:].strip()
        )

        if not query:
            await update.message.reply_text(
                "Podaj nazwę instrumentu.\n"
                "Np. SZUKAJ US100"
            )

            return

        await update.message.chat.send_action(
            action="typing"
        )

        result = (
            await asyncio.to_thread(
                search_twelve_data_symbol,
                query,
            )
        )

        await update.message.reply_text(
            f"🔎 Wyniki dla: {query}\n\n"
            f"{result}"
        )

        return


    # =====================================================
    # NORMALNA ANALIZA
    # =====================================================

    symbol = detect_symbol(
        text
    )

    if not symbol:
        await update.message.reply_text(
            "Nie rozpoznaję instrumentu.\n\n"
            "Aktualnie analizuję:\n"
            "XAUUSD\n"
            "EURUSD\n\n"
            "Do testu US100 wpisz:\n"
            "TEST US100"
        )

        return

    try:
        await update.message.chat.send_action(
            action="typing"
        )

        market_results = (
            await asyncio.to_thread(
                build_market_analysis,
                symbol,
            )
        )

        market_data = (
            format_market_data(
                market_results
            )
        )

        prompt = f"""
INSTRUMENT:
{symbol}

PYTANIE UŻYTKOWNIKA:
{text}

AKTUALNE DANE RYNKOWE:

{market_data}

Przeanalizuj aktualny rynek.

H1 traktuj jako główny kierunek.
15m służy do oceny struktury.
5m służy do oceny momentum.
1m służy do znalezienia timingu wejścia.

Jeżeli nie ma sensownego wejścia,
nie wymuszaj transakcji.
"""

        response = (
            await client.responses.create(
                model="gpt-5-mini",

                instructions=(
                    "Odpowiadaj po polsku. "

                    "Jesteś Trading AI Analyzer. "

                    "Analizuj instrument na "
                    "1m, 5m, 15m i 1h. "

                    "H1 traktuj jako główny kierunek. "
                    "15m jako strukturę rynku. "
                    "5m jako momentum. "
                    "1m jako timing wejścia. "

                    "Uwzględnij EMA20, EMA50, RSI, "
                    "MACD, histogram MACD, "
                    "wsparcie i opór. "

                    "Wybierz jedną decyzję: "
                    "LONG, SHORT albo CZEKAJ. "

                    "Jeżeli warunki są dobre, "
                    "podaj własną cenę lub "
                    "wąską strefę wejścia, "
                    "SL, TP1, TP2 i TP3. "

                    "Jeżeli rynek nie daje "
                    "dobrego setupu, napisz CZEKAJ. "

                    "Nie wymyślaj poziomów "
                    "tylko po to, żeby podać transakcję. "

                    "Odpowiedź ma być krótka "
                    "i czytelna na telefonie. "

                    "Użyj formatu:\n\n"

                    "📊 [INSTRUMENT]\n"
                    "Cena teraz: [cena]\n"
                    "Decyzja: "
                    "🟢 LONG / 🔴 SHORT / ⏳ CZEKAJ\n\n"

                    "Wejście AI: [cena/strefa lub -]\n"
                    "SL: [cena lub -]\n"
                    "TP1: [cena lub -]\n"
                    "TP2: [cena lub -]\n"
                    "TP3: [cena lub -]\n\n"

                    "1m: ✅/⚠️/❌\n"
                    "5m: ✅/⚠️/❌\n"
                    "15m: ✅/⚠️/❌\n"
                    "1h: ✅/⚠️/❌\n\n"

                    "Warunek wejścia: "
                    "jedno krótkie zdanie.\n"

                    "Unieważnienie: "
                    "jedno krótkie zdanie.\n\n"

                    "Nie gwarantuj zysku. "
                    "Nie przedstawiaj wyniku "
                    "jako pewnej transakcji."
                ),

                input=prompt,
            )
        )

        answer_text = (
            response.output_text
            or "AI nie zwróciło odpowiedzi."
        )

        for i in range(
            0,
            len(answer_text),
            4000,
        ):
            await update.message.reply_text(
                answer_text[
                    i:i + 4000
                ]
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


# =========================================================
# ERROR HANDLER
# =========================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    logger.error(
        "Błąd Telegrama:",
        exc_info=context.error,
    )


# =========================================================
# START BOTA
# =========================================================

def main():
    logger.info(
        "Uruchamianie Trading AI Analyzer..."
    )

    application = (
        Application.builder()
        .token(
            TELEGRAM_TOKEN
        )
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        CommandHandler(
            "id",
            show_id,
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND,
            answer,
        )
    )

    application.add_error_handler(
        error_handler
    )

    logger.info(
        "Bot działa."
    )

    application.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    main()
