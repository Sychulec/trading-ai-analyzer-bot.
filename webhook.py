import os
import re
import json
import logging
import threading
import urllib.parse
import urllib.request

from flask import Flask, request, jsonify
from openai import OpenAI

from bot import build_market_analysis, format_market_data


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

app = Flask(__name__)


# =========================================================
# ENVIRONMENT VARIABLES
# =========================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")


if not TELEGRAM_TOKEN:
    raise RuntimeError("Brak TELEGRAM_TOKEN")

if not TELEGRAM_CHAT_ID:
    raise RuntimeError("Brak TELEGRAM_CHAT_ID")

if not OPENAI_API_KEY:
    raise RuntimeError("Brak OPENAI_API_KEY")

if not TWELVE_DATA_API_KEY:
    raise RuntimeError("Brak TWELVE_DATA_API_KEY")

if not WEBHOOK_SECRET:
    raise RuntimeError("Brak WEBHOOK_SECRET")


client = OpenAI(
    api_key=OPENAI_API_KEY
)


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram_message(text):
    for i in range(0, len(text), 4000):
        chunk = text[i:i + 4000]

        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_TOKEN}/sendMessage"
        )

        data = urllib.parse.urlencode(
            {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
            }
        ).encode("utf-8")

        try:
            req = urllib.request.Request(
                url,
                data=data,
                method="POST",
            )

            with urllib.request.urlopen(
                req,
                timeout=15,
            ) as response:
                response.read()

        except Exception as error:
            logger.exception(
                "Błąd wysyłania Telegram: %s",
                error,
            )


# =========================================================
# PARSOWANIE ALERTU TRADINGVIEW
# =========================================================

def extract_number(pattern, text):
    match = re.search(
        pattern,
        text,
        re.IGNORECASE,
    )

    if not match:
        return None

    try:
        return float(
            match.group(1).replace(",", ".")
        )
    except ValueError:
        return None


def parse_alert(text):
    upper = text.upper()

    if "WEJŚCIE LONG" in upper:
        event = "ENTRY"
        side = "LONG"

    elif "WEJŚCIE SHORT" in upper:
        event = "ENTRY"
        side = "SHORT"

    elif "TAKE PROFIT" in upper:
        event = "TAKE_PROFIT"
        side = None

    elif "STOP LOSS" in upper:
        event = "STOP_LOSS"
        side = None

    else:
        event = "UNKNOWN"
        side = None

    if "XAUUSD" in upper:
        symbol = "XAUUSD"

    elif "US100" in upper:
        symbol = "US100"

    else:
        symbol = "UNKNOWN"

    tf_match = re.search(
        r"(?:XAUUSD|US100)\s+([A-Za-z0-9]+)\s*-",
        text,
        re.IGNORECASE,
    )

    timeframe = (
        tf_match.group(1)
        if tf_match
        else "?"
    )

    entry = extract_number(
        r"Cena:\s*([0-9.,]+)",
        text,
    )

    tp = extract_number(
        r"TP:\s*([0-9.,]+)",
        text,
    )

    sl = extract_number(
        r"SL:\s*([0-9.,]+)",
        text,
    )

    return {
        "event": event,
        "side": side,
        "symbol": symbol,
        "timeframe": timeframe,
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "raw": text,
    }


# =========================================================
# RISK / REWARD
# =========================================================

def calculate_rr(side, entry, sl, tp):
    if (
        entry is None
        or sl is None
        or tp is None
    ):
        return None

    if side == "LONG":
        risk = entry - sl
        reward = tp - entry

    elif side == "SHORT":
        risk = sl - entry
        reward = entry - tp

    else:
        return None

    if risk <= 0:
        return None

    return reward / risk


# =========================================================
# ANALIZA AI
# =========================================================

def analyze_xauusd_signal(signal):
    side = signal["side"]
    entry = signal["entry"]
    sl = signal["sl"]
    tp = signal["tp"]

    rr = calculate_rr(
        side,
        entry,
        sl,
        tp,
    )

    rr_text = (
        f"{rr:.2f}"
        if rr is not None
        else "brak"
    )

    try:
        market_results = build_market_analysis()

        market_data = format_market_data(
            market_results
        )

    except Exception as error:
        logger.exception(
            "Błąd danych rynkowych: %s",
            error,
        )
        return

    prompt = f"""
SYGNAŁ Z TRADINGVIEW

Instrument: {signal["symbol"]}
Timeframe sygnału: {signal["timeframe"]}
Kierunek: {side}

ENTRY: {entry}
STOP LOSS: {sl}
TAKE PROFIT: {tp}
RISK/REWARD: {rr_text}

AKTUALNE DANE RYNKOWE:

{market_data}

Oceń ten konkretny sygnał TradingView
na podstawie aktualnych danych rynkowych.
"""

    try:
        response = client.responses.create(
            model="gpt-5-mini",
            instructions=(
                "Odpowiadaj po polsku. "
                "Jesteś Trading AI Analyzer. "

                "Otrzymujesz gotowy sygnał "
                "ze strategii TradingView. "

                "Nie zmieniaj arbitralnie ENTRY, SL ani TP. "

                "Najpierw pokaż dokładnie: "
                "KIERUNEK, ENTRY, SL, TP i R:R. "

                "Następnie przeanalizuj osobno "
                "1m, 5m, 15m i 1h. "

                "Uwzględnij EMA20, EMA50, RSI i MACD. "

                "Porównaj kierunek strategii z aktualnym "
                "trendem i momentum. "

                "Zwróć uwagę, czy cena nie odjechała "
                "już za daleko od ENTRY. "

                "Na końcu podaj jedną ocenę:\n"
                "✅ SYGNAŁ POTWIERDZONY\n"
                "⚠️ SYGNAŁ MIESZANY\n"
                "❌ SYGNAŁ SŁABY\n"

                "Krótko uzasadnij ocenę. "
                "Nie gwarantuj zysku. "
                "Nie wymyślaj danych."
            ),
            input=prompt,
        )

        answer = (
            response.output_text
            or "OpenAI nie zwrócił analizy."
        )

        send_telegram_message(
            "🤖 ANALIZA AI SYGNAŁU\n\n"
            + answer
        )

    except Exception as error:
        logger.exception(
            "Błąd OpenAI: %s",
            error,
        )


# =========================================================
# OBSŁUGA ALERTU
# =========================================================

def process_alert(text):
    logger.info(
        "ODEBRANO TRADINGVIEW:\n%s",
        text,
    )

    signal = parse_alert(text)

    logger.info(
        "Rozpoznany sygnał: %s",
        signal,
    )

    # Reagujemy WYŁĄCZNIE na nowe wejście.
    # TP, SL i inne zamknięcia są ignorowane.
    if signal["event"] != "ENTRY":
        logger.info(
            "Alert pominięty: %s",
            signal["event"],
        )
        return

    # Na razie analizujemy tylko XAUUSD.
    if signal["symbol"] != "XAUUSD":
        logger.info(
            "Instrument pominięty: %s",
            signal["symbol"],
        )
        return

    analyze_xauusd_signal(
        signal
    )


# =========================================================
# WEB SERVER
# =========================================================

@app.route(
    "/",
    methods=["GET"],
)
def home():
    return jsonify(
        {
            "status": "ok",
            "service": "Trading AI Analyzer Webhook",
        }
    )


@app.route(
    "/health",
    methods=["GET"],
)
def health():
    return jsonify(
        {
            "status": "healthy",
        }
    )


@app.route(
    "/webhook",
    methods=["POST"],
)
def webhook():
    secret = request.args.get(
        "secret"
    )

    if secret != WEBHOOK_SECRET:
        return jsonify(
            {
                "error": "invalid secret",
            }
        ), 403

    try:
        if request.is_json:
            data = request.get_json(
                silent=True
            )

            if isinstance(data, dict):
                text = (
                    data.get("message")
                    or data.get("text")
                    or json.dumps(data)
                )

            else:
                text = str(data)

        else:
            text = request.get_data(
                as_text=True
            )

    except Exception as error:
        logger.exception(
            "Błąd odczytu webhooka: %s",
            error,
        )

        return jsonify(
            {
                "error": "invalid request",
            }
        ), 400

    if not text or not text.strip():
        return jsonify(
            {
                "error": "empty alert",
            }
        ), 400

    thread = threading.Thread(
        target=process_alert,
        args=(text,),
        daemon=True,
    )

    thread.start()

    return jsonify(
        {
            "status": "accepted",
        }
    ), 200


if __name__ == "__main__":
    port = int(
        os.getenv(
            "PORT",
            "10000",
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )
