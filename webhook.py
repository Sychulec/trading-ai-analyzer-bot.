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
    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    # Telegram ma limit długości wiadomości.
    for i in range(0, len(text), 4000):
        chunk = text[i:i + 4000]

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

    strategy_entry = extract_number(
        r"Cena:\s*([0-9.,]+)",
        text,
    )

    strategy_tp = extract_number(
        r"TP:\s*([0-9.,]+)",
        text,
    )

    strategy_sl = extract_number(
        r"SL:\s*([0-9.,]+)",
        text,
    )

    return {
        "event": event,
        "side": side,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_entry": strategy_entry,
        "strategy_tp": strategy_tp,
        "strategy_sl": strategy_sl,
        "raw": text,
    }


# =========================================================
# ANALIZA SETUPU H1
# =========================================================

def analyze_h1_setup(signal):
    try:
        market_results = build_market_analysis()

        market_data = format_market_data(
            market_results
        )

    except Exception as error:
        logger.exception(
            "Błąd pobierania danych rynkowych: %s",
            error,
        )

        send_telegram_message(
            "⚠️ XAUUSD\n"
            "Nie udało się pobrać danych rynkowych."
        )

        return

    prompt = f"""
SYGNAŁ BAZOWY Z TRADINGVIEW

Instrument: {signal["symbol"]}
Timeframe sygnału: {signal["timeframe"]}
Kierunek strategii: {signal["side"]}

Cena sygnału strategii:
{signal["strategy_entry"]}

SL strategii:
{signal["strategy_sl"]}

TP strategii:
{signal["strategy_tp"]}


AKTUALNE DANE RYNKOWE:

{market_data}


ZADANIE

Sygnał H1 z TradingView jest tylko sygnałem bazowym.

Przeanalizuj aktualny rynek samodzielnie.

H1 traktuj jako główny interwał kierunkowy.

15m, 5m i 1m wykorzystaj do oceny jakości setupu
oraz znalezienia możliwie sensownego momentu wejścia.

Nie musisz kopiować ceny wejścia, SL ani TP
podanych przez TradingView.

Na podstawie aktualnych danych zaproponuj własny,
technicznie uzasadniony plan albo odrzuć setup.
"""

    instructions = """
Odpowiadaj po polsku.

Jesteś Trading AI Analyzer.

Analizujesz sygnał XAUUSD z H1.

TradingView daje jedynie kierunek bazowy LONG albo SHORT.
Nie traktuj ceny ENTRY, SL i TP strategii jako obowiązkowych.

Najważniejszy jest interwał H1.
Następnie sprawdź 15m, 5m i 1m w celu określenia
timingu wejścia.

Uwzględnij:
- aktualną cenę,
- EMA20,
- EMA50,
- RSI,
- MACD,
- histogram MACD,
- orientacyjne wsparcia i opory,
- zgodność wielu interwałów.

Masz wybrać dokładnie jedną decyzję:

✅ WEJŚCIE
gdy aktualne warunki wystarczająco potwierdzają setup.

⏳ CZEKAJ
gdy kierunek H1 ma sens, ale obecna cena lub niższe
interwały nie dają jeszcze dobrego wejścia.

❌ POMIŃ
gdy aktualne dane wyraźnie przeczą sygnałowi H1
lub nie ma sensownego stosunku ryzyka do potencjalnego zysku.

Jeśli wybierasz WEJŚCIE lub CZEKAJ:
- podaj własną cenę wejścia albo wąską strefę wejścia,
- podaj techniczny STOP LOSS,
- podaj TP1,
- podaj TP2,
- podaj TP3.

SL powinien znajdować się za logicznym poziomem
unieważnienia setupu, a nie w przypadkowej odległości.

TP1, TP2 i TP3 powinny wynikać z aktualnych danych,
wsparć/oporów i rozsądnego risk/reward.

Nie wymyślaj danych, których nie otrzymałeś.

Jeżeli dane nie pozwalają wiarygodnie ustalić poziomu,
napisz CZEKAJ albo POMIŃ zamiast zgadywać.

Nie pisz długiej analizy.

Odpowiedź ma być krótka i czytelna na telefonie.

Użyj DOKŁADNIE tego układu:

🟢 XAUUSD LONG — H1
albo
🔴 XAUUSD SHORT — H1

Cena teraz: [cena]

Decyzja: [✅ WEJŚCIE / ⏳ CZEKAJ / ❌ POMIŃ]

Wejście AI: [cena lub strefa]
SL: [cena]
TP1: [cena]
TP2: [cena]
TP3: [cena]

1m: [✅ / ⚠️ / ❌]
5m: [✅ / ⚠️ / ❌]
15m: [✅ / ⚠️ / ❌]
1h: [✅ / ⚠️ / ❌]

Warunek wejścia: [jedno krótkie zdanie]
Unieważnienie: [jedno krótkie zdanie]

Jeżeli decyzja to POMIŃ, nie wymuszaj sztucznych
poziomów wejścia i TP. Możesz wpisać "-".

Nie dodawaj wielostronicowego komentarza.
Nie gwarantuj zysku.
"""

    try:
        response = client.responses.create(
            model="gpt-5-mini",
            instructions=instructions,
            input=prompt,
        )

        answer = (
            response.output_text
            or "AI nie zwróciło analizy."
        )

        send_telegram_message(
            answer
        )

    except Exception as error:
        logger.exception(
            "Błąd OpenAI: %s",
            error,
        )

        send_telegram_message(
            "⚠️ XAUUSD\n"
            "Wystąpił błąd podczas analizy AI."
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

    # Ignorujemy TP, SL i wszystkie inne alerty.
    # Analiza uruchamia się tylko dla nowego wejścia.

    if signal["event"] != "ENTRY":
        logger.info(
            "Alert pominięty: %s",
            signal["event"],
        )
        return

    # Tylko XAUUSD.
    if signal["symbol"] != "XAUUSD":
        logger.info(
            "Instrument pominięty: %s",
            signal["symbol"],
        )
        return

    # Docelowo pracujemy na sygnale H1.
    # Akceptujemy też "60", bo TradingView/Pine
    # może oznaczać godzinę jako 60 minut.

    timeframe = str(
        signal["timeframe"]
    ).lower()

    if timeframe not in (
        "1h",
        "60",
        "60m",
    ):
        logger.info(
            "Pominięto sygnał spoza H1: %s",
            timeframe,
        )
        return

    analyze_h1_setup(
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
            "service": (
                "Trading AI Analyzer Webhook"
            ),
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

            if isinstance(
                data,
                dict,
            ):
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
