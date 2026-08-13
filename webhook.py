import os
import re
import json
import time
import uuid
import logging
import threading
import urllib.parse
import urllib.request

from flask import Flask, request, jsonify
from openai import OpenAI

from bot import build_market_analysis, format_market_data


# =========================================================
# LOGOWANIE
# =========================================================

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
# USTAWIENIA MONITOROWANIA
# =========================================================

# Co ile sekund ponownie sprawdzać setup.
# 300 sekund = 5 minut.
MONITOR_INTERVAL_SECONDS = int(
    os.getenv(
        "MONITOR_INTERVAL_SECONDS",
        "300",
    )
)

# Ile razy maksymalnie sprawdzić setup.
# 12 x 5 minut = około 1 godzina.
MONITOR_MAX_CHECKS = int(
    os.getenv(
        "MONITOR_MAX_CHECKS",
        "12",
    )
)


# =========================================================
# AKTYWNE SETUPY
# =========================================================

monitor_lock = threading.Lock()

# Przykład:
# {
#   "XAUUSD": {
#       "id": "...",
#       "side": "LONG",
#       "started": 1234567890
#   }
# }
active_monitors = {}


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram_message(text):
    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    # Telegram ma limit pojedynczej wiadomości.
    for i in range(
        0,
        len(text),
        4000,
    ):
        chunk = text[
            i:i + 4000
        ]

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

def extract_number(
    pattern,
    text,
):
    match = re.search(
        pattern,
        text,
        re.IGNORECASE,
    )

    if not match:
        return None

    try:
        return float(
            match.group(1).replace(
                ",",
                ".",
            )
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
# ANALIZA AI
# =========================================================

def analyze_h1_setup(
    signal,
    monitoring=False,
):
    try:
        # Jawnie analizujemy XAUUSD,
        # żeby przyszłe zmiany bot.py
        # nie zmieniły przypadkiem instrumentu.
        market_results = build_market_analysis(
            "XAUUSD"
        )

        market_data = format_market_data(
            market_results
        )

    except Exception as error:
        logger.exception(
            "Błąd pobierania danych rynkowych: %s",
            error,
        )

        return {
            "decision": "ERROR",
            "message": (
                "⚠️ XAUUSD\n"
                "Nie udało się pobrać danych rynkowych."
            ),
        }


    if monitoring:
        analysis_mode = (
            "To jest PONOWNA analiza wcześniej "
            "aktywnego setupu. Sprawdź, czy warunki "
            "do wejścia pojawiły się TERAZ."
        )

    else:
        analysis_mode = (
            "To jest PIERWSZA analiza nowego "
            "sygnału H1 z TradingView."
        )


    prompt = f"""
{analysis_mode}

SYGNAŁ BAZOWY Z TRADINGVIEW

Instrument:
{signal["symbol"]}

Timeframe:
{signal["timeframe"]}

Kierunek strategii:
{signal["side"]}

Cena sygnału strategii:
{signal["strategy_entry"]}

SL strategii:
{signal["strategy_sl"]}

TP strategii:
{signal["strategy_tp"]}


AKTUALNE DANE RYNKOWE:

{market_data}


ZADANIE:

H1 jest głównym interwałem kierunkowym.

15m służy do oceny struktury.

5m służy do oceny momentum.

1m służy do określenia timingu wejścia.

TradingView daje tylko sygnał bazowy.

Samodzielnie oceń, czy setup ma sens TERAZ.

Nie musisz kopiować ceny wejścia,
SL ani TP strategii TradingView.

Jeżeli setup ma sens,
zaproponuj własne poziomy.

Jeżeli jeszcze za wcześnie,
wybierz CZEKAJ.

Jeżeli setup stracił sens,
wybierz POMIN.
"""


    instructions = """
Odpowiadaj po polsku.

Jesteś Trading AI Analyzer.

Analizujesz XAUUSD.

TradingView daje sygnał bazowy LONG albo SHORT,
ale ostateczną ocenę robisz na podstawie
aktualnych danych rynkowych.

Analizuj:

1h = główny kierunek
15m = struktura
5m = momentum
1m = timing

Uwzględniaj:

- aktualną cenę
- EMA20
- EMA50
- RSI
- MACD
- histogram MACD
- wsparcie
- opór
- zgodność interwałów


Wybierz DOKŁADNIE jedną decyzję:

WEJSCIE

gdy aktualne warunki wystarczająco
potwierdzają setup i wejście ma sens teraz.

CZEKAJ

gdy kierunek H1 nadal ma sens,
ale cena albo niższe interwały
nie dają jeszcze dobrego wejścia.

POMIN

gdy setup przestał mieć sens,
rynek wyraźnie przeczy kierunkowi
albo nie ma sensownego risk/reward.


Jeżeli wybierasz WEJSCIE lub CZEKAJ:

- podaj własną cenę albo wąską strefę wejścia
- podaj SL
- podaj TP1
- podaj TP2
- podaj TP3

SL powinien być za logicznym poziomem
unieważnienia setupu.

TP powinny wynikać z danych,
wsparć, oporów i rozsądnego R:R.

Nie wymyślaj danych.

Jeżeli setup jest niejasny,
wybierz CZEKAJ zamiast zgadywać.


PIERWSZA LINIA ODPOWIEDZI MUSI BYĆ:

DECYZJA=WEJSCIE

albo:

DECYZJA=CZEKAJ

albo:

DECYZJA=POMIN


Dalej użyj dokładnie takiego układu:

🟢 XAUUSD LONG — H1
albo
🔴 XAUUSD SHORT — H1

Cena teraz: [cena]

Decyzja: [✅ WEJŚCIE / ⏳ CZEKAJ / ❌ POMIŃ]

Wejście AI: [cena / strefa / -]
SL: [cena / -]
TP1: [cena / -]
TP2: [cena / -]
TP3: [cena / -]

1m: [✅ / ⚠️ / ❌]
5m: [✅ / ⚠️ / ❌]
15m: [✅ / ⚠️ / ❌]
1h: [✅ / ⚠️ / ❌]

Warunek wejścia: [jedno krótkie zdanie]

Unieważnienie: [jedno krótkie zdanie]

Nie pisz długiego komentarza.

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

    except Exception as error:
        logger.exception(
            "Błąd OpenAI: %s",
            error,
        )

        return {
            "decision": "ERROR",
            "message": (
                "⚠️ XAUUSD\n"
                "Wystąpił błąd podczas analizy AI."
            ),
        }


    upper = answer.upper()


    if "DECYZJA=WEJSCIE" in upper:
        decision = "ENTRY"

    elif "DECYZJA=CZEKAJ" in upper:
        decision = "WAIT"

    elif "DECYZJA=POMIN" in upper:
        decision = "SKIP"

    else:
        decision = "UNKNOWN"


    # Usuwamy linię techniczną DECYZJA=...
    # zanim wyślemy tekst na Telegram.
    display_answer = re.sub(
        r"^\s*DECYZJA\s*=\s*[A-ZĄĆĘŁŃÓŚŹŻ]+\s*",
        "",
        answer,
        count=1,
        flags=re.IGNORECASE,
    ).strip()


    return {
        "decision": decision,
        "message": display_answer,
    }


# =========================================================
# MONITOROWANIE SETUPU
# =========================================================

def monitor_setup(
    signal,
    monitor_id,
):
    symbol = signal["symbol"]

    logger.info(
        "Start monitorowania %s | ID=%s",
        symbol,
        monitor_id,
    )


    for check_number in range(
        1,
        MONITOR_MAX_CHECKS + 1,
    ):
        time.sleep(
            MONITOR_INTERVAL_SECONDS
        )


        # Sprawdzamy, czy ten monitor
        # nadal jest aktualny.
        with monitor_lock:
            active = active_monitors.get(
                symbol
            )

            if (
                not active
                or active.get("id") != monitor_id
            ):
                logger.info(
                    "Monitor %s został zastąpiony/anulowany.",
                    monitor_id,
                )
                return


        logger.info(
            "Ponowna analiza %s/%s | %s",
            check_number,
            MONITOR_MAX_CHECKS,
            symbol,
        )


        result = analyze_h1_setup(
            signal,
            monitoring=True,
        )

        decision = result[
            "decision"
        ]


        # =====================================
        # WEJŚCIE
        # =====================================

        if decision == "ENTRY":
            send_telegram_message(
                "🚨 WEJŚCIE POTWIERDZONE\n\n"
                + result["message"]
            )

            with monitor_lock:
                current = active_monitors.get(
                    symbol
                )

                if (
                    current
                    and current.get("id") == monitor_id
                ):
                    active_monitors.pop(
                        symbol,
                        None,
                    )

            logger.info(
                "Setup %s potwierdzony.",
                monitor_id,
            )

            return


        # =====================================
        # SETUP UNIEWAŻNIONY
        # =====================================

        if decision == "SKIP":
            send_telegram_message(
                "❌ SETUP UNIEWAŻNIONY\n\n"
                + result["message"]
            )

            with monitor_lock:
                current = active_monitors.get(
                    symbol
                )

                if (
                    current
                    and current.get("id") == monitor_id
                ):
                    active_monitors.pop(
                        symbol,
                        None,
                    )

            logger.info(
                "Setup %s anulowany.",
                monitor_id,
            )

            return


        # WAIT / UNKNOWN / ERROR:
        # nie wysyłamy wiadomości.
        # Dalej obserwujemy setup.


    # =========================================
    # KONIEC CZASU MONITOROWANIA
    # =========================================

    with monitor_lock:
        current = active_monitors.get(
            symbol
        )

        if (
            current
            and current.get("id") == monitor_id
        ):
            active_monitors.pop(
                symbol,
                None,
            )


    send_telegram_message(
        "⌛ XAUUSD — OBSERWACJA ZAKOŃCZONA\n\n"
        "Setup nie dał wystarczającego "
        "potwierdzenia w czasie obserwacji."
    )

    logger.info(
        "Monitor %s zakończony czasowo.",
        monitor_id,
    )


# =========================================================
# OBSŁUGA NOWEGO ALERTU
# =========================================================

def process_alert(text):
    logger.info(
        "ODEBRANO TRADINGVIEW:\n%s",
        text,
    )


    signal = parse_alert(
        text
    )


    logger.info(
        "Rozpoznany sygnał: %s",
        signal,
    )


    # =====================================================
    # IGNORUJEMY TP / SL / INNE
    # =====================================================

    if signal["event"] != "ENTRY":
        logger.info(
            "Alert pominięty: %s",
            signal["event"],
        )
        return


    # =====================================================
    # TYLKO XAUUSD
    # =====================================================

    if signal["symbol"] != "XAUUSD":
        logger.info(
            "Instrument pominięty: %s",
            signal["symbol"],
        )
        return


    # =====================================================
    # TYLKO H1
    # =====================================================

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


    # =====================================================
    # INFORMACJA: STRATEGIA WESZŁA
    # =====================================================

    side_icon = (
        "🟢"
        if signal["side"] == "LONG"
        else "🔴"
    )


    send_telegram_message(
        f"📡 STRATEGIA H1 — {signal['side']}\n\n"
        f"{side_icon} XAUUSD {signal['side']}\n"
        f"Cena sygnału: {signal['strategy_entry']}\n\n"
        "🤖 AI sprawdza teraz "
        "1h / 15m / 5m / 1m."
    )


    # =====================================================
    # PIERWSZA ANALIZA
    # =====================================================

    result = analyze_h1_setup(
        signal,
        monitoring=False,
    )

    decision = result[
        "decision"
    ]


    # =====================================================
    # WEJŚCIE OD RAZU
    # =====================================================

    if decision == "ENTRY":
        send_telegram_message(
            "🚨 WEJŚCIE POTWIERDZONE\n\n"
            + result["message"]
        )

        # Anulujemy wcześniejszy monitor,
        # jeśli jakiś istniał.
        with monitor_lock:
            active_monitors.pop(
                signal["symbol"],
                None,
            )

        return


    # =====================================================
    # POMIŃ
    # =====================================================

    if decision == "SKIP":
        send_telegram_message(
            "❌ SETUP ODRZUCONY\n\n"
            + result["message"]
        )

        with monitor_lock:
            active_monitors.pop(
                signal["symbol"],
                None,
            )

        return


    # =====================================================
    # CZEKAJ
    # =====================================================

    if decision == "WAIT":
        send_telegram_message(
            "⏳ AI CZEKA — SETUP OBSERWOWANY\n\n"
            + result["message"]
            + "\n\n"
            "🔄 Bot będzie ponownie "
            "sprawdzał rynek co 5 minut."
        )


        monitor_id = str(
            uuid.uuid4()
        )


        # Nowy setup XAUUSD zastępuje
        # poprzedni aktywny monitor.
        with monitor_lock:
            active_monitors[
                signal["symbol"]
            ] = {
                "id": monitor_id,
                "side": signal["side"],
                "started": time.time(),
            }


        monitor_thread = threading.Thread(
            target=monitor_setup,
            args=(
                signal,
                monitor_id,
            ),
            daemon=True,
        )

        monitor_thread.start()


        logger.info(
            "Uruchomiono monitor %s dla %s.",
            monitor_id,
            signal["side"],
        )

        return


    # =====================================================
    # NIEJEDNOZNACZNA ODPOWIEDŹ AI
    # =====================================================

    send_telegram_message(
        "⚠️ AI NIE ROZPOZNAŁO JEDNOZNACZNIE DECYZJI\n\n"
        + result["message"]
    )


# =========================================================
# WEB SERVER
# =========================================================

@app.route(
    "/",
    methods=["GET"],
)
def home():
    with monitor_lock:
        monitor_info = dict(
            active_monitors
        )

    return jsonify(
        {
            "status": "ok",
            "service": (
                "Trading AI Analyzer Webhook"
            ),
            "monitor_interval_seconds": (
                MONITOR_INTERVAL_SECONDS
            ),
            "monitor_max_checks": (
                MONITOR_MAX_CHECKS
            ),
            "active_monitors": monitor_info,
        }
    )


@app.route(
    "/health",
    methods=["GET"],
)
def health():
    with monitor_lock:
        monitors = list(
            active_monitors.keys()
        )

    return jsonify(
        {
            "status": "healthy",
            "active_monitors": monitors,
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


    # TradingView dostaje odpowiedź od razu,
    # a analiza działa w osobnym wątku.
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


# =========================================================
# LOCAL START
# =========================================================

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
