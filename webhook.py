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

from bot import (
    build_market_analysis,
    format_market_data,
    validate_market_data,
)


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
# USTAWIENIA
# =========================================================

# Co ile sekund ponownie sprawdzamy setup.
# 300 sekund = 5 minut.
MONITOR_INTERVAL_SECONDS = int(
    os.getenv(
        "MONITOR_INTERVAL_SECONDS",
        "300",
    )
)

# Maksymalnie 12 sprawdzeń.
# 12 x 5 minut = około 1 godzina.
MONITOR_MAX_CHECKS = int(
    os.getenv(
        "MONITOR_MAX_CHECKS",
        "12",
    )
)

# Maksymalna różnica między ceną alertu TradingView
# i aktualną ceną Twelve Data przy PIERWSZEJ analizie.
# 0.15 = 0,15%
MAX_PRICE_DIFF_PERCENT = float(
    os.getenv(
        "MAX_PRICE_DIFF_PERCENT",
        "0.15",
    )
)


# =========================================================
# AKTYWNE MONITORY
# =========================================================

monitor_lock = threading.Lock()

active_monitors = {}


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram_message(text):
    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

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

    # -----------------------------------------------------
    # TYP ALERTU
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # SYMBOL
    # -----------------------------------------------------

    if "XAUUSD" in upper:
        symbol = "XAUUSD"

    elif "US100" in upper:
        symbol = "US100"

    else:
        symbol = "UNKNOWN"

    # -----------------------------------------------------
    # TIMEFRAME
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # POZIOMY STRATEGII
    # -----------------------------------------------------

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
# KONTROLA DANYCH + ANALIZA AI
# =========================================================

def analyze_h1_setup(
    signal,
    monitoring=False,
):
    # -----------------------------------------------------
    # POBIERANIE DANYCH
    # -----------------------------------------------------

    try:
        market_results = build_market_analysis(
            "XAUUSD"
        )

        quality = validate_market_data(
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
                "⚠️ XAUUSD\n\n"
                "Nie udało się pobrać "
                "danych rynkowych."
            ),
        }

    # -----------------------------------------------------
    # KONTROLA JAKOŚCI DANYCH
    # -----------------------------------------------------

    if not quality["ok"]:
        problems = "\n".join(
            f"• {problem}"
            for problem in quality[
                "problems"
            ]
        )

        logger.warning(
            "Dane nie przeszły kontroli: %s",
            quality["problems"],
        )

        return {
            "decision": "WAIT",
            "message": (
                "⚠️ KONTROLA DANYCH\n\n"
                f"{problems}\n\n"
                "⏳ CZEKAJ — "
                "nie potwierdzam wejścia, "
                "dopóki dane nie będą poprawne."
            ),
        }

    current_price = quality[
        "current_price"
    ]

    # -----------------------------------------------------
    # PORÓWNANIE TRADINGVIEW ↔ TWELVE DATA
    #
    # Robimy je TYLKO przy pierwszej analizie.
    # Podczas monitorowania cena ma prawo oddalić się
    # od pierwotnego sygnału.
    # -----------------------------------------------------

    strategy_price = signal.get(
        "strategy_entry"
    )

    if (
        not monitoring
        and strategy_price
        and current_price
    ):
        difference_percent = (
            abs(
                current_price
                - strategy_price
            )
            / strategy_price
            * 100
        )

        logger.info(
            "Porównanie cen: TradingView=%.4f "
            "TwelveData=%.4f różnica=%.4f%%",
            strategy_price,
            current_price,
            difference_percent,
        )

        if (
            difference_percent
            > MAX_PRICE_DIFF_PERCENT
        ):
            return {
                "decision": "WAIT",
                "message": (
                    "⚠️ RÓŻNICA CEN\n\n"
                    f"TradingView: "
                    f"{strategy_price:.2f}\n"
                    f"Twelve Data: "
                    f"{current_price:.2f}\n"
                    f"Różnica: "
                    f"{difference_percent:.3f}%\n\n"
                    "⏳ CZEKAJ — "
                    "ceny wymagają ponownego "
                    "sprawdzenia."
                ),
            }

    # -----------------------------------------------------
    # FORMAT DANYCH DLA AI
    # -----------------------------------------------------

    market_data = format_market_data(
        market_results
    )

    if monitoring:
        mode = (
            "To jest PONOWNA analiza "
            "wcześniej aktywnego setupu. "
            "Sprawdź, czy warunki do wejścia "
            "są dobre TERAZ."
        )

    else:
        mode = (
            "To jest PIERWSZA analiza "
            "nowego sygnału H1."
        )

    prompt = f"""
{mode}

SYGNAŁ Z TRADINGVIEW

Instrument:
{signal["symbol"]}

Timeframe:
{signal["timeframe"]}

Kierunek strategii:
{signal["side"]}

Cena strategii:
{signal["strategy_entry"]}

SL strategii:
{signal["strategy_sl"]}

TP strategii:
{signal["strategy_tp"]}


AKTUALNA CENA TWELVE DATA:

{current_price}


AKTUALNE DANE TECHNICZNE:

{market_data}


ZADANIE:

H1 jest głównym kierunkiem.

15m służy do oceny struktury.

5m służy do oceny momentum.

1m służy do timingu wejścia.

TradingView daje tylko kierunek bazowy.

Sam oceń, czy wejście ma sens teraz.

Nie musisz kopiować ceny wejścia,
SL ani TP strategii.

Jeżeli setup jest dobry,
zaproponuj własne poziomy.

Jeżeli jeszcze za wcześnie,
wybierz CZEKAJ.

Jeżeli setup się zepsuł,
wybierz POMIN.
"""

    instructions = """
Odpowiadaj po polsku.

Jesteś Trading AI Analyzer.

Analizujesz XAUUSD.

Masz aktualne dane:
1m,
5m,
15m,
1h.

1h = główny kierunek.
15m = struktura.
5m = momentum.
1m = timing.

Uwzględniaj:
EMA20,
EMA50,
RSI,
MACD,
histogram MACD,
wsparcie,
opór,
aktualną cenę.

TradingView daje sygnał bazowy
LONG albo SHORT.

Nie kopiuj bezmyślnie poziomów
ze strategii TradingView.

Wybierz dokładnie jedną decyzję:

WEJSCIE

gdy warunki wystarczająco
potwierdzają wejście teraz.

CZEKAJ

gdy setup nadal ma sens,
ale timing nie jest jeszcze dobry.

POMIN

gdy setup stracił sens,
interwały wyraźnie przeczą kierunkowi
albo risk/reward jest niekorzystny.

Jeżeli decyzja to WEJSCIE albo CZEKAJ:

podaj:
- własną cenę lub wąską strefę wejścia,
- SL,
- TP1,
- TP2,
- TP3.

SL powinien znajdować się
za logicznym poziomem unieważnienia.

TP powinny wynikać
ze wsparć, oporów i struktury rynku.

Nie wymyślaj danych.

Jeżeli układ jest niejasny,
wybierz CZEKAJ.


PIERWSZA LINIA MUSI BYĆ:

DECYZJA=WEJSCIE

albo:

DECYZJA=CZEKAJ

albo:

DECYZJA=POMIN


Dalej użyj formatu:

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

Warunek wejścia:
[jedno krótkie zdanie]

Unieważnienie:
[jedno krótkie zdanie]

Nie pisz długiego komentarza.
Nie gwarantuj zysku.
"""

    # -----------------------------------------------------
    # OPENAI
    # -----------------------------------------------------

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
                "Wystąpił błąd "
                "podczas analizy AI."
            ),
        }

    # -----------------------------------------------------
    # ODCZYT DECYZJI
    # -----------------------------------------------------

    upper = answer.upper()

    if "DECYZJA=WEJSCIE" in upper:
        decision = "ENTRY"

    elif "DECYZJA=CZEKAJ" in upper:
        decision = "WAIT"

    elif "DECYZJA=POMIN" in upper:
        decision = "SKIP"

    else:
        decision = "UNKNOWN"

    # Usuwamy techniczną linię DECYZJA=...
    display_answer = re.sub(
        r"^\s*DECYZJA\s*=\s*"
        r"[A-ZĄĆĘŁŃÓŚŹŻ]+\s*",
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
    symbol = signal[
        "symbol"
    ]

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

        # -------------------------------------------------
        # CZY MONITOR NADAL JEST AKTYWNY
        # -------------------------------------------------

        with monitor_lock:
            active = active_monitors.get(
                symbol
            )

            if (
                not active
                or active.get("id")
                != monitor_id
            ):
                logger.info(
                    "Monitor %s "
                    "został zastąpiony.",
                    monitor_id,
                )
                return

        logger.info(
            "Ponowna analiza %s/%s | %s",
            check_number,
            MONITOR_MAX_CHECKS,
            symbol,
        )

        # -------------------------------------------------
        # PONOWNA ANALIZA
        # -------------------------------------------------

        result = analyze_h1_setup(
            signal,
            monitoring=True,
        )

        decision = result[
            "decision"
        ]

        # -------------------------------------------------
        # WEJŚCIE
        # -------------------------------------------------

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
                    and current.get("id")
                    == monitor_id
                ):
                    active_monitors.pop(
                        symbol,
                        None,
                    )

            logger.info(
                "Setup potwierdzony: %s",
                monitor_id,
            )

            return

        # -------------------------------------------------
        # SETUP ANULOWANY
        # -------------------------------------------------

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
                    and current.get("id")
                    == monitor_id
                ):
                    active_monitors.pop(
                        symbol,
                        None,
                    )

            logger.info(
                "Setup anulowany: %s",
                monitor_id,
            )

            return

        # WAIT / ERROR / UNKNOWN
        # nic nie wysyłamy.
        # Dalej obserwujemy.

    # -----------------------------------------------------
    # KONIEC CZASU OBSERWACJI
    # -----------------------------------------------------

    with monitor_lock:
        current = active_monitors.get(
            symbol
        )

        if (
            current
            and current.get("id")
            == monitor_id
        ):
            active_monitors.pop(
                symbol,
                None,
            )

    send_telegram_message(
        "⌛ XAUUSD — OBSERWACJA ZAKOŃCZONA\n\n"
        "Setup nie uzyskał wystarczającego "
        "potwierdzenia w czasie obserwacji."
    )


# =========================================================
# OBSŁUGA ALERTU TRADINGVIEW
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

    # -----------------------------------------------------
    # IGNORUJEMY TP / SL / INNE ALERTY
    # -----------------------------------------------------

    if signal["event"] != "ENTRY":
        logger.info(
            "Alert pominięty: %s",
            signal["event"],
        )
        return

    # -----------------------------------------------------
    # TYLKO XAUUSD
    # -----------------------------------------------------

    if signal["symbol"] != "XAUUSD":
        logger.info(
            "Instrument pominięty: %s",
            signal["symbol"],
        )
        return

    # -----------------------------------------------------
    # TYLKO H1
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # INFORMACJA, ŻE STRATEGIA DAŁA SYGNAŁ
    # -----------------------------------------------------

    side_icon = (
        "🟢"
        if signal["side"] == "LONG"
        else "🔴"
    )

    send_telegram_message(
        f"📡 STRATEGIA H1 — "
        f"{signal['side']}\n\n"
        f"{side_icon} XAUUSD "
        f"{signal['side']}\n"
        f"Cena sygnału: "
        f"{signal['strategy_entry']}\n\n"
        "🤖 Sprawdzam zgodność ceny "
        "TradingView z Twelve Data "
        "oraz 1h / 15m / 5m / 1m."
    )

    # -----------------------------------------------------
    # PIERWSZA ANALIZA
    # -----------------------------------------------------

    result = analyze_h1_setup(
        signal,
        monitoring=False,
    )

    decision = result[
        "decision"
    ]

    # -----------------------------------------------------
    # WEJŚCIE OD RAZU
    # -----------------------------------------------------

    if decision == "ENTRY":
        send_telegram_message(
            "🚨 WEJŚCIE POTWIERDZONE\n\n"
            + result["message"]
        )

        with monitor_lock:
            active_monitors.pop(
                signal["symbol"],
                None,
            )

        return

    # -----------------------------------------------------
    # SETUP ODRZUCONY
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # CZEKAJ
    # -----------------------------------------------------

    if decision == "WAIT":
        send_telegram_message(
            "⏳ AI CZEKA — "
            "SETUP OBSERWOWANY\n\n"
            + result["message"]
            + "\n\n"
            "🔄 Bot sprawdzi rynek "
            "ponownie co 5 minut."
        )

        monitor_id = str(
            uuid.uuid4()
        )

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
            "Uruchomiono monitor %s",
            monitor_id,
        )

        return

    # -----------------------------------------------------
    # BŁĄD
    # -----------------------------------------------------

    if decision == "ERROR":
        send_telegram_message(
            result["message"]
        )
        return

    # -----------------------------------------------------
    # NIEJEDNOZNACZNA DECYZJA
    # -----------------------------------------------------

    send_telegram_message(
        "⚠️ AI NIE ROZPOZNAŁO "
        "JEDNOZNACZNEJ DECYZJI\n\n"
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
        monitors = dict(
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
            "max_price_diff_percent": (
                MAX_PRICE_DIFF_PERCENT
            ),
            "active_monitors": monitors,
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
    # -----------------------------------------------------
    # SECRET
    # -----------------------------------------------------

    secret = request.args.get(
        "secret"
    )

    if secret != WEBHOOK_SECRET:
        return jsonify(
            {
                "error": "invalid secret",
            }
        ), 403

    # -----------------------------------------------------
    # ODCZYT WIADOMOŚCI
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # ANALIZA W TLE
    # -----------------------------------------------------

    thread = threading.Thread(
        target=process_alert,
        args=(text,),
        daemon=True,
    )

    thread.start()

    # TradingView dostaje odpowiedź natychmiast.
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
