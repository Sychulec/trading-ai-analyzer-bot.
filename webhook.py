import os
import re
import json
import time
import uuid
import logging
import threading
import urllib.parse
import urllib.request

try:
    import fcntl
except ImportError:
    fcntl = None

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
# ENV
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

# Monitoring sygnału strategii.
MONITOR_INTERVAL_SECONDS = int(
    os.getenv(
        "MONITOR_INTERVAL_SECONDS",
        "300",
    )
)

MONITOR_MAX_CHECKS = int(
    os.getenv(
        "MONITOR_MAX_CHECKS",
        "12",
    )
)

# Automatyczny skaner rynku.
AUTO_SCAN_ENABLED = (
    os.getenv(
        "AUTO_SCAN_ENABLED",
        "true",
    ).lower()
    == "true"
)

AUTO_SCAN_INTERVAL_SECONDS = int(
    os.getenv(
        "AUTO_SCAN_INTERVAL_SECONDS",
        "300",
    )
)

# Ile czasu nie powtarzać identycznego alertu.
AUTO_ALERT_COOLDOWN_SECONDS = int(
    os.getenv(
        "AUTO_ALERT_COOLDOWN_SECONDS",
        "1800",
    )
)

MAX_PRICE_DIFF_PERCENT = float(
    os.getenv(
        "MAX_PRICE_DIFF_PERCENT",
        "0.15",
    )
)


# =========================================================
# STAN
# =========================================================

monitor_lock = threading.Lock()
auto_lock = threading.Lock()

active_monitors = {}

auto_state = {
    "last_key": None,
    "last_sent_at": 0,
    "last_status": "NONE",
    "last_direction": "NONE",
}

scanner_started = False
scanner_start_lock = threading.Lock()

scanner_lock_file = None


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
                "Błąd Telegram: %s",
                error,
            )


# =========================================================
# ALERT TRADINGVIEW
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
        r"(?:XAUUSD|US100)\s+"
        r"([A-Za-z0-9]+)\s*-",
        text,
        re.IGNORECASE,
    )

    timeframe = (
        tf_match.group(1)
        if tf_match
        else "?"
    )


    return {
        "event": event,
        "side": side,
        "symbol": symbol,
        "timeframe": timeframe,

        "strategy_entry": extract_number(
            r"Cena:\s*([0-9.,]+)",
            text,
        ),

        "strategy_tp": extract_number(
            r"TP:\s*([0-9.,]+)",
            text,
        ),

        "strategy_sl": extract_number(
            r"SL:\s*([0-9.,]+)",
            text,
        ),

        "raw": text,
    }


# =========================================================
# WSKAŹNIKI DLA H4 / D1
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
        result.append(
            value * multiplier
            + result[-1]
            * (1 - multiplier)
        )

    return result


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
            max(
                change,
                0,
            )
        )

        losses.append(
            max(
                -change,
                0,
            )
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
            avg_gain
            * (period - 1)
            + gains[i]
        ) / period

        avg_loss = (
            avg_loss
            * (period - 1)
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

    macd = macd_line[-1]
    signal = signal_line[-1]

    return (
        macd,
        signal,
        macd - signal,
    )


# =========================================================
# TWELVE DATA H4 / D1
# =========================================================

def fetch_extra_timeframe(
    interval,
    outputsize=120,
):
    params = urllib.parse.urlencode(
        {
            "symbol": "XAU/USD",
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
                response.read().decode(
                    "utf-8"
                )
            )

        if "values" not in data:
            return {
                "interval": interval,
                "error": (
                    data.get(
                        "message",
                        "Brak danych",
                    )
                ),
            }

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

        if len(candles) < 55:
            return {
                "interval": interval,
                "error": (
                    "Za mało danych"
                ),
            }

        closes = [
            candle["close"]
            for candle in candles
        ]

        latest = candles[-1]

        ema20 = ema_series(
            closes,
            20,
        )[-1]

        ema50 = ema_series(
            closes,
            50,
        )[-1]

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

        recent = candles[-30:]

        support = min(
            candle["low"]
            for candle in recent
        )

        resistance = max(
            candle["high"]
            for candle in recent
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

    except Exception as error:
        logger.exception(
            "Błąd dodatkowego TF %s: %s",
            interval,
            error,
        )

        return {
            "interval": interval,
            "error": str(error),
        }


# =========================================================
# PEŁNY OBRAZ RYNKU
# =========================================================

def build_full_market_analysis():
    base = build_market_analysis(
        "XAUUSD"
    )

    h4 = fetch_extra_timeframe(
        "4h"
    )

    d1 = fetch_extra_timeframe(
        "1day"
    )

    return (
        base
        + [
            h4,
            d1,
        ]
    )


# =========================================================
# PROSTY PREFILTER
# =========================================================

def is_bullish(
    item,
):
    if (
        not item
        or "error" in item
    ):
        return False

    return (
        item["price"]
        > item["ema20"]
        and item["rsi"] >= 50
        and item["histogram"] > 0
    )


def is_bearish(
    item,
):
    if (
        not item
        or "error" in item
    ):
        return False

    return (
        item["price"]
        < item["ema20"]
        and item["rsi"] <= 50
        and item["histogram"] < 0
    )


def get_by_interval(
    results,
):
    return {
        item["interval"]: item
        for item in results
        if isinstance(
            item,
            dict,
        )
    }


def prefilter_market(
    results,
):
    by_tf = get_by_interval(
        results
    )

    h1 = by_tf.get(
        "1h"
    )

    m15 = by_tf.get(
        "15min"
    )

    m5 = by_tf.get(
        "5min"
    )

    m1 = by_tf.get(
        "1min"
    )

    if not all(
        [
            h1,
            m15,
            m5,
            m1,
        ]
    ):
        return {
            "candidate": False,
            "type": "NONE",
        }


    lower_bull = sum(
        [
            is_bullish(m15),
            is_bullish(m5),
            is_bullish(m1),
        ]
    )

    lower_bear = sum(
        [
            is_bearish(m15),
            is_bearish(m5),
            is_bearish(m1),
        ]
    )


    # Normalny setup zgodny z H1.
    if (
        h1["trend"]
        == "wzrostowy"
        and lower_bull >= 2
    ):
        return {
            "candidate": True,
            "type": "SETUP",
            "direction": "LONG",
        }


    if (
        h1["trend"]
        == "spadkowy"
        and lower_bear >= 2
    ):
        return {
            "candidate": True,
            "type": "SETUP",
            "direction": "SHORT",
        }


    # REVERSAL WATCH:
    # H1 nadal pokazuje stary trend,
    # ale niższe TF zaczynają grać przeciwnie.
    if (
        h1["trend"]
        == "spadkowy"
        and lower_bull >= 2
    ):
        return {
            "candidate": True,
            "type": "REVERSAL",
            "direction": "LONG",
        }


    if (
        h1["trend"]
        == "wzrostowy"
        and lower_bear >= 2
    ):
        return {
            "candidate": True,
            "type": "REVERSAL",
            "direction": "SHORT",
        }


    return {
        "candidate": False,
        "type": "NONE",
        "direction": "NONE",
    }


# =========================================================
# PARSOWANIE ODPOWIEDZI AI
# =========================================================

def parse_ai_meta(
    answer,
):
    upper = answer.upper()

    status = "NONE"
    direction = "NONE"
    score = 0


    status_match = re.search(
        r"STATUS\s*=\s*"
        r"(ENTRY|WAIT|SKIP|"
        r"REVERSAL|SETUP|NONE)",
        upper,
    )

    if status_match:
        status = (
            status_match.group(1)
        )


    direction_match = re.search(
        r"DIRECTION\s*=\s*"
        r"(LONG|SHORT|NONE)",
        upper,
    )

    if direction_match:
        direction = (
            direction_match.group(1)
        )


    score_match = re.search(
        r"SCORE\s*=\s*"
        r"([0-9]{1,3})",
        upper,
    )

    if score_match:
        try:
            score = int(
                score_match.group(1)
            )
        except ValueError:
            score = 0


    score = max(
        0,
        min(
            score,
            100,
        ),
    )


    display = re.sub(
        r"^\s*STATUS\s*=\s*"
        r"[A-Z]+\s*",
        "",
        answer,
        count=1,
        flags=re.IGNORECASE,
    )

    display = re.sub(
        r"^\s*DIRECTION\s*=\s*"
        r"[A-Z]+\s*",
        "",
        display,
        count=1,
        flags=re.IGNORECASE,
    )

    display = re.sub(
        r"^\s*SCORE\s*=\s*"
        r"[0-9]{1,3}\s*",
        "",
        display,
        count=1,
        flags=re.IGNORECASE,
    ).strip()


    return {
        "status": status,
        "direction": direction,
        "score": score,
        "message": display,
    }


# =========================================================
# 5 PYTAŃ BOTA + AI
# =========================================================

def analyze_market_ai(
    signal=None,
    autonomous=False,
    monitoring=False,
    prefilter=None,
):
    try:
        full_results = (
            build_full_market_analysis()
        )

        base_results = [
            item
            for item in full_results
            if item.get(
                "interval"
            )
            in (
                "1min",
                "5min",
                "15min",
                "1h",
            )
        ]

        quality = validate_market_data(
            base_results
        )

    except Exception as error:
        logger.exception(
            "Błąd danych rynku: %s",
            error,
        )

        return {
            "status": "ERROR",
            "direction": "NONE",
            "score": 0,
            "message": (
                "⚠️ Błąd danych rynkowych."
            ),
        }


    if not quality["ok"]:
        return {
            "status": "WAIT",
            "direction": (
                signal["side"]
                if signal
                else "NONE"
            ),
            "score": 0,
            "message": (
                "⚠️ Dane rynkowe "
                "nie przeszły kontroli.\n"
                "Nie potwierdzam wejścia."
            ),
        }


    current_price = quality[
        "current_price"
    ]


    # Cena TradingView kontra Twelve Data.
    if (
        signal
        and not monitoring
        and signal.get(
            "strategy_entry"
        )
    ):
        strategy_price = signal[
            "strategy_entry"
        ]

        diff_percent = (
            abs(
                current_price
                - strategy_price
            )
            / strategy_price
            * 100
        )

        if (
            diff_percent
            > MAX_PRICE_DIFF_PERCENT
        ):
            return {
                "status": "WAIT",
                "direction": signal[
                    "side"
                ],
                "score": 0,
                "message": (
                    "⚠️ Różnica cen.\n\n"
                    f"TradingView: "
                    f"{strategy_price:.2f}\n"
                    f"Twelve Data: "
                    f"{current_price:.2f}\n"
                    f"Różnica: "
                    f"{diff_percent:.3f}%\n\n"
                    "⏳ CZEKAJ."
                ),
            }


    market_data = format_market_data(
        full_results
    )


    if autonomous:
        source_text = (
            "To jest AUTOMATYCZNY SKAN rynku. "
            "Nie ma sygnału TradingView."
        )

        direction_text = (
            prefilter.get(
                "direction",
                "NONE",
            )
            if prefilter
            else "NONE"
        )

        candidate_text = (
            prefilter.get(
                "type",
                "NONE",
            )
            if prefilter
            else "NONE"
        )

    else:
        source_text = (
            "To jest analiza sygnału "
            "ze strategii TradingView."
        )

        direction_text = (
            signal["side"]
            if signal
            else "NONE"
        )

        candidate_text = (
            "STRATEGY_SIGNAL"
        )


    prompt = f"""
{source_text}

KANDYDAT:
{candidate_text}

KIERUNEK BAZOWY:
{direction_text}

AKTUALNA CENA:
{current_price}


DANE RYNKOWE:

{market_data}


ZASTOSUJ 5 PYTAŃ BOTA:

1. Czy handlujemy zgodnie
z większym trendem D1/H4?

2. Czy cena znajduje się
przy ważnym poziomie,
wsparciu, oporze,
poprzednim szczycie/dołku
lub strefie reakcji?

3. Czy wystąpiło prawdziwe
potwierdzenie na H1/15m/5m/1m?

4. Czy zachowanie ceny pasuje
do kierunku, którego oczekujemy?

5. Jeśli nie, czy setup należy
unieważnić?


WAŻNE:

Jeżeli H1 nadal pokazuje stary trend,
ale 15m i 5m zaczynają tworzyć
wyraźną zmianę struktury,
nie ignoruj tego.

Możesz wtedy użyć:
REVERSAL

To oznacza:
rynek nie dał jeszcze pełnego wejścia,
ale pojawia się realna możliwość
zmiany kierunku.
"""


    instructions = """
Odpowiadaj po polsku.

Jesteś Trading AI Analyzer v2.

Nie próbujesz przewidywać rynku
za wszelką cenę.

Masz ocenić jakość setupu
i zachowanie ceny.

Interwały:

D1 = szeroki kontekst
H4 = większa struktura
H1 = główny setup
15m = struktura krótkoterminowa
5m = momentum
1m = timing

D1 i H4 NIE są twardą blokadą.
Są kontekstem i wpływają na ryzyko.

Jeżeli H1 jest jeszcze spadkowe,
ale 15m/5m wyraźnie zmieniają
strukturę wzrostowo,
możesz zwrócić REVERSAL LONG.

Analogicznie w drugą stronę.

Nie wymagaj idealnej zgodności
wszystkich interwałów.

Nie dawaj wejścia tylko dlatego,
że jeden wskaźnik zmienił kierunek.

Szukaj:
- ważnego poziomu,
- zmiany zachowania ceny,
- struktury,
- momentum,
- potwierdzenia,
- sensownego risk/reward.

Wybierz dokładnie jeden STATUS:

ENTRY
gdy wejście ma sens TERAZ.

WAIT
gdy pomysł jest sensowny,
ale timing jest za słaby.

REVERSAL
gdy możliwe jest odwrócenie,
ale jeszcze nie ma pełnego wejścia.

SETUP
gdy istnieje ciekawy setup
do obserwacji.

SKIP
gdy setup jest słaby
lub został unieważniony.

NONE
gdy nie ma nic wartego uwagi.


Pierwsze 3 linie MUSZĄ być:

STATUS=ENTRY
DIRECTION=LONG
SCORE=82

Oczywiście wartości dopasuj
do swojej decyzji.

SCORE od 0 do 100 oznacza
jakość setupu.

Następnie odpowiedz krótko:

📊 XAUUSD

Cena: ...

Status: ...

Kierunek: ...

Score: .../100

D1: ...
H4: ...
H1: ...
15m: ...
5m: ...
1m: ...

Ważny poziom:
...

Potwierdzenie:
...

Warunek wejścia:
...

Unieważnienie:
...

Jeśli STATUS=ENTRY,
podaj dodatkowo:

Wejście AI: ...
SL: ...
TP1: ...
TP2: ...
TP3: ...

Nie gwarantuj zysku.
Nie pisz długiego komentarza.
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
            "status": "ERROR",
            "direction": "NONE",
            "score": 0,
            "message": (
                "⚠️ Błąd analizy AI."
            ),
        }


    return parse_ai_meta(
        answer
    )


# =========================================================
# FORMAT ALERTÓW AUTO
# =========================================================

def auto_alert_text(
    result,
):
    status = result[
        "status"
    ]

    message = result[
        "message"
    ]

    if status == "REVERSAL":
        return (
            "🔄 REVERSAL WATCH\n\n"
            + message
        )

    if status == "SETUP":
        return (
            "📡 SETUP WYKRYTY\n\n"
            + message
        )

    if status == "ENTRY":
        return (
            "🚨 WEJŚCIE POTWIERDZONE\n\n"
            + message
        )

    if status == "SKIP":
        return (
            "❌ SETUP UNIEWAŻNIONY\n\n"
            + message
        )

    return None


# =========================================================
# CZY WYSŁAĆ AUTO ALERT
# =========================================================

def should_send_auto_alert(
    result,
):
    status = result[
        "status"
    ]

    direction = result[
        "direction"
    ]

    score = result[
        "score"
    ]


    if status not in (
        "SETUP",
        "REVERSAL",
        "ENTRY",
        "SKIP",
    ):
        return False


    # Nie pokazujemy słabych
    # setupów obserwacyjnych.
    if (
        status in (
            "SETUP",
            "REVERSAL",
        )
        and score < 60
    ):
        return False


    key = (
        f"{status}|"
        f"{direction}|"
        f"{score // 10}"
    )

    now = time.time()


    with auto_lock:
        same = (
            auto_state[
                "last_key"
            ]
            == key
        )

        elapsed = (
            now
            - auto_state[
                "last_sent_at"
            ]
        )

        if (
            same
            and elapsed
            < AUTO_ALERT_COOLDOWN_SECONDS
        ):
            return False


        auto_state[
            "last_key"
        ] = key

        auto_state[
            "last_sent_at"
        ] = now

        auto_state[
            "last_status"
        ] = status

        auto_state[
            "last_direction"
        ] = direction


    return True


# =========================================================
# AUTOMATYCZNY SKAN
# =========================================================

def auto_scan_once():
    try:
        results = (
            build_full_market_analysis()
        )

        candidate = prefilter_market(
            results
        )


        if not candidate[
            "candidate"
        ]:
            logger.info(
                "AUTO SCAN: brak kandydata."
            )
            return


        logger.info(
            "AUTO SCAN: kandydat %s %s",
            candidate.get(
                "type"
            ),
            candidate.get(
                "direction"
            ),
        )


        result = analyze_market_ai(
            autonomous=True,
            prefilter=candidate,
        )


        logger.info(
            "AUTO SCAN AI: %s %s score=%s",
            result["status"],
            result["direction"],
            result["score"],
        )


        if should_send_auto_alert(
            result
        ):
            text = auto_alert_text(
                result
            )

            if text:
                send_telegram_message(
                    text
                )


    except Exception as error:
        logger.exception(
            "Błąd auto scan: %s",
            error,
        )


def auto_scanner_loop():
    logger.info(
        "AUTO SCANNER uruchomiony."
    )

    # Pierwszy skan chwilę po starcie.
    time.sleep(
        20
    )

    while True:
        auto_scan_once()

        time.sleep(
            AUTO_SCAN_INTERVAL_SECONDS
        )


# =========================================================
# BLOKADA JEDNEGO SCANNERA
# =========================================================

def acquire_scanner_lock():
    global scanner_lock_file

    if fcntl is None:
        return True

    try:
        scanner_lock_file = open(
            "/tmp/trading_ai_scanner.lock",
            "w",
        )

        fcntl.flock(
            scanner_lock_file,
            (
                fcntl.LOCK_EX
                | fcntl.LOCK_NB
            ),
        )

        return True

    except Exception:
        return False


def start_auto_scanner():
    global scanner_started

    if not AUTO_SCAN_ENABLED:
        logger.info(
            "AUTO SCANNER wyłączony."
        )
        return


    with scanner_start_lock:
        if scanner_started:
            return


        if not acquire_scanner_lock():
            logger.info(
                "AUTO SCANNER działa już "
                "w innym workerze."
            )
            return


        scanner_started = True


        thread = threading.Thread(
            target=auto_scanner_loop,
            daemon=True,
        )

        thread.start()


# =========================================================
# MONITOR STRATEGII
# =========================================================

def monitor_strategy_setup(
    signal,
    monitor_id,
):
    symbol = signal[
        "symbol"
    ]


    for check_number in range(
        1,
        MONITOR_MAX_CHECKS + 1,
    ):
        time.sleep(
            MONITOR_INTERVAL_SECONDS
        )


        with monitor_lock:
            current = active_monitors.get(
                symbol
            )

            if (
                not current
                or current.get(
                    "id"
                )
                != monitor_id
            ):
                return


        logger.info(
            "Monitor strategii %s/%s",
            check_number,
            MONITOR_MAX_CHECKS,
        )


        result = analyze_market_ai(
            signal=signal,
            monitoring=True,
        )


        status = result[
            "status"
        ]


        if status == "ENTRY":
            send_telegram_message(
                "🚨 WEJŚCIE POTWIERDZONE "
                "DLA SYGNAŁU STRATEGII\n\n"
                + result["message"]
            )

            with monitor_lock:
                active_monitors.pop(
                    symbol,
                    None,
                )

            return


        if status == "SKIP":
            send_telegram_message(
                "❌ SYGNAŁ STRATEGII "
                "UNIEWAŻNIONY\n\n"
                + result["message"]
            )

            with monitor_lock:
                active_monitors.pop(
                    symbol,
                    None,
                )

            return


        if status == "REVERSAL":
            send_telegram_message(
                "🔄 REVERSAL WATCH "
                "PODCZAS MONITOROWANIA\n\n"
                + result["message"]
            )


    with monitor_lock:
        active_monitors.pop(
            symbol,
            None,
        )


    send_telegram_message(
        "⌛ OBSERWACJA SYGNAŁU "
        "ZAKOŃCZONA\n\n"
        "Setup nie uzyskał "
        "wystarczającego potwierdzenia."
    )


# =========================================================
# ALERT STRATEGII
# =========================================================

def process_alert(text):
    signal = parse_alert(
        text
    )

    logger.info(
        "TradingView: %s",
        signal,
    )


    if signal[
        "event"
    ] != "ENTRY":
        return


    if signal[
        "symbol"
    ] != "XAUUSD":
        return


    timeframe = str(
        signal[
            "timeframe"
        ]
    ).lower()


    if timeframe not in (
        "1h",
        "60",
        "60m",
    ):
        return


    side_icon = (
        "🟢"
        if signal[
            "side"
        ]
        == "LONG"
        else "🔴"
    )


    send_telegram_message(
        f"📡 STRATEGIA H1 — "
        f"{signal['side']}\n\n"
        f"{side_icon} XAUUSD "
        f"{signal['side']}\n"
        f"Cena sygnału: "
        f"{signal['strategy_entry']}\n\n"
        "🤖 Bot v2 analizuje:\n"
        "D1 / H4 / H1 / 15m / 5m / 1m\n"
        "+ 5 pytań bota."
    )


    result = analyze_market_ai(
        signal=signal,
        monitoring=False,
    )


    status = result[
        "status"
    ]


    if status == "ENTRY":
        send_telegram_message(
            "🚨 WEJŚCIE POTWIERDZONE\n\n"
            + result["message"]
        )
        return


    if status == "SKIP":
        send_telegram_message(
            "❌ SETUP ODRZUCONY\n\n"
            + result["message"]
        )
        return


    if status == "REVERSAL":
        send_telegram_message(
            "🔄 REVERSAL WATCH\n\n"
            + result["message"]
        )


    else:
        send_telegram_message(
            "⏳ SETUP OBSERWOWANY\n\n"
            + result["message"]
        )


    monitor_id = str(
        uuid.uuid4()
    )


    with monitor_lock:
        active_monitors[
            signal["symbol"]
        ] = {
            "id": monitor_id,
            "side": signal[
                "side"
            ],
            "started": time.time(),
        }


    thread = threading.Thread(
        target=monitor_strategy_setup,
        args=(
            signal,
            monitor_id,
        ),
        daemon=True,
    )

    thread.start()


# =========================================================
# FLASK
# =========================================================

@app.route(
    "/",
    methods=[
        "GET",
    ],
)
def home():
    return jsonify(
        {
            "status": "ok",
            "service": (
                "Trading AI Analyzer v2"
            ),
            "auto_scan_enabled": (
                AUTO_SCAN_ENABLED
            ),
            "auto_scan_interval": (
                AUTO_SCAN_INTERVAL_SECONDS
            ),
        }
    )


@app.route(
    "/health",
    methods=[
        "GET",
    ],
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
            "auto_scanner": (
                scanner_started
            ),
        }
    )


@app.route(
    "/webhook",
    methods=[
        "POST",
    ],
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
                    data.get(
                        "message"
                    )
                    or data.get(
                        "text"
                    )
                    or json.dumps(
                        data
                    )
                )

            else:
                text = str(
                    data
                )

        else:
            text = request.get_data(
                as_text=True
            )


    except Exception as error:
        logger.exception(
            "Błąd webhooka: %s",
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
        args=(
            text,
        ),
        daemon=True,
    )

    thread.start()


    return jsonify(
        {
            "status": "accepted",
        }
    ), 200


# =========================================================
# START SCANNERA
# =========================================================

start_auto_scanner()


# =========================================================
# LOCAL
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
