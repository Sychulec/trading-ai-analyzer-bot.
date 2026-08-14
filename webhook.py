import os
import re
import json
import time
import uuid
import logging
import threading
import urllib.parse
import urllib.request
import urllib.error

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

# Monitoring sygnału TradingView.
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

# Automatyczny skaner.
AUTO_SCAN_ENABLED = (
    os.getenv(
        "AUTO_SCAN_ENABLED",
        "true",
    ).lower()
    == "true"
)

# Na początek 10 minut zamiast 5.
# Dzięki temu dużo trudniej uderzyć w limit Twelve Data.
AUTO_SCAN_INTERVAL_SECONDS = int(
    os.getenv(
        "AUTO_SCAN_INTERVAL_SECONDS",
        "600",
    )
)

# Nie powtarzaj identycznego alertu zbyt szybko.
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

# Cache podstawowych danych 1m/5m/15m/H1.
BASE_CACHE_SECONDS = int(
    os.getenv(
        "BASE_CACHE_SECONDS",
        "90",
    )
)

# H4 wystarczy odświeżać znacznie rzadziej.
H4_CACHE_SECONDS = int(
    os.getenv(
        "H4_CACHE_SECONDS",
        "1800",
    )
)

# D1 nie ma sensu pobierać co kilka minut.
D1_CACHE_SECONDS = int(
    os.getenv(
        "D1_CACHE_SECONDS",
        "14400",
    )
)

# Po Twelve Data 429 blokujemy nowe pobrania.
RATE_LIMIT_BACKOFF_SECONDS = int(
    os.getenv(
        "RATE_LIMIT_BACKOFF_SECONDS",
        "300",
    )
)


# =========================================================
# STAN
# =========================================================

monitor_lock = threading.Lock()
auto_lock = threading.Lock()

market_cache_lock = threading.Lock()
market_fetch_lock = threading.Lock()

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


market_cache = {
    "base": {
        "data": None,
        "timestamp": 0,
    },

    "4h": {
        "data": None,
        "timestamp": 0,
    },

    "1day": {
        "data": None,
        "timestamp": 0,
    },
}

rate_limit_until = 0


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram_message(text):
    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

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
                "Błąd Telegram: %s",
                error,
            )


# =========================================================
# RATE LIMIT TWELVE DATA
# =========================================================

def rate_limit_active():
    global rate_limit_until

    return time.time() < rate_limit_until


def activate_rate_limit_backoff():
    global rate_limit_until

    rate_limit_until = (
        time.time()
        + RATE_LIMIT_BACKOFF_SECONDS
    )

    logger.warning(
        "TWELVE DATA 429. "
        "Pauza na %s sekund.",
        RATE_LIMIT_BACKOFF_SECONDS,
    )


# =========================================================
# ALERT TRADINGVIEW
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
# WSKAŹNIKI
# =========================================================

def ema_series(values, period):
    if not values:
        return []

    multiplier = 2 / (period + 1)

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
            max(change, 0)
        )

        losses.append(
            max(-change, 0)
        )


    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
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

    rs = avg_gain / avg_loss

    return (
        100
        - (
            100
            / (1 + rs)
        )
    )


def calculate_macd(closes):
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
        for a, b
        in zip(
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
# H4 / D1 Z CACHE
# =========================================================

def fetch_extra_timeframe_raw(
    interval,
    outputsize=120,
):
    if rate_limit_active():
        return {
            "interval": interval,
            "error": (
                "Twelve Data rate-limit "
                "backoff aktywny"
            ),
        }


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


    except urllib.error.HTTPError as error:
        if error.code == 429:
            activate_rate_limit_backoff()

            return {
                "interval": interval,
                "error": (
                    "HTTP 429 "
                    "Too Many Requests"
                ),
            }

        logger.exception(
            "HTTP error %s dla %s",
            error.code,
            interval,
        )

        return {
            "interval": interval,
            "error": str(error),
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


    if "values" not in data:
        message = data.get(
            "message",
            "Brak danych",
        )

        if (
            "limit" in message.lower()
            or "credits" in message.lower()
        ):
            activate_rate_limit_backoff()

        return {
            "interval": interval,
            "error": message,
        }


    candles = []

    for item in reversed(
        data["values"]
    ):
        try:
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

        except Exception:
            continue


    if len(candles) < 55:
        return {
            "interval": interval,
            "error": "Za mało danych",
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


def get_extra_timeframe_cached(
    interval,
):
    now = time.time()

    if interval == "4h":
        ttl = H4_CACHE_SECONDS

    elif interval == "1day":
        ttl = D1_CACHE_SECONDS

    else:
        ttl = 600


    with market_cache_lock:
        cached = market_cache.get(
            interval
        )

        if cached:
            data = cached.get(
                "data"
            )

            timestamp = cached.get(
                "timestamp",
                0,
            )

            if (
                data is not None
                and now - timestamp < ttl
            ):
                return data


    fresh = fetch_extra_timeframe_raw(
        interval
    )


    # Jeżeli nowe pobranie dostało 429,
    # a mamy stare poprawne dane,
    # użyj starego cache zamiast błędu.
    if "error" in fresh:
        with market_cache_lock:
            old = market_cache.get(
                interval,
                {},
            ).get("data")

        if (
            old
            and "error" not in old
        ):
            logger.warning(
                "Używam starego cache %s.",
                interval,
            )

            return old

        return fresh


    with market_cache_lock:
        market_cache[
            interval
        ] = {
            "data": fresh,
            "timestamp": now,
        }


    return fresh


# =========================================================
# CACHE 1m / 5m / 15m / H1
# =========================================================

def get_base_market_cached(
    force=False,
):
    now = time.time()


    with market_cache_lock:
        cached_data = market_cache[
            "base"
        ]["data"]

        cached_timestamp = market_cache[
            "base"
        ]["timestamp"]


        if (
            not force
            and cached_data is not None
            and (
                now
                - cached_timestamp
                < BASE_CACHE_SECONDS
            )
        ):
            return cached_data


    # Dzięki lockowi dwa wątki
    # nie pobiorą danych jednocześnie.
    with market_fetch_lock:

        now = time.time()

        with market_cache_lock:
            cached_data = market_cache[
                "base"
            ]["data"]

            cached_timestamp = market_cache[
                "base"
            ]["timestamp"]


            if (
                not force
                and cached_data is not None
                and (
                    now
                    - cached_timestamp
                    < BASE_CACHE_SECONDS
                )
            ):
                return cached_data


        if rate_limit_active():
            if cached_data:
                logger.warning(
                    "Rate limit aktywny. "
                    "Używam ostatnich danych bazowych."
                )

                return cached_data

            raise RuntimeError(
                "Rate limit Twelve Data aktywny."
            )


        try:
            fresh = build_market_analysis(
                "XAUUSD"
            )


        except urllib.error.HTTPError as error:
            if error.code == 429:
                activate_rate_limit_backoff()

                if cached_data:
                    return cached_data

            raise


        except Exception as error:
            text = str(error)

            if (
                "429" in text
                or "Too Many Requests"
                in text
            ):
                activate_rate_limit_backoff()

                if cached_data:
                    return cached_data

            raise


        with market_cache_lock:
            market_cache[
                "base"
            ] = {
                "data": fresh,
                "timestamp": time.time(),
            }


        return fresh


# =========================================================
# JEDEN SNAPSHOT = JEDNO POBRANIE
# =========================================================

def build_full_market_analysis(
    force_base=False,
):
    base = get_base_market_cached(
        force=force_base
    )

    h4 = get_extra_timeframe_cached(
        "4h"
    )

    d1 = get_extra_timeframe_cached(
        "1day"
    )

    return (
        list(base)
        + [
            h4,
            d1,
        ]
    )


# =========================================================
# PREFILTER
# =========================================================

def is_bullish(item):
    if (
        not item
        or "error" in item
    ):
        return False


    histogram = item.get(
        "histogram"
    )

    rsi = item.get(
        "rsi"
    )

    price = item.get(
        "price"
    )

    ema20 = item.get(
        "ema20"
    )


    if (
        histogram is None
        or rsi is None
        or price is None
        or ema20 is None
    ):
        return False


    return (
        price > ema20
        and rsi >= 50
        and histogram > 0
    )


def is_bearish(item):
    if (
        not item
        or "error" in item
    ):
        return False


    histogram = item.get(
        "histogram"
    )

    rsi = item.get(
        "rsi"
    )

    price = item.get(
        "price"
    )

    ema20 = item.get(
        "ema20"
    )


    if (
        histogram is None
        or rsi is None
        or price is None
        or ema20 is None
    ):
        return False


    return (
        price < ema20
        and rsi <= 50
        and histogram < 0
    )


def get_by_interval(results):
    return {
        item.get(
            "interval"
        ): item
        for item in results
        if isinstance(
            item,
            dict,
        )
        and item.get(
            "interval"
        )
    }


def find_tf(
    by_tf,
    *names,
):
    for name in names:
        if name in by_tf:
            return by_tf[name]

    return None


def prefilter_market(results):
    by_tf = get_by_interval(
        results
    )

    h1 = find_tf(
        by_tf,
        "1h",
        "60min",
        "60",
    )

    m15 = find_tf(
        by_tf,
        "15min",
        "15m",
        "15",
    )

    m5 = find_tf(
        by_tf,
        "5min",
        "5m",
        "5",
    )

    m1 = find_tf(
        by_tf,
        "1min",
        "1m",
        "1",
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
            "direction": "NONE",
        }


    lower_bull = sum(
        [
            bool(is_bullish(m15)),
            bool(is_bullish(m5)),
            bool(is_bullish(m1)),
        ]
    )

    lower_bear = sum(
        [
            bool(is_bearish(m15)),
            bool(is_bearish(m5)),
            bool(is_bearish(m1)),
        ]
    )


    h1_trend = h1.get(
        "trend"
    )


    # =====================================================
    # NORMALNY SETUP
    # =====================================================

    if (
        h1_trend == "wzrostowy"
        and lower_bull >= 2
    ):
        return {
            "candidate": True,
            "type": "SETUP",
            "direction": "LONG",
        }


    if (
        h1_trend == "spadkowy"
        and lower_bear >= 2
    ):
        return {
            "candidate": True,
            "type": "SETUP",
            "direction": "SHORT",
        }


    # =====================================================
    # REVERSAL WATCH
    # =====================================================

    if (
        h1_trend == "spadkowy"
        and lower_bull >= 2
    ):
        return {
            "candidate": True,
            "type": "REVERSAL",
            "direction": "LONG",
        }


    if (
        h1_trend == "wzrostowy"
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

def parse_ai_meta(answer):
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
        status = status_match.group(1)


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
# AI
#
# UWAGA:
# results dostajemy już pobrane.
# NIE pobieramy rynku drugi raz.
# =========================================================

def analyze_market_ai(
    results,
    signal=None,
    autonomous=False,
    monitoring=False,
    prefilter=None,
):
    try:
        base_results = [
            item
            for item in results
            if item.get(
                "interval"
            )
            in (
                "1min",
                "5min",
                "15min",
                "1h",
                "1m",
                "5m",
                "15m",
                "60min",
            )
        ]


        quality = validate_market_data(
            base_results
        )


    except Exception as error:
        logger.exception(
            "Błąd kontroli danych rynku: %s",
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


    # =====================================================
    # KONTROLA CENY
    # =====================================================

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
        results
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


BARDZO WAŻNE:

Nie wymagaj idealnej zgodności
wszystkich interwałów.

Jeżeli H1 nadal pokazuje
dotychczasowy trend,
ale 15m oraz 5m zaczynają
wyraźnie zmieniać strukturę
w przeciwną stronę,
oceń możliwość REVERSAL.

Przykład:

H1 spadkowe,
ale cena broni wsparcia,
15m przestaje robić niższe dołki,
5m wybija lokalny szczyt,
momentum rośnie.

To NIE musi być jeszcze ENTRY,
ale może być REVERSAL LONG.

Analogicznie dla SHORT.

Najważniejsze jest zachowanie ceny,
a wskaźniki są potwierdzeniem.
"""


    instructions = """
Odpowiadaj po polsku.

Jesteś Trading AI Analyzer v2.

Twoim zadaniem nie jest
wymyślanie transakcji na siłę.

Masz oceniać:
- kontekst,
- poziom,
- strukturę,
- momentum,
- zachowanie ceny,
- jakość setupu.

Interwały:

D1 = szeroki kontekst.
H4 = większa struktura.
H1 = główny setup.
15m = struktura krótkoterminowa.
5m = momentum.
1m = timing.

D1 i H4 nie są twardą blokadą.

Jeżeli H1 nadal jest w starym
trendzie, ale 15m/5m wyraźnie
zmieniają strukturę, możesz
zwrócić REVERSAL.

Nie wymagaj idealnej zgodności
wszystkich interwałów.

Nie dawaj ENTRY tylko dlatego,
że jeden wskaźnik zmienił kolor.

Szukaj prawdziwego zachowania ceny.

Wybierz dokładnie jeden STATUS:

ENTRY
gdy wejście ma sens TERAZ.

WAIT
gdy kierunek może być dobry,
ale timing nie jest wystarczający.

REVERSAL
gdy pojawia się realna możliwość
zmiany kierunku, ale wejście
nie jest jeszcze w pełni potwierdzone.

SETUP
gdy pojawia się ciekawa sytuacja
do obserwacji.

SKIP
gdy setup jest słaby
lub został unieważniony.

NONE
gdy nie ma nic wartego uwagi.


Pierwsze trzy linie MUSZĄ mieć format:

STATUS=ENTRY
DIRECTION=LONG
SCORE=82

Dopasuj wartości do analizy.

SCORE 0-100 oznacza jakość setupu.

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

Co robi cena:
...

Potwierdzenie:
...

Warunek wejścia:
...

Unieważnienie:
...

Jeżeli STATUS=ENTRY,
podaj:

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
# FORMAT ALERTÓW
# =========================================================

def auto_alert_text(result):
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
# ANTY-SPAM
# =========================================================

def should_send_auto_alert(result):
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
#
# NAJWAŻNIEJSZA POPRAWKA:
# snapshot pobieramy JEDEN RAZ.
# =========================================================

def auto_scan_once():
    try:
        if rate_limit_active():
            logger.info(
                "AUTO SCAN: "
                "rate-limit backoff aktywny."
            )

            return


        # ================================================
        # JEDNO POBRANIE DANYCH
        # ================================================

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
                "AUTO SCAN: "
                "brak kandydata."
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


        # ================================================
        # AI DOSTAJE TE SAME DANE.
        # NICZEGO NIE POBIERA PONOWNIE.
        # ================================================

        result = analyze_market_ai(
            results=results,
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
        text = str(error)

        if (
            "429" in text
            or "Too Many Requests"
            in text
        ):
            activate_rate_limit_backoff()

        logger.exception(
            "Błąd auto scan: %s",
            error,
        )


def auto_scanner_loop():
    logger.info(
        "AUTO SCANNER uruchomiony."
    )

    # Po starcie Rendera czekamy,
    # żeby bot.py / webhook nie odpalały
    # wszystkiego dokładnie jednocześnie.
    time.sleep(30)


    while True:
        auto_scan_once()

        time.sleep(
            AUTO_SCAN_INTERVAL_SECONDS
        )


# =========================================================
# TYLKO JEDEN SCANNER NA RENDER
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
# MONITOR SYGNAŁU STRATEGII
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


        try:
            # Nowe dane tylko gdy cache wygasł.
            results = (
                build_full_market_analysis()
            )


            result = analyze_market_ai(
                results=results,
                signal=signal,
                monitoring=True,
            )


        except Exception as error:
            logger.exception(
                "Błąd monitoringu: %s",
                error,
            )

            continue


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
# SYGNAŁ TRADINGVIEW
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
        "+ 5 pytań bota\n"
        "+ REVERSAL WATCH."
    )


    try:
        # ================================================
        # ZNOWU: JEDEN SNAPSHOT
        # ================================================

        results = (
            build_full_market_analysis()
        )


        result = analyze_market_ai(
            results=results,
            signal=signal,
            monitoring=False,
        )


    except Exception as error:
        logger.exception(
            "Błąd analizy alertu: %s",
            error,
        )

        send_telegram_message(
            "⚠️ Nie udało się pobrać "
            "pełnych danych rynku.\n"
            "Nie potwierdzam wejścia."
        )

        return


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
    methods=["GET"],
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
            "rate_limit_active": (
                rate_limit_active()
            ),
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
            "auto_scanner": scanner_started,
            "rate_limit_active": (
                rate_limit_active()
            ),
            "rate_limit_until": (
                rate_limit_until
            ),
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
                text = str(data)


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
# START
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
