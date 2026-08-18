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

# 10 minut.
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

# Minimalna siła kierunku dla ENTRY.
MIN_ENTRY_PERCENT = int(
    os.getenv(
        "MIN_ENTRY_PERCENT",
        "70",
    )
)

# Minimalny score dla SETUP / REVERSAL.
MIN_SETUP_SCORE = int(
    os.getenv(
        "MIN_SETUP_SCORE",
        "60",
    )
)

# Cache danych 1m / 5m / 15m / H1.
BASE_CACHE_SECONDS = int(
    os.getenv(
        "BASE_CACHE_SECONDS",
        "90",
    )
)

# H4.
H4_CACHE_SECONDS = int(
    os.getenv(
        "H4_CACHE_SECONDS",
        "1800",
    )
)

# D1.
D1_CACHE_SECONDS = int(
    os.getenv(
        "D1_CACHE_SECONDS",
        "14400",
    )
)

# Twelve Data 429 backoff.
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
        "TWELVE DATA 429. Pauza na %s sekund.",
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

    result = [values[0]]

    for value in values[1:]:
        result.append(
            value * multiplier
            + result[-1] * (1 - multiplier)
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
            avg_gain * (period - 1)
            + gains[i]
        ) / period

        avg_loss = (
            avg_loss * (period - 1)
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
# PEŁNY SNAPSHOT
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
#
# H1 = KIERUNEK
# M15 = SETUP
# M5 = TIMING
# M1 = NIE DECYDUJE
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

