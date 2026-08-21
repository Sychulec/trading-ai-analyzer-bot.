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
# USTAWIENIA SETUPU
# =========================================================

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
        "600",
    )
)

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

MIN_ENTRY_PERCENT = int(
    os.getenv(
        "MIN_ENTRY_PERCENT",
        "70",
    )
)

MIN_SETUP_SCORE = int(
    os.getenv(
        "MIN_SETUP_SCORE",
        "60",
    )
)

MIN_ENTRY_RR = float(
    os.getenv(
        "MIN_ENTRY_RR",
        "1.30",
    )
)

LEVEL_PROXIMITY_PERCENT = float(
    os.getenv(
        "LEVEL_PROXIMITY_PERCENT",
        "0.20",
    )
)

TV_ALERT_DEDUP_SECONDS = int(
    os.getenv(
        "TV_ALERT_DEDUP_SECONDS",
        "900",
    )
)


# =========================================================
# CACHE
# =========================================================

BASE_CACHE_SECONDS = int(
    os.getenv(
        "BASE_CACHE_SECONDS",
        "90",
    )
)

H4_CACHE_SECONDS = int(
    os.getenv(
        "H4_CACHE_SECONDS",
        "1800",
    )
)

D1_CACHE_SECONDS = int(
    os.getenv(
        "D1_CACHE_SECONDS",
        "14400",
    )
)

RATE_LIMIT_BACKOFF_SECONDS = int(
    os.getenv(
        "RATE_LIMIT_BACKOFF_SECONDS",
        "300",
    )
)


# =========================================================
# TRADER V4.2
# =========================================================

ACTIVE_TRADE_INTERVAL_SECONDS = int(
    os.getenv(
        "ACTIVE_TRADE_INTERVAL_SECONDS",
        "300",
    )
)

ACTIVE_TRADE_MAX_CHECKS = int(
    os.getenv(
        "ACTIVE_TRADE_MAX_CHECKS",
        "72",
    )
)

# Od ilu R zabezpieczamy minimum BE.
PROTECT_AT_R = float(
    os.getenv(
        "PROTECT_AT_R",
        "1.0",
    )
)

# Pierwsza częściowa realizacja.
FIRST_PARTIAL_AT_R = float(
    os.getenv(
        "FIRST_PARTIAL_AT_R",
        "2.0",
    )
)

FIRST_PARTIAL_PERCENT = int(
    os.getenv(
        "FIRST_PARTIAL_PERCENT",
        "30",
    )
)

# Od ilu R zaczynamy mocniej prowadzić SL po M15.
TRAIL_START_R = float(
    os.getenv(
        "TRAIL_START_R",
        "2.5",
    )
)

# Bufor trailing SL od poziomu struktury.
TRAIL_BUFFER_PERCENT = float(
    os.getenv(
        "TRAIL_BUFFER_PERCENT",
        "0.08",
    )
)


# =========================================================
# STAN
# =========================================================

monitor_lock = threading.Lock()
auto_lock = threading.Lock()
strategy_alert_lock = threading.Lock()

market_cache_lock = threading.Lock()
market_fetch_lock = threading.Lock()

active_trade_lock = threading.Lock()


active_monitors = {}

active_trades = {}


auto_state = {
    "last_key": None,
    "last_sent_at": 0,
    "last_status": "NONE",
    "last_direction": "NONE",
}


strategy_alert_seen = {}


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
# RATE LIMIT TWELVE DATA
# =========================================================

def rate_limit_active():
    global rate_limit_until

    return (
        time.time()
        < rate_limit_until
    )


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
# DUPLIKAT ALERTU TV
# =========================================================

def build_strategy_alert_key(
    signal,
):
    price = signal.get(
        "strategy_entry"
    )

    if price is None:
        price_text = "NONE"

    else:
        price_text = (
            f"{price:.2f}"
        )


    return (
        f"{signal.get('symbol')}|"
        f"{signal.get('timeframe')}|"
        f"{signal.get('side')}|"
        f"{price_text}"
    )


def cleanup_old_strategy_alerts(
    now,
):
    expired = []


    for key, timestamp in (
        strategy_alert_seen.items()
    ):
        if (
            now - timestamp
            > TV_ALERT_DEDUP_SECONDS * 2
        ):
            expired.append(
                key
            )


    for key in expired:
        strategy_alert_seen.pop(
            key,
            None,
        )


def is_duplicate_strategy_alert(
    signal,
):
    now = time.time()

    key = (
        build_strategy_alert_key(
            signal
        )
    )


    with strategy_alert_lock:

        cleanup_old_strategy_alerts(
            now
        )

        previous = (
            strategy_alert_seen.get(
                key
            )
        )


        if (
            previous is not None
            and (
                now - previous
                < TV_ALERT_DEDUP_SECONDS
            )
        ):
            logger.info(
                "Duplikat TradingView zablokowany: %s",
                key,
            )

            return True


        strategy_alert_seen[
            key
        ] = now


    return False


# =========================================================
# WSKAŹNIKI
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
            * (
                1 - multiplier
            )
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
            - closes[
                i - 1
            ]
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
            * (
                period - 1
            )
            + gains[i]
        ) / period


        avg_loss = (
            avg_loss
            * (
                period - 1
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
            / (
                1 + rs
            )
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


    macd = (
        macd_line[-1]
    )

    signal = (
        signal_line[-1]
    )


    return (
        macd,
        signal,
        macd - signal,
    )


# =========================================================
# H4 / D1
# =========================================================

def fetch_extra_timeframe_raw(
    interval,
    outputsize=120,
):
    if rate_limit_active():
        return {
            "interval": interval,
            "error": (
                "Twelve Data "
                "rate-limit backoff aktywny"
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
            "limit"
            in message.lower()
            or "credits"
            in message.lower()
        ):
            activate_rate_limit_backoff()


        return {
            "interval": interval,
            "error": message,
        }


    candles = []


    for item in reversed(
        data[
            "values"
        ]
    ):
        try:
            candles.append(
                {
                    "datetime": item[
                        "datetime"
                    ],

                    "open": float(
                        item[
                            "open"
                        ]
                    ),

                    "high": float(
                        item[
                            "high"
                        ]
                    ),

                    "low": float(
                        item[
                            "low"
                        ]
                    ),

                    "close": float(
                        item[
                            "close"
                        ]
                    ),
                }
            )

        except Exception:
            continue


    if len(candles) < 55:

        return {
            "interval": interval,
            "error": (
                "Za mało danych"
            ),
        }


    closes = [
        candle[
            "close"
        ]
        for candle
        in candles
    ]


    latest = (
        candles[-1]
    )


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


    recent = (
        candles[-30:]
    )


    support = min(
        candle[
            "low"
        ]
        for candle
        in recent
    )


    resistance = max(
        candle[
            "high"
        ]
        for candle
        in recent
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
        ttl = (
            H4_CACHE_SECONDS
        )

    elif interval == "1day":
        ttl = (
            D1_CACHE_SECONDS
        )

    else:
        ttl = 600


    with market_cache_lock:

        cached = (
            market_cache.get(
                interval
            )
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
                and (
                    now
                    - timestamp
                    < ttl
                )
            ):
                return data


    fresh = (
        fetch_extra_timeframe_raw(
            interval
        )
    )


    if "error" in fresh:

        with market_cache_lock:

            old = (
                market_cache.get(
                    interval,
                    {},
                ).get(
                    "data"
                )
            )


        if (
            old
            and "error"
            not in old
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
# CACHE M1 / M5 / M15 / H1
# =========================================================

def get_base_market_cached(
    force=False,
):
    now = time.time()


    with market_cache_lock:

        cached_data = (
            market_cache[
                "base"
            ][
                "data"
            ]
        )

        cached_timestamp = (
            market_cache[
                "base"
            ][
                "timestamp"
            ]
        )


        if (
            not force
            and cached_data
            is not None
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

            cached_data = (
                market_cache[
                    "base"
                ][
                    "data"
                ]
            )

            cached_timestamp = (
                market_cache[
                    "base"
                ][
                    "timestamp"
                ]
            )


            if (
                not force
                and cached_data
                is not None
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
                    "Używam ostatnich danych."
                )

                return cached_data


            raise RuntimeError(
                "Rate limit Twelve Data aktywny."
            )


        try:

            fresh = (
                build_market_analysis(
                    "XAUUSD"
                )
            )


        except urllib.error.HTTPError as error:

            if error.code == 429:

                activate_rate_limit_backoff()

                if cached_data:
                    return cached_data

            raise


        except Exception as error:

            text = str(
                error
            )


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
    base = (
        get_base_market_cached(
            force=force_base
        )
    )


    h4 = (
        get_extra_timeframe_cached(
            "4h"
        )
    )


    d1 = (
        get_extra_timeframe_cached(
            "1day"
        )
    )


    return (
        list(base)
        + [
            h4,
            d1,
        ]
    )


# =========================================================
# HELPERY TF
# =========================================================

def get_by_interval(
    results,
):
    return {
        item.get(
            "interval"
        ): item
        for item
        in results
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
            return (
                by_tf[name]
            )


    return None


def is_bullish(
    item,
):
    if (
        not item
        or "error"
        in item
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


def is_bearish(
    item,
):
    if (
        not item
        or "error"
        in item
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


# =========================================================
# POZIOMY
# =========================================================

def percent_distance(
    price,
    level,
):
    if (
        price is None
        or level is None
        or level == 0
    ):
        return None


    return (
        abs(
            price
            - level
        )
        / abs(level)
        * 100
    )


def price_near_level(
    price,
    level,
):
    distance = (
        percent_distance(
            price,
            level,
        )
    )


    if distance is None:
        return False


    return (
        distance
        <= LEVEL_PROXIMITY_PERCENT
    )


def collect_levels(
    h1,
    m15,
):
    levels = []


    for tf_name, item in (
        (
            "H1",
            h1,
        ),
        (
            "M15",
            m15,
        ),
    ):

        if not item:
            continue


        support = item.get(
            "support"
        )

        resistance = item.get(
            "resistance"
        )


        if support is not None:

            levels.append(
                {
                    "tf": tf_name,
                    "type": "SUPPORT",
                    "price": support,
                }
            )


        if resistance is not None:

            levels.append(
                {
                    "tf": tf_name,
                    "type": "RESISTANCE",
                    "price": resistance,
                }
            )


    return levels


def find_nearest_level(
    price,
    levels,
):
    valid = []


    for level in levels:

        level_price = (
            level.get(
                "price"
            )
        )


        distance = (
            percent_distance(
                price,
                level_price,
            )
        )


        if distance is None:
            continue


        valid.append(
            (
                distance,
                level,
            )
        )


    if not valid:
        return None


    valid.sort(
        key=lambda x: x[0]
    )


    return (
        valid[0][1]
    )


# =========================================================
# SWEEP
# =========================================================

def detect_sweep(
    item,
    support=None,
    resistance=None,
):
    if (
        not item
        or "error"
        in item
    ):
        return "NONE"


    high = item.get(
        "high"
    )

    low = item.get(
        "low"
    )

    close = item.get(
        "price"
    )


    if close is None:
        close = (
            item.get(
                "close"
            )
        )


    if (
        high is None
        or low is None
        or close is None
    ):
        return "NONE"


    if (
        support is not None
        and low < support
        and close > support
    ):
        return (
            "BULLISH_SWEEP"
        )


    if (
        resistance is not None
        and high > resistance
        and close < resistance
    ):
        return (
            "BEARISH_SWEEP"
        )


    return "NONE"


# =========================================================
# PREFILTER V4.2
# =========================================================

def prefilter_market(
    results,
):
    by_tf = (
        get_by_interval(
            results
        )
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


    if not all(
        [
            h1,
            m15,
            m5,
        ]
    ):
        return {
            "candidate": False,
            "type": "NONE",
            "direction": "NONE",
            "wait_reason": "NONE",
        }


    price = (
        m5.get(
            "price"
        )
        or m15.get(
            "price"
        )
        or h1.get(
            "price"
        )
    )


    h1_trend = (
        h1.get(
            "trend"
        )
    )


    m15_bull = (
        is_bullish(
            m15
        )
    )

    m15_bear = (
        is_bearish(
            m15
        )
    )

    m5_bull = (
        is_bullish(
            m5
        )
    )

    m5_bear = (
        is_bearish(
            m5
        )
    )


    levels = (
        collect_levels(
            h1,
            m15,
        )
    )


    nearest = (
        find_nearest_level(
            price,
            levels,
        )
    )


    near_level = False
    level_type = "NONE"
    level_price = None


    if nearest:

        level_price = (
            nearest.get(
                "price"
            )
        )

        level_type = (
            nearest.get(
                "type"
            )
        )

        near_level = (
            price_near_level(
                price,
                level_price,
            )
        )


    support = (
        m15.get(
            "support"
        )
    )

    resistance = (
        m15.get(
            "resistance"
        )
    )


    sweep = (
        detect_sweep(
            m15,
            support=support,
            resistance=resistance,
        )
    )


    common = {
        "near_level": near_level,
        "level_type": level_type,
        "level_price": level_price,
        "sweep": sweep,
    }


    # =====================================================
    # H1 WZROSTOWY
    # =====================================================

    if h1_trend == "wzrostowy":

        if (
            m15_bull
            and m5_bull
        ):
            return {
                "candidate": True,
                "type": "SETUP",
                "direction": "LONG",
                "wait_reason": "READY",
                **common,
            }


        if (
            m15_bull
            and not m5_bull
        ):
            return {
                "candidate": True,
                "type": "SETUP",
                "direction": "LONG",
                "wait_reason": "WAIT_FOR_M5",
                **common,
            }


        if (
            m15_bear
            and m5_bear
        ):
            return {
                "candidate": True,
                "type": "REVERSAL",
                "direction": "SHORT",
                "wait_reason": "WAIT_FOR_RETEST",
                **common,
            }


        if (
            near_level
            and level_type
            == "RESISTANCE"
        ):
            return {
                "candidate": True,
                "type": "SETUP",
                "direction": "LONG",
                "wait_reason": "WAIT_FOR_BREAK",
                **common,
            }


    # =====================================================
    # H1 SPADKOWY
    # =====================================================

    if h1_trend == "spadkowy":

        if (
            m15_bear
            and m5_bear
        ):
            return {
                "candidate": True,
                "type": "SETUP",
                "direction": "SHORT",
                "wait_reason": "READY",
                **common,
            }


        if (
            m15_bear
            and not m5_bear
        ):
            return {
                "candidate": True,
                "type": "SETUP",
                "direction": "SHORT",
                "wait_reason": "WAIT_FOR_M5",
                **common,
            }


        if (
            m15_bull
            and m5_bull
        ):
            return {
                "candidate": True,
                "type": "REVERSAL",
                "direction": "LONG",
                "wait_reason": "WAIT_FOR_RETEST",
                **common,
            }


        if (
            near_level
            and level_type
            == "SUPPORT"
        ):
            return {
                "candidate": True,
                "type": "SETUP",
                "direction": "SHORT",
                "wait_reason": "WAIT_FOR_BREAK",
                **common,
            }


    # =====================================================
    # SWEEP
    # =====================================================

    if sweep == "BULLISH_SWEEP":

        return {
            "candidate": True,
            "type": "REVERSAL",
            "direction": "LONG",
            "wait_reason": "WAIT_FOR_M5",
            **common,
        }


    if sweep == "BEARISH_SWEEP":

        return {
            "candidate": True,
            "type": "REVERSAL",
            "direction": "SHORT",
            "wait_reason": "WAIT_FOR_M5",
            **common,
        }


    return {
        "candidate": False,
        "type": "NONE",
        "direction": "NONE",
        "wait_reason": "NONE",
    }


# =========================================================
# PARSER AI SETUP
# =========================================================

def parse_ai_meta(
    answer,
):
    upper = (
        answer.upper()
    )


    def get_int(
        pattern,
        default=0,
    ):
        match = re.search(
            pattern,
            upper,
        )


        if not match:
            return default


        try:
            return max(
                0,
                min(
                    int(
                        match.group(1)
                    ),
                    100,
                ),
            )

        except Exception:
            return default


    def get_float(
        pattern,
    ):
        match = re.search(
            pattern,
            answer,
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

        except Exception:
            return None


    status_match = re.search(
        r"STATUS\s*=\s*"
        r"(ENTRY|WAIT|SETUP|REVERSAL|"
        r"SKIP|INVALIDATED|NONE)",
        upper,
    )


    direction_match = re.search(
        r"DIRECTION\s*=\s*"
        r"(LONG|SHORT|NONE)",
        upper,
    )


    wait_reason_match = re.search(
        r"WAIT_REASON\s*=\s*"
        r"(READY|WAIT_FOR_BREAK|"
        r"WAIT_FOR_RETEST|WAIT_FOR_M5|"
        r"WAIT_FOR_RR|INVALIDATED|NONE)",
        upper,
    )


    activation_side_match = re.search(
        r"ACTIVATION_SIDE\s*=\s*"
        r"(ABOVE|BELOW|NONE)",
        upper,
    )


    rr_match = re.search(
        r"RR\s*=\s*([0-9.:]+)",
        answer,
        re.IGNORECASE,
    )


    reason_match = re.search(
        r"REASON\s*=\s*(.+)",
        answer,
        re.IGNORECASE,
    )


    status = (
        status_match.group(1)
        if status_match
        else "NONE"
    )


    direction = (
        direction_match.group(1)
        if direction_match
        else "NONE"
    )


    wait_reason = (
        wait_reason_match.group(1)
        if wait_reason_match
        else "NONE"
    )


    activation_side = (
        activation_side_match.group(1)
        if activation_side_match
        else "NONE"
    )


    long_pct = get_int(
        r"LONG_PCT\s*=\s*([0-9]{1,3})"
    )

    short_pct = get_int(
        r"SHORT_PCT\s*=\s*([0-9]{1,3})"
    )

    wait_pct = get_int(
        r"WAIT_PCT\s*=\s*([0-9]{1,3})",
        100,
    )


    total = (
        long_pct
        + short_pct
        + wait_pct
    )


    if total <= 0:

        long_pct = 0
        short_pct = 0
        wait_pct = 100


    elif total != 100:

        long_pct = round(
            long_pct
            / total
            * 100
        )

        short_pct = round(
            short_pct
            / total
            * 100
        )

        wait_pct = (
            100
            - long_pct
            - short_pct
        )


    reason = (
        reason_match.group(1).strip()
        if reason_match
        else ""
    )


    if len(reason) > 200:
        reason = (
            reason[:200]
        )


    return {
        "status": status,
        "direction": direction,

        "score": get_int(
            r"SCORE\s*=\s*([0-9]{1,3})"
        ),

        "long_pct": long_pct,
        "short_pct": short_pct,
        "wait_pct": wait_pct,

        "wait_reason": wait_reason,

        "price": get_float(
            r"PRICE\s*=\s*([0-9.,]+)"
        ),

        "entry": get_float(
            r"ENTRY\s*=\s*([0-9.,]+)"
        ),

        "activation": get_float(
            r"ACTIVATION\s*=\s*([0-9.,]+)"
        ),

        "activation_side": (
            activation_side
        ),

        "sl": get_float(
            r"SL\s*=\s*([0-9.,]+)"
        ),

        "tp1": get_float(
            r"TP1\s*=\s*([0-9.,]+)"
        ),

        "tp2": get_float(
            r"TP2\s*=\s*([0-9.,]+)"
        ),

        "invalidation": get_float(
            r"INVALIDATION\s*=\s*([0-9.,]+)"
        ),

        "rr": (
            rr_match.group(1)
            if rr_match
            else None
        ),

        "reason": reason,

        "message": answer,
    }


# =========================================================
# R:R
# =========================================================

def calculate_rr(
    direction,
    entry,
    sl,
    tp,
):
    if (
        entry is None
        or sl is None
        or tp is None
    ):
        return None


    if direction == "LONG":

        risk = (
            entry - sl
        )

        reward = (
            tp - entry
        )


    elif direction == "SHORT":

        risk = (
            sl - entry
        )

        reward = (
            entry - tp
        )


    else:
        return None


    if risk <= 0:
        return None


    if reward <= 0:
        return None


    return (
        reward / risk
    )


# =========================================================
# ANALIZA SETUPU AI
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
            for item
            in results
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


        quality = (
            validate_market_data(
                base_results
            )
        )


    except Exception as error:

        logger.exception(
            "Błąd kontroli danych: %s",
            error,
        )


        return {
            "status": "ERROR",
            "direction": "NONE",
            "score": 0,

            "long_pct": 0,
            "short_pct": 0,
            "wait_pct": 100,

            "wait_reason": "NONE",

            "message": (
                "Błąd danych"
            ),
        }


    if not quality[
        "ok"
    ]:

        return {
            "status": "WAIT",

            "direction": (
                signal[
                    "side"
                ]
                if signal
                else "NONE"
            ),

            "score": 0,

            "long_pct": 0,
            "short_pct": 0,
            "wait_pct": 100,

            "wait_reason": "NONE",

            "message": (
                "Błąd jakości danych"
            ),
        }


    current_price = (
        quality[
            "current_price"
        ]
    )


    if (
        signal
        and not monitoring
        and signal.get(
            "strategy_entry"
        )
    ):

        strategy_price = (
            signal[
                "strategy_entry"
            ]
        )


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

                "direction": (
                    signal[
                        "side"
                    ]
                ),

                "score": 0,

                "long_pct": 0,
                "short_pct": 0,
                "wait_pct": 100,

                "wait_reason": (
                    "WAIT_FOR_RETEST"
                ),

                "price": current_price,

                "reason": (
                    "Cena oddaliła się od sygnału."
                ),

                "message": (
                    "Różnica ceny"
                ),
            }


    market_data = (
        format_market_data(
            results
        )
    )


    if autonomous:

        source_text = (
            "AUTOMATYCZNY SKAN RYNKU."
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

        wait_hint = (
            prefilter.get(
                "wait_reason",
                "NONE",
            )
            if prefilter
            else "NONE"
        )

        sweep_text = (
            prefilter.get(
                "sweep",
                "NONE",
            )
            if prefilter
            else "NONE"
        )

        level_price = (
            prefilter.get(
                "level_price"
            )
            if prefilter
            else None
        )

        level_type = (
            prefilter.get(
                "level_type",
                "NONE",
            )
            if prefilter
            else "NONE"
        )


    else:

        source_text = (
            "SYGNAŁ STRATEGII TRADINGVIEW."
        )

        direction_text = (
            signal[
                "side"
            ]
            if signal
            else "NONE"
        )

        candidate_text = (
            "STRATEGY_SIGNAL"
        )

        wait_hint = "NONE"
        sweep_text = "NONE"
        level_price = None
        level_type = "NONE"


    prompt = f"""
{source_text}

KANDYDAT:
{candidate_text}

KIERUNEK BAZOWY:
{direction_text}

CENA:
{current_price}

WAIT PREFILTER:
{wait_hint}

SWEEP:
{sweep_text}

POZIOM:
{level_type} {level_price}

DANE:

{market_data}


KOLEJNOŚĆ:

H1
→ ważny poziom
→ sweep / break
→ M15
→ retest
→ M5
→ ENTRY


H1 = kontekst.

M15 = główny setup.

M5 = timing.

M1 nie decyduje.


Jeżeli to sygnał TradingView,
nie oceniaj kierunku od zera.

Strategia podała hipotezę bazową.

Masz szukać:

- potwierdzenia,

- konkretnego warunku wejścia,

- albo mocnego powodu anulowania.


Jeżeli H1 jest jeszcze w starym trendzie,
ale M15 realnie zmienia strukturę
i M5 potwierdza momentum,
nie blokuj dobrego reversal.


Jeśli nie ma ENTRY,
podaj jeden konkretny powód:

WAIT_FOR_BREAK
WAIT_FOR_RETEST
WAIT_FOR_M5
WAIT_FOR_RR
INVALIDATED
"""


    instructions = f"""
Jesteś Trading AI Analyzer V4.2.

Odpowiadaj po polsku.

Nie wymuszaj wejścia.

Nie blokuj dobrego setupu
tylko dlatego,
że jeden interwał jest spóźniony.

Najważniejsze:

- zachowanie ceny,
- ważny poziom,
- M15,
- M5,
- kontekst H1.


LONG_PCT + SHORT_PCT + WAIT_PCT
muszą dawać dokładnie 100.


ENTRY preferowane od:

{MIN_ENTRY_PERCENT}%


Minimalne R:R:

1:{MIN_ENTRY_RR:.2f}


STATUS:

ENTRY
WAIT
SETUP
REVERSAL
SKIP
INVALIDATED
NONE


WAIT_REASON:

READY
WAIT_FOR_BREAK
WAIT_FOR_RETEST
WAIT_FOR_M5
WAIT_FOR_RR
INVALIDATED
NONE


FORMAT:

STATUS=WAIT
DIRECTION=LONG
SCORE=68
LONG_PCT=58
SHORT_PCT=12
WAIT_PCT=30
WAIT_REASON=WAIT_FOR_BREAK
PRICE=4441.30
ENTRY=0
ACTIVATION=4449.00
ACTIVATION_SIDE=ABOVE
SL=4431.00
TP1=4465.00
TP2=4480.00
INVALIDATION=4431.00
RR=1:1.8
REASON=Czekamy na wybicie oporu.


ENTRY=0 jeśli nie ma wejścia.

SL ma wynikać ze struktury.

TP1 i TP2 mają wynikać
z ważnych poziomów.

TP1 i TP2 są planem początkowym.
Później TRADER może prowadzić
pozycję dalej.

Jedno krótkie REASON.
"""


    try:

        response = (
            client.responses.create(
                model="gpt-5-mini",
                instructions=instructions,
                input=prompt,
            )
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

            "long_pct": 0,
            "short_pct": 0,
            "wait_pct": 100,

            "wait_reason": "NONE",

            "message": (
                "Błąd AI"
            ),
        }


    result = (
        parse_ai_meta(
            answer
        )
    )


    if not result.get(
        "price"
    ):
        result[
            "price"
        ] = current_price


    if (
        result[
            "status"
        ]
        == "ENTRY"
    ):

        if (
            result[
                "direction"
            ]
            == "LONG"
        ):

            strength = (
                result[
                    "long_pct"
                ]
            )


        elif (
            result[
                "direction"
            ]
            == "SHORT"
        ):

            strength = (
                result[
                    "short_pct"
                ]
            )


        else:
            strength = 0


        if (
            strength
            < MIN_ENTRY_PERCENT
        ):

            result[
                "status"
            ] = "WAIT"

            result[
                "wait_reason"
            ] = (
                "WAIT_FOR_M5"
            )


        entry = (
            result.get(
                "entry"
            )
            or current_price
        )


        calculated_rr = (
            calculate_rr(
                result[
                    "direction"
                ],
                entry,
                result.get(
                    "sl"
                ),
                result.get(
                    "tp1"
                ),
            )
        )


        if (
            calculated_rr
            is not None
            and calculated_rr
            < MIN_ENTRY_RR
        ):

            result[
                "status"
            ] = "WAIT"

            result[
                "wait_reason"
            ] = (
                "WAIT_FOR_RR"
            )


    return result


# =========================================================
# FORMAT SETUPU
# =========================================================

def format_compact_signal(
    result,
):
    status = result.get(
        "status",
        "NONE",
    )

    direction = result.get(
        "direction",
        "NONE",
    )

    price = result.get(
        "price"
    )

    long_pct = result.get(
        "long_pct",
        0,
    )

    short_pct = result.get(
        "short_pct",
        0,
    )

    wait_pct = result.get(
        "wait_pct",
        0,
    )

    wait_reason = result.get(
        "wait_reason",
        "NONE",
    )


    wait_labels = {
        "READY": "GOTOWY",

        "WAIT_FOR_BREAK": (
            "CZEKAJ NA WYBICIE"
        ),

        "WAIT_FOR_RETEST": (
            "CZEKAJ NA RETEST"
        ),

        "WAIT_FOR_M5": (
            "CZEKAJ NA M5"
        ),

        "WAIT_FOR_RR": (
            "CZEKAJ — SŁABE R:R"
        ),

        "INVALIDATED": (
            "SETUP ZANEGOWANY"
        ),

        "NONE": "CZEKAJ",
    }


    if status == "ENTRY":
        icon = "🚨"

    elif status == "REVERSAL":
        icon = "🔄"

    elif status in (
        "INVALIDATED",
        "SKIP",
    ):
        icon = "❌"

    else:
        icon = "📡"


    lines = [
        f"{icon} XAUUSD | H1 + M15"
    ]


    if price is not None:
        lines.append(
            f"Cena: {price:.2f}"
        )


    lines.extend(
        [
            "",

            f"🟢 LONG: {long_pct}%",

            f"🔴 SHORT: {short_pct}%",

            f"⚪ CZEKAJ: {wait_pct}%",

            "",
        ]
    )


    if status == "ENTRY":

        lines.append(
            f"✅ DECYZJA: {direction}"
        )


    elif status == "REVERSAL":

        lines.append(
            f"🔄 REVERSAL: {direction}"
        )


    elif status in (
        "INVALIDATED",
        "SKIP",
    ):

        lines.append(
            "❌ DECYZJA: POMIŃ"
        )


    else:

        lines.append(
            f"⏳ "
            f"{wait_labels.get(wait_reason, 'CZEKAJ')}"
        )


    entry = result.get(
        "entry"
    )

    activation = result.get(
        "activation"
    )

    activation_side = result.get(
        "activation_side",
        "NONE",
    )


    if (
        status == "ENTRY"
        and entry
        and entry > 0
    ):

        lines.append(
            f"📍 ENTRY: {entry:.2f}"
        )


    elif (
        activation
        and activation > 0
        and direction != "NONE"
    ):

        if (
            activation_side
            == "ABOVE"
        ):
            sign = ">"

        elif (
            activation_side
            == "BELOW"
        ):
            sign = "<"

        else:
            sign = "@"


        lines.append(
            f"📍 Aktywacja {direction}: "
            f"{sign} {activation:.2f}"
        )


    for label, key, icon_text in (
        (
            "SL",
            "sl",
            "🛑",
        ),
        (
            "TP1",
            "tp1",
            "🎯",
        ),
        (
            "TP2",
            "tp2",
            "🎯",
        ),
    ):

        value = (
            result.get(
                key
            )
        )


        if (
            value
            and value > 0
            and direction != "NONE"
        ):

            lines.append(
                f"{icon_text} "
                f"{label}: "
                f"{value:.2f}"
            )


    invalidation = (
        result.get(
            "invalidation"
        )
    )


    if (
        invalidation
        and invalidation > 0
        and direction != "NONE"
    ):

        lines.append(
            f"❌ Zanegowanie: "
            f"{invalidation:.2f}"
        )


    if result.get(
        "rr"
    ):

        lines.append(
            f"📊 R:R: "
            f"{result['rr']}"
        )


    reason = (
        result.get(
            "reason"
        )
    )


    if (
        reason
        and reason.upper()
        != "NONE"
    ):

        lines.append("")

        lines.append(
            f"💬 {reason}"
        )


    return "\n".join(
        lines
    )


# =========================================================
# ACTIVE TRADE HELPERY V4.2
# =========================================================

def trade_r_multiple(
    direction,
    entry,
    original_sl,
    price,
):
    if (
        entry is None
        or original_sl is None
        or price is None
    ):
        return 0.0


    risk = abs(
        entry
        - original_sl
    )


    if risk <= 0:
        return 0.0


    if direction == "LONG":

        move = (
            price
            - entry
        )


    elif direction == "SHORT":

        move = (
            entry
            - price
        )


    else:
        return 0.0


    return (
        move / risk
    )


def active_trade_hit_sl(
    trade,
    price,
):
    sl = trade.get(
        "current_sl"
    )


    if (
        sl is None
        or price is None
    ):
        return False


    if (
        trade[
            "direction"
        ]
        == "LONG"
    ):
        return (
            price <= sl
        )


    return (
        price >= sl
    )


def get_active_trade_timeframes(
    results,
):
    by_tf = (
        get_by_interval(
            results
        )
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


    return (
        h1,
        m15,
        m5,
    )


def get_structure_trailing_sl(
    trade,
    results,
    current_price,
):
    h1, m15, m5 = (
        get_active_trade_timeframes(
            results
        )
    )


    if not m15:
        return None


    direction = (
        trade[
            "direction"
        ]
    )

    current_sl = (
        trade.get(
            "current_sl"
        )
    )

    entry = (
        trade[
            "entry"
        ]
    )

    support = (
        m15.get(
            "support"
        )
    )

    resistance = (
        m15.get(
            "resistance"
        )
    )


    buffer_value = (
        current_price
        * TRAIL_BUFFER_PERCENT
        / 100
    )


    # =====================================================
    # LONG
    # =====================================================

    if direction == "LONG":

        if support is None:
            return None


        proposed_sl = (
            support
            - buffer_value
        )


        if trade.get(
            "protected",
            False,
        ):

            proposed_sl = max(
                proposed_sl,
                entry,
            )


        if (
            proposed_sl
            >= current_price
        ):
            return None


        if (
            current_sl is not None
            and proposed_sl
            <= current_sl
        ):
            return None


        return proposed_sl


    # =====================================================
    # SHORT
    # =====================================================

    if direction == "SHORT":

        if resistance is None:
            return None


        proposed_sl = (
            resistance
            + buffer_value
        )


        if trade.get(
            "protected",
            False,
        ):

            proposed_sl = min(
                proposed_sl,
                entry,
            )


        if (
            proposed_sl
            <= current_price
        ):
            return None


        if (
            current_sl is not None
            and proposed_sl
            >= current_sl
        ):
            return None


        return proposed_sl


    return None


# =========================================================
# PARSER TRADER AI
# =========================================================

def parse_active_trade_ai(
    answer,
):
    upper = (
        answer.upper()
    )


    status_match = re.search(
        r"TRADE_STATUS\s*=\s*"
        r"(HOLD|PROTECT|"
        r"TAKE_PROFIT_PARTIAL|"
        r"TRAIL|EXIT_EARLY|"
        r"INVALIDATED)",
        upper,
    )


    move_sl_match = re.search(
        r"MOVE_SL_TO\s*=\s*"
        r"([0-9.,]+)",
        answer,
        re.IGNORECASE,
    )


    partial_match = re.search(
        r"TAKE_PARTIAL_PCT\s*=\s*"
        r"([0-9]{1,3})",
        upper,
    )


    reason_match = re.search(
        r"REASON\s*=\s*(.+)",
        answer,
        re.IGNORECASE,
    )


    status = (
        status_match.group(1)
        if status_match
        else "HOLD"
    )


    move_sl_to = None


    if move_sl_match:

        try:
            value = float(
                move_sl_match.group(1).replace(
                    ",",
                    ".",
                )
            )


            if value > 0:
                move_sl_to = value


        except Exception:
            move_sl_to = None


    partial_pct = 0


    if partial_match:

        try:
            partial_pct = int(
                partial_match.group(1)
            )

        except Exception:
            partial_pct = 0


    partial_pct = max(
        0,
        min(
            partial_pct,
            100,
        ),
    )


    reason = (
        reason_match.group(1).strip()
        if reason_match
        else ""
    )


    if len(reason) > 200:
        reason = (
            reason[:200]
        )


    return {
        "trade_status": status,

        "move_sl_to": (
            move_sl_to
        ),

        "take_partial_pct": (
            partial_pct
        ),

        "reason": reason,
    }


# =========================================================
# ACTIVE TRADE AI V4.2
# =========================================================

def analyze_active_trade_ai(
    trade,
    results,
):
    try:

        base_results = [
            item
            for item
            in results
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


        quality = (
            validate_market_data(
                base_results
            )
        )


        if not quality[
            "ok"
        ]:

            return {
                "trade_status": "HOLD",

                "move_sl_to": None,

                "take_partial_pct": 0,

                "reason": (
                    "Dane niepełne — "
                    "pozycja bez zmian."
                ),

                "price": None,

                "r_multiple": 0,
            }


        current_price = (
            quality[
                "current_price"
            ]
        )


    except Exception as error:

        logger.exception(
            "ACTIVE TRADE dane: %s",
            error,
        )


        return {
            "trade_status": "HOLD",

            "move_sl_to": None,

            "take_partial_pct": 0,

            "reason": (
                "Błąd danych — "
                "bez zmiany pozycji."
            ),

            "price": None,

            "r_multiple": 0,
        }


    r_multiple = (
        trade_r_multiple(
            trade[
                "direction"
            ],
            trade[
                "entry"
            ],
            trade[
                "original_sl"
            ],
            current_price,
        )
    )


    h1, m15, m5 = (
        get_active_trade_timeframes(
            results
        )
    )


    h1_trend = (
        h1.get(
            "trend"
        )
        if h1
        else "NONE"
    )


    m15_trend = (
        m15.get(
            "trend"
        )
        if m15
        else "NONE"
    )


    m15_rsi = (
        m15.get(
            "rsi"
        )
        if m15
        else None
    )


    m15_hist = (
        m15.get(
            "histogram"
        )
        if m15
        else None
    )


    m5_rsi = (
        m5.get(
            "rsi"
        )
        if m5
        else None
    )


    m5_hist = (
        m5.get(
            "histogram"
        )
        if m5
        else None
    )


    market_data = (
        format_market_data(
            results
        )
    )


    prompt = f"""
AKTYWNA POZYCJA XAUUSD

KIERUNEK:
{trade['direction']}

ENTRY:
{trade['entry']}

ORIGINAL_SL:
{trade['original_sl']}

CURRENT_SL:
{trade['current_sl']}

TP1 PIERWOTNY:
{trade.get('tp1')}

TP2 PIERWOTNY:
{trade.get('tp2')}

AKTUALNA CENA:
{current_price}

WYNIK:
{r_multiple:.2f}R

MAX R:
{trade.get('max_r', 0):.2f}R

PROTECTED:
{trade.get('protected', False)}

PARTIAL_TAKEN:
{trade.get('partial_taken', False)}

H1 TREND:
{h1_trend}

M15 TREND:
{m15_trend}

M15 RSI:
{m15_rsi}

M15 HISTOGRAM:
{m15_hist}

M5 RSI:
{m5_rsi}

M5 HISTOGRAM:
{m5_hist}


DANE:

{market_data}


========================================
NAJWAŻNIEJSZA ZASADA
========================================

TP1 i TP2 z pierwotnego setupu
NIE KOŃCZĄ automatycznie pozycji.

Jeżeli H1/M15 nadal wspierają trend,
pozwól pozycji rosnąć.


H1 = kontekst.

M15 = główna struktura prowadzenia.

M5 = ostrzeżenie / momentum.


Nie wychodź przez jedną słabą M5.


HOLD:
trend żyje.

PROTECT:
zabezpiecz SL.

TAKE_PROFIT_PARTIAL:
weź część zysku,
ale pozycja nadal żyje.

TRAIL:
prowadź SL po strukturze M15.

EXIT_EARLY:
M15 realnie się psuje
i cena przestaje zachowywać się
zgodnie z setupem.

INVALIDATED:
setup został wyraźnie zanegowany.


Jeżeli pozycja ma już kilka R,
nie zamykaj jej tylko dlatego,
że osiągnęła dawny TP1 lub TP2.

Jeżeli trend jest mocny:
HOLD albo TRAIL.
"""


    instructions = """
Jesteś modułem TRADER
Trading AI Analyzer V4.2.

Prowadzisz aktywną pozycję.

Masz pozwalać dużym trendom rosnąć.

Nie zamykaj pozycji
na sztywnym TP,
jeżeli M15/H1 nadal wspierają trend.

Dozwolone decyzje:

HOLD
PROTECT
TAKE_PROFIT_PARTIAL
TRAIL
EXIT_EARLY
INVALIDATED


HOLD:
pozycja zachowuje się prawidłowo.

PROTECT:
zabezpiecz SL.

TAKE_PROFIT_PARTIAL:
zrealizuj część,
ale reszta nadal jest aktywna.

TRAIL:
przesuń SL po strukturze.

EXIT_EARLY:
wyjdź wcześniej,
gdy struktura realnie się psuje.

INVALIDATED:
pierwotny setup już nie istnieje.


M15 jest ważniejsze od M5.


FORMAT:

TRADE_STATUS=HOLD
MOVE_SL_TO=0
TAKE_PARTIAL_PCT=0
REASON=M15 nadal utrzymuje trend.


MOVE_SL_TO=0
jeśli SL bez zmian.

TAKE_PARTIAL_PCT:
0
25
30
50
75

Nie używaj 100
dla TAKE_PROFIT_PARTIAL.

REASON:
jedno krótkie zdanie.
"""


    try:

        response = (
            client.responses.create(
                model="gpt-5-mini",
                instructions=instructions,
                input=prompt,
            )
        )


        answer = (
            response.output_text
            or ""
        )


        result = (
            parse_active_trade_ai(
                answer
            )
        )


    except Exception as error:

        logger.exception(
            "ACTIVE TRADE AI: %s",
            error,
        )


        result = {
            "trade_status": "HOLD",

            "move_sl_to": None,

            "take_partial_pct": 0,

            "reason": (
                "Błąd AI — bez zmiany."
            ),
        }


    result[
        "price"
    ] = current_price


    result[
        "r_multiple"
    ] = r_multiple


    # =====================================================
    # +1R → minimum BE
    # =====================================================

    if (
        r_multiple
        >= PROTECT_AT_R
        and not trade.get(
            "protected",
            False,
        )
        and result[
            "trade_status"
        ]
        in (
            "HOLD",
            "PROTECT",
            "TRAIL",
        )
    ):

        result[
            "trade_status"
        ] = "PROTECT"


        result[
            "move_sl_to"
        ] = (
            trade[
                "entry"
            ]
        )


        result[
            "reason"
        ] = (
            f"Pozycja ma "
            f"{r_multiple:.2f}R — "
            "zabezpieczamy minimum BE."
        )


    # =====================================================
    # +2R → pierwszy partial
    # =====================================================

    if (
        r_multiple
        >= FIRST_PARTIAL_AT_R
        and not trade.get(
            "partial_taken",
            False,
        )
        and result[
            "trade_status"
        ]
        not in (
            "EXIT_EARLY",
            "INVALIDATED",
        )
    ):

        result[
            "trade_status"
        ] = (
            "TAKE_PROFIT_PARTIAL"
        )


        result[
            "take_partial_pct"
        ] = (
            FIRST_PARTIAL_PERCENT
        )


        result[
            "reason"
        ] = (
            f"Pozycja ma "
            f"{r_multiple:.2f}R — "
            f"rozważ realizację "
            f"{FIRST_PARTIAL_PERCENT}%, "
            "reszta nadal w trendzie."
        )


    # =====================================================
    # TRAIL PO M15
    # =====================================================

    if (
        r_multiple
        >= TRAIL_START_R
        and trade.get(
            "protected",
            False,
        )
        and result[
            "trade_status"
        ]
        not in (
            "EXIT_EARLY",
            "INVALIDATED",
        )
    ):

        structure_sl = (
            get_structure_trailing_sl(
                trade,
                results,
                current_price,
            )
        )


        if (
            structure_sl
            is not None
        ):

            result[
                "trade_status"
            ] = "TRAIL"


            result[
                "move_sl_to"
            ] = (
                structure_sl
            )


            result[
                "reason"
            ] = (
                f"Trend nadal żyje "
                f"({r_multiple:.2f}R) — "
                "prowadzimy SL po M15."
            )


    return result


# =========================================================
# WALIDACJA NOWEGO SL
# =========================================================

def sanitize_new_sl(
    trade,
    suggested_sl,
    current_price,
):
    if (
        suggested_sl is None
        or suggested_sl <= 0
    ):
        return None


    current_sl = (
        trade.get(
            "current_sl"
        )
    )


    # LONG
    if (
        trade[
            "direction"
        ]
        == "LONG"
    ):

        if (
            suggested_sl
            >= current_price
        ):
            return None


        if (
            current_sl is not None
            and suggested_sl
            <= current_sl
        ):
            return None


    # SHORT
    else:

        if (
            suggested_sl
            <= current_price
        ):
            return None


        if (
            current_sl is not None
            and suggested_sl
            >= current_sl
        ):
            return None


    return suggested_sl


# =========================================================
# FORMAT ACTIVE TRADE
# =========================================================

def format_active_trade_message(
    trade,
    result,
):
    status = (
        result[
            "trade_status"
        ]
    )

    price = (
        result.get(
            "price"
        )
    )

    r_multiple = (
        result.get(
            "r_multiple",
            0,
        )
    )


    if status == "HOLD":

        icon = "🟢"
        title = "HOLD"


    elif status == "PROTECT":

        icon = "🟠"
        title = "PROTECT"


    elif status == "TAKE_PROFIT_PARTIAL":

        icon = "💰"
        title = (
            "TAKE PROFIT PARTIAL"
        )


    elif status == "TRAIL":

        icon = "🔒"
        title = "TRAIL"


    elif status == "EXIT_EARLY":

        icon = "⚠️"
        title = "EXIT EARLY"


    else:

        icon = "❌"
        title = "INVALIDATED"


    lines = [
        f"{icon} XAUUSD ACTIVE TRADE",

        "",

        f"Pozycja: "
        f"{trade['direction']}",

        f"ENTRY: "
        f"{trade['entry']:.2f}",
    ]


    if price is not None:

        lines.append(
            f"Cena: {price:.2f}"
        )


    lines.append(
        f"Wynik: "
        f"{r_multiple:.2f}R"
    )


    lines.append("")


    lines.append(
        f"➡️ {title}"
    )


    new_sl = (
        result.get(
            "move_sl_to"
        )
    )


    if (
        new_sl
        and new_sl > 0
    ):

        lines.append(
            f"🛑 Nowy SL: "
            f"{new_sl:.2f}"
        )


    partial = (
        result.get(
            "take_partial_pct",
            0,
        )
    )


    if partial > 0:

        lines.append(
            f"💰 Częściowa realizacja: "
            f"{partial}%"
        )


    if status in (
        "HOLD",
        "PROTECT",
        "TAKE_PROFIT_PARTIAL",
        "TRAIL",
    ):

        lines.append(
            "📈 Reszta pozycji: "
            "prowadzona po M15"
        )


    reason = (
        result.get(
            "reason"
        )
    )


    if (
        reason
        and reason.upper()
        != "NONE"
    ):

        lines.append("")

        lines.append(
            f"💬 {reason}"
        )


    return "\n".join(
        lines
    )


# =========================================================
# START ACTIVE TRADE
# =========================================================

def start_active_trade(
    result,
    source="AI",
):
    direction = (
        result.get(
            "direction"
        )
    )


    if direction not in (
        "LONG",
        "SHORT",
    ):
        return


    entry = (
        result.get(
            "entry"
        )
        or result.get(
            "price"
        )
    )


    sl = (
        result.get(
            "sl"
        )
    )


    if (
        entry is None
        or sl is None
    ):

        logger.warning(
            "ACTIVE TRADE brak ENTRY/SL."
        )

        return


    symbol = "XAUUSD"


    with active_trade_lock:

        if (
            symbol
            in active_trades
        ):

            logger.info(
                "ACTIVE TRADE już istnieje."
            )

            return


        trade_id = (
            str(
                uuid.uuid4()
            )
        )


        active_trades[
            symbol
        ] = {

            "id": trade_id,

            "symbol": symbol,

            "direction": direction,

            "entry": float(
                entry
            ),

            "original_sl": float(
                sl
            ),

            "current_sl": float(
                sl
            ),

            # TP informacyjne.
            # Nie kończą już pozycji.
            "tp1": result.get(
                "tp1"
            ),

            "tp2": result.get(
                "tp2"
            ),

            "started": time.time(),

            "source": source,

            "protected": False,

            "partial_taken": False,

            "last_status": None,

            "max_r": 0.0,
        }


    send_telegram_message(
        "🤖 TRADER V4.2 AKTYWNY\n\n"
        f"XAUUSD {direction}\n"
        f"ENTRY: {entry:.2f}\n"
        f"SL: {sl:.2f}\n\n"
        "TP1/TP2 nie kończą już "
        "automatycznie pozycji.\n"
        "Bot prowadzi trend po M15."
    )


    thread = threading.Thread(
        target=monitor_active_trade,
        args=(
            symbol,
            trade_id,
        ),
        daemon=True,
    )


    thread.start()


# =========================================================
# CLOSE ACTIVE TRADE
# =========================================================

def close_active_trade(
    symbol,
    reason=None,
):
    with active_trade_lock:

        trade = (
            active_trades.pop(
                symbol,
                None,
            )
        )


    if trade:

        logger.info(
            "ACTIVE TRADE zakończony: %s",
            reason,
        )


# =========================================================
# MONITOR ACTIVE TRADE V4.2
# =========================================================

def monitor_active_trade(
    symbol,
    trade_id,
):
    logger.info(
        "ACTIVE TRADE V4.2 start: %s",
        trade_id,
    )


    for check_number in range(
        1,
        ACTIVE_TRADE_MAX_CHECKS + 1,
    ):

        time.sleep(
            ACTIVE_TRADE_INTERVAL_SECONDS
        )


        with active_trade_lock:

            trade = (
                active_trades.get(
                    symbol
                )
            )


            if (
                not trade
                or trade.get(
                    "id"
                )
                != trade_id
            ):
                return


            trade_snapshot = dict(
                trade
            )


        try:

            results = (
                build_full_market_analysis()
            )


            result = (
                analyze_active_trade_ai(
                    trade_snapshot,
                    results,
                )
            )


        except Exception as error:

            logger.exception(
                "ACTIVE TRADE monitor: %s",
                error,
            )

            continue


        price = (
            result.get(
                "price"
            )
        )


        if price is None:
            continue


        r_multiple = (
            result.get(
                "r_multiple",
                0,
            )
        )


        # =================================================
        # MAX R
        # =================================================

        with active_trade_lock:

            current = (
                active_trades.get(
                    symbol
                )
            )


            if (
                current
                and current[
                    "id"
                ]
                == trade_id
            ):

                current[
                    "max_r"
                ] = max(
                    current.get(
                        "max_r",
                        0,
                    ),
                    r_multiple,
                )


        # =================================================
        # AKTUALNY SL
        # =================================================

        if (
            active_trade_hit_sl(
                trade_snapshot,
                price,
            )
        ):

            logger.info(
                "ACTIVE TRADE SL "
                "price=%.2f sl=%.2f",
                price,
                trade_snapshot[
                    "current_sl"
                ],
            )


            # Nie wysyłamy osobnego
            # powiadomienia o SL.
            close_active_trade(
                symbol,
                "CURRENT_SL",
            )

            return


        # =================================================
        # WAŻNE:
        #
        # NIE MA JUŻ TP2 → CLOSE.
        #
        # TP1 i TP2 nie kończą pozycji.
        # =================================================

        status = (
            result[
                "trade_status"
            ]
        )


        new_sl = (
            sanitize_new_sl(
                trade_snapshot,
                result.get(
                    "move_sl_to"
                ),
                price,
            )
        )


        # =================================================
        # NOWY SL
        # =================================================

        if new_sl is not None:

            result[
                "move_sl_to"
            ] = new_sl


            with active_trade_lock:

                current = (
                    active_trades.get(
                        symbol
                    )
                )


                if (
                    current
                    and current[
                        "id"
                    ]
                    == trade_id
                ):

                    current[
                        "current_sl"
                    ] = new_sl


                    if (
                        current[
                            "direction"
                        ]
                        == "LONG"
                        and new_sl
                        >= current[
                            "entry"
                        ]
                    ):

                        current[
                            "protected"
                        ] = True


                    elif (
                        current[
                            "direction"
                        ]
                        == "SHORT"
                        and new_sl
                        <= current[
                            "entry"
                        ]
                    ):

                        current[
                            "protected"
                        ] = True


        # =================================================
        # PARTIAL
        # =================================================

        partial = (
            result.get(
                "take_partial_pct",
                0,
            )
        )


        if (
            partial > 0
            and not trade_snapshot.get(
                "partial_taken",
                False,
            )
        ):

            with active_trade_lock:

                current = (
                    active_trades.get(
                        symbol
                    )
                )


                if (
                    current
                    and current[
                        "id"
                    ]
                    == trade_id
                ):

                    current[
                        "partial_taken"
                    ] = True


        # =================================================
        # POPRZEDNI STATUS
        # =================================================

        with active_trade_lock:

            current = (
                active_trades.get(
                    symbol
                )
            )


            if not current:
                return


            previous_status = (
                current.get(
                    "last_status"
                )
            )


            current[
                "last_status"
            ] = status


            current_max_r = (
                current.get(
                    "max_r",
                    0,
                )
            )


        # =================================================
        # HOLD
        # =================================================

        if status == "HOLD":

            logger.info(
                "ACTIVE TRADE HOLD "
                "check=%s R=%.2f maxR=%.2f",
                check_number,
                r_multiple,
                current_max_r,
            )

            continue


        # =================================================
        # PROTECT / PARTIAL / TRAIL
        # =================================================

        if status in (
            "PROTECT",
            "TAKE_PROFIT_PARTIAL",
            "TRAIL",
        ):

            should_notify = False


            if (
                status
                != previous_status
            ):
                should_notify = True


            if new_sl is not None:
                should_notify = True


            if (
                partial > 0
                and not trade_snapshot.get(
                    "partial_taken",
                    False,
                )
            ):
                should_notify = True


            if should_notify:

                send_telegram_message(
                    format_active_trade_message(
                        trade_snapshot,
                        result,
                    )
                )


            # Pozycja nadal aktywna.
            continue


        # =================================================
        # EXIT / INVALIDATED
        # =================================================

        if status in (
            "EXIT_EARLY",
            "INVALIDATED",
        ):

            send_telegram_message(
                format_active_trade_message(
                    trade_snapshot,
                    result,
                )
            )


            close_active_trade(
                symbol,
                status,
            )

            return


    logger.warning(
        "ACTIVE TRADE osiągnął MAX_CHECKS."
    )


    close_active_trade(
        symbol,
        "MAX_CHECKS",
    )


# =========================================================
# AUTO ALERT
# =========================================================

def auto_alert_text(
    result,
):
    if (
        result[
            "status"
        ]
        not in (
            "ENTRY",
            "WAIT",
            "SETUP",
            "REVERSAL",
            "SKIP",
            "INVALIDATED",
        )
    ):
        return None


    return (
        format_compact_signal(
            result
        )
    )


def should_send_auto_alert(
    result,
):
    status = (
        result[
            "status"
        ]
    )

    direction = (
        result[
            "direction"
        ]
    )

    score = (
        result[
            "score"
        ]
    )


    if status not in (
        "SETUP",
        "REVERSAL",
        "ENTRY",
        "SKIP",
        "INVALIDATED",
    ):
        return False


    if (
        status
        in (
            "SETUP",
            "REVERSAL",
        )
        and score
        < MIN_SETUP_SCORE
    ):
        return False


    if direction == "LONG":

        strength = (
            result.get(
                "long_pct",
                0,
            )
            // 10
        )


    elif direction == "SHORT":

        strength = (
            result.get(
                "short_pct",
                0,
            )
            // 10
        )


    else:
        strength = 0


    key = (
        f"{status}|"
        f"{direction}|"
        f"{strength}|"
        f"{result.get('wait_reason')}"
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
# AUTO SCAN
# =========================================================

def auto_scan_once():
    try:

        if rate_limit_active():

            logger.info(
                "AUTO SCAN: backoff."
            )

            return


        # Jeśli TRADER już prowadzi XAUUSD,
        # nie otwieramy drugiej pozycji.
        with active_trade_lock:

            if (
                "XAUUSD"
                in active_trades
            ):

                logger.info(
                    "AUTO SCAN: ACTIVE TRADE."
                )

                return


        results = (
            build_full_market_analysis()
        )


        candidate = (
            prefilter_market(
                results
            )
        )


        if not candidate[
            "candidate"
        ]:

            logger.info(
                "AUTO SCAN: brak kandydata."
            )

            return


        logger.info(
            "AUTO SCAN: kandydat %s %s "
            "wait=%s sweep=%s",

            candidate.get(
                "type"
            ),

            candidate.get(
                "direction"
            ),

            candidate.get(
                "wait_reason"
            ),

            candidate.get(
                "sweep"
            ),
        )


        result = (
            analyze_market_ai(
                results=results,
                autonomous=True,
                prefilter=candidate,
            )
        )


        logger.info(
            "AUTO SCAN AI: "
            "%s %s score=%s "
            "LONG=%s SHORT=%s WAIT=%s "
            "reason=%s",

            result[
                "status"
            ],

            result[
                "direction"
            ],

            result[
                "score"
            ],

            result.get(
                "long_pct"
            ),

            result.get(
                "short_pct"
            ),

            result.get(
                "wait_pct"
            ),

            result.get(
                "wait_reason"
            ),
        )


        if (
            should_send_auto_alert(
                result
            )
        ):

            text = (
                auto_alert_text(
                    result
                )
            )


            if text:

                send_telegram_message(
                    text
                )


        # =================================================
        # ENTRY → TRADER
        # =================================================

        if (
            result[
                "status"
            ]
            == "ENTRY"
        ):

            start_active_trade(
                result,
                source="AUTO",
            )


    except Exception as error:

        text = str(
            error
        )


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
        "AUTO SCANNER V4.2 uruchomiony."
    )


    time.sleep(
        30
    )


    while True:

        auto_scan_once()


        time.sleep(
            AUTO_SCAN_INTERVAL_SECONDS
        )


# =========================================================
# JEDEN SCANNER
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
                "AUTO SCANNER działa "
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
# MONITOR SETUPU STRATEGII
# =========================================================

def monitor_strategy_setup(
    signal,
    monitor_id,
):
    symbol = (
        signal[
            "symbol"
        ]
    )


    last_wait_reason = None


    for check_number in range(
        1,
        MONITOR_MAX_CHECKS + 1,
    ):

        time.sleep(
            MONITOR_INTERVAL_SECONDS
        )


        with monitor_lock:

            current = (
                active_monitors.get(
                    symbol
                )
            )


            if (
                not current
                or current.get(
                    "id"
                )
                != monitor_id
            ):
                return


        try:

            results = (
                build_full_market_analysis()
            )


            result = (
                analyze_market_ai(
                    results=results,
                    signal=signal,
                    monitoring=True,
                )
            )


        except Exception as error:

            logger.exception(
                "Monitor setupu: %s",
                error,
            )

            continue


        status = (
            result[
                "status"
            ]
        )


        wait_reason = (
            result.get(
                "wait_reason",
                "NONE",
            )
        )


        # =================================================
        # ENTRY → TRADER
        # =================================================

        if status == "ENTRY":

            send_telegram_message(
                format_compact_signal(
                    result
                )
            )


            with monitor_lock:

                active_monitors.pop(
                    symbol,
                    None,
                )


            start_active_trade(
                result,
                source=(
                    "TRADINGVIEW_MONITOR"
                ),
            )


            return


        # =================================================
        # INVALIDATED / SKIP
        # =================================================

        if status in (
            "INVALIDATED",
            "SKIP",
        ):

            send_telegram_message(
                format_compact_signal(
                    result
                )
            )


            with monitor_lock:

                active_monitors.pop(
                    symbol,
                    None,
                )


            return


        # =================================================
        # REVERSAL
        # =================================================

        if status == "REVERSAL":

            send_telegram_message(
                format_compact_signal(
                    result
                )
            )


        # =================================================
        # ZMIANA WAIT
        # =================================================

        elif (
            wait_reason
            != last_wait_reason
            and wait_reason
            not in (
                "NONE",
                "READY",
            )
        ):

            send_telegram_message(
                format_compact_signal(
                    result
                )
            )


        last_wait_reason = (
            wait_reason
        )


    with monitor_lock:

        active_monitors.pop(
            symbol,
            None,
        )


    send_telegram_message(
        "⌛ XAUUSD | H1 + M15\n\n"
        "Obserwacja zakończona.\n"
        "Brak pełnego potwierdzenia."
    )


# =========================================================
# TRADINGVIEW
# =========================================================

def process_alert(
    text,
):
    signal = (
        parse_alert(
            text
        )
    )


    logger.info(
        "TradingView: %s",
        signal,
    )


    # =====================================================
    # STOP LOSS TV
    # =====================================================

    if (
        signal[
            "event"
        ]
        == "STOP_LOSS"
    ):

        close_active_trade(
            signal[
                "symbol"
            ],
            "STOP_LOSS_TV",
        )

        return


    # =====================================================
    # TAKE PROFIT TV
    #
    # UWAGA:
    # Alert strategii może zakończyć jej własny trade.
    # Jeżeli chcesz, żeby TRADER prowadził pozycję
    # niezależnie od TP strategii, nie zamykamy tutaj.
    # =====================================================

    if (
        signal[
            "event"
        ]
        == "TAKE_PROFIT"
    ):

        logger.info(
            "TradingView TAKE PROFIT "
            "otrzymany — V4.2 nie kończy "
            "automatycznie ACTIVE TRADE."
        )

        return


    # =====================================================
    # ENTRY
    # =====================================================

    if (
        signal[
            "event"
        ]
        != "ENTRY"
    ):
        return


    if (
        signal[
            "symbol"
        ]
        != "XAUUSD"
    ):
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


    if (
        is_duplicate_strategy_alert(
            signal
        )
    ):
        return


    # Nie uruchamiamy drugiej pozycji,
    # gdy ACTIVE TRADE już działa.
    with active_trade_lock:

        if (
            "XAUUSD"
            in active_trades
        ):

            logger.info(
                "TradingView ENTRY pominięty: "
                "ACTIVE TRADE istnieje."
            )

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

        "🤖 V4.2 analizuje:\n"
        "H1 → poziom → M15 → M5"
    )


    try:

        results = (
            build_full_market_analysis()
        )


        result = (
            analyze_market_ai(
                results=results,
                signal=signal,
                monitoring=False,
            )
        )


    except Exception as error:

        logger.exception(
            "Błąd analizy alertu: %s",
            error,
        )


        send_telegram_message(
            "⚠️ Nie udało się pobrać danych."
        )

        return


    send_telegram_message(
        format_compact_signal(
            result
        )
    )


    status = (
        result[
            "status"
        ]
    )


    # =====================================================
    # ENTRY OD RAZU → ACTIVE TRADE
    # =====================================================

    if status == "ENTRY":

        start_active_trade(
            result,
            source="TRADINGVIEW",
        )

        return


    if status in (
        "SKIP",
        "INVALIDATED",
    ):
        return


    monitor_id = (
        str(
            uuid.uuid4()
        )
    )


    with monitor_lock:

        active_monitors[
            signal[
                "symbol"
            ]
        ] = {

            "id": monitor_id,

            "side": signal[
                "side"
            ],

            "started": time.time(),

            "strategy_entry": (
                signal.get(
                    "strategy_entry"
                )
            ),
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
        "GET"
    ],
)
def home():

    with active_trade_lock:

        trade_active = (
            "XAUUSD"
            in active_trades
        )


    return jsonify(
        {
            "status": "ok",

            "service": (
                "Trading AI Analyzer V4.2"
            ),

            "decision_flow": (
                "H1 -> LEVEL -> M15 "
                "-> RETEST -> M5 -> ENTRY "
                "-> ACTIVE_TRADE -> TRAIL"
            ),

            "trader": True,

            "active_trade": (
                trade_active
            ),

            "auto_scan_enabled": (
                AUTO_SCAN_ENABLED
            ),

            "auto_scan_interval": (
                AUTO_SCAN_INTERVAL_SECONDS
            ),

            "active_trade_interval": (
                ACTIVE_TRADE_INTERVAL_SECONDS
            ),

            "min_entry_percent": (
                MIN_ENTRY_PERCENT
            ),

            "min_entry_rr": (
                MIN_ENTRY_RR
            ),

            "protect_at_r": (
                PROTECT_AT_R
            ),

            "first_partial_at_r": (
                FIRST_PARTIAL_AT_R
            ),

            "first_partial_percent": (
                FIRST_PARTIAL_PERCENT
            ),

            "trail_start_r": (
                TRAIL_START_R
            ),

            "rate_limit_active": (
                rate_limit_active()
            ),
        }
    )


@app.route(
    "/health",
    methods=[
        "GET"
    ],
)
def health():

    with monitor_lock:

        monitors = list(
            active_monitors.keys()
        )


    with active_trade_lock:

        trades = {}


        for symbol, trade in (
            active_trades.items()
        ):

            trades[
                symbol
            ] = {

                "direction": trade.get(
                    "direction"
                ),

                "entry": trade.get(
                    "entry"
                ),

                "current_sl": trade.get(
                    "current_sl"
                ),

                "protected": trade.get(
                    "protected"
                ),

                "partial_taken": trade.get(
                    "partial_taken"
                ),

                "max_r": trade.get(
                    "max_r"
                ),
            }


    return jsonify(
        {
            "status": "healthy",

            "version": "V4.2",

            "active_monitors": (
                monitors
            ),

            "active_trades": (
                trades
            ),

            "auto_scanner": (
                scanner_started
            ),

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
    methods=[
        "POST"
    ],
)
def webhook():

    secret = request.args.get(
        "secret"
    )


    if secret != WEBHOOK_SECRET:

        return jsonify(
            {
                "error": (
                    "invalid secret"
                ),
            }
        ), 403


    try:

        if request.is_json:

            data = (
                request.get_json(
                    silent=True
                )
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

            text = (
                request.get_data(
                    as_text=True
                )
            )


    except Exception as error:

        logger.exception(
            "Webhook: %s",
            error,
        )


        return jsonify(
            {
                "error": (
                    "invalid request"
                ),
            }
        ), 400


    if (
        not text
        or not text.strip()
    ):

        return jsonify(
            {
                "error": (
                    "empty alert"
                ),
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
            "status": (
                "accepted"
            ),
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
