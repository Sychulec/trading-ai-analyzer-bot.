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
        }


    h1_trend = h1.get(
        "trend"
    )

    m15_bull = is_bullish(
        m15
    )

    m15_bear = is_bearish(
        m15
    )

    m5_bull = is_bullish(
        m5
    )

    m5_bear = is_bearish(
        m5
    )


    # H1 + M15
    if (
        h1_trend == "wzrostowy"
        and m15_bull
    ):
        return {
            "candidate": True,
            "type": "SETUP",
            "direction": "LONG",
        }


    if (
        h1_trend == "spadkowy"
        and m15_bear
    ):
        return {
            "candidate": True,
            "type": "SETUP",
            "direction": "SHORT",
        }


    # REVERSAL
    if (
        h1_trend == "spadkowy"
        and m15_bull
        and m5_bull
    ):
        return {
            "candidate": True,
            "type": "REVERSAL",
            "direction": "LONG",
        }


    if (
        h1_trend == "wzrostowy"
        and m15_bear
        and m5_bear
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
# PARSOWANIE AI
# =========================================================

def parse_ai_meta(answer):
    upper = answer.upper()

    status = "NONE"
    direction = "NONE"
    score = 0

    long_pct = 0
    short_pct = 0
    wait_pct = 100


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


    def get_float(pattern):
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


    score = get_int(
        r"SCORE\s*=\s*([0-9]{1,3})"
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


    price = get_float(
        r"PRICE\s*=\s*([0-9.,]+)"
    )

    entry = get_float(
        r"ENTRY\s*=\s*([0-9.,]+)"
    )

    activation = get_float(
        r"ACTIVATION\s*=\s*([0-9.,]+)"
    )

    sl = get_float(
        r"SL\s*=\s*([0-9.,]+)"
    )

    tp1 = get_float(
        r"TP1\s*=\s*([0-9.,]+)"
    )

    tp2 = get_float(
        r"TP2\s*=\s*([0-9.,]+)"
    )

    invalidation = get_float(
        r"INVALIDATION\s*=\s*([0-9.,]+)"
    )


    activation_side_match = re.search(
        r"ACTIVATION_SIDE\s*=\s*"
        r"(ABOVE|BELOW|NONE)",
        upper,
    )

    activation_side = (
        activation_side_match.group(1)
        if activation_side_match
        else "NONE"
    )


    rr_match = re.search(
        r"RR\s*=\s*([0-9.:]+)",
        answer,
        re.IGNORECASE,
    )

    rr = (
        rr_match.group(1)
        if rr_match
        else None
    )


    reason_match = re.search(
        r"REASON\s*=\s*(.+)",
        answer,
        re.IGNORECASE,
    )

    reason = (
        reason_match.group(1).strip()
        if reason_match
        else ""
    )


    if len(reason) > 180:
        reason = reason[:180]


    return {
        "status": status,
        "direction": direction,
        "score": score,

        "long_pct": long_pct,
        "short_pct": short_pct,
        "wait_pct": wait_pct,

        "price": price,
        "entry": entry,
        "activation": activation,
        "activation_side": activation_side,

        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,

        "invalidation": invalidation,
        "rr": rr,
        "reason": reason,

        "message": answer,
    }


# =========================================================
# AI
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
            "long_pct": 0,
            "short_pct": 0,
            "wait_pct": 100,
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
            "long_pct": 0,
            "short_pct": 0,
            "wait_pct": 100,
            "message": (
                "⚠️ Dane rynkowe "
                "nie przeszły kontroli."
            ),
        }


    current_price = quality[
        "current_price"
    ]


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
                "long_pct": 0,
                "short_pct": 0,
                "wait_pct": 100,
                "price": current_price,
                "message": (
                    "⚠️ Różnica cen."
                ),
            }


    market_data = format_market_data(
        results
    )


    if autonomous:
        source_text = (
            "AUTOMATYCZNY SKAN rynku. "
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
            "Analiza sygnału "
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

1. Czy H1 wspiera kierunek?

2. Czy cena znajduje się
przy ważnym poziomie?

3. Czy M15 daje prawdziwe
potwierdzenie struktury?

4. Czy M5 potwierdza timing
i momentum wejścia?

5. Jeśli cena nie zachowuje się
zgodnie z oczekiwaniem,
czy setup należy anulować?


HIERARCHIA:

D1 = szeroki kontekst.
H4 = większy kontekst.

H1 = GŁÓWNY KIERUNEK.
M15 = GŁÓWNY SETUP.
M5 = TIMING.

M1 NIE MOŻE DECYDOWAĆ
O KIERUNKU.

Nie wymagaj idealnej zgodności
D1/H4 z H1.

Nie wymuszaj wejścia.

Najważniejsza jest
struktura ceny.
"""


    instructions = """
Odpowiadaj po polsku.

Jesteś Trading AI Analyzer.

GŁÓWNE INTERWAŁY:

H1 = kierunek.
M15 = setup.
M5 = timing.

D1 i H4 = szerszy kontekst.

M1 nie może decydować
o kierunku transakcji.

Najważniejsze są:

- struktura ceny,
- ważny poziom,
- H1,
- M15,
- reakcja ceny,
- momentum M5.

Jeżeli H1 i M15 nie dają
wystarczającego potwierdzenia,
wybierz WAIT.

Oceń osobno:

LONG
SHORT
CZEKAJ

Procenty oznaczają siłę argumentów.
Nie oznaczają gwarancji zysku.

LONG_PCT + SHORT_PCT + WAIT_PCT
muszą dawać dokładnie 100.

ENTRY tylko wtedy,
gdy kierunek LONG lub SHORT
ma co najmniej około 70%
i H1 + M15 potwierdzają setup.

Jeżeli cena jest przy wsparciu
lub oporze i nie ma wybicia,
wybierz WAIT.


STATUS:

ENTRY
WAIT
SETUP
REVERSAL
SKIP
NONE


DIRECTION:

LONG
SHORT
NONE


ODPOWIEDŹ MUSI ZACZYNAĆ SIĘ TAK:

STATUS=WAIT
DIRECTION=SHORT
SCORE=62
LONG_PCT=25
SHORT_PCT=60
WAIT_PCT=15
PRICE=4355.11
ENTRY=0
ACTIVATION=4352.70
ACTIVATION_SIDE=BELOW
SL=4363.00
TP1=4340.00
TP2=4325.00
INVALIDATION=4363.00
RR=1:2.3
REASON=H1 spadkowe, ale M15 nie potwierdziło jeszcze wybicia.


ACTIVATION_SIDE:

ABOVE
BELOW
NONE

ENTRY=0,
jeśli nie ma wejścia teraz.

SL ma wynikać ze struktury.

TP1 i TP2 mają wynikać
z ważnych poziomów.

Nie podawaj TP3.

REASON maksymalnie jedno zdanie.

Nie wypisuj później osobno
D1, H4, H1, M15, M5, M1.

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
            "long_pct": 0,
            "short_pct": 0,
            "wait_pct": 100,
            "message": (
                "⚠️ Błąd analizy AI."
            ),
        }


    result = parse_ai_meta(
        answer
    )


    if not result.get(
        "price"
    ):
        result["price"] = (
            current_price
        )


    if result["status"] == "ENTRY":

        if result["direction"] == "LONG":
            direction_strength = result[
                "long_pct"
            ]

        elif result["direction"] == "SHORT":
            direction_strength = result[
                "short_pct"
            ]

        else:
            direction_strength = 0


        if (
            direction_strength
            < MIN_ENTRY_PERCENT
        ):
            logger.warning(
                "ENTRY ma tylko %s%%. "
                "Zmiana na WAIT.",
                direction_strength,
            )

            result["status"] = "WAIT"


    return result


# =========================================================
# FORMAT TELEGRAM
# =========================================================

def format_compact_signal(result):
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

    sl = result.get(
        "sl"
    )

    tp1 = result.get(
        "tp1"
    )

    tp2 = result.get(
        "tp2"
    )

    invalidation = result.get(
        "invalidation"
    )

    rr = result.get(
        "rr"
    )

    reason = result.get(
        "reason",
        "",
    )


    lines = []


    if status == "ENTRY":
        icon = "🚨"

    elif status == "REVERSAL":
        icon = "🔄"

    elif status in (
        "WAIT",
        "SETUP",
    ):
        icon = "📡"

    elif status == "SKIP":
        icon = "❌"

    else:
        icon = "📊"


    lines.append(
        f"{icon} XAUUSD | H1 + M15"
    )


    if price:
        lines.append(
            f"Cena: {price:.2f}"
        )


    lines.append("")


    lines.append(
        f"🟢 LONG: {long_pct}%"
    )

    lines.append(
        f"🔴 SHORT: {short_pct}%"
    )

    lines.append(
        f"⚪ CZEKAJ: {wait_pct}%"
    )


    lines.append("")


    if status == "ENTRY":
        lines.append(
            f"✅ DECYZJA: {direction}"
        )

    elif status == "REVERSAL":
        lines.append(
            f"🔄 REVERSAL: {direction}"
        )

    elif status == "SKIP":
        lines.append(
            "❌ DECYZJA: POMIŃ"
        )

    else:
        lines.append(
            "⏳ DECYZJA: CZEKAJ"
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

        if activation_side == "ABOVE":
            sign = ">"

        elif activation_side == "BELOW":
            sign = "<"

        else:
            sign = "@"

        lines.append(
            f"📍 Aktywacja {direction}: "
            f"{sign} {activation:.2f}"
        )


    if (
        sl
        and sl > 0
        and direction != "NONE"
    ):
        lines.append(
            f"🛑 SL: {sl:.2f}"
        )


    if (
        tp1
        and tp1 > 0
        and direction != "NONE"
    ):
        lines.append(
            f"🎯 TP1: {tp1:.2f}"
        )


    if (
        tp2
        and tp2 > 0
        and direction != "NONE"
    ):
        lines.append(
            f"🎯 TP2: {tp2:.2f}"
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


    if rr:
        lines.append(
            f"📊 R:R: {rr}"
        )


    if reason:
        lines.append("")
        lines.append(
            f"💬 {reason}"
        )


    return "\n".join(
        lines
    )


# =========================================================
# ALERTY
# =========================================================

def auto_alert_text(result):
    if result["status"] not in (
        "ENTRY",
        "WAIT",
        "SETUP",
        "REVERSAL",
        "SKIP",
    ):
        return None

    return format_compact_signal(
        result
    )


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

    long_pct = result.get(
        "long_pct",
        0,
    )

    short_pct = result.get(
        "short_pct",
        0,
    )


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
        and score < MIN_SETUP_SCORE
    ):
        return False


    if direction == "LONG":
        strength_bucket = (
            long_pct // 10
        )

    elif direction == "SHORT":
        strength_bucket = (
            short_pct // 10
        )

    else:
        strength_bucket = 0


    key = (
        f"{status}|"
        f"{direction}|"
        f"{strength_bucket}"
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
                "AUTO SCAN: "
                "rate-limit backoff aktywny."
            )

            return


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


        result = analyze_market_ai(
            results=results,
            autonomous=True,
            prefilter=candidate,
        )


        logger.info(
            (
                "AUTO SCAN AI: "
                "%s %s score=%s "
                "LONG=%s SHORT=%s WAIT=%s"
            ),
            result["status"],
            result["direction"],
            result["score"],
            result.get(
                "long_pct"
            ),
            result.get(
                "short_pct"
            ),
            result.get(
                "wait_pct"
            ),
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

    time.sleep(30)


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


        if status == "SKIP":
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


        if status == "REVERSAL":
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


    send_telegram_message(
        "⌛ XAUUSD | H1 + M15\n\n"
        "Obserwacja zakończona.\n"
        "Brak wystarczającego "
        "potwierdzenia wejścia."
    )


# =========================================================
# TRADINGVIEW
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
        "🤖 Analizuję H1 + M15.\n"
        "M5 = timing."
    )


    try:
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
            "pełnych danych rynku."
        )

        return


    status = result[
        "status"
    ]


    send_telegram_message(
        format_compact_signal(
            result
        )
    )


    if status in (
        "ENTRY",
        "SKIP",
    ):
        return


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
                "Trading AI Analyzer v3"
            ),
            "decision_timeframes": (
                "H1 + M15"
            ),
            "timing_timeframe": (
                "M5"
            ),
            "auto_scan_enabled": (
                AUTO_SCAN_ENABLED
            ),
            "auto_scan_interval": (
                AUTO_SCAN_INTERVAL_SECONDS
            ),
            "min_entry_percent": (
                MIN_ENTRY_PERCENT
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
# START AUTO SCANNER
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
