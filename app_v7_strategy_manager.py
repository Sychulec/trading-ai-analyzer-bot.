import os
import re
import time
import math
import uuid
import json
import threading
import unicodedata

from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import requests

from openai import OpenAI

from flask import Flask, jsonify, redirect, request

from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints

from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAApplicationAuthRes,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOAGetAccountListByAccessTokenRes,
    ProtoOAAccountAuthReq,
    ProtoOAAccountAuthRes,
    ProtoOAAccountDisconnectEvent,
    ProtoOAAccountsTokenInvalidatedEvent,
    ProtoOATraderReq,
    ProtoOATraderRes,
    ProtoOAReconcileReq,
    ProtoOAReconcileRes,
    ProtoOASymbolsListReq,
    ProtoOASymbolsListRes,
    ProtoOASymbolByIdReq,
    ProtoOASymbolByIdRes,
    ProtoOAGetTrendbarsReq,
    ProtoOAGetTrendbarsRes,
    ProtoOAGetPositionUnrealizedPnLReq,
    ProtoOAGetPositionUnrealizedPnLRes,
    ProtoOANewOrderReq,
    ProtoOAAmendPositionSLTPReq,
    ProtoOAClosePositionReq,
    ProtoOAExecutionEvent,
    ProtoOAOrderErrorEvent,
    ProtoOAErrorRes,
)

from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    ProtoOATrendbarPeriod,
    ProtoOAOrderType,
    ProtoOATradeSide,
    ProtoOAExecutionType,
)

from twisted.internet import reactor


app = Flask(__name__)

print("[BOOT] V7 USING PROVEN FTMO RISK BOT CTRADER MARKET CORE")


# ============================================================
# CONFIG
# ============================================================

CTRADER_CLIENT_ID = os.environ.get("CTRADER_CLIENT_ID")
CTRADER_CLIENT_SECRET = os.environ.get("CTRADER_CLIENT_SECRET")
CTRADER_REDIRECT_URI = os.environ.get("CTRADER_REDIRECT_URI")

# Klucz potrzebny do /auto/on i /auto/off
AUTO_CONTROL_KEY = os.environ.get("AUTO_CONTROL_KEY", "")

FTMO_START_BALANCE = 100000.0

BASE_RISK_PERCENT = 0.50
OWN_DAILY_STOP_PERCENT = 1.00
MAX_TRADES_PER_DAY = 2
MAX_OPEN_POSITIONS = 2

MIN_SIGNAL_SCORE = 68
AUTO_REFRESH_SECONDS = 300

# V7: strategia TradingView wybiera wejście, bot tylko wykonuje i prowadzi pozycję.
TARGET_TRADER_LOGIN = 17188951
print(f"[CTRADER] LIVE ROUTE | TARGET traderLogin={TARGET_TRADER_LOGIN} | XAUUSD ONLY")
STRATEGY_ONLY_MODE = True
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
MANAGER_INTERVAL_SECONDS = int(os.environ.get("MANAGER_INTERVAL_SECONDS", "60"))
FIXED_LOTS_XAUUSD = float(os.environ.get("FIXED_LOTS_XAUUSD", "0.50"))
MANAGER_MODEL = os.environ.get("MANAGER_MODEL", "gpt-5-mini")

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

ALLOWED_INSTRUMENTS = (
    "XAUUSD",
)

BOT_LABEL = "FTMO_AUTO_V6_3"

# Persistent Disk on Render should be mounted at /var/data.
# You can override the directory with BOT_STATE_DIR.
BOT_STATE_DIR = os.environ.get(
    "BOT_STATE_DIR",
    "/var/data",
)
BOT_STATE_FILE = os.path.join(
    BOT_STATE_DIR,
    "ftmo_bot_state.json",
)

# Default is OFF only when there is no saved state.
# If saved state says AUTO was ON, V6.1 can restore it after restart.
auto_trading_enabled = False


# ============================================================
# STATES
# ============================================================

ctrader_state = {
    "access_token": None,
    "refresh_token": None,
    "token_refreshed_at": None,

    "connected": False,
    "application_authorized": False,
    "account_authorized": False,

    "account_id": None,

    "balance": None,
    "equity": None,
    "unrealized_pnl": 0.0,

    "positions": [],
    "orders": [],
    "positions_reconciled_at": None,

    "market_loading": False,
    "market_ready": False,

    "last_market_refresh": None,

    "error": None,
}


trade_state = {
    "day": None,
    "day_start_equity": None,

    "trades_today": 0,

    "pending_symbols": set(),
    "last_signal_key": {},

    "counted_order_ids": set(),

    "last_order": None,
    "last_order_error": None,
}


persistence_state = {
    "loaded": False,
    "last_saved": None,
    "error": None,
}

state_lock = threading.Lock()


def ensure_state_dir():
    try:
        os.makedirs(
            BOT_STATE_DIR,
            exist_ok=True,
        )
        return True

    except Exception as error:
        persistence_state["error"] = (
            "STATE_DIR: " + str(error)
        )
        print(
            "[PERSISTENCE ERROR]",
            persistence_state["error"],
        )
        return False


def save_persistent_state():
    """Save tokens + AUTO flag + daily risk counters atomically."""
    global auto_trading_enabled

    if not ensure_state_dir():
        return False

    payload = {
        "version": "6.3",

        "access_token":
            ctrader_state.get(
                "access_token"
            ),

        "refresh_token":
            ctrader_state.get(
                "refresh_token"
            ),

        "token_refreshed_at":
            ctrader_state.get(
                "token_refreshed_at"
            ),

        "auto_trading_enabled":
            bool(auto_trading_enabled),

        "trade": {
            "day":
                trade_state.get("day"),

            "day_start_equity":
                trade_state.get(
                    "day_start_equity"
                ),

            "trades_today":
                int(
                    trade_state.get(
                        "trades_today",
                        0,
                    )
                ),

            "last_signal_key":
                dict(
                    trade_state.get(
                        "last_signal_key",
                        {},
                    )
                ),

            "counted_order_ids":
                list(
                    trade_state.get(
                        "counted_order_ids",
                        set(),
                    )
                ),
        },
    }

    temp_file = (
        BOT_STATE_FILE
        + ".tmp"
    )

    try:
        with state_lock:
            with open(
                temp_file,
                "w",
                encoding="utf-8",
            ) as handle:
                json.dump(
                    payload,
                    handle,
                    ensure_ascii=False,
                )

            try:
                os.chmod(
                    temp_file,
                    0o600,
                )
            except Exception:
                pass

            os.replace(
                temp_file,
                BOT_STATE_FILE,
            )

            try:
                os.chmod(
                    BOT_STATE_FILE,
                    0o600,
                )
            except Exception:
                pass

        persistence_state[
            "last_saved"
        ] = int(
            time.time()
        )
        persistence_state[
            "error"
        ] = None

        return True

    except Exception as error:
        persistence_state[
            "error"
        ] = (
            "STATE_SAVE: "
            + str(error)
        )

        print(
            "[PERSISTENCE ERROR]",
            persistence_state[
                "error"
            ],
        )

        return False


def load_persistent_state():
    """Restore tokens, AUTO flag and daily limits after a Render restart."""
    global auto_trading_enabled

    if not os.path.exists(
        BOT_STATE_FILE
    ):
        print(
            "[PERSISTENCE] No saved state"
        )
        return False

    try:
        with state_lock:
            with open(
                BOT_STATE_FILE,
                "r",
                encoding="utf-8",
            ) as handle:
                payload = json.load(
                    handle
                )

        ctrader_state[
            "access_token"
        ] = payload.get(
            "access_token"
        )

        ctrader_state[
            "refresh_token"
        ] = payload.get(
            "refresh_token"
        )

        ctrader_state[
            "token_refreshed_at"
        ] = payload.get(
            "token_refreshed_at"
        )

        auto_trading_enabled = bool(
            payload.get(
                "auto_trading_enabled",
                False,
            )
        )

        saved_trade = payload.get(
            "trade",
            {},
        )

        trade_state["day"] = (
            saved_trade.get("day")
        )

        trade_state[
            "day_start_equity"
        ] = saved_trade.get(
            "day_start_equity"
        )

        trade_state[
            "trades_today"
        ] = int(
            saved_trade.get(
                "trades_today",
                0,
            )
            or 0
        )

        trade_state[
            "last_signal_key"
        ] = dict(
            saved_trade.get(
                "last_signal_key",
                {},
            )
            or {}
        )

        trade_state[
            "counted_order_ids"
        ] = set(
            int(value)
            for value
            in saved_trade.get(
                "counted_order_ids",
                [],
            )
        )

        # Never restore a pending flag across a process restart.
        # Reconcile + last_signal_key protect against duplicate orders.
        trade_state[
            "pending_symbols"
        ].clear()

        persistence_state[
            "loaded"
        ] = True
        persistence_state[
            "error"
        ] = None

        print(
            "[PERSISTENCE] State restored, AUTO =",
            auto_trading_enabled,
        )

        return True

    except Exception as error:
        persistence_state[
            "error"
        ] = (
            "STATE_LOAD: "
            + str(error)
        )

        print(
            "[PERSISTENCE ERROR]",
            persistence_state[
                "error"
            ],
        )

        return False


def empty_symbol():
    return {
        "found": False,

        "symbol_id": None,
        "symbol_name": None,

        "digits": None,
        "pip_position": None,

        "min_volume_raw": None,
        "max_volume_raw": None,
        "step_volume_raw": None,
        "lot_size_raw": None,

        "candles": {
            "M5": [],
            "M15": [],
            "H1": [],
            "H4": [],
        },
    }


market_state = {
    "XAUUSD": empty_symbol(),
    "US100": empty_symbol(),
}


ctrader_client = None
reactor_started = False
auto_refresh_started = False


# ============================================================
# BASIC HELPERS
# ============================================================

def set_error(error):
    ctrader_state["error"] = str(error)
    print("[ERROR]", str(error))


def clear_error():
    ctrader_state["error"] = None


def safe_errback(failure):
    text = str(failure)

    if (
        ctrader_state.get("application_authorized")
        and "TimeoutError" in text
    ):
        print("[CTRADER] Ignored late app-auth timeout")
        return

    set_error(text)


def normalize_symbol(name):
    if not name:
        return ""

    return re.sub(
        r"[^A-Z0-9]",
        "",
        name.upper(),
    )


def detect_instrument(name):
    value = normalize_symbol(name)

    if "XAUUSD" in value or value == "GOLD":
        return "XAUUSD"

    if (
        "US100" in value
        or "NAS100" in value
        or "USTEC" in value
        or "NASDAQ100" in value
    ):
        return "US100"

    return None


def period_to_name(period):
    mapping = {
        ProtoOATrendbarPeriod.M5: "M5",
        ProtoOATrendbarPeriod.M15: "M15",
        ProtoOATrendbarPeriod.H1: "H1",
        ProtoOATrendbarPeriod.H4: "H4",
    }

    return mapping.get(period)


def trendbar_to_dict(bar, digits):
    digits = digits if digits is not None else 2

    low_raw = int(bar.low)

    open_raw = low_raw + int(bar.deltaOpen)
    high_raw = low_raw + int(bar.deltaHigh)
    close_raw = low_raw + int(bar.deltaClose)

    return {
        "timestamp": int(bar.utcTimestampInMinutes) * 60,

        "open": round(open_raw / 100000.0, digits),
        "high": round(high_raw / 100000.0, digits),
        "low": round(low_raw / 100000.0, digits),
        "close": round(close_raw / 100000.0, digits),

        "volume": int(bar.volume),
    }


# ============================================================
# FTMO DAY
# ============================================================

def ftmo_day():
    return datetime.now(
        ZoneInfo("Europe/Prague")
    ).strftime("%Y-%m-%d")


def current_equity():
    if ctrader_state["equity"] is not None:
        return ctrader_state["equity"]

    if ctrader_state["balance"] is not None:
        return ctrader_state["balance"]

    return FTMO_START_BALANCE


def reset_daily_state_if_needed():
    today = ftmo_day()

    if trade_state["day"] == today:
        return

    trade_state["day"] = today
    trade_state["day_start_equity"] = current_equity()

    trade_state["trades_today"] = 0
    trade_state["pending_symbols"].clear()
    trade_state["last_signal_key"] = {}
    trade_state["counted_order_ids"].clear()

    save_persistent_state()


def daily_loss_percent():
    start = trade_state.get("day_start_equity")

    if not start:
        return 0.0

    equity = current_equity()

    return max(
        0.0,
        (start - equity) / start * 100.0,
    )


# ============================================================
# RISK ENGINE
# ============================================================

def calculate_risk(equity):
    drawdown_percent = max(
        0.0,
        (
            FTMO_START_BALANCE
            - equity
        )
        / FTMO_START_BALANCE
        * 100.0,
    )

    if drawdown_percent >= 2.0:
        risk_percent = 0.25

    elif drawdown_percent >= 1.0:
        risk_percent = 0.35

    else:
        risk_percent = BASE_RISK_PERCENT

    return {
        "drawdown_percent": round(
            drawdown_percent,
            3,
        ),

        "risk_percent": risk_percent,

        "risk_usd": round(
            equity
            * risk_percent
            / 100.0,
            2,
        ),
    }


# ============================================================
# INDICATORS
# ============================================================

def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2.0 / (period + 1)

    current = sum(values[:period]) / period

    for value in values[period:]:
        current = (
            value * multiplier
            + current * (1 - multiplier)
        )

    return current


def ema_series(values, period):
    if len(values) < period:
        return []

    output = [None] * (period - 1)

    current = sum(values[:period]) / period
    output.append(current)

    multiplier = 2.0 / (period + 1)

    for value in values[period:]:
        current = (
            value * multiplier
            + current * (1 - multiplier)
        )

        output.append(current)

    return output


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(
        len(values) - period,
        len(values),
    ):
        change = (
            values[i]
            - values[i - 1]
        )

        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def macd(values):
    if len(values) < 35:
        return None

    ema12 = ema_series(values, 12)
    ema26 = ema_series(values, 26)

    macd_values = []

    for i in range(len(values)):
        if (
            i < len(ema12)
            and i < len(ema26)
            and ema12[i] is not None
            and ema26[i] is not None
        ):
            macd_values.append(
                ema12[i] - ema26[i]
            )

    if len(macd_values) < 9:
        return None

    signal = ema(macd_values, 9)
    line = macd_values[-1]

    return {
        "line": line,
        "signal": signal,
        "histogram": line - signal,
    }


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    values = []

    for i in range(
        len(candles) - period,
        len(candles),
    ):
        candle = candles[i]
        previous_close = candles[i - 1]["close"]

        true_range = max(
            candle["high"] - candle["low"],

            abs(
                candle["high"]
                - previous_close
            ),

            abs(
                candle["low"]
                - previous_close
            ),
        )

        values.append(true_range)

    return sum(values) / len(values)


def volume_average(
    candles,
    period=20,
):
    if len(candles) < period:
        return None

    return sum(
        candle["volume"]
        for candle
        in candles[-period:]
    ) / period


def simple_vwap(
    candles,
    period=50,
):
    sample = candles[-period:]

    if not sample:
        return None

    price_volume = 0.0
    total_volume = 0.0

    for candle in sample:
        volume = max(
            candle["volume"],
            1,
        )

        typical = (
            candle["high"]
            + candle["low"]
            + candle["close"]
        ) / 3.0

        price_volume += (
            typical * volume
        )

        total_volume += volume

    if total_volume == 0:
        return None

    return price_volume / total_volume


# ============================================================
# STRUCTURE
# ============================================================

def recent_range(
    candles,
    lookback=20,
):
    if len(candles) < lookback + 1:
        return None, None

    sample = candles[
        -(lookback + 1):-1
    ]

    return (
        max(
            candle["high"]
            for candle
            in sample
        ),

        min(
            candle["low"]
            for candle
            in sample
        ),
    )


def structure_signal(candles):
    if len(candles) < 16:
        return 0

    recent = candles[-16:]

    first = recent[:8]
    second = recent[8:]

    high1 = max(
        candle["high"]
        for candle
        in first
    )

    high2 = max(
        candle["high"]
        for candle
        in second
    )

    low1 = min(
        candle["low"]
        for candle
        in first
    )

    low2 = min(
        candle["low"]
        for candle
        in second
    )

    if high2 > high1 and low2 > low1:
        return 1

    if high2 < high1 and low2 < low1:
        return -1

    return 0


def detect_bos(
    candles,
    lookback=20,
):
    if len(candles) < lookback + 2:
        return 0

    last = candles[-1]

    previous = candles[
        -(lookback + 1):-1
    ]

    high = max(
        candle["high"]
        for candle
        in previous
    )

    low = min(
        candle["low"]
        for candle
        in previous
    )

    if last["close"] > high:
        return 1

    if last["close"] < low:
        return -1

    return 0


def detect_sweep(
    candles,
    lookback=20,
):
    if len(candles) < lookback + 2:
        return 0

    last = candles[-1]

    previous = candles[
        -(lookback + 1):-1
    ]

    high = max(
        candle["high"]
        for candle
        in previous
    )

    low = min(
        candle["low"]
        for candle
        in previous
    )

    if (
        last["low"] < low
        and last["close"] > low
    ):
        return 1

    if (
        last["high"] > high
        and last["close"] < high
    ):
        return -1

    return 0


def detect_fvg(candles):
    if len(candles) < 3:
        return 0

    first = candles[-3]
    third = candles[-1]

    if third["low"] > first["high"]:
        return 1

    if third["high"] < first["low"]:
        return -1

    return 0


def detect_retest(
    candles,
    current_atr,
):
    if (
        len(candles) < 25
        or current_atr is None
    ):
        return 0

    old = candles[-24:-4]
    recent = candles[-3:]

    old_high = max(
        candle["high"]
        for candle
        in old
    )

    old_low = min(
        candle["low"]
        for candle
        in old
    )

    tolerance = (
        current_atr * 0.25
    )

    for candle in recent:
        if (
            abs(
                candle["low"]
                - old_high
            )
            <= tolerance

            and candle["close"]
            > old_high
        ):
            return 1

        if (
            abs(
                candle["high"]
                - old_low
            )
            <= tolerance

            and candle["close"]
            < old_low
        ):
            return -1

    return 0


def detect_impulse(
    candles,
    avg_volume,
):
    if (
        len(candles) < 20
        or avg_volume is None
    ):
        return 0

    last = candles[-1]

    bodies = [
        abs(
            candle["close"]
            - candle["open"]
        )
        for candle
        in candles[-20:-1]
    ]

    if not bodies:
        return 0

    average_body = (
        sum(bodies)
        / len(bodies)
    )

    body = abs(
        last["close"]
        - last["open"]
    )

    if (
        last["volume"]
        <= avg_volume * 1.10
        or body
        < average_body * 1.35
    ):
        return 0

    if last["close"] > last["open"]:
        return 1

    return -1


# ============================================================
# TIMEFRAME ANALYSIS
# ============================================================

def analyse_timeframe(candles):
    if len(candles) < 210:
        return {
            "ready": False,
            "candles": len(candles),
        }

    closes = [
        candle["close"]
        for candle
        in candles
    ]

    last = candles[-1]
    price = last["close"]

    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    ema200 = ema(closes, 200)

    current_rsi = rsi(closes, 14)
    current_macd = macd(closes)
    current_atr = atr(candles, 14)
    current_vwap = simple_vwap(
        candles,
        50,
    )

    avg_volume = volume_average(
        candles,
        20,
    )

    structure = structure_signal(candles)
    bos = detect_bos(candles)
    sweep = detect_sweep(candles)
    fvg = detect_fvg(candles)

    retest = detect_retest(
        candles,
        current_atr,
    )

    impulse = detect_impulse(
        candles,
        avg_volume,
    )

    range_high, range_low = (
        recent_range(candles)
    )

    long_score = 0
    short_score = 0

    reasons_long = []
    reasons_short = []

    if price > ema20 and price > ema50:
        long_score += 8
        reasons_long.append(
            "cena nad EMA20/50"
        )

    elif price < ema20 and price < ema50:
        short_score += 8
        reasons_short.append(
            "cena pod EMA20/50"
        )

    if ema20 > ema50 > ema200:
        long_score += 12
        reasons_long.append(
            "EMA20>EMA50>EMA200"
        )

    elif ema20 < ema50 < ema200:
        short_score += 12
        reasons_short.append(
            "EMA20<EMA50<EMA200"
        )

    if price > ema200:
        long_score += 5

    elif price < ema200:
        short_score += 5

    if current_rsi is not None:
        if 55 <= current_rsi <= 75:
            long_score += 7
            reasons_long.append(
                "RSI bullish"
            )

        elif 25 <= current_rsi <= 45:
            short_score += 7
            reasons_short.append(
                "RSI bearish"
            )

    if current_macd:
        if (
            current_macd["line"]
            > current_macd["signal"]
        ):
            long_score += 8
            reasons_long.append(
                "MACD bullish"
            )

        elif (
            current_macd["line"]
            < current_macd["signal"]
        ):
            short_score += 8
            reasons_short.append(
                "MACD bearish"
            )

    if current_vwap is not None:
        if price > current_vwap:
            long_score += 6
            reasons_long.append(
                "nad VWAP"
            )

        elif price < current_vwap:
            short_score += 6
            reasons_short.append(
                "pod VWAP"
            )

    if structure == 1:
        long_score += 12
        reasons_long.append(
            "HH+HL"
        )

    elif structure == -1:
        short_score += 12
        reasons_short.append(
            "LH+LL"
        )

    if bos == 1:
        long_score += 12
        reasons_long.append(
            "BOS bullish"
        )

    elif bos == -1:
        short_score += 12
        reasons_short.append(
            "BOS bearish"
        )

    if sweep == 1:
        long_score += 10
        reasons_long.append(
            "sweep dolem"
        )

    elif sweep == -1:
        short_score += 10
        reasons_short.append(
            "sweep gora"
        )

    if retest == 1:
        long_score += 8
        reasons_long.append(
            "retest bullish"
        )

    elif retest == -1:
        short_score += 8
        reasons_short.append(
            "retest bearish"
        )

    if fvg == 1:
        long_score += 5
        reasons_long.append(
            "FVG bullish"
        )

    elif fvg == -1:
        short_score += 5
        reasons_short.append(
            "FVG bearish"
        )

    if impulse == 1:
        long_score += 7
        reasons_long.append(
            "impuls+volume"
        )

    elif impulse == -1:
        short_score += 7
        reasons_short.append(
            "impuls+volume"
        )

    if len(closes) >= 6:
        momentum = (
            closes[-1]
            - closes[-6]
        )

        if momentum > 0:
            long_score += 5
            reasons_long.append(
                "momentum+"
            )

        elif momentum < 0:
            short_score += 5
            reasons_short.append(
                "momentum-"
            )

    return {
        "ready": True,

        "price": price,

        "long": min(
            long_score,
            100,
        ),

        "short": min(
            short_score,
            100,
        ),

        "ema20": round(ema20, 4),
        "ema50": round(ema50, 4),
        "ema200": round(ema200, 4),

        "rsi": (
            round(
                current_rsi,
                2,
            )
            if current_rsi is not None
            else None
        ),

        "macd": (
            round(
                current_macd["histogram"],
                5,
            )
            if current_macd
            else None
        ),

        "atr": (
            round(
                current_atr,
                4,
            )
            if current_atr is not None
            else None
        ),

        "vwap": (
            round(
                current_vwap,
                4,
            )
            if current_vwap is not None
            else None
        ),

        "structure": (
            "BULLISH"
            if structure == 1
            else
            "BEARISH"
            if structure == -1
            else
            "NEUTRAL"
        ),

        "bos": (
            "BULLISH"
            if bos == 1
            else
            "BEARISH"
            if bos == -1
            else
            "NONE"
        ),

        "sweep": (
            "BULLISH"
            if sweep == 1
            else
            "BEARISH"
            if sweep == -1
            else
            "NONE"
        ),

        "fvg": (
            "BULLISH"
            if fvg == 1
            else
            "BEARISH"
            if fvg == -1
            else
            "NONE"
        ),

        "retest": (
            "BULLISH"
            if retest == 1
            else
            "BEARISH"
            if retest == -1
            else
            "NONE"
        ),

        "range_high": (
            round(
                range_high,
                4,
            )
            if range_high is not None
            else None
        ),

        "range_low": (
            round(
                range_low,
                4,
            )
            if range_low is not None
            else None
        ),

        "reasons_long": reasons_long,
        "reasons_short": reasons_short,
    }


# ============================================================
# STOP
# ============================================================

def structural_stop(
    candles,
    direction,
    entry,
    current_atr,
):
    if (
        not candles
        or entry is None
        or current_atr is None
    ):
        return None

    sample = candles[-20:]

    recent_high = max(
        candle["high"]
        for candle
        in sample
    )

    recent_low = min(
        candle["low"]
        for candle
        in sample
    )

    minimum_distance = (
        current_atr * 0.80
    )

    maximum_distance = (
        current_atr * 2.20
    )

    buffer = (
        current_atr * 0.20
    )

    if direction == "LONG":
        raw_sl = (
            recent_low
            - buffer
        )

        distance = max(
            minimum_distance,

            min(
                entry - raw_sl,
                maximum_distance,
            ),
        )

        return (
            entry - distance
        )

    raw_sl = (
        recent_high
        + buffer
    )

    distance = max(
        minimum_distance,

        min(
            raw_sl - entry,
            maximum_distance,
        ),
    )

    return (
        entry + distance
    )


# ============================================================
# MULTI TF ANALYSIS
# ============================================================

def analyse_instrument(instrument):
    data = market_state[instrument]

    # V6.3: H4 is context only. H1/M15 drive intraday direction,
    # M5 refines timing. H4 no longer delays a valid counter-trend move.
    weights = {
        "H4": 0.00,
        "H1": 0.50,
        "M15": 0.35,
        "M5": 0.15,
    }

    analyses = {}

    weighted_long = 0.0
    weighted_short = 0.0
    total_weight = 0.0

    for timeframe, weight in weights.items():
        result = analyse_timeframe(
            data["candles"][timeframe]
        )

        analyses[timeframe] = result

        if result.get("ready"):
            weighted_long += (
                result["long"]
                * weight
            )

            weighted_short += (
                result["short"]
                * weight
            )

            total_weight += weight

    if total_weight == 0:
        return {
            "status": "NOT_READY",
            "instrument": instrument,
            "timeframes": analyses,
        }

    long_score = round(
        weighted_long
        / total_weight
    )

    short_score = round(
        weighted_short
        / total_weight
    )

    h4 = analyses.get("H4", {})
    h1 = analyses.get("H1", {})
    m15 = analyses.get("M15", {})
    m5 = analyses.get("M5", {})

    h4_bias = (
        h4.get("long", 0)
        - h4.get("short", 0)
    )

    h1_bias = (
        h1.get("long", 0)
        - h1.get("short", 0)
    )

    m15_bias = (
        m15.get("long", 0)
        - m15.get("short", 0)
    )

    decision = "CZEKAJ"

    if (
        long_score
        >= MIN_SIGNAL_SCORE

        and h1_bias >= 10
        and m15_bias >= 0
        and (
            m5.get("long", 0) - m5.get("short", 0)
        ) >= -15
    ):
        decision = "LONG"

    elif (
        short_score
        >= MIN_SIGNAL_SCORE

        and h1_bias <= -10
        and m15_bias <= 0
        and (
            m5.get("long", 0) - m5.get("short", 0)
        ) <= 15
    ):
        decision = "SHORT"

    # V6.3 REVERSAL/FLIP:
    # If H4 still shows the old trend but H1+M15 have already turned
    # structurally, allow an earlier intraday entry. H4 remains observation.
    m5_bias = (
        m5.get("long", 0)
        - m5.get("short", 0)
    )

    long_flip = (
        h4_bias < 0
        and h1_bias >= 15
        and m15_bias >= 8
        and m5_bias >= -12
        and (
            m15.get("structure") == "BULLISH"
            or m15.get("bos") == "BULLISH"
            or m15.get("sweep") == "BULLISH"
            or m15.get("retest") == "BULLISH"
        )
    )

    short_flip = (
        h4_bias > 0
        and h1_bias <= -15
        and m15_bias <= -8
        and m5_bias <= 12
        and (
            m15.get("structure") == "BEARISH"
            or m15.get("bos") == "BEARISH"
            or m15.get("sweep") == "BEARISH"
            or m15.get("retest") == "BEARISH"
        )
    )

    if decision == "CZEKAJ":
        if long_flip and long_score >= 62:
            decision = "LONG"
        elif short_flip and short_score >= 62:
            decision = "SHORT"

    strongest = max(
        long_score,
        short_score,
    )

    if strongest >= 82:
        quality = "A+"

    elif strongest >= 75:
        quality = "A"

    elif strongest >= 68:
        quality = "B"

    elif strongest >= 60:
        quality = "WATCH"

    else:
        quality = "BRAK"

    equity = current_equity()
    risk = calculate_risk(equity)

    reference = (
        m15
        if m15.get("ready")
        else h1
    )

    entry = reference.get("price")
    current_atr = reference.get("atr")

    sl = None
    tp1 = None
    tp2 = None

    if (
        decision in (
            "LONG",
            "SHORT",
        )

        and entry is not None
        and current_atr is not None
    ):
        reference_candles = (
            data["candles"]["M15"]
            if m15.get("ready")
            else
            data["candles"]["H1"]
        )

        sl = structural_stop(
            reference_candles,
            decision,
            entry,
            current_atr,
        )

        if sl is not None:
            distance = abs(
                entry - sl
            )

            if decision == "LONG":
                tp1 = (
                    entry
                    + distance * 1.8
                )

                tp2 = (
                    entry
                    + distance * 2.5
                )

            else:
                tp1 = (
                    entry
                    - distance * 1.8
                )

                tp2 = (
                    entry
                    - distance * 2.5
                )

    digits = (
        data["digits"]
        if data["digits"] is not None
        else 2
    )

    return {
        "instrument": instrument,
        "symbol": data["symbol_name"],

        "decision": decision,
        "setup_quality": quality,

        "setup_mode": (
            "REVERSAL_FLIP"
            if (
                (decision == "LONG" and long_flip)
                or (decision == "SHORT" and short_flip)
            )
            else "INTRADAY"
            if decision in ("LONG", "SHORT")
            else "NONE"
        ),

        "score": {
            "LONG": long_score,
            "SHORT": short_score,

            "CZEKAJ": max(
                0,
                100 - strongest,
            ),
        },

        "entry": (
            round(entry, digits)
            if entry is not None
            else None
        ),

        "sl": (
            round(sl, digits)
            if sl is not None
            else None
        ),

        "tp1": (
            round(tp1, digits)
            if tp1 is not None
            else None
        ),

        "tp2": (
            round(tp2, digits)
            if tp2 is not None
            else None
        ),

        "risk": risk,

        "confirmation": {
            "H4": (
                "LONG"
                if h4_bias > 0
                else
                "SHORT"
                if h4_bias < 0
                else
                "NEUTRAL"
            ),

            "H1": (
                "LONG"
                if h1_bias > 0
                else
                "SHORT"
                if h1_bias < 0
                else
                "NEUTRAL"
            ),

            "M15": (
                "LONG"
                if m15_bias > 0
                else
                "SHORT"
                if m15_bias < 0
                else
                "NEUTRAL"
            ),

            "M5_long_score":
                m5.get("long", 0),

            "M5_short_score":
                m5.get("short", 0),
        },

        "timeframes": analyses,
    }


# ============================================================
# POSITION HELPERS
# ============================================================

def open_position_symbol_ids():
    return {
        position["symbol_id"]
        for position
        in ctrader_state["positions"]
        if position.get("symbol_id")
        is not None
    }


def size_position(
    instrument,
    entry,
    sl,
    risk_usd,
):
    data = market_state[instrument]

    min_raw = data["min_volume_raw"]
    max_raw = data["max_volume_raw"]
    step_raw = data["step_volume_raw"]

    if (
        min_raw is None
        or max_raw is None
        or step_raw is None
        or step_raw <= 0
    ):
        return {
            "ok": False,
            "reason":
                "BRAK_PARAMETROW_WOLUMENU",
        }

    distance = abs(
        entry - sl
    )

    if distance <= 0:
        return {
            "ok": False,
            "reason": "ZLY_SL",
        }

    # Dla naszych XAUUSD / US100
    # P/L w USD ~ jednostki * zmiana ceny.
    units = (
        risk_usd
        / distance
    )

    desired_raw = (
        units * 100.0
    )

    raw_volume = int(
        math.floor(
            desired_raw
            / step_raw
        )
        * step_raw
    )

    if raw_volume < min_raw:
        return {
            "ok": False,
            "reason":
                "MIN_VOLUME_ZA_DUZE_DLA_RYZYKA",
        }

    raw_volume = min(
        raw_volume,
        max_raw,
    )

    actual_units = (
        raw_volume
        / 100.0
    )

    estimated_risk = (
        actual_units
        * distance
    )

    if estimated_risk > risk_usd + 0.01:
        return {
            "ok": False,
            "reason":
                "RYZYKO_PRZEKROCZONE",
        }

    lot_size_units = (
        data["lot_size_raw"]
        / 100.0
        if data["lot_size_raw"]
        else None
    )

    lots = (
        actual_units
        / lot_size_units
        if lot_size_units
        else None
    )

    return {
        "ok": True,

        "volume_raw": raw_volume,

        "units": round(
            actual_units,
            4,
        ),

        "lots": (
            round(lots, 4)
            if lots is not None
            else None
        ),

        "estimated_risk_usd": round(
            estimated_risk,
            2,
        ),

        "stop_distance": round(
            distance,
            6,
        ),
    }


# ============================================================
# CAN TRADE
# ============================================================

def can_trade(
    instrument,
    analysis,
):
    reset_daily_state_if_needed()

    if not auto_trading_enabled:
        return False, "AUTO_OFF"

    if not ctrader_state[
        "account_authorized"
    ]:
        return False, "BRAK_AUTORYZACJI"

    if not ctrader_state[
        "market_ready"
    ]:
        return False, "MARKET_NOT_READY"

    if analysis.get("decision") not in (
        "LONG",
        "SHORT",
    ):
        return False, "CZEKAJ"

    if (
        analysis.get("entry")
        is None

        or analysis.get("sl")
        is None

        or analysis.get("tp2")
        is None
    ):
        return False, "BRAK_SL_TP"

    if (
        trade_state["trades_today"]
        >= MAX_TRADES_PER_DAY
    ):
        return False, "LIMIT_TRANSAKCJI"

    if (
        daily_loss_percent()
        >= OWN_DAILY_STOP_PERCENT
    ):
        return False, "STOP_DZIENNY"

    if (
        len(
            ctrader_state["positions"]
        )
        >= MAX_OPEN_POSITIONS
    ):
        return False, "LIMIT_POZYCJI"

    if (
        instrument
        in trade_state["pending_symbols"]
    ):
        return False, "ORDER_PENDING"

    symbol_id = market_state[
        instrument
    ]["symbol_id"]

    if (
        symbol_id
        in open_position_symbol_ids()
    ):
        return False, "POZYCJA_JUZ_OTWARTA"

    candles = market_state[
        instrument
    ]["candles"]["M15"]

    if not candles:
        return False, "BRAK_M15"

    signal_key = (
        analysis["decision"]
        + ":"
        + str(
            candles[-1]["timestamp"]
        )
    )

    if (
        trade_state[
            "last_signal_key"
        ].get(instrument)
        == signal_key
    ):
        return False, "SYGNAL_JUZ_UZYTY"

    return True, signal_key


# ============================================================
# SUBMIT ORDER
# ============================================================

def submit_market_order(
    instrument,
    analysis,
    signal_key,
):
    if ctrader_client is None:
        return

    equity = current_equity()

    risk = calculate_risk(
        equity
    )

    sizing = size_position(
        instrument,

        float(
            analysis["entry"]
        ),

        float(
            analysis["sl"]
        ),

        float(
            risk["risk_usd"]
        ),
    )

    if not sizing["ok"]:
        trade_state[
            "last_order_error"
        ] = {
            "instrument": instrument,
            "reason": sizing["reason"],
            "time": int(time.time()),
        }

        print(
            "[TRADE BLOCKED]",
            instrument,
            sizing["reason"],
        )

        return

    req = ProtoOANewOrderReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
    )

    req.symbolId = int(
        market_state[
            instrument
        ]["symbol_id"]
    )

    req.orderType = (
        ProtoOAOrderType.MARKET
    )

    if analysis["decision"] == "LONG":
        req.tradeSide = (
            ProtoOATradeSide.BUY
        )

    else:
        req.tradeSide = (
            ProtoOATradeSide.SELL
        )

    req.volume = int(
        sizing["volume_raw"]
    )

    stop_distance = abs(
        float(
            analysis["entry"]
        )
        - float(
            analysis["sl"]
        )
    )

    tp_distance = abs(
        float(
            analysis["tp2"]
        )
        - float(
            analysis["entry"]
        )
    )

    req.relativeStopLoss = max(
        1,

        int(
            round(
                stop_distance
                * 100000.0
            )
        ),
    )

    req.relativeTakeProfit = max(
        1,

        int(
            round(
                tp_distance
                * 100000.0
            )
        ),
    )

    req.label = BOT_LABEL

    req.comment = (
        f"{instrument} "
        f"{analysis['decision']} "
        f"risk={risk['risk_percent']}%"
    )[:512]

    req.clientOrderId = (
        instrument
        + "-"
        + str(
            int(time.time())
        )
        + "-"
        + uuid.uuid4().hex[:6]
    )[:50]

    trade_state[
        "pending_symbols"
    ].add(instrument)

    trade_state[
        "last_signal_key"
    ][instrument] = signal_key

    trade_state["last_order"] = {
        "instrument": instrument,
        "decision":
            analysis["decision"],

        "entry_reference":
            analysis["entry"],

        "sl_reference":
            analysis["sl"],

        "tp_reference":
            analysis["tp2"],

        "volume_raw":
            sizing["volume_raw"],

        "units":
            sizing["units"],

        "lots":
            sizing["lots"],

        "estimated_risk_usd":
            sizing[
                "estimated_risk_usd"
            ],

        "status": "SENT",

        "sent_at": int(
            time.time()
        ),
    }

    # Persist the used signal immediately so a restart cannot reuse it.
    save_persistent_state()

    print(
        "[ORDER SEND]",
        trade_state["last_order"],
    )

    ctrader_client.send(
        req
    ).addErrback(
        safe_errback
    )


# ============================================================
# AUTO EVALUATION
# ============================================================

def evaluate_auto_trading():
    # V7: nie otwieramy transakcji z wewnętrznego skanera.
    # Źródłem wejścia jest wyłącznie sygnał strategii TradingView.
    if STRATEGY_ONLY_MODE:
        return

    if (
        not auto_trading_enabled
        or not ctrader_state[
            "market_ready"
        ]
    ):
        return

    for instrument in ALLOWED_INSTRUMENTS:
        analysis = analyse_instrument(
            instrument
        )

        allowed, reason = can_trade(
            instrument,
            analysis,
        )

        if allowed:
            submit_market_order(
                instrument,
                analysis,
                reason,
            )


# ============================================================
# REACTOR
# ============================================================

def start_reactor():
    global reactor_started

    if reactor_started:
        return

    reactor_started = True

    reactor.run(
        installSignalHandlers=False
    )


# ============================================================
# TOKEN REFRESH / AUTO RECOVERY
# ============================================================

def refresh_ctrader_token():
    """Refresh cTrader OAuth tokens without exposing them in logs."""
    refresh_token = ctrader_state.get(
        "refresh_token"
    )

    if not refresh_token:
        print(
            "[CTRADER] No refresh token"
        )
        return False

    try:
        response = requests.get(
            "https://openapi.ctrader.com/apps/token",

            params={
                "grant_type":
                    "refresh_token",

                "refresh_token":
                    refresh_token,

                "client_id":
                    CTRADER_CLIENT_ID,

                "client_secret":
                    CTRADER_CLIENT_SECRET,
            },

            timeout=15,
        )

        response.raise_for_status()
        data = response.json()

        access_token = data.get(
            "accessToken"
        )

        new_refresh_token = data.get(
            "refreshToken"
        )

        if not access_token:
            raise RuntimeError(
                "Brak accessToken po refresh"
            )

        ctrader_state[
            "access_token"
        ] = access_token

        # cTrader can rotate the refresh token.
        if new_refresh_token:
            ctrader_state[
                "refresh_token"
            ] = new_refresh_token

        ctrader_state[
            "token_refreshed_at"
        ] = int(
            time.time()
        )

        save_persistent_state()
        clear_error()

        print(
            "[CTRADER] TOKEN REFRESHED"
        )

        return True

    except Exception as error:
        set_error(
            "Token refresh error: "
            + str(error)
        )

        return False


def refresh_and_reconnect():
    """Refresh token in a worker thread, then reconnect on the Twisted reactor."""
    if not refresh_ctrader_token():
        return

    ctrader_state[
        "account_authorized"
    ] = False

    ctrader_state[
        "market_ready"
    ] = False

    ctrader_state[
        "market_loading"
    ] = False

    if reactor_started:
        reactor.callFromThread(
            start_ctrader_connection
        )


def bootstrap_reconnect_watchdog():
    """Fallback: refresh the token only if the restored access token does not reconnect."""
    time.sleep(12)

    if ctrader_state.get("account_authorized"):
        return

    if not ctrader_state.get("refresh_token"):
        print("[BOOT] Reconnect watchdog: no refresh token")
        return

    print("[BOOT] Reconnect watchdog: refreshing token")

    if not refresh_ctrader_token():
        print("[BOOT] Reconnect watchdog: token refresh failed")
        return

    ctrader_state["connected"] = False
    ctrader_state["application_authorized"] = False
    ctrader_state["account_authorized"] = False
    ctrader_state["market_ready"] = False
    ctrader_state["market_loading"] = False

    if reactor_started:
        reactor.callFromThread(start_ctrader_connection)
        print("[BOOT] Reconnect watchdog: second connection attempt started")


def bootstrap_bot():
    """Restore state and reconnect cTrader after a Gunicorn restart."""
    global reactor_started

    restored = load_persistent_state()

    if not restored:
        return

    # Start cTrader immediately with the persisted access token.  This avoids
    # blocking startup on an OAuth refresh request.  If the access token is no
    # longer accepted, the watchdog refreshes it and retries automatically.
    if not ctrader_state.get("access_token"):
        if ctrader_state.get("refresh_token"):
            print("[BOOT] No saved access token; refreshing")
            if not refresh_ctrader_token():
                print("[BOOT] Token refresh failed; manual /ctrader/login required")
                return
        else:
            print("[BOOT] Saved state has no cTrader tokens")
            return

    if not reactor_started:
        threading.Thread(
            target=start_reactor,
            daemon=True,
        ).start()

    # Wait briefly until the Twisted reactor is actually running.
    for _ in range(50):
        if getattr(reactor, "running", False):
            break
        time.sleep(0.1)

    if not getattr(reactor, "running", False):
        print("[BOOT] Reactor did not start")
        return

    reactor.callFromThread(start_ctrader_connection)
    print("[BOOT] Automatic cTrader recovery started")

    threading.Thread(
        target=bootstrap_reconnect_watchdog,
        daemon=True,
    ).start()


# ============================================================
# CTRADER AUTH
# ============================================================

def send_account_list_request(client):
    token = ctrader_state.get(
        "access_token"
    )

    if not token:
        set_error(
            "Brak access_token"
        )

        return

    req = (
        ProtoOAGetAccountListByAccessTokenReq()
    )

    req.accessToken = token

    print(
        "[CTRADER] REQUEST ACCOUNT LIST"
    )

    client.send(
        req
    ).addErrback(
        safe_errback
    )


def authorize_account(client):
    token = ctrader_state.get(
        "access_token"
    )

    account_id = ctrader_state.get(
        "account_id"
    )

    if not token or not account_id:
        return

    req = ProtoOAAccountAuthReq()

    req.ctidTraderAccountId = int(
        account_id
    )

    req.accessToken = token

    print(
        "[CTRADER] REQUEST ACCOUNT AUTH"
    )

    client.send(
        req
    ).addErrback(
        safe_errback
    )


# ============================================================
# ACCOUNT DATA
# ============================================================

symbol_retry_scheduled = False

def request_account_data(client):
    """
    XAU market bootstrap first.
    Important: request the symbol list BEFORE trader/reconcile/PnL.
    This prevents an earlier account-data request from blocking market bootstrap.
    """
    if not ctrader_state["account_authorized"]:
        print("[CTRADER] MARKET BOOT ABORTED: account not authorized", flush=True)
        return

    account_id = int(ctrader_state["account_id"])

    print(
        f"[CTRADER] MARKET BOOT START account_id={account_id}",
        flush=True,
    )

    # 1. SYMBOL LIST FIRST — this is required for XAUUSD market readiness.
    symbols_ready = all(
        market_state[instrument]["found"]
        for instrument in ALLOWED_INSTRUMENTS
    )

    if not symbols_ready:
        symbols_req = ProtoOASymbolsListReq()
        symbols_req.ctidTraderAccountId = account_id
        symbols_req.includeArchivedSymbols = False

        print("[CTRADER] REQUEST SYMBOL LIST", flush=True)

        try:
            d = client.send(symbols_req)
            d.addErrback(safe_errback)
            print("[CTRADER] SYMBOL LIST REQUEST SENT", flush=True)
        except Exception as error:
            print(
                "[CTRADER] SYMBOL LIST SEND EXCEPTION:",
                repr(error),
                flush=True,
            )
            set_error("Symbol list send exception: " + str(error))
            return

    # 2. Remaining account data are secondary and must not prevent symbol loading.
    account_requests = [
        ("TRADER", ProtoOATraderReq()),
        ("RECONCILE", ProtoOAReconcileReq()),
        ("UNREALIZED_PNL", ProtoOAGetPositionUnrealizedPnLReq()),
    ]

    for name, req in account_requests:
        req.ctidTraderAccountId = account_id
        try:
            client.send(req).addErrback(safe_errback)
            print(f"[CTRADER] {name} REQUEST SENT", flush=True)
        except Exception as error:
            # Log it, but DO NOT stop XAU market bootstrap.
            print(
                f"[CTRADER] {name} SEND EXCEPTION: {error!r}",
                flush=True,
            )


# ============================================================
# MARKET DATA
# ============================================================

def request_trendbars(
    client,
    instrument,
    timeframe,
):
    if not ctrader_state[
        "account_authorized"
    ]:
        return

    symbol_id = market_state[
        instrument
    ]["symbol_id"]

    if symbol_id is None:
        return

    period_map = {
        "M5":
            ProtoOATrendbarPeriod.M5,

        "M15":
            ProtoOATrendbarPeriod.M15,

        "H1":
            ProtoOATrendbarPeriod.H1,

        "H4":
            ProtoOATrendbarPeriod.H4,
    }

    days_map = {
        "M5": 5,
        "M15": 14,
        "H1": 30,
        "H4": 120,
    }

    now_ms = int(
        time.time() * 1000
    )

    req = ProtoOAGetTrendbarsReq()

    req.ctidTraderAccountId = int(
        ctrader_state["account_id"]
    )

    req.symbolId = int(
        symbol_id
    )

    req.period = (
        period_map[timeframe]
    )

    req.fromTimestamp = (
        now_ms
        - days_map[timeframe]
        * 86400000
    )

    req.toTimestamp = now_ms
    req.count = 250

    client.send(
        req
    ).addErrback(
        safe_errback
    )


def start_market_queue(client):
    if not ctrader_state[
        "account_authorized"
    ]:
        return

    if ctrader_state[
        "market_loading"
    ]:
        return

    symbols_ready = all(
        market_state[
            instrument
        ]["found"]

        and market_state[
            instrument
        ]["digits"]
        is not None

        for instrument
        in ALLOWED_INSTRUMENTS
    )

    if not symbols_ready:
        return

    ctrader_state[
        "market_loading"
    ] = True

    ctrader_state[
        "market_ready"
    ] = False

    delay = 0.0

    for timeframe in (
        "M5",
        "M15",
        "H1",
        "H4",
    ):
        for instrument in ALLOWED_INSTRUMENTS:
            reactor.callLater(
                delay,

                request_trendbars,

                client,
                instrument,
                timeframe,
            )

            delay += 0.55

    reactor.callLater(
        delay + 2.0,
        mark_market_ready,
    )


def mark_market_ready():
    ready = all(
        len(
            market_state[
                instrument
            ]["candles"][timeframe]
        )
        >= 210

        for instrument
        in ALLOWED_INSTRUMENTS

        for timeframe
        in (
            "M5",
            "M15",
            "H1",
            "H4",
        )
    )

    ctrader_state[
        "market_ready"
    ] = ready

    ctrader_state[
        "market_loading"
    ] = False

    if ready:
        ctrader_state[
            "last_market_refresh"
        ] = int(
            time.time()
        )

        clear_error()

        print(
            "[MARKET] READY"
        )

        evaluate_auto_trading()

    else:
        print(
            "[MARKET] NOT READY"
        )

    start_auto_refresh()


# ============================================================
# AUTO REFRESH
# ============================================================

def auto_refresh_market():
    reactor.callLater(
        AUTO_REFRESH_SECONDS,
        auto_refresh_market,
    )

    if (
        ctrader_client is None

        or not ctrader_state[
            "connected"
        ]

        or not ctrader_state[
            "account_authorized"
        ]

        or ctrader_state[
            "market_loading"
        ]
    ):
        return

    print(
        "[AUTO REFRESH] START"
    )

    request_account_data(
        ctrader_client
    )

    start_market_queue(
        ctrader_client
    )


def start_auto_refresh():
    global auto_refresh_started

    if auto_refresh_started:
        return

    auto_refresh_started = True

    print(
        "[AUTO REFRESH] STARTED"
    )

    reactor.callLater(
        AUTO_REFRESH_SECONDS,
        auto_refresh_market,
    )


# ============================================================
# CTRADER CONNECTION
# ============================================================

def start_ctrader_connection():
    global ctrader_client

    if (
        ctrader_client is not None
        and ctrader_state[
            "connected"
        ]
    ):
        print(
            "[CTRADER] EXISTING CONNECTION"
        )

        if ctrader_state[
            "account_authorized"
        ]:
            request_account_data(
                ctrader_client
            )

        elif ctrader_state[
            "account_id"
        ]:
            authorize_account(
                ctrader_client
            )

        elif ctrader_state[
            "application_authorized"
        ]:
            send_account_list_request(
                ctrader_client
            )

        else:
            req = (
                ProtoOAApplicationAuthReq()
            )

            req.clientId = (
                CTRADER_CLIENT_ID
            )

            req.clientSecret = (
                CTRADER_CLIENT_SECRET
            )

            ctrader_client.send(
                req
            ).addErrback(
                safe_errback
            )

        return

    ctrader_client = Client(
        EndPoints.PROTOBUF_LIVE_HOST,
        EndPoints.PROTOBUF_PORT,
        TcpProtocol,
    )

    def connected(client):
        ctrader_state[
            "connected"
        ] = True

        clear_error()

        print(
            "[CTRADER] CONNECTED"
        )

        req = (
            ProtoOAApplicationAuthReq()
        )

        req.clientId = (
            CTRADER_CLIENT_ID
        )

        req.clientSecret = (
            CTRADER_CLIENT_SECRET
        )

        client.send(
            req
        ).addErrback(
            safe_errback
        )

    def disconnected(
        client,
        reason,
    ):
        ctrader_state[
            "connected"
        ] = False

        ctrader_state[
            "account_authorized"
        ] = False

        ctrader_state[
            "market_loading"
        ] = False

        print(
            "[CTRADER] DISCONNECTED",
            reason,
        )

    def on_message(
        client,
        message,
    ):
        payload_type = (
            message.payloadType
        )

        # APP AUTH
        if (
            payload_type
            == ProtoOAApplicationAuthRes().payloadType
        ):
            ctrader_state["application_authorized"] = True
            ctrader_state["account_authorized"] = False
            ctrader_state["account_id"] = None
            ctrader_state["market_ready"] = False
            ctrader_state["market_loading"] = False

            clear_error()

            print(
                "[CTRADER] APP AUTHORIZED -> ACCOUNT LIST",
                flush=True,
            )

            send_account_list_request(
                client
            )

        # ACCOUNT LIST
        elif (
            payload_type
            == ProtoOAGetAccountListByAccessTokenRes().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            if not response.ctidTraderAccount:
                set_error(
                    "Brak rachunku"
                )

                return

            accounts_debug = []
            for account_item in response.ctidTraderAccount:
                accounts_debug.append({
                    "ctidTraderAccountId": int(account_item.ctidTraderAccountId),
                    "traderLogin": int(getattr(account_item, "traderLogin", 0) or 0),
                    "isLive": bool(getattr(account_item, "isLive", False)),
                })

            print(
                f"[CTRADER] ACCOUNT LIST {accounts_debug}",
                flush=True,
            )

            matching_accounts = [
                account_item
                for account_item in response.ctidTraderAccount
                if int(getattr(account_item, "traderLogin", 0) or 0)
                == int(TARGET_TRADER_LOGIN)
            ]

            if not matching_accounts:
                ctrader_state["account_id"] = None
                ctrader_state["account_authorized"] = False
                ctrader_state["market_ready"] = False

                set_error(
                    f"Nie znaleziono traderLogin={TARGET_TRADER_LOGIN}; "
                    f"cTrader zwrocil {accounts_debug}"
                )

                print(
                    f"[CTRADER] TARGET LOGIN NOT FOUND traderLogin={TARGET_TRADER_LOGIN}",
                    flush=True,
                )
                return

            account = matching_accounts[0]

            # IMPORTANT:
            # All Open API requests use the INTERNAL ctidTraderAccountId,
            # not the visible traderLogin.
            ctrader_state["account_id"] = int(
                account.ctidTraderAccountId
            )

            print(
                f"[CTRADER] TARGET FOUND "
                f"traderLogin={int(getattr(account, 'traderLogin', 0) or 0)} "
                f"ctidTraderAccountId={ctrader_state['account_id']} "
                f"isLive={bool(getattr(account, 'isLive', False))}",
                flush=True,
            )

            authorize_account(
                client
            )

        # ACCOUNT AUTH
        elif (
            payload_type
            == ProtoOAAccountAuthRes().payloadType
        ):
            ctrader_state[
                "account_authorized"
            ] = True

            clear_error()

            print(
                f"[CTRADER] ACCOUNT AUTHORIZED "
                f"traderLogin={TARGET_TRADER_LOGIN} "
                f"ctidTraderAccountId={ctrader_state.get('account_id')} "
                f"-> REQUEST MARKET DATA",
                flush=True,
            )

            request_account_data(
                client
            )

        # ACCOUNT DISCONNECT
        elif (
            payload_type
            == ProtoOAAccountDisconnectEvent().payloadType
        ):
            ctrader_state[
                "account_authorized"
            ] = False

            ctrader_state[
                "market_loading"
            ] = False

            reactor.callLater(
                1.0,
                authorize_account,
                client,
            )

        # TOKEN INVALID
        elif (
            payload_type
            == ProtoOAAccountsTokenInvalidatedEvent().payloadType
        ):
            ctrader_state[
                "account_authorized"
            ] = False

            ctrader_state[
                "market_ready"
            ] = False

            set_error(
                "Token cTrader wygasl - odswiezam"
            )

            threading.Thread(
                target=refresh_and_reconnect,
                daemon=True,
            ).start()

        # BALANCE
        elif (
            payload_type
            == ProtoOATraderRes().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            trader = response.trader

            divisor = (
                10
                ** trader.moneyDigits
            )

            ctrader_state[
                "balance"
            ] = round(
                trader.balance
                / divisor,
                2,
            )

            if ctrader_state[
                "equity"
            ] is None:
                ctrader_state[
                    "equity"
                ] = ctrader_state[
                    "balance"
                ]

            reset_daily_state_if_needed()

        # UNREALIZED PNL
        elif (
            payload_type
            == ProtoOAGetPositionUnrealizedPnLRes().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            divisor = (
                10
                ** response.moneyDigits
            )

            pnl = sum(
                item.netUnrealizedPnL
                for item
                in response.positionUnrealizedPnL
            ) / divisor

            ctrader_state[
                "unrealized_pnl"
            ] = round(
                pnl,
                2,
            )

            if ctrader_state[
                "balance"
            ] is not None:
                ctrader_state[
                    "equity"
                ] = round(
                    ctrader_state[
                        "balance"
                    ]
                    + pnl,
                    2,
                )

        # POSITIONS / ORDERS
        elif (
            payload_type
            == ProtoOAReconcileRes().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            positions = []

            for position in response.position:
                positions.append({
                    "position_id": int(
                        position.positionId
                    ),

                    "symbol_id": int(
                        position.tradeData.symbolId
                    ),

                    "volume_raw": int(
                        position.tradeData.volume
                    ),

                    "side": int(
                        position.tradeData.tradeSide
                    ),

                    "price": float(getattr(position, "price", 0.0) or 0.0),
                    "stop_loss": float(getattr(position, "stopLoss", 0.0) or 0.0),
                    "take_profit": float(getattr(position, "takeProfit", 0.0) or 0.0),

                    "label": str(
                        getattr(
                            position.tradeData,
                            "label",
                            "",
                        )
                    ),
                })

            ctrader_state[
                "positions"
            ] = positions
            ctrader_state["positions_reconciled_at"] = int(time.time())

            # cTrader RECONCILE is the source of truth for live positions.
            # Synchronize manager state only after a fresh reconcile response,
            # never from a stale/temporarily empty in-memory list.
            sync_manager_with_reconcile()

            ctrader_state[
                "orders"
            ] = [
                int(order.orderId)
                for order
                in response.order
            ]

        # SYMBOL LIST
        elif (
            payload_type
            == ProtoOASymbolsListRes().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            found_ids = []

            for symbol in response.symbol:
                instrument = detect_instrument(
                    symbol.symbolName
                )

                if instrument is None:
                    continue

                if market_state[
                    instrument
                ]["found"]:
                    continue

                market_state[
                    instrument
                ]["found"] = True

                market_state[
                    instrument
                ]["symbol_id"] = int(
                    symbol.symbolId
                )

                market_state[
                    instrument
                ]["symbol_name"] = (
                    symbol.symbolName
                )

                found_ids.append(
                    int(symbol.symbolId)
                )

            if len(found_ids) < len(ALLOWED_INSTRUMENTS):
                set_error(
                    "Nie znaleziono wymaganych symboli"
                )

                return

            req = ProtoOASymbolByIdReq()

            req.ctidTraderAccountId = int(
                ctrader_state[
                    "account_id"
                ]
            )

            for symbol_id in found_ids:
                req.symbolId.append(
                    symbol_id
                )

            print(
                "[CTRADER] SYMBOLS FOUND"
            )

            client.send(
                req
            ).addErrback(
                safe_errback
            )

        # SYMBOL DETAILS
        elif (
            payload_type
            == ProtoOASymbolByIdRes().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            for symbol in response.symbol:
                symbol_id = int(
                    symbol.symbolId
                )

                for instrument in ALLOWED_INSTRUMENTS:
                    if (
                        market_state[
                            instrument
                        ]["symbol_id"]
                        != symbol_id
                    ):
                        continue

                    data = market_state[
                        instrument
                    ]

                    data["digits"] = int(
                        symbol.digits
                    )

                    data["pip_position"] = int(
                        getattr(
                            symbol,
                            "pipPosition",
                            0,
                        )
                    )

                    data[
                        "min_volume_raw"
                    ] = int(
                        getattr(
                            symbol,
                            "minVolume",
                            0,
                        )
                    )

                    data[
                        "max_volume_raw"
                    ] = int(
                        getattr(
                            symbol,
                            "maxVolume",
                            0,
                        )
                    )

                    data[
                        "step_volume_raw"
                    ] = int(
                        getattr(
                            symbol,
                            "stepVolume",
                            0,
                        )
                    )

                    data[
                        "lot_size_raw"
                    ] = int(
                        getattr(
                            symbol,
                            "lotSize",
                            0,
                        )
                    )

            print(
                "[CTRADER] SYMBOL DETAILS READY"
            )

            start_market_queue(
                client
            )

        # CANDLES
        elif (
            payload_type
            == ProtoOAGetTrendbarsRes().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            symbol_id = int(
                response.symbolId
            )

            timeframe = period_to_name(
                response.period
            )

            instrument = None

            for key in ALLOWED_INSTRUMENTS:
                if (
                    market_state[
                        key
                    ]["symbol_id"]
                    == symbol_id
                ):
                    instrument = key
                    break

            if (
                instrument is None
                or timeframe is None
            ):
                return

            digits = market_state[
                instrument
            ]["digits"]

            candles = [
                trendbar_to_dict(
                    bar,
                    digits,
                )
                for bar
                in response.trendbar
            ]

            candles.sort(
                key=lambda item:
                    item["timestamp"]
            )

            market_state[
                instrument
            ]["candles"][timeframe] = (
                candles
            )

        # EXECUTION
        elif (
            payload_type
            == ProtoOAExecutionEvent().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            execution_type = int(
                response.executionType
            )

            symbol_id = None
            order_id = None

            if response.HasField("order"):
                symbol_id = int(
                    response.order.tradeData.symbolId
                )

                order_id = int(
                    response.order.orderId
                )

            elif response.HasField("position"):
                symbol_id = int(
                    response.position.tradeData.symbolId
                )

            instrument = None

            for key in ALLOWED_INSTRUMENTS:
                if (
                    market_state[
                        key
                    ]["symbol_id"]
                    == symbol_id
                ):
                    instrument = key
                    break

            if instrument:
                trade_state[
                    "pending_symbols"
                ].discard(instrument)

            if (
                execution_type
                == ProtoOAExecutionType.ORDER_FILLED
            ):
                last_order = trade_state.get("last_order") or {}
                is_entry_fill = last_order.get("status") == "SENT" and last_order.get("instrument") == instrument
                if (
                    is_entry_fill
                    and order_id is not None
                    and order_id not in trade_state["counted_order_ids"]
                ):
                    trade_state["counted_order_ids"].add(order_id)
                    trade_state["trades_today"] += 1
                    save_persistent_state()

                if trade_state[
                    "last_order"
                ]:
                    trade_state[
                        "last_order"
                    ]["status"] = "FILLED"

                print(
                    "[ORDER FILLED]",
                    instrument,
                )

                handle_execution_for_manager(response, instrument)

                reactor.callLater(
                    0.5,
                    request_account_data,
                    client,
                )

            elif (
                execution_type
                == ProtoOAExecutionType.ORDER_REJECTED
            ):
                trade_state[
                    "last_order_error"
                ] = {
                    "instrument": instrument,

                    "reason": str(
                        getattr(
                            response,
                            "errorCode",
                            "ORDER_REJECTED",
                        )
                    ),

                    "time": int(
                        time.time()
                    ),
                }

                if trade_state[
                    "last_order"
                ]:
                    trade_state[
                        "last_order"
                    ][
                        "status"
                    ] = "REJECTED"

        # ORDER ERROR
        elif (
            payload_type
            == ProtoOAOrderErrorEvent().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            trade_state[
                "pending_symbols"
            ].clear()

            trade_state[
                "last_order_error"
            ] = {
                "reason": str(
                    response.errorCode
                ),

                "description": str(
                    response.description
                ),

                "time": int(
                    time.time()
                ),
            }

            set_error(
                str(response.errorCode)
                + ": "
                + str(response.description)
            )

        # API ERROR
        elif (
            payload_type
            == ProtoOAErrorRes().payloadType
        ):
            response = (
                Protobuf.extract(
                    message
                )
            )

            error_code = str(
                response.errorCode
            )

            description = str(
                response.description
            )

            if (
                "ALREADY_LOGGED_IN"
                in error_code

                or "ALREADY_LOGGED_IN"
                in description
            ):
                clear_error()

                # If we already selected the exact target account and just sent
                # ACCOUNT AUTH, ALREADY_LOGGED_IN means that account session is
                # already authorized. Do NOT restart APP AUTH / ACCOUNT LIST,
                # otherwise we create an endless auth loop.
                if (
                    ctrader_state.get("account_id")
                    and int(ctrader_state.get("account_id"))
                    > 0
                ):
                    ctrader_state["application_authorized"] = True
                    ctrader_state["account_authorized"] = True
                    ctrader_state["market_ready"] = False
                    ctrader_state["market_loading"] = False

                    print(
                        f"[CTRADER] ALREADY_LOGGED_IN -> "
                        f"ACCOUNT SESSION ACCEPTED "
                        f"traderLogin={TARGET_TRADER_LOGIN} "
                        f"ctidTraderAccountId={ctrader_state.get('account_id')} "
                        f"-> REQUEST MARKET DATA",
                        flush=True,
                    )

                    reactor.callLater(
                        0.2,
                        request_account_data,
                        client,
                    )

                    return

                # Fallback only when no target account has been selected yet:
                # ALREADY_LOGGED_IN applies to application auth.
                ctrader_state["application_authorized"] = True
                ctrader_state["account_authorized"] = False
                ctrader_state["market_ready"] = False
                ctrader_state["market_loading"] = False

                print(
                    "[CTRADER] ALREADY_LOGGED_IN (APP) -> ACCOUNT LIST",
                    flush=True,
                )

                reactor.callLater(
                    0.2,
                    send_account_list_request,
                    client,
                )

                return

            set_error(
                error_code
                + ": "
                + description
            )

    ctrader_client.setConnectedCallback(
        connected
    )

    ctrader_client.setDisconnectedCallback(
        disconnected
    )

    ctrader_client.setMessageReceivedCallback(
        on_message
    )

    ctrader_client.startService()



# ============================================================
# V7 - STRATEGY WEBHOOK + TELEGRAM + TRADE MANAGER
# ============================================================

MANAGER_STATE_FILE = os.path.join(BOT_STATE_DIR, "ftmo_trade_manager_state.json")
manager_state = {
    "active": {},
    "last_action": {},
    "last_error": None,
}
manager_thread_started = False


def send_telegram_message(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM DISABLED]", text)
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        for i in range(0, len(text), 3900):
            requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "text": text[i:i+3900]},
                timeout=15,
            ).raise_for_status()
    except Exception as error:
        print("[TELEGRAM ERROR]", error)


def save_manager_state():
    try:
        ensure_state_dir()
        temp = MANAGER_STATE_FILE + ".tmp"
        with open(temp, "w", encoding="utf-8") as handle:
            json.dump(manager_state, handle, ensure_ascii=False, indent=2)
        os.replace(temp, MANAGER_STATE_FILE)
    except Exception as error:
        manager_state["last_error"] = str(error)
        print("[MANAGER SAVE ERROR]", error)


def load_manager_state():
    if not os.path.exists(MANAGER_STATE_FILE):
        return
    try:
        with open(MANAGER_STATE_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            manager_state["active"] = payload.get("active", {}) or {}
            manager_state["last_action"] = payload.get("last_action", {}) or {}
            manager_state["last_error"] = payload.get("last_error")
    except Exception as error:
        manager_state["last_error"] = str(error)
        print("[MANAGER LOAD ERROR]", error)


def extract_number(pattern, text):
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except Exception:
        return None


def parse_strategy_alert(text):
    upper = text.upper()

    # Robust normalization for TradingView text:
    # - handles Polish diacritics (WEJŚCIE -> WEJSCIE),
    # - handles Unicode composed/decomposed characters,
    # - tolerates extra spaces/newlines.
    normalized = unicodedata.normalize("NFKD", upper)
    normalized = "".join(
        ch for ch in normalized
        if not unicodedata.combining(ch)
    )
    normalized = re.sub(r"\s+", " ", normalized).strip()

    # Avoid fragile regex boundaries here: TradingView text may contain
    # non-breaking spaces / Unicode variants. After normalization, simple
    # token matching is the most reliable.
    if "WEJSCIE LONG" in normalized:
        side = "LONG"
    elif "WEJSCIE SHORT" in normalized:
        side = "SHORT"
    else:
        side = None

    symbol = detect_instrument(upper)
    tf_match = re.search(r"(?:XAUUSD|GOLD|US100|NAS100|USTEC)\s+([A-Za-z0-9]+)\s*-", text, re.IGNORECASE)
    timeframe = tf_match.group(1) if tf_match else "?"

    return {
        "event": "ENTRY" if side else "UNKNOWN",
        "side": side,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_entry": extract_number(r"Cena:\s*([0-9.,]+)", text),
        "strategy_tp": extract_number(r"TP:\s*([0-9.,]+)", text),
        "strategy_sl": extract_number(r"SL:\s*([0-9.,]+)", text),
        "raw": text,
    }


def fixed_volume_for_lots(instrument, lots):
    data = market_state[instrument]
    lot_size_raw = data.get("lot_size_raw")
    min_raw = data.get("min_volume_raw")
    max_raw = data.get("max_volume_raw")
    step_raw = data.get("step_volume_raw")
    if not lot_size_raw or not min_raw or not max_raw or not step_raw:
        return None
    desired = int(round(float(lots) * int(lot_size_raw)))
    raw = int(math.floor(desired / step_raw) * step_raw)
    raw = max(int(min_raw), min(raw, int(max_raw)))
    return raw


def latest_market_price(instrument):
    for tf in ("M5", "M15", "H1"):
        candles = market_state[instrument]["candles"].get(tf, [])
        if candles:
            return float(candles[-1]["close"])
    return None


def submit_strategy_market_order(signal):
    instrument = signal["symbol"]
    if ctrader_client is None or not ctrader_state.get("account_authorized"):
        send_telegram_message("⚠️ cTrader nie jest gotowy. Sygnał nie został wykonany.")
        return
    if instrument not in ALLOWED_INSTRUMENTS:
        return
    if market_state[instrument].get("symbol_id") is None:
        send_telegram_message(f"⚠️ {instrument}: brak symbolu cTrader.")
        return
    if market_state[instrument]["symbol_id"] in open_position_symbol_ids():
        send_telegram_message(f"⚠️ {instrument}: pozycja już jest otwarta. Nowy sygnał pominięty.")
        return

    entry = signal.get("strategy_entry") or latest_market_price(instrument)
    sl = signal.get("strategy_sl")
    tp = signal.get("strategy_tp")
    if entry is None or sl is None or tp is None:
        send_telegram_message(f"⚠️ {instrument}: sygnał bez pełnego Cena/SL/TP. Nie otwieram.")
        return

    # Walidacja kierunku poziomów - nie zmieniamy strategii, tylko odrzucamy technicznie błędny alert.
    if signal["side"] == "LONG" and not (sl < entry < tp):
        send_telegram_message(f"⚠️ {instrument}: błędny układ cen LONG. Cena {entry}, SL {sl}, TP {tp}.")
        return
    if signal["side"] == "SHORT" and not (tp < entry < sl):
        send_telegram_message(f"⚠️ {instrument}: błędny układ cen SHORT. Cena {entry}, SL {sl}, TP {tp}.")
        return

    lots = FIXED_LOTS_XAUUSD if instrument == "XAUUSD" else 0.0
    volume_raw = fixed_volume_for_lots(instrument, lots) if lots > 0 else None
    if volume_raw is None:
        # fallback do starego risk engine dla innych instrumentów
        sizing = size_position(instrument, entry, sl, calculate_risk(current_equity())["risk_usd"])
        if not sizing.get("ok"):
            send_telegram_message(f"⚠️ {instrument}: nie mogę wyliczyć wolumenu: {sizing.get('reason')}")
            return
        volume_raw = int(sizing["volume_raw"])
        lots = sizing.get("lots")

    req = ProtoOANewOrderReq()
    req.ctidTraderAccountId = int(ctrader_state["account_id"])
    req.symbolId = int(market_state[instrument]["symbol_id"])
    req.orderType = ProtoOAOrderType.MARKET
    req.tradeSide = ProtoOATradeSide.BUY if signal["side"] == "LONG" else ProtoOATradeSide.SELL
    req.volume = int(volume_raw)
    req.relativeStopLoss = max(1, int(round(abs(entry - sl) * 100000.0)))
    req.relativeTakeProfit = max(1, int(round(abs(tp - entry) * 100000.0)))
    req.label = BOT_LABEL + "_TV"
    req.comment = f"TV {instrument} {signal['side']} strategy-managed"[:512]
    req.clientOrderId = (f"TV-{instrument}-{int(time.time())}-{uuid.uuid4().hex[:6]}")[:50]

    signal_key = f"{instrument}:{signal['side']}:{round(entry,5)}:{int(time.time()//60)}"
    trade_state["pending_symbols"].add(instrument)
    trade_state["last_signal_key"][instrument] = signal_key
    trade_state["last_order"] = {
        "source": "TRADINGVIEW",
        "instrument": instrument,
        "decision": signal["side"],
        "entry_reference": entry,
        "sl_reference": sl,
        "tp_reference": tp,
        "volume_raw": int(volume_raw),
        "lots": lots,
        "status": "SENT",
        "sent_at": int(time.time()),
    }
    save_persistent_state()

    send_telegram_message(
        f"📥 {instrument} {signal['side']} — SYGNAŁ STRATEGII\n"
        f"Cena strategii: {entry:.2f}\nSL: {sl:.2f}\nTP: {tp:.2f}\n"
        f"Wolumen: {lots if lots is not None else '?'} lot\n"
        f"➡️ Wysyłam MARKET do cTrader."
    )
    ctrader_client.send(req).addErrback(safe_errback)


def process_strategy_alert(text):
    # Never log secrets; only the TradingView body.
    raw_preview = str(text).replace("\n", " | ")[:1200]
    print(f"[TV] RAW ALERT: {raw_preview}", flush=True)

    signal = parse_strategy_alert(text)

    print(
        "[TV] PARSED "
        f"event={signal.get('event')} "
        f"side={signal.get('side')} "
        f"symbol={signal.get('symbol')} "
        f"timeframe={signal.get('timeframe')} "
        f"entry={signal.get('strategy_entry')} "
        f"sl={signal.get('strategy_sl')} "
        f"tp={signal.get('strategy_tp')}",
        flush=True,
    )

    if signal["event"] != "ENTRY" or not signal.get("side"):
        debug_norm = unicodedata.normalize("NFKD", str(text).upper())
        debug_norm = "".join(
            ch for ch in debug_norm
            if not unicodedata.combining(ch)
        )
        debug_norm = re.sub(r"\\s+", " ", debug_norm).strip()
        print(
            f"[TV] REJECTED reason=NO_ENTRY_SIDE normalized={debug_norm[:500]}",
            flush=True,
        )
        return

    if signal.get("symbol") not in ALLOWED_INSTRUMENTS:
        print(
            f"[TV] REJECTED reason=SYMBOL symbol={signal.get('symbol')}",
            flush=True,
        )
        return

    tf = str(signal.get("timeframe", "?")).lower()
    if tf not in ("1h", "60", "60m", "h1"):
        print(f"[TV] REJECTED reason=TIMEFRAME timeframe={tf}", flush=True)
        return

    if not getattr(reactor, "running", False):
        print("[TV] REJECTED reason=REACTOR_NOT_RUNNING", flush=True)
        send_telegram_message(
            "⚠️ cTrader reactor nie działa. Sygnał nie został wykonany."
        )
        return

    print(
        f"[TV] ACCEPTED {signal.get('symbol')} {signal.get('side')} H1 "
        "-> SUBMIT TO CTRADER",
        flush=True,
    )

    reactor.callFromThread(submit_strategy_market_order, signal)


def register_filled_strategy_position(response, instrument):
    last = trade_state.get("last_order") or {}
    if last.get("source") != "TRADINGVIEW" or last.get("instrument") != instrument:
        return
    if not response.HasField("position"):
        return
    position = response.position
    position_id = int(position.positionId)
    actual_entry = float(getattr(position, "price", 0.0) or last.get("entry_reference") or 0.0)
    original_sl = float(last["sl_reference"])
    original_tp = float(last["tp_reference"])
    volume_raw = int(position.tradeData.volume)
    side = last["decision"]

    manager_state["active"][str(position_id)] = {
        "position_id": position_id,
        "instrument": instrument,
        "side": side,
        "entry": actual_entry,
        "strategy_entry": float(last.get("entry_reference") or actual_entry),
        "original_sl": original_sl,
        "current_sl": float(getattr(position, "stopLoss", 0.0) or original_sl),
        "strategy_tp": original_tp,
        "current_tp": float(getattr(position, "takeProfit", 0.0) or original_tp),
        "initial_volume_raw": volume_raw,
        "remaining_volume_raw": volume_raw,
        "partial_done": False,
        "best_price": actual_entry,
        "opened_at": int(time.time()),
        "last_managed_at": 0,
        "reconcile_misses": 0,
        "status": "OPEN",
    }
    save_manager_state()
    send_telegram_message(
        f"✅ {instrument} {side} — POZYCJA OTWARTA\n"
        f"Entry: {actual_entry:.2f}\nSL: {original_sl:.2f}\nTP strategii: {original_tp:.2f}\n"
        f"➡️ AI Manager przejął prowadzenie pozycji."
    )


def find_live_position(position_id):
    for position in ctrader_state.get("positions", []):
        if int(position.get("position_id", 0)) == int(position_id):
            return position
    return None


def sync_manager_with_reconcile():
    """
    Synchronize AI Manager state from a FRESH cTrader RECONCILE snapshot.

    Important:
    - cTrader is the source of truth.
    - A single missing snapshot never closes a managed trade.
    - If an old manager state says CLOSED but cTrader still contains the same
      position_id, the manager restores it to OPEN automatically.
    """
    positions = ctrader_state.get("positions", []) or []
    live_by_id = {
        int(position.get("position_id", 0)): position
        for position in positions
        if int(position.get("position_id", 0) or 0) > 0
    }

    changed = False
    now = int(time.time())

    for key, trade in list(manager_state.get("active", {}).items()):
        try:
            position_id = int(trade.get("position_id") or key)
        except Exception:
            continue

        live = live_by_id.get(position_id)

        if live is not None:
            was_closed = trade.get("status") == "CLOSED"

            # A live cTrader position always wins over stale manager state.
            if was_closed:
                trade["status"] = "OPEN"
                trade["last_managed_at"] = 0
                print(
                    f"[MANAGER SYNC] RESTORED OPEN position_id={position_id}",
                    flush=True,
                )
                send_telegram_message(
                    f"🔄 {trade.get('instrument', 'XAUUSD')} "
                    f"{trade.get('side', '')} — MANAGER WZNOWIONY\n"
                    f"Pozycja {position_id} nadal jest otwarta w cTrader.\n"
                    f"➡️ AI Manager ponownie ją prowadzi."
                )

            trade["reconcile_misses"] = 0
            trade["remaining_volume_raw"] = int(
                live.get("volume_raw")
                or trade.get("remaining_volume_raw")
                or trade.get("initial_volume_raw")
                or 0
            )

            live_sl = float(live.get("stop_loss") or 0.0)
            live_tp = float(live.get("take_profit") or 0.0)
            if live_sl:
                trade["current_sl"] = live_sl
            if live_tp:
                trade["current_tp"] = live_tp

            changed = True
            continue

        # Missing from ONE reconcile is not enough to declare a close.
        if trade.get("status") in ("OPEN", "CLOSING"):
            misses = int(trade.get("reconcile_misses") or 0) + 1
            trade["reconcile_misses"] = misses
            changed = True

            print(
                f"[MANAGER SYNC] position_id={position_id} missing "
                f"from reconcile {misses}/2",
                flush=True,
            )

            # Require two consecutive fresh reconcile snapshots. This avoids
            # the race that previously produced false "POZYCJA ZAMKNIĘTA".
            if misses >= 2 and now - int(trade.get("opened_at") or 0) >= 5:
                trade["status"] = "CLOSED"
                print(
                    f"[MANAGER SYNC] CONFIRMED CLOSED position_id={position_id}",
                    flush=True,
                )
                send_telegram_message(
                    f"🏁 {trade['instrument']} {trade['side']} — "
                    f"POZYCJA ZAMKNIĘTA\n"
                    f"Entry: {float(trade['entry']):.2f}\n"
                    f"Ostatni SL: {float(trade.get('current_sl') or 0):.2f}\n"
                    f"TP strategii: {float(trade.get('strategy_tp') or 0):.2f}\n"
                    f"✅ Zamknięcie potwierdzone przez 2 kolejne "
                    f"odczyty cTrader RECONCILE."
                )

    if changed:
        save_manager_state()


def amend_position(position_id, stop_loss=None, take_profit=None):
    if ctrader_client is None:
        return
    req = ProtoOAAmendPositionSLTPReq()
    req.ctidTraderAccountId = int(ctrader_state["account_id"])
    req.positionId = int(position_id)
    if stop_loss is not None:
        req.stopLoss = float(stop_loss)
    if take_profit is not None:
        req.takeProfit = float(take_profit)
    ctrader_client.send(req).addErrback(safe_errback)


def close_position_volume(position_id, volume_raw):
    if ctrader_client is None or volume_raw <= 0:
        return
    req = ProtoOAClosePositionReq()
    req.ctidTraderAccountId = int(ctrader_state["account_id"])
    req.positionId = int(position_id)
    req.volume = int(volume_raw)
    ctrader_client.send(req).addErrback(safe_errback)


def m15_structure_levels(instrument):
    candles = market_state[instrument]["candles"].get("M15", [])
    if len(candles) < 8:
        return None, None
    closed = candles[-8:-1] if len(candles) >= 9 else candles[-8:]
    return min(c["low"] for c in closed), max(c["high"] for c in closed)


def safe_trailing_price(trade, current_price, r_multiple):
    entry = trade["entry"]
    original_sl = trade["original_sl"]
    risk = abs(entry - original_sl)
    low15, high15 = m15_structure_levels(trade["instrument"])
    side = trade["side"]

    if side == "LONG":
        # Po +1R nie cofamy SL poniżej wejścia; po +2R blokujemy minimum +0.75R.
        floor = entry if r_multiple >= 1.0 else original_sl
        if r_multiple >= 2.0:
            floor = max(floor, entry + 0.75 * risk)
        structural = (low15 - 0.10 * risk) if low15 is not None else floor
        candidate = max(floor, structural)
        candidate = min(candidate, current_price - 0.15 * risk)
        return candidate
    else:
        ceiling = entry if r_multiple >= 1.0 else original_sl
        if r_multiple >= 2.0:
            ceiling = min(ceiling, entry - 0.75 * risk)
        structural = (high15 + 0.10 * risk) if high15 is not None else ceiling
        candidate = min(ceiling, structural)
        candidate = max(candidate, current_price + 0.15 * risk)
        return candidate


def manager_ai_decision(trade, current_price, r_multiple, analysis):
    # AI nie może poszerzać SL ani zwiększać pozycji. Decyzje są dodatkowo ograniczane regułami poniżej.
    if openai_client is None:
        return {"action": "HOLD", "reason": "OPENAI_OFF"}
    compact = {
        "instrument": trade["instrument"],
        "side": trade["side"],
        "entry": trade["entry"],
        "original_sl": trade["original_sl"],
        "current_sl": trade["current_sl"],
        "strategy_tp": trade["strategy_tp"],
        "current_price": current_price,
        "r": round(r_multiple, 2),
        "partial_done": trade.get("partial_done", False),
        "scores": {
            "H1_long": analysis.get("H1_long_score"),
            "H1_short": analysis.get("H1_short_score"),
            "M15_long": analysis.get("M15_long_score"),
            "M15_short": analysis.get("M15_short_score"),
            "M5_long": analysis.get("M5_long_score"),
            "M5_short": analysis.get("M5_short_score"),
        },
    }
    prompt = (
        "Zarządzasz JUŻ OTWARTĄ pozycją ze strategii H1. Nie oceniasz ponownie wejścia. "
        "Priorytet: nie wycinać dużych zwycięzców za wcześnie. H1=teza, M15=struktura, M5=momentum. "
        "M1 pomijamy. Nie wolno poszerzać ryzyka ani zwiększać pozycji. "
        "Przed +0.8R preferuj HOLD. BE dopiero około +1R i tylko przy sensownej strukturze. "
        "Partial zwykle 30% od około +1.5R/+2R. Przy silnym trendzie prowadź resztę M15. "
        "Zwróć WYŁĄCZNIE JSON: {\"action\":\"HOLD|PROTECT|TRAIL|PARTIAL|EXIT\",\"reason\":\"krótko\"}.\n"
        + json.dumps(compact, ensure_ascii=False)
    )
    try:
        response = openai_client.responses.create(
            model=MANAGER_MODEL,
            instructions="Odpowiadaj wyłącznie poprawnym JSON bez markdown.",
            input=prompt,
        )
        raw = (response.output_text or "").strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        data = json.loads(raw)
        if data.get("action") not in ("HOLD", "PROTECT", "TRAIL", "PARTIAL", "EXIT"):
            return {"action": "HOLD", "reason": "AI_BAD_ACTION"}
        return data
    except Exception as error:
        print("[AI MANAGER ERROR]", error)
        return {"action": "HOLD", "reason": "AI_ERROR"}


def manage_one_trade(trade):
    position_id = int(trade["position_id"])
    live = find_live_position(position_id)
    if live is None:
        # Nie zamykamy managera na podstawie chwilowo pustej/starej listy.
        # Status CLOSED może ustawić execution event albo dopiero
        # sync_manager_with_reconcile() po 2 świeżych brakach.
        print(
            f"[MANAGER] WAIT position_id={position_id} "
            f"not present in current position cache",
            flush=True,
        )
        return

    instrument = trade["instrument"]
    price = latest_market_price(instrument)
    if price is None:
        return

    entry = float(trade["entry"])
    original_sl = float(trade["original_sl"])
    risk = abs(entry - original_sl)
    if risk <= 0:
        return

    if trade["side"] == "LONG":
        trade["best_price"] = max(float(trade.get("best_price", entry)), price)
        r_multiple = (price - entry) / risk
    else:
        trade["best_price"] = min(float(trade.get("best_price", entry)), price)
        r_multiple = (entry - price) / risk

    analysis = analyse_instrument(instrument)
    ai = manager_ai_decision(trade, price, r_multiple, analysis)
    action = ai.get("action", "HOLD")
    reason = ai.get("reason", "")

    # Główne zabezpieczenie przed zbyt wczesnym BE / partialem.
    if r_multiple < 0.80 and action in ("PROTECT", "TRAIL", "PARTIAL"):
        action = "HOLD"
        reason = "Za wcześnie na zabezpieczenie (<0.8R)"
    if r_multiple < 1.45 and action == "PARTIAL":
        action = "HOLD"
        reason = "Za wcześnie na partial (<1.45R)"

    # EXIT przed SL tylko przy jednoczesnym zanegowaniu H1 i M15.
    if action == "EXIT" and r_multiple > -0.95:
        h1_long = analysis.get("H1_long_score", 50) or 50
        h1_short = analysis.get("H1_short_score", 50) or 50
        m15_long = analysis.get("M15_long_score", 50) or 50
        m15_short = analysis.get("M15_short_score", 50) or 50
        invalid = (
            trade["side"] == "LONG" and h1_short > h1_long + 12 and m15_short > m15_long + 12
        ) or (
            trade["side"] == "SHORT" and h1_long > h1_short + 12 and m15_long > m15_short + 12
        )
        if not invalid:
            action = "HOLD"
            reason = "Brak pełnego zanegowania H1+M15"

    if action in ("PROTECT", "TRAIL") and r_multiple >= 0.80:
        new_sl = safe_trailing_price(trade, price, r_multiple)
        current_sl = float(trade.get("current_sl") or original_sl)
        improve = new_sl > current_sl if trade["side"] == "LONG" else new_sl < current_sl
        if improve:
            trade["current_sl"] = round(new_sl, market_state[instrument].get("digits") or 2)
            reactor.callFromThread(amend_position, position_id, trade["current_sl"], trade.get("current_tp"))
            send_telegram_message(
                f"🛡️ {instrument} {trade['side']} — {action}\n"
                f"Cena teraz: {price:.2f}\nEntry: {entry:.2f}\n"
                f"Nowy SL: {trade['current_sl']:.2f}\nCel: {trade['current_tp']:.2f}\n"
                f"Powód: {reason}"
            )

    elif action == "PARTIAL" and r_multiple >= 1.45 and not trade.get("partial_done"):
        remaining = int(live.get("volume_raw") or trade.get("remaining_volume_raw") or 0)
        step = int(market_state[instrument].get("step_volume_raw") or 1)
        close_raw = int(math.floor((remaining * 0.30) / step) * step)
        min_raw = int(market_state[instrument].get("min_volume_raw") or step)
        if close_raw >= min_raw and remaining - close_raw >= min_raw:
            trade["partial_done"] = True
            trade["remaining_volume_raw"] = remaining - close_raw
            new_sl = safe_trailing_price(trade, price, max(r_multiple, 1.5))
            current_sl = float(trade.get("current_sl") or original_sl)
            improve = new_sl > current_sl if trade["side"] == "LONG" else new_sl < current_sl
            if improve:
                trade["current_sl"] = round(new_sl, market_state[instrument].get("digits") or 2)
            reactor.callFromThread(close_position_volume, position_id, close_raw)
            if improve:
                reactor.callFromThread(amend_position, position_id, trade["current_sl"], trade.get("current_tp"))
            send_telegram_message(
                f"📤 {instrument} {trade['side']} — ZLECAM PARTIAL 30%\n"
                f"Cena zamknięcia części: {price:.2f}\nPozostała pozycja: 70%\n"
                f"Nowy SL: {trade['current_sl']:.2f}\nCel: {trade['current_tp']:.2f}\n"
                f"Powód: {reason}"
            )

    elif action == "EXIT":
        remaining = int(live.get("volume_raw") or trade.get("remaining_volume_raw") or 0)
        if remaining > 0:
            trade["status"] = "CLOSING"
            reactor.callFromThread(close_position_volume, position_id, remaining)
            send_telegram_message(
                f"⛔ {instrument} {trade['side']} — EXIT EARLY\n"
                f"Cena: {price:.2f}\nEntry: {entry:.2f}\nPowód: {reason}"
            )

    trade["last_managed_at"] = int(time.time())
    manager_state["last_action"][str(position_id)] = {
        "time": int(time.time()), "price": price, "r": round(r_multiple, 2),
        "action": action, "reason": reason,
    }
    save_manager_state()


def manager_loop():
    while True:
        try:
            if ctrader_state.get("account_authorized"):
                # Odśwież konto i świece, potem zarządzaj aktywnymi pozycjami.
                if getattr(reactor, "running", False) and ctrader_client is not None:
                    reactor.callFromThread(request_account_data, ctrader_client)
                    reactor.callFromThread(start_market_queue, ctrader_client)
                for trade in list(manager_state.get("active", {}).values()):
                    if trade.get("status") in ("OPEN", "CLOSING"):
                        manage_one_trade(trade)
        except Exception as error:
            manager_state["last_error"] = str(error)
            print("[MANAGER LOOP ERROR]", error)
        time.sleep(max(30, MANAGER_INTERVAL_SECONDS))


def start_manager_thread():
    global manager_thread_started
    if manager_thread_started:
        return
    manager_thread_started = True
    load_manager_state()
    threading.Thread(target=manager_loop, daemon=True).start()
    print("[MANAGER] STARTED")


def handle_execution_for_manager(response, instrument):
    try:
        execution_type = int(response.executionType)
        if execution_type != ProtoOAExecutionType.ORDER_FILLED:
            return
        closing = bool(
            response.HasField("order")
            and getattr(response.order, "closingOrder", False)
        )

        if response.HasField("deal"):
            deal = response.deal
            position_id = int(getattr(deal, "positionId", 0) or 0)
            execution_price = float(getattr(deal, "executionPrice", 0.0) or 0.0)
            filled_volume_raw = int(getattr(deal, "filledVolume", 0) or getattr(deal, "volume", 0) or 0)
            if closing and position_id and str(position_id) in manager_state["active"]:
                trade = manager_state["active"][str(position_id)]
                before_remaining = int(trade.get("remaining_volume_raw") or trade.get("initial_volume_raw") or 0)
                after_remaining = max(0, before_remaining - filled_volume_raw)
                if response.HasField("position"):
                    after_remaining = int(getattr(response.position.tradeData, "volume", after_remaining) or 0)
                trade["remaining_volume_raw"] = after_remaining
                if execution_price:
                    trade["last_exit_price"] = execution_price

                entry = float(trade["entry"])
                points = (execution_price - entry) if trade["side"] == "LONG" else (entry - execution_price)
                units_closed = filled_volume_raw / 100.0
                realized_piece = points * units_closed
                trade["realized_estimate_usd"] = round(float(trade.get("realized_estimate_usd", 0.0)) + realized_piece, 2)
                closed_pct = 0.0
                if trade.get("initial_volume_raw"):
                    closed_pct = filled_volume_raw / float(trade["initial_volume_raw"]) * 100.0

                is_final = after_remaining <= 0 or trade.get("status") == "CLOSING"
                if is_final:
                    trade["status"] = "CLOSED"
                    send_telegram_message(
                        f"🏁 {trade['instrument']} {trade['side']} — POZYCJA ZAMKNIĘTA\n"
                        f"Entry: {entry:.2f}\nExit: {execution_price:.2f}\n"
                        f"Ruch na ostatnim zamknięciu: {points:+.2f} pkt\n"
                        f"Łączny wynik szacunkowy: ${trade['realized_estimate_usd']:+.2f}"
                    )
                else:
                    remaining_pct = after_remaining / float(trade["initial_volume_raw"]) * 100.0 if trade.get("initial_volume_raw") else 0.0
                    send_telegram_message(
                        f"💰 {trade['instrument']} {trade['side']} — PARTIAL WYKONANY\n"
                        f"Cena wykonania: {execution_price:.2f}\n"
                        f"Zamknięto: {closed_pct:.0f}%\nPozostało: {remaining_pct:.0f}%\n"
                        f"Aktualny SL: {float(trade.get('current_sl') or 0):.2f}\n"
                        f"Cel: {float(trade.get('current_tp') or 0):.2f}"
                    )
                save_manager_state()

        # Register only an ENTRY fill. A closing fill must never recreate
        # the same position as a fresh managed trade.
        if not closing:
            register_filled_strategy_position(response, instrument)
    except Exception as error:
        print("[MANAGER EXEC ERROR]", error)


# ============================================================
# WEB ROUTES
# ============================================================

@app.route("/")
def home():
    reset_daily_state_if_needed()

    return jsonify({
        "bot": "FTMO Auto Bot V6.3",
        "status": "ONLINE",

        "auto_trading_enabled":
            auto_trading_enabled,

        "connected":
            ctrader_state["connected"],

        "application_authorized":
            ctrader_state[
                "application_authorized"
            ],

        "account_authorized":
            ctrader_state[
                "account_authorized"
            ],

        "market_ready":
            ctrader_state[
                "market_ready"
            ],

        "balance":
            ctrader_state["balance"],

        "equity":
            ctrader_state["equity"],

        "unrealized_pnl":
            ctrader_state[
                "unrealized_pnl"
            ],

        "trades_today":
            trade_state[
                "trades_today"
            ],

        "daily_loss_percent":
            round(
                daily_loss_percent(),
                3,
            ),

        "last_order":
            trade_state[
                "last_order"
            ],

        "last_order_error":
            trade_state[
                "last_order_error"
            ],

        "last_market_refresh":
            ctrader_state[
                "last_market_refresh"
            ],

        "error":
            ctrader_state["error"],

        "persistence": {
            "loaded":
                persistence_state[
                    "loaded"
                ],

            "last_saved":
                persistence_state[
                    "last_saved"
                ],

            "error":
                persistence_state[
                    "error"
                ],

            "has_refresh_token":
                bool(
                    ctrader_state.get(
                        "refresh_token"
                    )
                ),
        },
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",

        "connected":
            ctrader_state["connected"],

        "account_authorized":
            ctrader_state[
                "account_authorized"
            ],

        "market_ready":
            ctrader_state[
                "market_ready"
            ],

        "auto_trading_enabled":
            auto_trading_enabled,
    })


@app.route("/risk")
def risk_route():
    reset_daily_state_if_needed()

    equity = current_equity()

    return jsonify({
        "equity": equity,

        "risk":
            calculate_risk(equity),

        "daily_loss_percent":
            round(
                daily_loss_percent(),
                3,
            ),

        "daily_stop_percent":
            OWN_DAILY_STOP_PERCENT,

        "trades_today":
            trade_state[
                "trades_today"
            ],

        "max_trades_day":
            MAX_TRADES_PER_DAY,

        "auto_trading_enabled":
            auto_trading_enabled,
    })


# ============================================================
# AUTO CONTROL
# ============================================================

def check_auto_key():
    if not AUTO_CONTROL_KEY:
        return False

    return (
        request.args.get("key")
        == AUTO_CONTROL_KEY
    )


@app.route("/auto/on")
def auto_on():
    global auto_trading_enabled

    if not check_auto_key():
        return jsonify({
            "status": "error",
            "message":
                "Brak lub zly AUTO_CONTROL_KEY",
        }), 403

    if not ctrader_state[
        "account_authorized"
    ]:
        return jsonify({
            "status": "error",
            "message":
                "Konto cTrader nieautoryzowane",
        }), 409

    if not ctrader_state[
        "market_ready"
    ]:
        return jsonify({
            "status": "error",
            "message":
                "Market not ready",
        }), 409

    auto_trading_enabled = True
    save_persistent_state()

    print(
        "[AUTO] ENABLED"
    )

    reactor.callFromThread(
        evaluate_auto_trading
    )

    return jsonify({
        "status": "success",
        "auto_trading_enabled": True,

        "risk_percent":
            calculate_risk(
                current_equity()
            )["risk_percent"],

        "max_trades_day":
            MAX_TRADES_PER_DAY,

        "daily_stop_percent":
            OWN_DAILY_STOP_PERCENT,
    })


@app.route("/auto/off")
def auto_off():
    global auto_trading_enabled

    if not check_auto_key():
        return jsonify({
            "status": "error",
            "message":
                "Brak lub zly AUTO_CONTROL_KEY",
        }), 403

    auto_trading_enabled = False
    save_persistent_state()

    print(
        "[AUTO] DISABLED"
    )

    return jsonify({
        "status": "success",
        "auto_trading_enabled": False,
    })


# ============================================================
# LOGIN
# ============================================================

@app.route("/ctrader/login")
def ctrader_login():
    params = {
        "client_id":
            CTRADER_CLIENT_ID,

        "redirect_uri":
            CTRADER_REDIRECT_URI,

        "scope":
            "trading",

        "product":
            "web",
    }

    return redirect(
        "https://id.ctrader.com/"
        "my/settings/openapi/"
        "grantingaccess/?"
        + urlencode(params)
    )


@app.route("/ctrader/callback")
def ctrader_callback():
    global reactor_started

    code = request.args.get("code")

    if not code:
        return jsonify({
            "status": "error",
            "message":
                "Brak kodu OAuth",
        }), 400

    try:
        response = requests.get(
            "https://openapi.ctrader.com/apps/token",

            params={
                "grant_type":
                    "authorization_code",

                "code": code,

                "redirect_uri":
                    CTRADER_REDIRECT_URI,

                "client_id":
                    CTRADER_CLIENT_ID,

                "client_secret":
                    CTRADER_CLIENT_SECRET,
            },

            timeout=15,
        )

        response.raise_for_status()

        data = response.json()

    except Exception as error:
        set_error(error)

        return jsonify({
            "status": "error",
            "message":
                "Token error",
        }), 500

    ctrader_state[
        "access_token"
    ] = data.get(
        "accessToken"
    )

    ctrader_state[
        "refresh_token"
    ] = data.get(
        "refreshToken"
    )

    ctrader_state[
        "token_refreshed_at"
    ] = int(
        time.time()
    )

    save_persistent_state()

    ctrader_state[
        "account_authorized"
    ] = False

    ctrader_state[
        "account_id"
    ] = None

    ctrader_state[
        "market_ready"
    ] = False

    ctrader_state[
        "market_loading"
    ] = False

    print(
        f"[CTRADER] OAUTH TOKEN RECEIVED -> traderLogin={TARGET_TRADER_LOGIN}",
        flush=True,
    )

    if not reactor_started:
        threading.Thread(
            target=start_reactor,
            daemon=True,
        ).start()

        time.sleep(0.5)

    def continue_after_oauth():
        if (
            ctrader_client is not None
            and ctrader_state.get("connected")
            and ctrader_state.get("application_authorized")
        ):
            print(
                "[CTRADER] OAUTH -> REQUEST ACCOUNT LIST ON EXISTING CONNECTION",
                flush=True,
            )
            send_account_list_request(ctrader_client)
        else:
            print(
                "[CTRADER] OAUTH -> START/RESTORE CONNECTION",
                flush=True,
            )
            start_ctrader_connection()

    reactor.callFromThread(
        continue_after_oauth
    )

    return jsonify({
        "status": "success",
        "permission": "TRADING",
        "trading_permission": True,

        "auto_trading_enabled":
            auto_trading_enabled,
    })



@app.route("/ctrader/status")
def ctrader_status():
    return jsonify({
        "target_trader_login": TARGET_TRADER_LOGIN,
        "ctid_trader_account_id": ctrader_state.get("account_id"),
        "connected": bool(ctrader_state.get("connected")),
        "application_authorized": bool(ctrader_state.get("application_authorized")),
        "account_authorized": bool(ctrader_state.get("account_authorized")),
        "market_ready": bool(ctrader_state.get("market_ready")),
        "balance": ctrader_state.get("balance"),
        "last_error": ctrader_state.get("last_error"),
        "route": "LIVE",
        "strategy_only_mode": STRATEGY_ONLY_MODE,
        "fixed_lots_xauusd": FIXED_LOTS_XAUUSD,
    })

# ============================================================
# PERSISTENCE STATUS
# ============================================================

@app.route("/persistence")
def persistence_route():
    return jsonify({
        "state_file":
            BOT_STATE_FILE,

        "loaded":
            persistence_state[
                "loaded"
            ],

        "last_saved":
            persistence_state[
                "last_saved"
            ],

        "error":
            persistence_state[
                "error"
            ],

        "has_access_token":
            bool(
                ctrader_state.get(
                    "access_token"
                )
            ),

        "has_refresh_token":
            bool(
                ctrader_state.get(
                    "refresh_token"
                )
            ),

        "token_refreshed_at":
            ctrader_state.get(
                "token_refreshed_at"
            ),

        "auto_trading_enabled":
            auto_trading_enabled,

        # Token values are intentionally NEVER returned.
    })


# ============================================================
# SYMBOLS
# ============================================================

@app.route("/symbols")
def symbols_route():
    result = {}

    for instrument in ALLOWED_INSTRUMENTS:
        data = market_state[instrument]

        min_units = (
            data["min_volume_raw"]
            / 100.0

            if data[
                "min_volume_raw"
            ] is not None

            else None
        )

        step_units = (
            data["step_volume_raw"]
            / 100.0

            if data[
                "step_volume_raw"
            ] is not None

            else None
        )

        lot_size_units = (
            data["lot_size_raw"]
            / 100.0

            if data["lot_size_raw"]
            else None
        )

        result[instrument] = {
            "symbol":
                data["symbol_name"],

            "min_volume_units":
                min_units,

            "step_volume_units":
                step_units,

            "lot_size_units":
                lot_size_units,

            "min_lots": (
                min_units
                / lot_size_units

                if (
                    min_units
                    is not None
                    and lot_size_units
                )

                else None
            ),
        }

    return jsonify(result)


# ============================================================
# MARKET
# ============================================================

@app.route("/market")
def market_route():
    result = {
        "market_ready":
            ctrader_state[
                "market_ready"
            ],

        "market_loading":
            ctrader_state[
                "market_loading"
            ],

        "last_market_refresh":
            ctrader_state[
                "last_market_refresh"
            ],

        "instruments": {},
    }

    for instrument in ALLOWED_INSTRUMENTS:
        data = market_state[instrument]

        result[
            "instruments"
        ][instrument] = {
            "symbol":
                data["symbol_name"],

            "M5_count":
                len(
                    data["candles"]["M5"]
                ),

            "M15_count":
                len(
                    data["candles"]["M15"]
                ),

            "H1_count":
                len(
                    data["candles"]["H1"]
                ),

            "H4_count":
                len(
                    data["candles"]["H4"]
                ),
        }

    return jsonify(result)


@app.route("/market/refresh")
def market_refresh():
    if (
        ctrader_client is None
        or not ctrader_state[
            "account_authorized"
        ]
    ):
        return jsonify({
            "status": "error",
            "message":
                "cTrader niepolaczony",
        }), 503

    reactor.callFromThread(
        request_account_data,
        ctrader_client,
    )

    reactor.callFromThread(
        start_market_queue,
        ctrader_client,
    )

    return jsonify({
        "status":
            "refresh_started",
    })


# ============================================================
# ANALYSIS
# ============================================================

@app.route("/analysis")
def analysis_all():
    return jsonify({
        "US100":
            analyse_instrument(
                "US100"
            ),

        "XAUUSD":
            analyse_instrument(
                "XAUUSD"
            ),

        "market_ready":
            ctrader_state[
                "market_ready"
            ],

        "auto_trading_enabled":
            auto_trading_enabled,

        "last_market_refresh":
            ctrader_state[
                "last_market_refresh"
            ],
    })


@app.route("/analysis/<instrument>")
def analysis_single(instrument):
    key = instrument.upper()

    if key in (
        "GOLD",
        "XAU",
        "XAUUSD",
    ):
        key = "XAUUSD"

    elif key in (
        "US100",
        "NAS100",
        "USTEC",
        "NASDAQ",
        "NASDAQ100",
    ):
        key = "US100"

    if key not in market_state:
        return jsonify({
            "status": "error",
            "message":
                "Nieznany instrument",
        }), 404

    return jsonify(
        analyse_instrument(key)
    )



# ============================================================
# TRADINGVIEW WEBHOOK / MANAGER STATUS
# ============================================================

@app.route("/webhook", methods=["POST"])
def tradingview_webhook():
    if WEBHOOK_SECRET and request.args.get("secret") != WEBHOOK_SECRET:
        return jsonify({"status": "error", "message": "invalid secret"}), 403
    if request.is_json:
        data = request.get_json(silent=True)
        if isinstance(data, dict):
            text = data.get("message") or data.get("text") or json.dumps(data)
        else:
            text = str(data)
    else:
        text = request.get_data(as_text=True)
    if not text or not text.strip():
        print("[WEBHOOK] EMPTY ALERT", flush=True)
        return jsonify({"status": "error", "message": "empty alert"}), 400

    print(
        f"[WEBHOOK] RECEIVED content_type={request.content_type} "
        f"bytes={len(text.encode('utf-8', errors='ignore'))}",
        flush=True,
    )

    threading.Thread(
        target=process_strategy_alert,
        args=(text,),
        daemon=True,
    ).start()

    return jsonify({
        "status": "accepted",
        "mode": "STRATEGY_MANAGER",
    }), 200


@app.route("/manager/status")
def manager_status():
    return jsonify({
        "strategy_only_mode": STRATEGY_ONLY_MODE,
        "manager_interval_seconds": MANAGER_INTERVAL_SECONDS,
        "fixed_lots_xauusd": FIXED_LOTS_XAUUSD,
        "openai_enabled": bool(openai_client),
        "telegram_enabled": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID),
        "active": manager_state.get("active", {}),
        "last_action": manager_state.get("last_action", {}),
        "last_error": manager_state.get("last_error"),
    })


# ============================================================
# TRADE STATUS
# ============================================================

@app.route("/trade/status")
def trade_status():
    reset_daily_state_if_needed()

    return jsonify({
        "auto_trading_enabled":
            auto_trading_enabled,

        "trades_today":
            trade_state[
                "trades_today"
            ],

        "max_trades_day":
            MAX_TRADES_PER_DAY,

        "daily_loss_percent":
            round(
                daily_loss_percent(),
                3,
            ),

        "daily_stop_percent":
            OWN_DAILY_STOP_PERCENT,

        "pending_symbols":
            list(
                trade_state[
                    "pending_symbols"
                ]
            ),

        "positions":
            ctrader_state[
                "positions"
            ],

        "last_order":
            trade_state[
                "last_order"
            ],

        "last_order_error":
            trade_state[
                "last_order_error"
            ],
    })


# ============================================================
# START
# ============================================================

# IMPORTANT: deploy this build with: python app_v7_strategy_manager.py
# The embedded Twisted reactor must stay in the same long-lived process as Flask.
# Start recovery in a daemon thread at import time.
threading.Thread(
    target=bootstrap_bot,
    daemon=True,
).start()

start_manager_thread()


if __name__ == "__main__":
    port = int(
        os.environ.get(
            "PORT",
            10000,
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )
