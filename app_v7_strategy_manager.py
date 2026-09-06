
import os
import threading
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, redirect, request

from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAApplicationAuthRes,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOAGetAccountListByAccessTokenRes,
    ProtoOAAccountAuthReq,
    ProtoOAAccountAuthRes,
    ProtoOATraderReq,
    ProtoOATraderRes,
    ProtoOAErrorRes,
)
from twisted.internet import reactor

app = Flask(__name__)

CLIENT_ID = os.environ.get("CTRADER_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("CTRADER_CLIENT_SECRET", "")
REDIRECT_URI = os.environ.get("CTRADER_REDIRECT_URI", "")
TARGET_ACCOUNT_ID = 17188951

state = {
    "access_token": None,
    "connected": False,
    "app_authorized": False,
    "account_found": False,
    "account_authorized": False,
    "account_id": None,
    "is_live": None,
    "balance_raw": None,
    "last_error": None,
}

client = None
reactor_started = False


def log(msg):
    print(msg, flush=True)


def set_error(msg):
    state["last_error"] = str(msg)
    log(f"[ERROR] {msg}")


def send_app_auth():
    if client is None:
        return
    req = ProtoOAApplicationAuthReq()
    req.clientId = CLIENT_ID
    req.clientSecret = CLIENT_SECRET
    log("[TEST] SEND APP AUTH")
    client.send(req).addErrback(lambda e: set_error(e))


def send_account_list():
    if client is None or not state["access_token"]:
        set_error("Brak klienta lub access token")
        return
    req = ProtoOAGetAccountListByAccessTokenReq()
    req.accessToken = state["access_token"]
    log("[TEST] SEND ACCOUNT LIST")
    client.send(req).addErrback(lambda e: set_error(e))


def send_account_auth():
    if client is None or not state["access_token"] or not state["account_id"]:
        set_error("Brak danych do ACCOUNT AUTH")
        return
    req = ProtoOAAccountAuthReq()
    req.ctidTraderAccountId = int(state["account_id"])
    req.accessToken = state["access_token"]
    log(f"[TEST] SEND ACCOUNT AUTH id={state['account_id']}")
    client.send(req).addErrback(lambda e: set_error(e))


def send_trader():
    if client is None or not state["account_authorized"]:
        return
    req = ProtoOATraderReq()
    req.ctidTraderAccountId = int(state["account_id"])
    log(f"[TEST] SEND TRADER/BALANCE id={state['account_id']}")
    client.send(req).addErrback(lambda e: set_error(e))


def start_ctrader():
    global client

    if client is not None:
        return

    log("[TEST] CONNECT LIVE ROUTE")
    client = Client(
        EndPoints.PROTOBUF_LIVE_HOST,
        EndPoints.PROTOBUF_PORT,
        TcpProtocol,
    )

    def on_connected(c):
        state["connected"] = True
        log("[TEST] CONNECTED")
        send_app_auth()

    def on_disconnected(c, reason):
        state["connected"] = False
        state["app_authorized"] = False
        state["account_authorized"] = False
        log(f"[TEST] DISCONNECTED {reason}")

    def on_message(c, message):
        pt = message.payloadType

        if pt == ProtoOAApplicationAuthRes().payloadType:
            state["app_authorized"] = True
            log("[TEST] APP AUTHORIZED")
            if state["access_token"]:
                send_account_list()
            else:
                log("[TEST] WAITING FOR OAUTH TOKEN")

        elif pt == ProtoOAGetAccountListByAccessTokenRes().payloadType:
            res = Protobuf.extract(message)
            ids = [
                (int(a.ctidTraderAccountId), bool(getattr(a, "isLive", False)))
                for a in res.ctidTraderAccount
            ]
            log(f"[TEST] ACCOUNT LIST = {ids}")

            match = [
                a for a in res.ctidTraderAccount
                if int(a.ctidTraderAccountId) == TARGET_ACCOUNT_ID
            ]

            if not match:
                set_error(f"TARGET {TARGET_ACCOUNT_ID} NOT FOUND; returned={ids}")
                return

            account = match[0]
            state["account_found"] = True
            state["account_id"] = int(account.ctidTraderAccountId)
            state["is_live"] = bool(getattr(account, "isLive", False))
            log(f"[TEST] TARGET FOUND id={state['account_id']} isLive={state['is_live']}")
            send_account_auth()

        elif pt == ProtoOAAccountAuthRes().payloadType:
            state["account_authorized"] = True
            log(f"[TEST] ACCOUNT AUTHORIZED id={state['account_id']}")
            send_trader()

        elif pt == ProtoOATraderRes().payloadType:
            res = Protobuf.extract(message)
            trader = getattr(res, "trader", None)
            balance = getattr(trader, "balance", None) if trader else None
            state["balance_raw"] = balance
            log(f"[TEST] TRADER RESPONSE balance_raw={balance}")

        elif pt == ProtoOAErrorRes().payloadType:
            res = Protobuf.extract(message)
            code = getattr(res, "errorCode", "")
            desc = getattr(res, "description", "")
            set_error(f"{code}: {desc}")

    client.setConnectedCallback(on_connected)
    client.setDisconnectedCallback(on_disconnected)
    client.setMessageReceivedCallback(on_message)
    client.startService()


def start_reactor():
    global reactor_started
    if reactor_started:
        return
    reactor_started = True
    reactor.callWhenRunning(start_ctrader)
    reactor.run(installSignalHandlers=False)


@app.route("/")
def root():
    return jsonify({
        "status": "ctrader-auth-test",
        "target_account": TARGET_ACCOUNT_ID,
        "state": state,
    })


@app.route("/ctrader/login")
def ctrader_login():
    if not CLIENT_ID or not REDIRECT_URI:
        return jsonify({"error": "Brak CLIENT_ID lub REDIRECT_URI"}), 500

    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": "trading",
        "product": "web",
    }
    url = "https://id.ctrader.com/my/settings/openapi/grantingaccess/?" + urlencode(params)
    return redirect(url)


@app.route("/ctrader/callback")
def ctrader_callback():
    code = request.args.get("code")
    if not code:
        return jsonify({"error": "Brak code"}), 400

    try:
        r = requests.get(
            "https://openapi.ctrader.com/apps/token",
            params={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        set_error(e)
        return jsonify({"status": "token_error", "error": str(e)}), 500

    token = data.get("accessToken")
    if not token:
        set_error("Brak accessToken")
        return jsonify({"status": "token_error"}), 500

    state["access_token"] = token
    state["account_found"] = False
    state["account_authorized"] = False
    state["account_id"] = None
    state["is_live"] = None
    state["balance_raw"] = None
    state["last_error"] = None

    log("[TEST] OAUTH TOKEN RECEIVED")

    def after_oauth():
        if state["app_authorized"]:
            send_account_list()
        elif state["connected"]:
            send_app_auth()
        else:
            start_ctrader()

    reactor.callFromThread(after_oauth)

    return jsonify({
        "status": "success",
        "target_account": TARGET_ACCOUNT_ID,
        "next": "/",
    })


threading.Thread(target=start_reactor, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))
