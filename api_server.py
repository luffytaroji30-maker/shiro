#!/usr/bin/env python3
"""
Shiro Shopify API v5.1 - API-only (no Selenium)
Uses shopifyapi async checkout directly. CAPTCHA = Declined.
"""

import os
import sys
import time
import asyncio
import threading
import random
import logging
from flask import Flask, request, jsonify
from datetime import datetime
from shopifyapi import format_proxy, run_shopify_check

logger = logging.getLogger("shiro_api")
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
TIMEOUT = 120
MAX_QUEUE = 200

# ══════════════════════════════════════════════════════════════════════════════
# SHARED EVENT LOOP (runs in a daemon thread)
# ══════════════════════════════════════════════════════════════════════════════
_shared_loop = asyncio.new_event_loop()

def _start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=_start_loop, args=(_shared_loop,), daemon=True).start()
_active = 0
_active_lock = threading.Lock()


def _check(site, card, proxy=None):
    """Run one Shopify check via shopifyapi (async, no browser)."""
    global _active
    with _active_lock:
        if _active >= MAX_QUEUE:
            return {"status": "Error", "message": f"Busy ({_active})", "error_code": "SERVER_BUSY"}
        _active += 1
    try:
        fut = asyncio.run_coroutine_threadsafe(
            run_shopify_check(site, card, proxy_url=proxy, verbose=False, timeout=TIMEOUT, max_captcha_retries=0),
            _shared_loop,
        )
        result = fut.result(timeout=TIMEOUT + 10)
    except Exception as e:
        result = {"status": "Error", "message": str(e)[:200]}
    finally:
        with _active_lock:
            _active -= 1

    # Treat CAPTCHA / Checkpoint as Declined (no Selenium)
    code = str(result.get("error_code", "")).upper()
    msg = str(result.get("message", "")).upper()
    if "CAPTCHA" in code or "CAPTCHA" in msg or "CHECKPOINT" in code or "CHECKPOINT" in msg:
        result["status"] = "Declined"
        result["message"] = "Card Declined"
        result["error_code"] = "CARD_DECLINED"

    return result


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════
@app.route('/shopify', methods=['GET'])
def shopify():
    site = request.args.get('site', '').strip()
    cc = request.args.get('cc', '').strip()
    proxy = request.args.get('proxy', '').strip()
    
    if not site or not cc or cc.count('|') != 3:
        return jsonify({"status": "Error", "message": "Bad params"}), 400
    
    with _active_lock:
        qs = _active
    if qs > MAX_QUEUE:
        return jsonify({"status": "Error", "message": f"Busy ({qs})", "error_code": "SERVER_BUSY"})
    
    t0 = time.time()
    res = _check(site, cc, format_proxy(proxy) if proxy else None)
    elapsed = time.time() - t0
    
    out = {
        "status": res.get("status", "Error"),
        "message": res.get("message", ""),
        "error_code": res.get("error_code", ""),
        "price": res.get("price", ""),
        "product": res.get("product", ""),
        "gateway": "Shopify Payments",
        "time": f"{elapsed:.1f}s",
        "site": site.replace("https://","").replace("http://","").split("/")[0]
    }
    
    mask = cc[:6] + "****" + cc.split("|")[0][-4:] if len(cc) > 10 else "****"
    with _active_lock:
        busy = _active
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {out['status']} | {mask} | {elapsed:.1f}s | q:{qs} w:{busy}")
    
    return jsonify(out)


@app.route('/health')
def health():
    with _active_lock:
        busy = _active
    return jsonify({"status": "ok", "v": "5.2", "active": busy})


@app.route('/')
def index():
    return jsonify({"name": "Shiro API", "v": "5.2"})


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    print(f"[Shiro API] v5.2 | API-only (no Selenium) | port {port}")
    app.run(host='0.0.0.0', port=port, threaded=True)
