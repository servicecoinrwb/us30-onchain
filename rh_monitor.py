#!/usr/bin/env python3
"""
US30 ONCHAIN — Robinhood Chain Index Proxy Monitor (Layer 1)
=============================================================
Reads Chainlink stock-token feeds + Uniswap v3 pool prices on Robinhood Chain
(chain id 4663) and alerts on:

  1. Onchain premium/discount: pool price vs Chainlink feed (weekday flow signal)
  2. Weekend drift: pool price vs last feed print while feeds are dark (24/5 feeds)
  3. Dow coverage: new Dow-30 component tokens appearing in the official docs
     (i.e. "the US30 basket vault just got more buildable")

Designed to sit alongside scalper_alerts.py on Railway or a Pi.
State in SQLite. Alerts via Telegram (same bot pattern as MT Dispatch).

ENV VARS (put in Railway vars or /etc/mtobserver/observer.env):
  RH_RPC_URL          default: https://rpc.mainnet.chain.robinhood.com
                      (public RPC is rate-limited; use an Alchemy key for prod:
                       https://robinhood-mainnet.g.alchemy.com/v2/{KEY})
  TELEGRAM_BOT_TOKEN  existing MT Dispatch bot token
  TELEGRAM_CHAT_ID    your chat id
  RH_DB_PATH          default: ./rh_monitor.db
  RH_POLL_SECONDS     default: 300 (5 min, Cypher cadence)
  RH_DRIFT_ALERT_BPS  default: 30 (alert when |pool vs feed| >= 30 bps)

FILL-IN BEFORE FIRST RUN (see ASSETS below):
  - Chainlink feed proxy addresses:
      https://docs.chain.link/data-feeds/price-feeds/addresses?network=robinhood
    (docs say read from there, don't trust third-party lists)
  - Uniswap v3 pool addresses: find on https://robinhoodchain.blockscout.com
    (search the token address, look at its top USDG/WETH pool), or via the
    Uniswap interface once you've confirmed the canonical pool.
  Leave feed/pool as None to skip that half of the check for an asset.
"""

import json
import os
import re
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone

from web3 import Web3

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

RPC_URL = os.environ.get("RH_RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
DB_PATH = os.environ.get("RH_DB_PATH", "./rh_monitor.db")
POLL_SECONDS = int(os.environ.get("RH_POLL_SECONDS", "300"))
DRIFT_ALERT_BPS = float(os.environ.get("RH_DRIFT_ALERT_BPS", "30"))
FEED_STALE_SECONDS = 2 * 3600  # feed older than this => market closed / paused
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")

CHAIN_ID = 4663
DOCS_CONTRACTS_URL = "https://docs.robinhood.com/chain/contracts"

# Canonical quote tokens (docs.robinhood.com/chain/contracts)
USDG = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"   # USD-pegged — pool price is USD directly
WETH = "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"   # needs ETH/USD conversion
ETH_USD_FEED = os.environ.get("RH_ETH_USD_FEED", "0x78F3556b67E17Df817D51Ef5a990cDaF09E8d3A9")  # Chainlink ETH/USD Standard Proxy

# Canonical token addresses from docs.robinhood.com/chain/contracts (July 2026).
# feed = Chainlink AggregatorV3 proxy (fill from docs.chain.link, network=robinhood)
# pool = Uniswap v3 pool vs USDG or WETH (fill from Blockscout)
ASSETS = {
    "SPY":  {"token": "0x117cc2133c37B721F49dE2A7a74833232B3B4C0C", "feed": "0x319724394D3A0e3669269846abE664Cd621f9f6A", "pool": None},
    "QQQ":  {"token": "0xD5f3879160bc7c32ebb4dC785F8a4F505888de68", "feed": "0x80901d846d5D7B030F26B480776EE3b29374C2ae", "pool": None},
    "NVDA": {"token": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC", "feed": "0x379EC4f7C378F34a1B47E4F3cbeBCbAC3E8E9F15", "pool": None},
    "AAPL": {"token": "0xaF3D76f1834A1d425780943C99Ea8A608f8a93f9", "feed": "0x6B22A786bAa607d76728168703a39Ea9C99f2cD0", "pool": None},
    "MSFT": {"token": "0xe93237C50D904957Cf27E7B1133b510C669c2e74", "feed": "0x45C3C877C15E6BA2EBB19eA114Ea508d14C1Af2E", "pool": None},
    "AMZN": {"token": "0x12f190a9F9d7D37a250758b26824B97CE941bF54", "feed": "0xD5a1508ceD74c084eBf3cBe853e2C968fB2a651C", "pool": None},
}

# Dow 30 roster (early 2026 — update on index changes; this drives coverage alerts)
DOW_30 = [
    "MMM", "AXP", "AMGN", "AMZN", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO",
    "DIS", "GS", "HD", "HON", "IBM", "JNJ", "JPM", "MCD", "MRK", "MSFT",
    "NKE", "NVDA", "PG", "CRM", "SHW", "TRV", "UNH", "V", "VZ", "WMT",
]

# ----------------------------------------------------------------------------
# ABIs (minimal)
# ----------------------------------------------------------------------------

FEED_ABI = json.loads("""[
 {"name":"latestRoundData","outputs":[{"type":"uint80"},{"type":"int256"},
  {"type":"uint256"},{"type":"uint256"},{"type":"uint80"}],
  "inputs":[],"stateMutability":"view","type":"function"},
 {"name":"decimals","outputs":[{"type":"uint8"}],"inputs":[],
  "stateMutability":"view","type":"function"},
 {"name":"description","outputs":[{"type":"string"}],"inputs":[],
  "stateMutability":"view","type":"function"}
]""")

TOKEN_ABI = json.loads("""[
 {"name":"decimals","outputs":[{"type":"uint8"}],"inputs":[],
  "stateMutability":"view","type":"function"},
 {"name":"symbol","outputs":[{"type":"string"}],"inputs":[],
  "stateMutability":"view","type":"function"},
 {"name":"uiMultiplier","outputs":[{"type":"uint256"}],"inputs":[],
  "stateMutability":"view","type":"function"},
 {"name":"oraclePaused","outputs":[{"type":"bool"}],"inputs":[],
  "stateMutability":"view","type":"function"}
]""")

POOL_ABI = json.loads("""[
 {"name":"slot0","outputs":[{"type":"uint160","name":"sqrtPriceX96"},
  {"type":"int24"},{"type":"uint16"},{"type":"uint16"},{"type":"uint16"},
  {"type":"uint8"},{"type":"bool"}],
  "inputs":[],"stateMutability":"view","type":"function"},
 {"name":"token0","outputs":[{"type":"address"}],"inputs":[],
  "stateMutability":"view","type":"function"},
 {"name":"token1","outputs":[{"type":"address"}],"inputs":[],
  "stateMutability":"view","type":"function"}
]""")

# ----------------------------------------------------------------------------
# Plumbing
# ----------------------------------------------------------------------------

def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS readings(
        ts INTEGER, ticker TEXT, feed_price REAL, feed_updated INTEGER,
        pool_price REAL, multiplier REAL, drift_bps REAL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT)""")
    return con

def kv_get(con, k, default=None):
    row = con.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return row[0] if row else default

def kv_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", (k, str(v)))
    con.commit()

def tg_send(text):
    if not TG_TOKEN or not TG_CHAT:
        print("[TG disabled]", text)
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = json.dumps({"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        print("Telegram send failed:", e)

# ----------------------------------------------------------------------------
# Chain reads
# ----------------------------------------------------------------------------

def connect():
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 20}))
    assert w3.is_connected(), f"RPC not reachable: {RPC_URL}"
    cid = w3.eth.chain_id
    assert cid == CHAIN_ID, f"Wrong chain id {cid}, expected {CHAIN_ID}"
    return w3

def read_feed(w3, feed_addr):
    """Returns (price_float, updated_at_unix). Price is per-TOKEN (multiplier-adjusted)."""
    feed = w3.eth.contract(address=Web3.to_checksum_address(feed_addr), abi=FEED_ABI)
    _, answer, _, updated_at, _ = feed.functions.latestRoundData().call()
    dec = feed.functions.decimals().call()
    if answer <= 0:
        return None, updated_at
    return answer / (10 ** dec), updated_at

def read_token_meta(w3, token_addr):
    """Returns (uiMultiplier_float, oracle_paused). Tolerant of missing methods."""
    t = w3.eth.contract(address=Web3.to_checksum_address(token_addr), abi=TOKEN_ABI)
    mult, paused = 1.0, False
    try:
        mult = t.functions.uiMultiplier().call() / 1e18
    except Exception:
        pass
    try:
        paused = t.functions.oraclePaused().call()
    except Exception:
        pass
    return mult, paused

_dec_cache = {}
def erc20_decimals(w3, addr):
    addr = Web3.to_checksum_address(addr)
    if addr not in _dec_cache:
        c = w3.eth.contract(address=addr, abi=TOKEN_ABI)
        _dec_cache[addr] = c.functions.decimals().call()
    return _dec_cache[addr]

def read_pool_price(w3, pool_addr, stock_token_addr):
    """
    Uniswap v3 slot0 → price of the stock token in quote-token units.
    Handles either token ordering and mixed decimals (USDG vs 18-dec stock tokens).
    """
    pool = w3.eth.contract(address=Web3.to_checksum_address(pool_addr), abi=POOL_ABI)
    sqrt_px = pool.functions.slot0().call()[0]
    t0 = pool.functions.token0().call()
    t1 = pool.functions.token1().call()
    d0, d1 = erc20_decimals(w3, t0), erc20_decimals(w3, t1)
    # raw price = token1 per token0
    p = (sqrt_px / 2**96) ** 2 * (10 ** (d0 - d1))
    stock = Web3.to_checksum_address(stock_token_addr)
    if Web3.to_checksum_address(t0) == stock:
        price, quote = p, Web3.to_checksum_address(t1)          # quote per stock
    elif Web3.to_checksum_address(t1) == stock:
        price, quote = (1.0 / p if p else None), Web3.to_checksum_address(t0)
    else:
        return None

    if price is None:
        return None
    if quote == Web3.to_checksum_address(USDG):
        return price                                            # already USD
    if quote == Web3.to_checksum_address(WETH):
        if not ETH_USD_FEED:
            print("WETH-quoted pool but RH_ETH_USD_FEED not set — skipping", pool_addr)
            return None
        eth_usd, _ = read_feed(w3, ETH_USD_FEED)
        return price * eth_usd if eth_usd else None
    print("Unknown quote token", quote, "for pool", pool_addr, "— skipping")
    return None

# ----------------------------------------------------------------------------
# Dow coverage tracker — scrape official docs, diff tickers
# ----------------------------------------------------------------------------

def fetch_docs_tickers():
    """Pull the canonical Stock Token ticker list from the official docs page."""
    req = urllib.request.Request(DOCS_CONTRACTS_URL, headers={"User-Agent": "us30-onchain/1.0"})
    html = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore")
    # rows look like: | AAPL | [`0x...`](https://robinhoodchain.blockscout.com/address/0x...)
    tickers = set(re.findall(r"robinhoodchain\.blockscout\.com/address/0x[a-fA-F0-9]{40}", html))
    # extract ticker labels near addresses instead: grab table cell tickers
    labels = set(re.findall(r">\s*([A-Z]{1,6})\s*<", html))
    return {t for t in labels if 1 <= len(t) <= 6}

def check_dow_coverage(con):
    try:
        live = fetch_docs_tickers()
    except Exception as e:
        print("docs fetch failed:", e)
        return
    known = set(json.loads(kv_get(con, "known_tickers", "[]")))
    new = live - known
    if known:  # skip alert storm on first run
        new_dow = sorted(t for t in new if t in DOW_30)
        if new_dow:
            covered = sorted(t for t in live if t in DOW_30)
            tg_send(
                "🏗️ <b>US30 ONCHAIN — Dow coverage grew</b>\n"
                f"New Dow component token(s): <b>{', '.join(new_dow)}</b>\n"
                f"Coverage now {len(covered)}/30: {', '.join(covered)}\n"
                "Verify canonical address at docs.robinhood.com/chain/contracts "
                "before touching it."
            )
    kv_set(con, "known_tickers", json.dumps(sorted(live | known)))

# ----------------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------------

def now_utc():
    return int(datetime.now(timezone.utc).timestamp())

def cycle(w3, con):
    ts = now_utc()
    lines = []
    for ticker, a in ASSETS.items():
        feed_price = feed_updated = pool_price = None
        mult, paused = read_token_meta(w3, a["token"])

        if a["feed"]:
            try:
                feed_price, feed_updated = read_feed(w3, a["feed"])
            except Exception as e:
                print(ticker, "feed read failed:", e)
        if a["pool"]:
            try:
                pool_price = read_pool_price(w3, a["pool"], a["token"])
            except Exception as e:
                print(ticker, "pool read failed:", e)

        drift_bps = None
        if feed_price and pool_price:
            drift_bps = (pool_price / feed_price - 1.0) * 10_000
            stale = feed_updated and (ts - feed_updated) > FEED_STALE_SECONDS
            mode = "WEEKEND DRIFT" if stale else "LIVE PREMIUM"
            share_px = feed_price / mult if mult else feed_price

            # de-dupe: only alert when crossing threshold or flipping sign
            last = float(kv_get(con, f"last_drift_{ticker}", "0") or 0)
            crossed = abs(drift_bps) >= DRIFT_ALERT_BPS and (
                abs(last) < DRIFT_ALERT_BPS or (last > 0) != (drift_bps > 0)
            )
            if paused:
                lines.append(f"⏸️ {ticker}: oracle paused (corporate action) — ignore price")
            elif crossed:
                arrow = "🟢" if drift_bps > 0 else "🔴"
                lines.append(
                    f"{arrow} <b>{ticker}</b> {mode}: pool {pool_price:,.2f} vs "
                    f"feed {feed_price:,.2f} → <b>{drift_bps:+.0f} bps</b>"
                    f" (share px ≈ {share_px:,.2f}, mult {mult:.4f})"
                )
            kv_set(con, f"last_drift_{ticker}", drift_bps)

        con.execute(
            "INSERT INTO readings VALUES (?,?,?,?,?,?,?)",
            (ts, ticker, feed_price, feed_updated, pool_price, mult, drift_bps),
        )
    con.commit()
    if lines:
        tg_send("📡 <b>US30 ONCHAIN</b>\n" + "\n".join(lines))

def validate_config(w3):
    """Fail loudly at startup if a feed field holds a token address or a dead feed."""
    token_addrs = {Web3.to_checksum_address(a["token"]) for a in ASSETS.values()}
    problems = []
    for ticker, a in ASSETS.items():
        if not a["feed"]:
            continue
        feed = Web3.to_checksum_address(a["feed"])
        if feed in token_addrs:
            problems.append(f"{ticker}: feed field contains a TOKEN address ({feed}) — "
                            "use the Chainlink feed PROXY address instead")
            continue
        try:
            price, updated = read_feed(w3, feed)
            desc = ""
            try:
                c = w3.eth.contract(address=feed, abi=FEED_ABI)
                desc = c.functions.description().call()
            except Exception:
                pass
            if desc and ticker.upper() not in desc.upper():
                problems.append(f"{ticker}: feed {feed} says it is '{desc}' — "
                                "wrong feed in this slot")
                continue
            print(f"  {ticker} feed OK: {price} ({desc or 'no description'}, updated {updated})")
        except Exception as e:
            problems.append(f"{ticker}: feed {feed} failed latestRoundData(): {e}")
    if problems:
        raise SystemExit("CONFIG ERRORS:\n  " + "\n  ".join(problems))

def main():
    con = db()
    w3 = connect()
    print(f"Connected to Robinhood Chain (id {CHAIN_ID}) via {RPC_URL}")
    validate_config(w3)
    tg_send("📡 US30 ONCHAIN monitor started.")
    last_coverage_check = 0
    while True:
        try:
            cycle(w3, con)
            if now_utc() - last_coverage_check > 6 * 3600:  # docs diff every 6h
                check_dow_coverage(con)
                last_coverage_check = now_utc()
        except Exception as e:
            print("cycle error:", e)
            try:
                w3 = connect()
            except Exception:
                pass
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
