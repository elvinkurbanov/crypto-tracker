#!/usr/bin/env python3
"""
Crypto Tracker: Bitcoin, Ethereum, Solana, Tether (USDT)
==========================================================

Pulls, from free public APIs (no keys required):
  * Market data      CoinGecko   price, market cap, rank, volume, 24h high/low,
                                 % change (1h/24h/7d/30d/1y), supply, ATH & drawdown
  * Technicals       CoinGecko   1-year daily history -> SMA50/SMA200, RSI(14),
                                 30d annualised volatility, 1y max drawdown
  * Global market    CoinGecko   total market cap, BTC/ETH dominance
  * Sentiment        alternative.me  Crypto Fear & Greed Index
  * Ecosystem        DefiLlama   TVL on Ethereum / Solana / Bitcoin,
                                 USDT supply, 1d/7d/30d supply change, top chains
  * Network health   mempool.space (BTC fees, hashrate), public Ethereum RPC
                                 (gas price), Solana RPC (TPS, epoch progress)
  * Money flows      Farside Investors (TFTC backup for BTC): US spot ETF net
                                 inflow/outflow for BTC, ETH, SOL, daily, last 7d,
                                 previous 7d, last 14d, streaks, biggest funds
                     DefiLlama   USDT net issuance (mint - burn) and stablecoin
                                 supply change on Ethereum / Solana / Tron
  * News             RSS feeds from CoinDesk, Cointelegraph, Decrypt, The Block,
                                 Bitcoin Magazine, CryptoSlate, tagged per coin

Outputs:
  * Coloured tables in the terminal
  * reports/latest.html + a timestamped HTML report (open in any browser)
  * data/snapshot_latest.json   full raw snapshot
  * data/price_history.csv      one row per coin per run (build your own time series)
  * data/etf_flows_daily.csv    full daily ETF flow history per coin and per fund

Usage:
  pip install requests rich cloudscraper
  python crypto_tracker.py                    # one run, prices in USD
  python crypto_tracker.py --currency krw     # prices in Korean won
  python crypto_tracker.py --watch 15         # refresh every 15 minutes
  python crypto_tracker.py --no-news          # skip news
  python crypto_tracker.py --no-history       # skip 1y history (faster, fewer API calls)
  python crypto_tracker.py --no-flows         # skip ETF / stablecoin flows
  python crypto_tracker.py --no-open          # do not open the report in the browser
  python crypto_tracker.py --serve            # run as a small web server (port 8000) that
                                              # refreshes every 15 min; open it from your
                                              # phone or another device on the same network
  python crypto_tracker.py --serve --watch 2  # live page that updates every 2 minutes
                                              # (history refreshes hourly, ETF flows every 30 min)

Optional: set COINGECKO_API_KEY (free "Demo" key) for higher rate limits.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import os
import queue
import re
import socket
import sys
import threading
import time
import webbrowser
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

import requests

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box

    console = Console()
    HAS_RICH = True
except ImportError:  # works without rich, just plainer output
    HAS_RICH = False
    console = None

# ---------------------------------------------------------------------------
# Configuration: edit freely
# ---------------------------------------------------------------------------

COINS = {
    # coingecko id: display info
    "bitcoin":  {"symbol": "BTC",  "name": "Bitcoin",  "llama_chain": "Bitcoin"},
    "ethereum": {"symbol": "ETH",  "name": "Ethereum", "llama_chain": "Ethereum"},
    "solana":   {"symbol": "SOL",  "name": "Solana",   "llama_chain": "Solana"},
    "tether":   {"symbol": "USDT", "name": "Tether",   "llama_chain": None},
}
STABLECOINS = {"tether"}

# Price alerts (in the --currency you run with). None = off.
PRICE_ALERTS = {
    "bitcoin":  {"above": None, "below": None},
    "ethereum": {"above": None, "below": None},
    "solana":   {"above": None, "below": None},
}
BIG_MOVE_PCT = 5.0        # alert when |24h change| exceeds this
DEPEG_PCT = 0.5           # alert when USDT is more than this % away from $1

NEWS_FEEDS = {
    "CoinDesk":         "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "Cointelegraph":    "https://cointelegraph.com/rss",
    "Decrypt":          "https://decrypt.co/feed",
    "The Block":        "https://www.theblock.co/rss.xml",
    "Bitcoin Magazine": "https://bitcoinmagazine.com/.rss/full/",
    "CryptoSlate":      "https://cryptoslate.com/feed/",
}
NEWS_PER_COIN = 8
NEWS_MAX_AGE_HOURS = 72

# Name keywords are case-insensitive; tickers must be upper-case to avoid false hits.
NEWS_KEYWORDS = {
    "bitcoin":  {"names": ["bitcoin", "satoshi", "microstrategy", "strategy inc", "spot btc"],
                 "tickers": ["BTC"]},
    "ethereum": {"names": ["ethereum", "ether", "vitalik", "layer-2", "layer 2"],
                 "tickers": ["ETH"]},
    "solana":   {"names": ["solana", "firedancer", "alpenglow", "jupiter", "phantom wallet", "pump.fun"],
                 "tickers": ["SOL"]},
    "tether":   {"names": ["tether", "stablecoin", "paolo ardoino"],
                 "tickers": ["USDT"]},
}

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
REPORT_DIR = BASE_DIR / "reports"

COINGECKO = "https://api.coingecko.com/api/v3"
HEADERS = {"User-Agent": "Mozilla/5.0 (crypto-tracker; +python-requests)",
           "Accept": "application/json, application/xml, text/xml, */*"}
TIMEOUT = (8, 15)   # (connect, read) seconds per request
SOURCE_TIMEOUT = 45  # hard limit per data source; slower sources are skipped

CURRENCY_SYMBOLS = {"usd": "$", "krw": "₩", "eur": "€", "jpy": "¥", "gbp": "£"}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

session = requests.Session()
session.headers.update(HEADERS)
if os.getenv("COINGECKO_API_KEY"):
    session.headers["x-cg-demo-api-key"] = os.environ["COINGECKO_API_KEY"]


def get_json(url: str, params: dict | None = None, retries: int = 3) -> Any:
    """GET with retry/backoff on rate limits (HTTP 429) and transient errors."""
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 0)) or 15 * (attempt + 1)
                log(f"Rate limited by {url.split('/')[2]}, waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout):
            if attempt == retries - 1:
                raise
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Gave up on {url} after {retries} attempts")


def rpc(url: str, method: str, params: list | None = None) -> Any:
    r = session.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method,
                                "params": params or []}, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"])
    return data["result"]


ERRORS: list[str] = []


def safe(label: str, fn: Callable, default=None):
    """Run one data source; record the failure and carry on if it breaks."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        ERRORS.append(f"{label}: {type(e).__name__}: {e}")
        return default


def log(msg: str):
    if HAS_RICH:
        from rich.markup import escape
        console.print(f"[dim]{escape(msg)}[/dim]")
    else:
        print(msg)


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def fetch_markets(currency: str) -> dict[str, dict]:
    rows = get_json(f"{COINGECKO}/coins/markets", {
        "vs_currency": currency,
        "ids": ",".join(COINS),
        "price_change_percentage": "1h,24h,7d,30d,1y",
        "sparkline": "false",
    })
    out = {}
    for c in rows:
        out[c["id"]] = {
            "price": c.get("current_price"),
            "market_cap": c.get("market_cap"),
            "rank": c.get("market_cap_rank"),
            "fdv": c.get("fully_diluted_valuation"),
            "volume_24h": c.get("total_volume"),
            "high_24h": c.get("high_24h"),
            "low_24h": c.get("low_24h"),
            "chg_1h": c.get("price_change_percentage_1h_in_currency"),
            "chg_24h": c.get("price_change_percentage_24h_in_currency",
                             c.get("price_change_percentage_24h")),
            "chg_7d": c.get("price_change_percentage_7d_in_currency"),
            "chg_30d": c.get("price_change_percentage_30d_in_currency"),
            "chg_1y": c.get("price_change_percentage_1y_in_currency"),
            "circulating_supply": c.get("circulating_supply"),
            "total_supply": c.get("total_supply"),
            "max_supply": c.get("max_supply"),
            "ath": c.get("ath"),
            "ath_change_pct": c.get("ath_change_percentage"),
            "ath_date": (c.get("ath_date") or "")[:10],
            "last_updated": c.get("last_updated"),
        }
        mc, vol = out[c["id"]]["market_cap"], out[c["id"]]["volume_24h"]
        out[c["id"]]["vol_to_mcap"] = (vol / mc * 100) if mc and vol else None
    return out


def fetch_history(coin_id: str, currency: str) -> list[float]:
    data = get_json(f"{COINGECKO}/coins/{coin_id}/market_chart",
                    {"vs_currency": currency, "days": 365, "interval": "daily"})
    return [p[1] for p in data.get("prices", []) if p and p[1] is not None]


def sma(xs: list[float], n: int) -> float | None:
    return sum(xs[-n:]) / n if len(xs) >= n else None


def rsi(xs: list[float], n: int = 14) -> float | None:
    """Wilder's RSI on daily closes."""
    if len(xs) < n + 1:
        return None
    deltas = [xs[i] - xs[i - 1] for i in range(1, len(xs))]
    gains = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]
    avg_g = sum(gains[:n]) / n
    avg_l = sum(losses[:n]) / n
    for g, l in zip(gains[n:], losses[n:]):
        avg_g = (avg_g * (n - 1) + g) / n
        avg_l = (avg_l * (n - 1) + l) / n
    if avg_l == 0:
        return 100.0
    return 100 - 100 / (1 + avg_g / avg_l)


def technicals(prices: list[float]) -> dict:
    if len(prices) < 31:
        return {}
    last = prices[-1]
    rets = [math.log(prices[i] / prices[i - 1]) for i in range(len(prices) - 30, len(prices))
            if prices[i - 1] > 0 and prices[i] > 0]
    mean = sum(rets) / len(rets)
    vol30 = math.sqrt(sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)) * math.sqrt(365) * 100
    peak, mdd = prices[0], 0.0
    for p in prices:
        peak = max(peak, p)
        mdd = min(mdd, (p - peak) / peak)
    s50, s200 = sma(prices, 50), sma(prices, 200)
    r = rsi(prices)
    if s50 and s200:
        if last > s50 > s200:
            trend = "Uptrend (price > SMA50 > SMA200)"
        elif last < s50 < s200:
            trend = "Downtrend (price < SMA50 < SMA200)"
        elif last < s50 and last < s200:
            trend = "Weakening (price below both averages)"
        elif last > s50 and last > s200:
            trend = "Recovering (price above both averages)"
        else:
            trend = "Mixed (price between averages)"
    else:
        trend = "n/a"
    return {
        "sma50": s50, "sma200": s200,
        "vs_sma50_pct": (last / s50 - 1) * 100 if s50 else None,
        "vs_sma200_pct": (last / s200 - 1) * 100 if s200 else None,
        "rsi14": r,
        "rsi_label": None if r is None else ("Overbought" if r >= 70 else "Oversold" if r <= 30 else "Neutral"),
        "volatility_30d_ann_pct": vol30,
        "max_drawdown_1y_pct": mdd * 100,
        "trend": trend,
        "high_1y": max(prices), "low_1y": min(prices),
    }


def fetch_global() -> dict:
    d = get_json(f"{COINGECKO}/global")["data"]
    return {
        "total_market_cap_usd": d["total_market_cap"].get("usd"),
        "total_volume_usd": d["total_volume"].get("usd"),
        "mcap_change_24h_pct": d.get("market_cap_change_percentage_24h_usd"),
        "btc_dominance": d["market_cap_percentage"].get("btc"),
        "eth_dominance": d["market_cap_percentage"].get("eth"),
        "usdt_dominance": d["market_cap_percentage"].get("usdt"),
        "active_cryptocurrencies": d.get("active_cryptocurrencies"),
    }


def fetch_fear_greed() -> dict:
    d = get_json("https://api.alternative.me/fng/", {"limit": 8})["data"]
    return {
        "value": int(d[0]["value"]),
        "label": d[0]["value_classification"],
        "yesterday": int(d[1]["value"]) if len(d) > 1 else None,
        "week_ago": int(d[7]["value"]) if len(d) > 7 else None,
    }


def fetch_chain_tvl() -> dict:
    chains = get_json("https://api.llama.fi/v2/chains")
    wanted = {v["llama_chain"] for v in COINS.values() if v["llama_chain"]}
    ranked = sorted(chains, key=lambda c: c.get("tvl") or 0, reverse=True)
    out = {}
    for i, c in enumerate(ranked, 1):
        if c.get("name") in wanted:
            out[c["name"]] = {"tvl_usd": c.get("tvl"), "tvl_rank": i}
    return out


def fetch_usdt() -> dict:
    data = get_json("https://stablecoins.llama.fi/stablecoins", {"includePrices": "true"})
    assets = data.get("peggedAssets", [])
    usdt = next(a for a in assets if a.get("symbol") == "USDT")
    total_stables = sum((a.get("circulating") or {}).get("peggedUSD", 0) or 0 for a in assets)

    def amt(key):
        return (usdt.get(key) or {}).get("peggedUSD")

    now, d1, d7, d30 = amt("circulating"), amt("circulatingPrevDay"), amt("circulatingPrevWeek"), amt("circulatingPrevMonth")
    pct = lambda prev: (now / prev - 1) * 100 if now and prev else None  # noqa: E731
    by_chain = []
    for chain, v in (usdt.get("chainCirculating") or {}).items():
        cur = ((v or {}).get("current") or {}).get("peggedUSD")
        if cur:
            by_chain.append((chain, cur))
    by_chain.sort(key=lambda x: x[1], reverse=True)
    return {
        "circulating_usd": now,
        "supply_chg_1d_pct": pct(d1), "supply_chg_7d_pct": pct(d7), "supply_chg_30d_pct": pct(d30),
        "share_of_all_stablecoins_pct": now / total_stables * 100 if now and total_stables else None,
        "total_stablecoin_supply_usd": total_stables,
        "top_chains": [{"chain": c, "usd": u, "share_pct": u / now * 100 if now else None}
                       for c, u in by_chain[:6]],
    }


def fetch_btc_network() -> dict:
    out = {}
    fees = safe("BTC fees", lambda: get_json("https://mempool.space/api/v1/fees/recommended"), {})
    if fees:
        out.update({"fee_fast_sat_vb": fees.get("fastestFee"),
                    "fee_30min_sat_vb": fees.get("halfHourFee"),
                    "fee_1h_sat_vb": fees.get("hourFee")})
    hr = safe("BTC hashrate", lambda: get_json("https://mempool.space/api/v1/mining/hashrate/3d"), {})
    if hr and hr.get("currentHashrate"):
        out["hashrate_ehs"] = hr["currentHashrate"] / 1e18
    adj = safe("BTC difficulty", lambda: get_json("https://mempool.space/api/v1/difficulty-adjustment"), {})
    if adj:
        out["next_difficulty_change_pct"] = adj.get("difficultyChange")
        out["blocks_to_retarget"] = adj.get("remainingBlocks")
    if not out:
        raise RuntimeError("mempool.space did not answer (fees, hashrate and difficulty all failed)")
    return out


def fetch_eth_network() -> dict:
    for url in ("https://ethereum-rpc.publicnode.com", "https://eth.llamarpc.com", "https://cloudflare-eth.com"):
        try:
            gas = int(rpc(url, "eth_gasPrice"), 16) / 1e9
            block = int(rpc(url, "eth_blockNumber"), 16)
            return {"gas_gwei": gas, "block": block}
        except Exception as e:  # noqa: BLE001
            last = e
    raise RuntimeError(f"all ETH RPCs failed ({last})")


def fetch_sol_network() -> dict:
    url = "https://api.mainnet-beta.solana.com"
    samples = rpc(url, "getRecentPerformanceSamples", [10])
    tps_total = [s["numTransactions"] / s["samplePeriodSecs"] for s in samples if s.get("samplePeriodSecs")]
    tps_user = [s["numNonVoteTransactions"] / s["samplePeriodSecs"] for s in samples
                if s.get("samplePeriodSecs") and s.get("numNonVoteTransactions") is not None]
    epoch = rpc(url, "getEpochInfo")
    return {
        "tps_total": sum(tps_total) / len(tps_total) if tps_total else None,
        "tps_non_vote": sum(tps_user) / len(tps_user) if tps_user else None,
        "epoch": epoch.get("epoch"),
        "epoch_progress_pct": epoch["slotIndex"] / epoch["slotsInEpoch"] * 100 if epoch.get("slotsInEpoch") else None,
    }


# ---------------------------------------------------------------------------
# Money flows: spot ETF flows + stablecoin issuance
# ---------------------------------------------------------------------------
# Positive number = money coming IN, negative = money going OUT.
#  * Spot ETF flows: net creations/redemptions of US spot ETF shares, i.e. new
#    investor money buying (or selling) the underlying coin. Source: Farside
#    Investors (daily, ~1 trading day lag), TFTC/SoSoValue as backup for BTC.
#  * Stablecoin supply change: new dollars minted onto a chain (in) or redeemed
#    (out). Source: DefiLlama.

FARSIDE_PAGES = {
    "bitcoin":  "https://farside.co.uk/btc/",
    "ethereum": "https://farside.co.uk/eth/",
    "solana":   "https://farside.co.uk/sol/",
}
TFTC_BTC_JSON = "https://www.tftc.io/bitcoin-etf-flows/data.json"
STABLE_CHAINS = ["Ethereum", "Solana", "Tron"]   # chains to track stablecoin in/outflows on
USDT_LLAMA_ID = 1
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0 Safari/537.36")

from html.parser import HTMLParser  # noqa: E402

DATE_RE = re.compile(r"^\d{1,2} [A-Z][a-z]{2} \d{4}$")
TICKER_RE = re.compile(r"^[A-Z]{2,6}$")


class _TableParser(HTMLParser):
    """Minimal HTML table -> list of rows of cell text (no lxml/bs4 needed)."""

    def __init__(self):
        super().__init__()
        self.tables, self._row, self._cell = [], None, None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.tables.append([])
        elif tag == "tr" and self.tables:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None and self._row is not None:
            self._row.append(re.sub(r"\s+", " ", "".join(self._cell)).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row and self.tables:
                self.tables[-1].append(self._row)
            self._row = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def _flow_num(s: str) -> float | None:
    """Farside style: '1,175.6' -> 1175.6, '(161.7)' -> -161.7, '-' -> None."""
    s = (s or "").replace(",", "").replace("$", "").strip()
    if s in ("", "-", "–", "—", "n/a", "N/A"):
        return None
    neg = s.startswith("(") and s.endswith(")")
    try:
        v = float(s.strip("()"))
    except ValueError:
        return None
    return -v if neg else v


def parse_farside(page: str) -> list[dict]:
    """Return [{'date': date, 'total_usd': float|None, 'funds': {ticker: usd}}] oldest first."""
    p = _TableParser()
    p.feed(page)
    best_rows, best_dated = None, []
    for rows in p.tables:
        dated = [r for r in rows if r and DATE_RE.match(r[0])]
        if len(dated) > len(best_dated):
            best_rows, best_dated = rows, dated
    if not best_dated:
        raise ValueError("no ETF flow table found on page (layout changed or blocked)")

    tickers: list[str] = []
    for r in best_rows:
        if r and DATE_RE.match(r[0]):
            break
        cands = [c for c in r if TICKER_RE.match(c) and c != "TOTAL"]
        if len(cands) >= 2:
            tickers = cands

    days = []
    for r in best_dated:
        vals = r[1:]
        if not vals:
            continue
        total = _flow_num(vals[-1])
        fund_vals = vals[:-1]
        funds = {}
        if tickers and len(fund_vals) == len(tickers):
            for t, v in zip(tickers, fund_vals):
                n = _flow_num(v)
                if n is not None:
                    funds[t] = n * 1e6
        days.append({"date": datetime.strptime(r[0], "%d %b %Y").date(),
                     "total_usd": total * 1e6 if total is not None else None,
                     "funds": funds})
    days.sort(key=lambda d: d["date"])
    return days


def parse_tftc(data: Any) -> list[dict]:
    """Parse TFTC's data.json defensively (only netFlowUsd is documented)."""
    def find_rows(o):
        if isinstance(o, list) and o and isinstance(o[0], dict) and "netFlowUsd" in o[0]:
            return o
        if isinstance(o, dict):
            for v in o.values():
                found = find_rows(v)
                if found:
                    return found
        return None

    days = []
    for r in find_rows(data) or []:
        ds = next((v for v in r.values() if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}", v)), None)
        flow = r.get("netFlowUsd")
        if not ds or flow is None:
            continue
        funds = next((v for v in r.values() if isinstance(v, dict) and v
                      and all(isinstance(x, (int, float)) for x in v.values())), {})
        if flow == 0 and not funds:   # market holiday
            continue
        days.append({"date": datetime.strptime(ds[:10], "%Y-%m-%d").date(),
                     "total_usd": float(flow), "funds": {k: float(v) for k, v in funds.items()}})
    days.sort(key=lambda d: d["date"])
    return days


def _fetch_html(url: str) -> str:
    r = session.get(url, headers={"User-Agent": BROWSER_UA,
                                  "Accept": "text/html,application/xhtml+xml"}, timeout=TIMEOUT)
    if r.status_code in (403, 503):
        try:
            import cloudscraper  # optional: gets past Cloudflare bot checks
        except ImportError:
            raise RuntimeError(f"HTTP {r.status_code} (bot protection). Try: pip install cloudscraper")
        r = cloudscraper.create_scraper().get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def summarize_flows(days: list[dict]) -> dict:
    """Net flows over the last trading week (5 sessions), the week before, and 2 weeks
    (10 sessions); current streak; biggest funds in/out over 2 weeks."""
    done = [d for d in days if d["total_usd"] is not None]
    if not done:
        return {}

    w7, prev7, w14 = done[-5:], done[-10:-5], done[-10:]
    streak, sign = 0, None
    for d in reversed(done):
        s = d["total_usd"] > 0
        if sign is None:
            sign = s
        if d["total_usd"] == 0 or s != sign:
            break
        streak += 1

    fund_14 = {}
    for d in w14:
        for t, v in d["funds"].items():
            fund_14[t] = fund_14.get(t, 0) + v
    ranked = sorted(fund_14.items(), key=lambda kv: kv[1], reverse=True)

    return {
        "latest_date": done[-1]["date"].isoformat(),
        "latest_usd": done[-1]["total_usd"],
        "net_7d_usd": sum(d["total_usd"] for d in w7),
        "net_prev_7d_usd": sum(d["total_usd"] for d in prev7),
        "net_14d_usd": sum(d["total_usd"] for d in w14),
        "trading_days_14d": len(w14),
        "inflow_days_14d": sum(1 for d in w14 if d["total_usd"] > 0),
        "outflow_days_14d": sum(1 for d in w14 if d["total_usd"] < 0),
        "streak_days": streak,
        "streak_dir": "inflow" if sign else "outflow",
        "cumulative_usd": sum(d["total_usd"] for d in done),
        "cumulative_since": done[0]["date"].isoformat(),
        "top_in_14d": [(t, v) for t, v in ranked if v > 0][:3],
        "top_out_14d": [(t, v) for t, v in reversed(ranked) if v < 0][:3],
        "daily": [{"date": d["date"].isoformat(), "usd": d["total_usd"]} for d in done[-10:]],
        "pending_dates": [d["date"].isoformat() for d in days if d["total_usd"] is None][-2:],
    }


ETF_RAW: dict[str, list[dict]] = {}   # full daily history, saved to CSV


def fetch_etf_flows() -> dict:
    out = {}
    for coin, url in FARSIDE_PAGES.items():
        days, source = None, None
        try:
            days, source = parse_farside(_fetch_html(url)), "Farside Investors"
        except Exception as e:  # noqa: BLE001
            ERRORS.append(f"ETF flows {COINS[coin]['symbol']} (Farside): {type(e).__name__}: {e}")
        if not days and coin == "bitcoin":
            days = safe("ETF flows BTC (TFTC backup)", lambda: parse_tftc(get_json(TFTC_BTC_JSON)), None)
            source = "TFTC / SoSoValue" if days else None
        if days:
            ETF_RAW[coin] = days
            summ = summarize_flows(days)
            if summ:
                summ["source"] = source
                out[coin] = summ
    return out


def _supply_series(chain: str, stablecoin_id: int | None = None) -> list[tuple[int, float]]:
    params = {"stablecoin": stablecoin_id} if stablecoin_id else None
    rows = get_json(f"https://stablecoins.llama.fi/stablecoincharts/{chain}", params)
    series = []
    for r in rows:
        v = ((r.get("totalCirculatingUSD") or {}).get("peggedUSD")
             or (r.get("totalCirculating") or {}).get("peggedUSD"))
        if v is not None:
            series.append((int(r["date"]), float(v)))
    series.sort()
    return series


def supply_changes(series: list[tuple[int, float]]) -> dict:
    if len(series) < 2:
        return {}
    last_t, last_v = series[-1]

    def value_days_ago(n):
        older = [v for t, v in series if t <= last_t - n * 86400]
        return older[-1] if older else None

    res = {"as_of": datetime.fromtimestamp(last_t, timezone.utc).date().isoformat(), "now_usd": last_v}
    for n in (1, 7, 14, 30):
        prev = value_days_ago(n)
        res[f"chg_{n}d_usd"] = last_v - prev if prev is not None else None
    p7, p14 = value_days_ago(7), value_days_ago(14)
    res["chg_prev_7d_usd"] = p7 - p14 if p7 is not None and p14 is not None else None
    tail = series[-15:]
    res["daily"] = [{"date": datetime.fromtimestamp(t, timezone.utc).date().isoformat(), "usd": v - pv}
                    for (pt, pv), (t, v) in zip(tail, tail[1:])]
    return res


def fetch_stablecoin_flows() -> dict:
    out = {
        "USDT (all chains)": safe("Stablecoin flows USDT", lambda: supply_changes(_supply_series("all", USDT_LLAMA_ID)), None),
        "All stablecoins": safe("Stablecoin flows (all)", lambda: supply_changes(_supply_series("all")), None),
    }
    for ch in STABLE_CHAINS:
        res = safe(f"Stablecoin flows {ch}", lambda ch=ch: supply_changes(_supply_series(ch)), None)
        if res:
            out[f"Stablecoins on {ch}"] = res
    return {k: v for k, v in out.items() if v}


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------

def _text(el, tag):
    found = el.find(tag)
    return (found.text or "").strip() if found is not None and found.text else ""


def _clean(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", html.unescape(s or ""))
    return re.sub(r"\s+", " ", s).strip()


def parse_feed(source: str, xml_bytes: bytes) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    items = []
    atom = "{http://www.w3.org/2005/Atom}"
    entries = root.findall(".//item") or root.findall(f".//{atom}entry")
    for it in entries:
        title = _text(it, "title") or _text(it, f"{atom}title")
        link = _text(it, "link")
        if not link:
            le = it.find(f"{atom}link")
            link = le.get("href", "") if le is not None else ""
        date_raw = _text(it, "pubDate") or _text(it, f"{atom}updated") or _text(it, f"{atom}published")
        summary = _text(it, "description") or _text(it, f"{atom}summary")
        try:
            dt = parsedate_to_datetime(date_raw) if "," in date_raw else datetime.fromisoformat(date_raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:  # noqa: BLE001
            dt = None
        if title and link:
            items.append({"source": source, "title": _clean(title), "link": link.strip(),
                          "published": dt, "summary": _clean(summary)[:280]})
    return items


def tag_coins(article: dict) -> list[str]:
    text = f"{article['title']} {article['summary']}"
    tags = []
    for coin, kw in NEWS_KEYWORDS.items():
        if any(re.search(rf"\b{re.escape(w)}\b", text, re.I) for w in kw["names"]) or \
           any(re.search(rf"\b{re.escape(t)}\b", text) for t in kw["tickers"]):
            tags.append(coin)
    return tags


def fetch_news() -> dict[str, list[dict]]:
    articles = []
    for source, url in NEWS_FEEDS.items():
        def _get(url=url, source=source):
            r = session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return parse_feed(source, r.content)
        articles += safe(f"News feed {source}", _get, []) or []

    cutoff = time.time() - NEWS_MAX_AGE_HOURS * 3600
    seen, by_coin = set(), {c: [] for c in COINS}
    by_coin["general"] = []
    articles.sort(key=lambda a: a["published"].timestamp() if a["published"] else 0, reverse=True)
    for a in articles:
        key = re.sub(r"\W+", "", a["title"].lower())[:80]
        if key in seen or (a["published"] and a["published"].timestamp() < cutoff):
            continue
        seen.add(key)
        a["coins"] = tag_coins(a)
        for c in a["coins"]:
            if len(by_coin[c]) < NEWS_PER_COIN:
                by_coin[c].append(a)
        if not a["coins"] and len(by_coin["general"]) < NEWS_PER_COIN:
            by_coin["general"].append(a)
    return by_coin


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

def build_alerts(markets: dict, cur: str) -> list[str]:
    alerts = []
    sym = CURRENCY_SYMBOLS.get(cur, "")
    for cid, m in markets.items():
        name = COINS[cid]["symbol"]
        p = m.get("price")
        if p is None:
            continue
        cfg = PRICE_ALERTS.get(cid, {})
        if cfg.get("above") is not None and p >= cfg["above"]:
            alerts.append(f"{name} is above {sym}{cfg['above']:,} (now {sym}{p:,.2f})")
        if cfg.get("below") is not None and p <= cfg["below"]:
            alerts.append(f"{name} is below {sym}{cfg['below']:,} (now {sym}{p:,.2f})")
        if cid in STABLECOINS:
            if cur == "usd" and abs(p - 1) * 100 > DEPEG_PCT:
                alerts.append(f"{name} off its peg: ${p:.4f} ({(p - 1) * 100:+.2f}%)")
        elif m.get("chg_24h") is not None and abs(m["chg_24h"]) >= BIG_MOVE_PCT:
            alerts.append(f"{name} moved {m['chg_24h']:+.1f}% in 24h")
    return alerts


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def money(x, cur="usd", decimals=None) -> str:
    if x is None:
        return "–"
    sym = CURRENCY_SYMBOLS.get(cur, cur.upper() + " ")
    ax = abs(x)
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M")):
        if ax >= div:
            return f"{sym}{x / div:,.2f}{suf}"
    if decimals is None:
        decimals = 4 if ax < 2 else 2
        if cur in ("krw", "jpy") and ax >= 100:
            decimals = 0
    return f"{sym}{x:,.{decimals}f}"


def pct(x, plus=True) -> str:
    return "–" if x is None else (f"{x:+.2f}%" if plus else f"{x:.2f}%")


def num(x, d=0) -> str:
    return "–" if x is None else f"{x:,.{d}f}"


def rich_pct(x) -> str:
    if x is None:
        return "[dim]–[/dim]"
    color = "green" if x > 0 else "red" if x < 0 else "white"
    return f"[{color}]{x:+.2f}%[/{color}]"


def flow(x) -> str:
    """Signed money for flows: +$1.23B / -$450.4M."""
    if x is None:
        return "–"
    sign = "+" if x > 0 else "-" if x < 0 else ""
    ax = abs(x)
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if ax >= div:
            return f"{sign}${ax / div:,.{2 if suf == 'B' else 1}f}{suf}"
    return f"{sign}${ax:,.0f}"


def rich_flow(x) -> str:
    if x is None:
        return "[dim]–[/dim]"
    color = "green" if x > 0 else "red" if x < 0 else "white"
    return f"[{color}]{flow(x)}[/{color}]"


def sol_tps_text(so: dict) -> str:
    t, u = so.get("tps_total"), so.get("tps_non_vote")
    if t and u and abs(t - u) > 1:
        return f"{num(t)} TPS ({num(u)} user transactions, rest are validator votes)"
    return f"{num(u or t)} transactions/sec"


def ago(dt) -> str:
    if not dt:
        return ""
    mins = (datetime.now(timezone.utc) - dt).total_seconds() / 60
    return f"{mins:.0f}m ago" if mins < 60 else f"{mins / 60:.0f}h ago" if mins < 1440 else f"{mins / 1440:.0f}d ago"


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

def print_terminal(s: dict):
    cur = s["currency"]
    m, t = s["markets"], s["technicals"]
    if not HAS_RICH:
        print(json.dumps(s, indent=2, default=str))
        return

    console.rule(f"[bold]Crypto Tracker  ·  {s['generated_at_local']}[/bold]")

    tb = Table(box=box.SIMPLE_HEAVY, title="Market", title_justify="left")
    for col in ("Coin", "Price", "1h", "24h", "7d", "30d", "1y", "Mkt cap", "Rank", "24h vol", "Vol/MC", "From ATH"):
        tb.add_column(col, justify="right" if col != "Coin" else "left")
    for cid, info in COINS.items():
        x = m.get(cid, {})
        tb.add_row(f"[bold]{info['symbol']}[/bold] {info['name']}", money(x.get("price"), cur),
                   rich_pct(x.get("chg_1h")), rich_pct(x.get("chg_24h")), rich_pct(x.get("chg_7d")),
                   rich_pct(x.get("chg_30d")), rich_pct(x.get("chg_1y")), money(x.get("market_cap"), cur),
                   num(x.get("rank")), money(x.get("volume_24h"), cur), pct(x.get("vol_to_mcap"), False),
                   rich_pct(None if cid in STABLECOINS else x.get("ath_change_pct")))
    console.print(tb)

    if t:
        tt = Table(box=box.SIMPLE_HEAVY, title="Technicals (daily, 1y history)", title_justify="left")
        for col in ("Coin", "vs SMA50", "vs SMA200", "Trend", "RSI14", "30d vol (ann.)", "1y max DD", "1y range"):
            tt.add_column(col, justify="left" if col in ("Coin", "Trend") else "right")
        for cid, x in t.items():
            if not x:
                continue
            tt.add_row(COINS[cid]["symbol"], rich_pct(x.get("vs_sma50_pct")), rich_pct(x.get("vs_sma200_pct")),
                       x.get("trend", "–"), f"{num(x.get('rsi14'), 1)} {x.get('rsi_label') or ''}",
                       pct(x.get("volatility_30d_ann_pct"), False), pct(x.get("max_drawdown_1y_pct")),
                       f"{money(x.get('low_1y'), cur)} – {money(x.get('high_1y'), cur)}")
        console.print(tt)

    g, fg = s.get("global") or {}, s.get("fear_greed") or {}
    lines = []
    if g:
        lines.append(f"Total crypto market cap: {money(g.get('total_market_cap_usd'))} ({pct(g.get('mcap_change_24h_pct'))} 24h)")
        lines.append(f"Dominance: BTC {pct(g.get('btc_dominance'), False)} · ETH {pct(g.get('eth_dominance'), False)} · USDT {pct(g.get('usdt_dominance'), False)}")
    if fg:
        lines.append(f"Fear & Greed: [bold]{fg['value']} ({fg['label']})[/bold]  ·  yesterday {fg.get('yesterday')} · week ago {fg.get('week_ago')}")
    if lines:
        console.print(Panel("\n".join(lines), title="Global market & sentiment", expand=False))

    eco = []
    for name, v in (s.get("chain_tvl") or {}).items():
        eco.append(f"{name} DeFi TVL: {money(v.get('tvl_usd'))} (#{v.get('tvl_rank')} chain)")
    u = s.get("usdt") or {}
    if u:
        eco.append(f"USDT supply: {money(u.get('circulating_usd'))}  ·  1d {pct(u.get('supply_chg_1d_pct'))} · 7d {pct(u.get('supply_chg_7d_pct'))} · 30d {pct(u.get('supply_chg_30d_pct'))}")
        eco.append(f"USDT share of all stablecoins: {pct(u.get('share_of_all_stablecoins_pct'), False)}  ·  top chains: "
                   + ", ".join(f"{c['chain']} {c['share_pct']:.0f}%" for c in u.get("top_chains", [])[:4]))
    if eco:
        console.print(Panel("\n".join(eco), title="Ecosystem (DefiLlama)", expand=False))

    net = []
    b, e, so = s.get("btc_network") or {}, s.get("eth_network") or {}, s.get("sol_network") or {}
    if b:
        net.append((f"BTC fees (sat/vB): fast {b.get('fee_fast_sat_vb')} · 30m {b.get('fee_30min_sat_vb')} · 1h {b.get('fee_1h_sat_vb')}" if b.get("fee_fast_sat_vb") is not None else "BTC")
                   + (f"  ·  hashrate {b['hashrate_ehs']:,.0f} EH/s" if b.get("hashrate_ehs") else "")
                   + (f"  ·  next difficulty {pct(b.get('next_difficulty_change_pct'))}" if b.get("next_difficulty_change_pct") is not None else ""))
    if e:
        net.append(f"ETH gas: {e['gas_gwei']:.2f} gwei  ·  block {e['block']:,}")
    if so:
        net.append(f"SOL: {sol_tps_text(so)}  ·  epoch {so.get('epoch')} ({pct(so.get('epoch_progress_pct'), False)} done)")
    if net:
        console.print(Panel("\n".join(net), title="Network health", expand=False))

    print_flows_terminal(s)

    for a in s.get("alerts", []):
        console.print(f"[bold yellow]⚠ ALERT[/bold yellow] {a}")

    news = s.get("news") or {}
    for key in list(COINS) + ["general"]:
        items = news.get(key) or []
        if not items:
            continue
        label = COINS[key]["name"] if key in COINS else "General crypto"
        console.print(f"\n[bold underline]{label} news[/bold underline]")
        for a in items[:5]:
            console.print(f"  • {a['title']}  [dim]{a['source']} · {ago(a['published'])}[/dim]")
            console.print(f"    [link={a['link']}][blue]{a['link']}[/blue][/link]")

    if ERRORS:
        console.print(f"\n[dim]{len(ERRORS)} source(s) failed (other data still shown):[/dim]")
        for err in ERRORS:
            console.print(f"[dim]  - {err[:160]}[/dim]")


def print_flows_terminal(s: dict):
    etf, stab = s.get("etf_flows") or {}, s.get("stablecoin_flows") or {}
    if not etf and not stab:
        return
    ft = Table(box=box.SIMPLE_HEAVY, title="Money flows  (+ = money in, − = money out)", title_justify="left",
               caption="ETF rows: trading weeks (5 sessions). Stablecoin rows: 7 / 14 calendar days.", caption_justify="left")
    for col in ("Flow", "Latest day", "1 week", "Previous week", "2 weeks", "Notes"):
        ft.add_column(col, justify="left" if col in ("Flow", "Notes") else "right")
    for coin, f in etf.items():
        note = (f"{f['streak_days']}-day {f['streak_dir']} streak · "
                f"{f['inflow_days_14d']} in / {f['outflow_days_14d']} out of last 10 days")
        if f.get("top_in_14d"):
            note += " · top in: " + ", ".join(f"{t} {flow(v)}" for t, v in f["top_in_14d"][:2])
        if f.get("top_out_14d"):
            note += " · top out: " + ", ".join(f"{t} {flow(v)}" for t, v in f["top_out_14d"][:2])
        ft.add_row(f"[bold]{COINS[coin]['symbol']}[/bold] spot ETFs [dim]({f['latest_date']})[/dim]",
                   rich_flow(f["latest_usd"]), rich_flow(f["net_7d_usd"]), rich_flow(f["net_prev_7d_usd"]),
                   rich_flow(f["net_14d_usd"]), f"[dim]{note}[/dim]")
    for name, c in stab.items():
        label = "net issuance (mint − burn)" if name.startswith("USDT") else "supply change"
        ft.add_row(f"{name} [dim]{label}[/dim]", rich_flow(c.get("chg_1d_usd")), rich_flow(c.get("chg_7d_usd")),
                   rich_flow(c.get("chg_prev_7d_usd")), rich_flow(c.get("chg_14d_usd")),
                   f"[dim]now {money(c.get('now_usd'))} · 30d {flow(c.get('chg_30d_usd'))}[/dim]")
    console.print(ft)

    if etf:
        dates = sorted({d["date"] for f in etf.values() for d in f["daily"]})[-10:]
        dt = Table(box=box.SIMPLE, title="Daily spot ETF net flows (last 10 trading days)", title_justify="left")
        dt.add_column("Date")
        for coin in etf:
            dt.add_column(COINS[coin]["symbol"], justify="right")
        lookup = {coin: {d["date"]: d["usd"] for d in f["daily"]} for coin, f in etf.items()}
        for d in dates:
            dt.add_row(datetime.fromisoformat(d).strftime("%a %d %b"),
                       *[rich_flow(lookup[c].get(d)) for c in etf])
        console.print(dt)


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def _bars_svg(daily: list[dict], width=360, height=120) -> str:
    """Tiny inline SVG bar chart of daily flows (green in / red out)."""
    vals = [d["usd"] for d in daily if d.get("usd") is not None]
    if not vals:
        return ""
    peak = max(abs(v) for v in vals) or 1
    n = len(daily)
    bw = width / n
    mid = height / 2
    bars = []
    for i, d in enumerate(daily):
        v = d.get("usd")
        if v is None:
            continue
        h = abs(v) / peak * (mid - 6)
        y = mid - h if v >= 0 else mid
        color = "var(--up)" if v >= 0 else "var(--down)"
        tip = html.escape(f"{d['date']}: {flow(v)}")
        bars.append(f'<rect x="{i * bw + bw * 0.15:.1f}" y="{y:.1f}" width="{bw * 0.7:.1f}" '
                    f'height="{max(h, 1):.1f}" fill="{color}" rx="1.5"><title>{tip}</title></rect>')
    first, last = daily[0]["date"][5:], daily[-1]["date"][5:]
    return (f'<svg viewBox="0 0 {width} {height + 16}" width="100%" role="img" aria-label="daily flows">'
            f'<line x1="0" x2="{width}" y1="{mid}" y2="{mid}" stroke="var(--line)"/>{"".join(bars)}'
            f'<text x="0" y="{height + 13}" font-size="10" fill="var(--muted)">{first}</text>'
            f'<text x="{width}" y="{height + 13}" font-size="10" fill="var(--muted)" text-anchor="end">{last}</text></svg>')


def render_flows_html(s: dict) -> str:
    etf, stab = s.get("etf_flows") or {}, s.get("stablecoin_flows") or {}
    if not etf and not stab:
        return ""
    E = html.escape

    def td(x):
        return f'<td class="num {_cls(x)}">{flow(x)}</td>'

    rows = ""
    for coin, f in etf.items():
        rows += (f"<tr><td><b>{COINS[coin]['symbol']}</b> spot ETFs <span class=muted>to {f['latest_date']}</span></td>"
                 f"{td(f['latest_usd'])}{td(f['net_7d_usd'])}{td(f['net_prev_7d_usd'])}{td(f['net_14d_usd'])}"
                 f"<td class=muted>{f['streak_days']}-day {f['streak_dir']} streak · "
                 f"{f['inflow_days_14d']} in / {f['outflow_days_14d']} out of last 10 days · {flow(f['cumulative_usd'])} since {f['cumulative_since']}</td></tr>")
    for name, c in stab.items():
        label = "net issuance" if name.startswith("USDT") else "supply change"
        rows += (f"<tr><td><b>{E(name)}</b> <span class=muted>{label}</span></td>"
                 f"{td(c.get('chg_1d_usd'))}{td(c.get('chg_7d_usd'))}{td(c.get('chg_prev_7d_usd'))}{td(c.get('chg_14d_usd'))}"
                 f"<td class=muted>now {money(c.get('now_usd'))} · 30d {flow(c.get('chg_30d_usd'))}</td></tr>")

    charts = ""
    for coin, f in etf.items():
        tops = ""
        if f.get("top_in_14d") or f.get("top_out_14d"):
            tops = ('<div class=muted style="margin-top:6px">2 weeks · in: '
                    + (", ".join(f"<b>{E(t)}</b> {flow(v)}" for t, v in f["top_in_14d"]) or "–")
                    + " · out: " + (", ".join(f"<b>{E(t)}</b> {flow(v)}" for t, v in f["top_out_14d"]) or "–")
                    + "</div>")
        charts += (f'<div class=card><h3>{COINS[coin]["name"]} spot ETFs · daily net flow '
                   f'<span class=muted>({E(f["source"])})</span></h3>'
                   f'<div class="big {_cls(f["net_14d_usd"])}" style="font-size:22px">{flow(f["net_14d_usd"])} '
                   f'<span class=muted style="font-size:12px">last 10 trading days</span></div>'
                   f'{_bars_svg(f["daily"])}{tops}</div>')
    for name in ("USDT (all chains)",):
        c = stab.get(name)
        if c and c.get("daily"):
            charts += (f'<div class=card><h3>Tether · daily net issuance</h3>'
                       f'<div class="big {_cls(c.get("chg_14d_usd"))}" style="font-size:22px">{flow(c.get("chg_14d_usd"))} '
                       f'<span class=muted style="font-size:12px">last 14 days</span></div>{_bars_svg(c["daily"])}</div>')

    return (f'<h2>Money flows <span class=muted>(+ money in · − money out)</span></h2>'
            f'<div class=wrap><table><tr><th>Flow</th><th>Latest day</th><th>1 week</th><th>Previous week</th>'
            f'<th>2 weeks</th><th style="text-align:left">Notes</th></tr>{rows}</table></div>'
            f'<div class=cards style="margin-top:12px">{charts}</div>'
            f'<p class=muted>ETF flows = net new investor money into US spot ETFs (share creations minus redemptions), '
            f'reported after US market close, so the latest day is usually yesterday; a week = 5 trading sessions. Stablecoin supply change = new '
            f'dollars minted onto a chain minus dollars redeemed, over 7 / 14 calendar days. Sources: Farside Investors, DefiLlama.</p>')

def _cls(x):
    return "" if x is None else ("up" if x > 0 else "down" if x < 0 else "")


def _td_pct(x, plus=True):
    return f'<td class="num {_cls(x) if plus else ""}">{pct(x, plus)}</td>'


def render_html(s: dict) -> str:
    cur, m, t = s["currency"], s["markets"], s["technicals"]
    E = html.escape
    rows = ""
    for cid, info in COINS.items():
        x = m.get(cid, {})
        rows += (f"<tr><td><b>{info['symbol']}</b> <span class=muted>{info['name']}</span></td>"
                 f"<td class=num><b>{money(x.get('price'), cur)}</b></td>"
                 + "".join(_td_pct(x.get(k)) for k in ("chg_1h", "chg_24h", "chg_7d", "chg_30d", "chg_1y"))
                 + f"<td class=num>{money(x.get('market_cap'), cur)}</td><td class=num>{num(x.get('rank'))}</td>"
                 f"<td class=num>{money(x.get('volume_24h'), cur)}</td>{_td_pct(x.get('vol_to_mcap'), False)}"
                 f"<td class=num>{money(x.get('low_24h'), cur)} – {money(x.get('high_24h'), cur)}</td>"
                 f"{_td_pct(None if cid in STABLECOINS else x.get('ath_change_pct'))}</tr>")

    tech_rows = ""
    for cid, x in (t or {}).items():
        if not x:
            continue
        tech_rows += (f"<tr><td><b>{COINS[cid]['symbol']}</b></td>{_td_pct(x.get('vs_sma50_pct'))}{_td_pct(x.get('vs_sma200_pct'))}"
                      f"<td>{E(x.get('trend', '–'))}</td><td class=num>{num(x.get('rsi14'), 1)} <span class=muted>{E(x.get('rsi_label') or '')}</span></td>"
                      f"{_td_pct(x.get('volatility_30d_ann_pct'), False)}{_td_pct(x.get('max_drawdown_1y_pct'))}"
                      f"<td class=num>{money(x.get('low_1y'), cur)} – {money(x.get('high_1y'), cur)}</td></tr>")

    supply_rows = ""
    for cid, info in COINS.items():
        x = m.get(cid, {})
        supply_rows += (f"<tr><td><b>{info['symbol']}</b></td><td class=num>{num(x.get('circulating_supply'))}</td>"
                        f"<td class=num>{num(x.get('total_supply'))}</td><td class=num>{num(x.get('max_supply')) if x.get('max_supply') else '∞'}</td>"
                        f"<td class=num>{money(x.get('fdv'), cur)}</td><td class=num>{money(x.get('ath'), cur)} <span class=muted>{E(x.get('ath_date') or '')}</span></td></tr>")

    def card(title, items):
        items = [i for i in items if i]
        if not items:
            return ""
        lis = "".join(f"<li>{i}</li>" for i in items)
        return f'<div class=card><h3>{title}</h3><ul>{lis}</ul></div>'

    g, fg, u = s.get("global") or {}, s.get("fear_greed") or {}, s.get("usdt") or {}
    b, e, so = s.get("btc_network") or {}, s.get("eth_network") or {}, s.get("sol_network") or {}
    fg_html = ""
    if fg:
        fg_html = (f'<div class="card fg"><h3>Fear &amp; Greed</h3><div class=big>{fg["value"]}</div>'
                   f'<div>{E(fg["label"])}</div><div class=muted>yesterday {fg.get("yesterday")} · week ago {fg.get("week_ago")}</div>'
                   f'<div class=bar><span style="left:{fg["value"]}%"></span></div></div>')
    cards = fg_html + card("Global market", [
        g and f"Total market cap <b>{money(g.get('total_market_cap_usd'))}</b> <span class={_cls(g.get('mcap_change_24h_pct'))}>{pct(g.get('mcap_change_24h_pct'))}</span>",
        g and f"24h volume {money(g.get('total_volume_usd'))}",
        g and f"BTC dominance <b>{pct(g.get('btc_dominance'), False)}</b>",
        g and f"ETH dominance {pct(g.get('eth_dominance'), False)} · USDT {pct(g.get('usdt_dominance'), False)}",
    ]) + card("DeFi TVL (DefiLlama)", [
        f"{E(n)} <b>{money(v.get('tvl_usd'))}</b> <span class=muted>#{v.get('tvl_rank')}</span>"
        for n, v in (s.get("chain_tvl") or {}).items()
    ]) + card("Tether (USDT) supply", [
        u and f"Circulating <b>{money(u.get('circulating_usd'))}</b>",
        u and f"1d <span class={_cls(u.get('supply_chg_1d_pct'))}>{pct(u.get('supply_chg_1d_pct'))}</span> · 7d <span class={_cls(u.get('supply_chg_7d_pct'))}>{pct(u.get('supply_chg_7d_pct'))}</span> · 30d <span class={_cls(u.get('supply_chg_30d_pct'))}>{pct(u.get('supply_chg_30d_pct'))}</span>",
        u and f"{pct(u.get('share_of_all_stablecoins_pct'), False)} of all stablecoins ({money(u.get('total_stablecoin_supply_usd'))})",
        u and "Top chains: " + ", ".join(f"{E(c['chain'])} {c['share_pct']:.0f}%" for c in u.get("top_chains", [])[:4]),
    ]) + card("Network health", [
        b and ((f"<b>BTC</b> fees fast {b.get('fee_fast_sat_vb')} / 1h {b.get('fee_1h_sat_vb')} sat/vB" if b.get("fee_fast_sat_vb") is not None else "<b>BTC</b>")
               + (f" · {b['hashrate_ehs']:,.0f} EH/s" if b.get("hashrate_ehs") else "")),
        b.get("next_difficulty_change_pct") is not None and f"<b>BTC</b> next difficulty {pct(b.get('next_difficulty_change_pct'))} in {num(b.get('blocks_to_retarget'))} blocks",
        e and f"<b>ETH</b> gas {e['gas_gwei']:.2f} gwei · block {e['block']:,}",
        so and f"<b>SOL</b> {sol_tps_text(so)} · epoch {so.get('epoch')} {pct(so.get('epoch_progress_pct'), False)} done",
    ])

    alerts = "".join(f'<div class=alert>⚠ {E(a)}</div>' for a in s.get("alerts", []))

    news_html = ""
    for key in list(COINS) + ["general"]:
        items = (s.get("news") or {}).get(key) or []
        if not items:
            continue
        label = COINS[key]["name"] if key in COINS else "General crypto"
        lis = "".join(
            f'<li><a href="{E(a["link"])}" target=_blank rel=noopener>{E(a["title"])}</a>'
            f'<div class=muted>{E(a["source"])} · {ago(a["published"])}</div>'
            f'<div class=sum>{E(a["summary"][:200])}</div></li>' for a in items)
        news_html += f'<div class=news><h3>{label}</h3><ul>{lis}</ul></div>'

    errs = ""
    if s.get("errors"):
        errs = "<details class=muted><summary>{} source(s) failed</summary><ul>{}</ul></details>".format(
            len(s["errors"]), "".join(f"<li>{E(x[:200])}</li>" for x in s["errors"]))

    return f"""<!DOCTYPE html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Crypto Tracker · {E(s['generated_at_local'])}</title>
{_live_reload_js(s['generated_at_utc']) if s.get('live') else ''}
<style>
:root{{--bg:#fff;--fg:#1a1a1a;--muted:#6b7280;--line:#e5e7eb;--card:#f9fafb;--up:#15803d;--down:#b91c1c;--accent:#2563eb}}
@media (prefers-color-scheme:dark){{:root{{--bg:#0f1115;--fg:#e5e7eb;--muted:#9ca3af;--line:#2a2f3a;--card:#171a21;--up:#4ade80;--down:#f87171;--accent:#60a5fa}}}}
body{{font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--fg);margin:0;padding:24px;max-width:1280px;margin:auto}}
h1{{font-size:22px;margin:0}} h2{{font-size:16px;margin:28px 0 8px}} h3{{font-size:14px;margin:0 0 8px}}
.muted{{color:var(--muted);font-size:12px}} .up{{color:var(--up)}} .down{{color:var(--down)}}
table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}} .wrap{{overflow-x:auto}}
th,td{{padding:7px 10px;border-bottom:1px solid var(--line);white-space:nowrap}} th{{text-align:right;color:var(--muted);font-weight:500;font-size:12px}}
th:first-child,td:first-child{{text-align:left}} td.num{{text-align:right}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px}} .card ul{{margin:0;padding-left:16px}} .card li{{margin:3px 0}}
.fg .big{{font-size:34px;font-weight:700}} .bar{{position:relative;height:8px;border-radius:4px;margin-top:10px;background:linear-gradient(90deg,#b91c1c,#f59e0b,#15803d)}}
.bar span{{position:absolute;top:-4px;width:4px;height:16px;background:var(--fg);border-radius:2px;transform:translateX(-2px)}}
.alert{{background:#fef3c7;color:#78350f;border:1px solid #f59e0b;border-radius:8px;padding:8px 12px;margin:6px 0}}
.newsgrid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:16px}}
.news ul{{list-style:none;padding:0;margin:0}} .news li{{padding:8px 0;border-bottom:1px solid var(--line)}}
.news a{{color:var(--accent);text-decoration:none;font-weight:500}} .news a:hover{{text-decoration:underline}} .sum{{font-size:12.5px;margin-top:2px}}
</style></head><body>
<h1>Crypto Tracker</h1>
<div class=muted>{E(s['generated_at_local'])} · prices in {cur.upper()} · data: CoinGecko, DefiLlama, alternative.me, mempool.space, public RPCs, RSS{f" · live: updates every {s['live']:g} min, this page reloads itself" if s.get('live') else ''}</div>
{alerts}
<h2>Market</h2><div class=wrap><table>
<tr><th>Coin</th><th>Price</th><th>1h</th><th>24h</th><th>7d</th><th>30d</th><th>1y</th><th>Market cap</th><th>Rank</th><th>24h volume</th><th>Vol/MC</th><th>24h range</th><th>From ATH</th></tr>
{rows}</table></div>
{'<h2>Technicals <span class=muted>(daily closes, 1-year history)</span></h2><div class=wrap><table><tr><th>Coin</th><th>vs SMA50</th><th>vs SMA200</th><th style="text-align:left">Trend</th><th>RSI 14</th><th>30d vol (ann.)</th><th>1y max drawdown</th><th>1y range</th></tr>' + tech_rows + '</table></div>' if tech_rows else ''}
<h2>Supply</h2><div class=wrap><table><tr><th>Coin</th><th>Circulating</th><th>Total</th><th>Max</th><th>Fully diluted value</th><th>All-time high</th></tr>{supply_rows}</table></div>
{render_flows_html(s)}
<h2>Market, ecosystem &amp; network</h2><div class=cards>{cards}</div>
<h2>News <span class=muted>(last {NEWS_MAX_AGE_HOURS}h)</span></h2><div class=newsgrid>{news_html or '<div class=muted>No news loaded.</div>'}</div>
<p>{errs}</p>
</body></html>"""


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_outputs(s: dict) -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    REPORT_DIR.mkdir(exist_ok=True)

    hist = DATA_DIR / "price_history.csv"
    new = not hist.exists()
    with hist.open("a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["timestamp_utc", "currency", "coin", "symbol", "price", "market_cap",
                        "volume_24h", "chg_24h_pct", "chg_7d_pct", "fear_greed", "btc_dominance"])
        for cid, x in s["markets"].items():
            w.writerow([s["generated_at_utc"], s["currency"], cid, COINS[cid]["symbol"], x.get("price"),
                        x.get("market_cap"), x.get("volume_24h"), x.get("chg_24h"), x.get("chg_7d"),
                        (s.get("fear_greed") or {}).get("value"), (s.get("global") or {}).get("btc_dominance")])

    (DATA_DIR / "snapshot_latest.json").write_text(json.dumps(s, indent=2, default=str))

    if ETF_RAW:  # full daily ETF flow history, long format (one row per date/coin/fund)
        with (DATA_DIR / "etf_flows_daily.csv").open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["date", "coin", "ticker", "flow_usd"])
            for coin, days in ETF_RAW.items():
                for d in days:
                    if d["total_usd"] is not None:
                        w.writerow([d["date"].isoformat(), COINS[coin]["symbol"], "TOTAL", d["total_usd"]])
                    for t, v in d["funds"].items():
                        w.writerow([d["date"].isoformat(), COINS[coin]["symbol"], t, v])

    page = render_html(s)
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    (REPORT_DIR / f"crypto_report_{stamp}.html").write_text(page, encoding="utf-8")
    latest = REPORT_DIR / "latest.html"
    latest.write_text(page, encoding="utf-8")
    return latest


# ---------------------------------------------------------------------------
# Web server mode (--serve): view the report from any device on your network
# ---------------------------------------------------------------------------

KEEP_REPORTS = 672          # in --serve mode keep only the newest 672 timestamped reports

_LIVE_LOCK = threading.Lock()
_LIVE = {"page": None, "stamp": "", "snapshot": None}


def _live_reload_js(stamp: str) -> str:
    """Poll /stamp every 30s and reload only when a new report exists, so the
    page does not jump while you are reading it."""
    return ("<script>(function(){var v=" + json.dumps(stamp) + ";"
            "setInterval(function(){fetch('/stamp',{cache:'no-store'}).then(function(r){return r.text()})"
            ".then(function(t){if(t&&t!==v)location.reload()}).catch(function(){})},30000)})();</script>")


WAITING_PAGE = ("<!DOCTYPE html><html><head><meta charset=utf-8>"
                "<meta name=viewport content='width=device-width,initial-scale=1'>"
                "<title>Crypto Tracker · starting</title>" + _live_reload_js("")
                + "<style>body{font:15px system-ui,sans-serif;padding:40px;color:#555}"
                "@media (prefers-color-scheme:dark){body{background:#0f1115;color:#9ca3af}}</style></head>"
                "<body><h2>Crypto Tracker is collecting the first snapshot…</h2>"
                "<p>This takes about a minute. The page will load by itself.</p></body></html>")


def publish_live(snap: dict, page: str):
    with _LIVE_LOCK:
        _LIVE["page"] = page.encode("utf-8")
        _LIVE["stamp"] = snap["generated_at_utc"]
        _LIVE["snapshot"] = json.dumps(snap, indent=2, default=str).encode("utf-8")


class _Handler(BaseHTTPRequestHandler):
    server_version = "CryptoTracker"

    def _send(self, code: int, body: bytes, ctype: str, head_only: bool = False):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def do_GET(self, head_only: bool = False):
        path = self.path.split("?", 1)[0]
        with _LIVE_LOCK:
            page, stamp, snapshot = _LIVE["page"], _LIVE["stamp"], _LIVE["snapshot"]
        if path in ("/", "/index.html", "/latest.html"):
            self._send(200, page or WAITING_PAGE.encode(), "text/html; charset=utf-8", head_only)
        elif path == "/stamp":
            self._send(200, stamp.encode(), "text/plain; charset=utf-8", head_only)
        elif path == "/snapshot.json" and snapshot:
            self._send(200, snapshot, "application/json; charset=utf-8", head_only)
        else:
            self._send(404, b"Not found", "text/plain; charset=utf-8", head_only)

    def do_HEAD(self):
        self.do_GET(head_only=True)

    def log_message(self, *args):  # keep the terminal quiet
        pass


def lan_addresses() -> list[str]:
    """Best guess at this machine's addresses on the local network."""
    ips = set()
    try:  # no packet is sent; this only asks the OS which interface it would use
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ips.add(s.getsockname()[0])
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass
    return sorted(ip for ip in ips if not ip.startswith("127."))


def start_server(host: str, port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), _Handler)
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    lines = [f"Crypto Tracker is being served on port {port}. Open one of these:",
             f"  this laptop:          http://localhost:{port}"]
    for ip in lan_addresses():
        lines.append(f"  other devices:        http://{ip}:{port}")
    lines.append(f"  via Tailscale:        http://{socket.gethostname().lower()}:{port}  (if Tailscale is on)")
    if HAS_RICH:
        console.print(Panel("\n".join(lines), title="Web server", expand=False))
    else:
        print("\n".join(lines))
    return httpd


def prune_reports(keep: int = KEEP_REPORTS):
    old = sorted(REPORT_DIR.glob("crypto_report_*.html"))
    for p in old[:-keep] if keep else []:
        try:
            p.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_parallel(tasks: dict[str, Callable], timeout: float = SOURCE_TIMEOUT) -> dict:
    """Run data sources at the same time, print progress, and give up on any
    source that takes longer than `timeout` seconds (it is reported, not fatal)."""
    results: dict[str, Any] = {}
    q: queue.Queue = queue.Queue()

    def worker(name, fn):
        t0 = time.time()
        try:
            q.put((name, True, fn(), time.time() - t0))
        except Exception as e:  # noqa: BLE001
            q.put((name, False, f"{type(e).__name__}: {e}", time.time() - t0))

    for name, fn in tasks.items():
        # daemon threads: a hung connection can never stop the script from exiting
        threading.Thread(target=worker, args=(name, fn), daemon=True).start()

    pending, start, last_note = set(tasks), time.time(), time.time()
    while pending:
        remaining = timeout - (time.time() - start)
        if remaining <= 0:
            break
        try:
            name, ok, val, dt = q.get(timeout=min(1.0, remaining))
        except queue.Empty:
            if time.time() - last_note >= 10:
                log(f"   ...still waiting for: {', '.join(sorted(pending))}")
                last_note = time.time()
            continue
        pending.discard(name)
        if ok:
            results[name] = val
            log(f"   ✓ {name} ({dt:.1f}s)")
        else:
            ERRORS.append(f"{name}: {val}")
            log(f"   ✗ {name}: {val[:120]}")
    for name in sorted(pending):
        ERRORS.append(f"{name}: no answer within {timeout:.0f}s, skipped")
        log(f"   ✗ {name}: no answer within {timeout:.0f}s, skipped")
    return results


# Slow-changing data is re-fetched less often in --watch mode, so short refresh
# intervals (e.g. --watch 2) stay within the free APIs' rate limits.
HISTORY_EVERY_MIN = 60   # 1y daily history / technicals
FLOWS_EVERY_MIN = 30     # ETF flows (published once a day) and stablecoin supply
_SLOW_CACHE: dict[str, tuple[float, Any]] = {}


def _fresh(key: str, max_age_min: float):
    hit = _SLOW_CACHE.get(key)
    return hit[1] if hit and time.time() - hit[0] < max_age_min * 60 else None


def fetch_all_technicals(currency: str) -> dict:
    techs = {}
    # CoinGecko's free tier allows only a few calls per minute, so these go one by one
    for cid in COINS:
        if cid in STABLECOINS:
            continue
        name = f"1y history {COINS[cid]['symbol']} (CoinGecko)"
        prices = run_parallel({name: lambda cid=cid: fetch_history(cid, currency)},
                              timeout=100).get(name) or []
        techs[cid] = technicals(prices) if prices else {}
        time.sleep(4)  # free CoinGecko allows only a few calls per minute
    return techs


def collect(currency: str, with_history: bool, with_news: bool, with_flows: bool = True) -> dict:
    ERRORS.clear()
    log("Fetching market data...")
    markets = run_parallel({"Prices (CoinGecko)": lambda: fetch_markets(currency)}).get("Prices (CoinGecko)", {})
    techs = {}
    if with_history:
        techs = _fresh(f"techs:{currency}", HISTORY_EVERY_MIN)
        if techs is None:
            techs = fetch_all_technicals(currency)
            if techs and all(techs.values()):   # only reuse a complete result
                _SLOW_CACHE[f"techs:{currency}"] = (time.time(), techs)
        else:
            log(f"   · technicals: reusing data from the last {HISTORY_EVERY_MIN} min")

    log("Fetching everything else (all sources at once)...")
    tasks = {
        "Global market (CoinGecko)": fetch_global,
        "Fear & Greed": fetch_fear_greed,
        "DeFi TVL (DefiLlama)": fetch_chain_tvl,
        "USDT supply (DefiLlama)": fetch_usdt,
        "BTC network (mempool.space)": fetch_btc_network,
        "ETH network (public RPC)": fetch_eth_network,
        "SOL network (Solana RPC)": fetch_sol_network,
    }
    cached_flows = _fresh("flows", FLOWS_EVERY_MIN) if with_flows else None
    if with_flows and cached_flows is None:
        ETF_RAW.clear()
        tasks["ETF flows (Farside)"] = fetch_etf_flows
        tasks["Stablecoin flows (DefiLlama)"] = fetch_stablecoin_flows
    elif cached_flows is not None:
        log(f"   · money flows: reusing data from the last {FLOWS_EVERY_MIN} min")
    if with_news:
        tasks["News (RSS)"] = fetch_news
    got = run_parallel(tasks)

    if cached_flows is not None:
        etf, stab = cached_flows
    else:
        etf = got.get("ETF flows (Farside)") or {}
        stab = got.get("Stablecoin flows (DefiLlama)") or {}
        if with_flows and etf and stab:   # only reuse a complete result
            _SLOW_CACHE["flows"] = (time.time(), (etf, stab))

    snap = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "generated_at_local": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
        "currency": currency,
        "markets": markets,
        "technicals": techs,
        "global": got.get("Global market (CoinGecko)"),
        "fear_greed": got.get("Fear & Greed"),
        "chain_tvl": got.get("DeFi TVL (DefiLlama)"),
        "usdt": got.get("USDT supply (DefiLlama)"),
        "btc_network": got.get("BTC network (mempool.space)"),
        "eth_network": got.get("ETH network (public RPC)"),
        "sol_network": got.get("SOL network (Solana RPC)"),
        "etf_flows": etf,
        "stablecoin_flows": stab,
        "news": got.get("News (RSS)") or {},
    }
    snap["alerts"] = build_alerts(markets, currency)
    snap["errors"] = list(ERRORS)
    return snap


def main():
    ap = argparse.ArgumentParser(description="Track BTC, ETH, SOL and USDT: market data, on-chain stats and news.")
    ap.add_argument("--currency", default="usd", help="Quote currency, e.g. usd, krw, eur (default usd)")
    ap.add_argument("--watch", type=float, metavar="MIN", help="Refresh every MIN minutes until Ctrl+C")
    ap.add_argument("--no-news", action="store_true", help="Skip news feeds")
    ap.add_argument("--no-history", action="store_true", help="Skip 1y history / technicals")
    ap.add_argument("--no-flows", action="store_true", help="Skip ETF / stablecoin money flows")
    ap.add_argument("--quiet", action="store_true", help="Only write files, no terminal tables")
    ap.add_argument("--no-open", action="store_true", help="Don't open the report in the browser")
    ap.add_argument("--serve", type=int, nargs="?", const=8000, metavar="PORT",
                    help="Serve the report as a live web page other devices can open (default port 8000). "
                         "Refreshes every 15 min unless --watch is given; opens http://localhost:PORT once.")
    ap.add_argument("--host", default="0.0.0.0",
                    help="Address to listen on in --serve mode (default 0.0.0.0 = all networks; "
                         "use 127.0.0.1 for this computer only)")
    args = ap.parse_args()
    currency = args.currency.lower()

    if args.serve is not None:
        args.watch = args.watch or 15
        start_server(args.host, args.serve)
        if not args.no_open:   # open the live page once; it reloads itself from then on
            webbrowser.open(f"http://localhost:{args.serve}")
        args.no_open = True

    opened = False
    while True:
        cycle_start = time.time()
        try:
            snap = collect(currency, not args.no_history, not args.no_news, not args.no_flows)
            if args.serve is not None:
                snap["live"] = args.watch
            report = save_outputs(snap)
        except Exception as e:  # noqa: BLE001  a long-running server must survive a bad cycle
            if not args.watch:
                raise
            log(f"This update failed ({type(e).__name__}: {e}). Trying again next cycle.")
        else:
            if args.serve is not None:
                publish_live(snap, report.read_text(encoding="utf-8"))
                prune_reports()
            if not args.quiet:
                print_terminal(snap)
            log(f"\nReport: {report}\nHistory: {DATA_DIR / 'price_history.csv'}")
            if not snap["markets"]:
                log("No market data. Check your internet connection or try again in a minute (rate limit).")
            if not args.no_open and not opened:   # open once; in --watch mode just reload the tab
                webbrowser.open(report.as_uri())
                opened = True
        if not args.watch:
            break
        # count the interval from the start of this update, so --watch 2 really means every 2 min
        wait = max(5.0, args.watch * 60 - (time.time() - cycle_start))
        log(f"Next refresh in {wait / 60:.1f} min (Ctrl+C to stop)...")
        try:
            time.sleep(wait)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
        sys.exit(0)