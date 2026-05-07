import os
import time
import math
import requests
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
import ta
import pandas as pd

# ── Load API keys ─────────────────────────────────────────────
load_dotenv()
API_KEY    = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

client = Client(API_KEY, API_SECRET)
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ── Konfigurasi ───────────────────────────────────────────────
INTERVAL         = Client.KLINE_INTERVAL_5MINUTE
TP_PCT           = 0.012   # Take Profit 1.2%  → RR 1:2
SL_PCT           = 0.006   # Stop Loss  0.6%
MIN_SCORE        = 45
MAX_POSITIONS    = 3
ORDER_VALUE_USDT = 55      # $55 per order (cukup untuk BTC min $50)

TOP_SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","MATICUSDT",
    "LINKUSDT","LTCUSDT","UNIUSDT","ATOMUSDT","ETCUSDT",
    "FILUSDT","NEARUSDT","APTUSDT","ARBUSDT","OPUSDT"
]

open_positions = {}  # { symbol: {side, entry, qty} }
trade_log      = []  # riwayat trade

# ── Symbol Info (precision) ───────────────────────────────────
_symbol_info = {}

def get_symbol_info(symbol):
    if symbol in _symbol_info:
        return _symbol_info[symbol]
    try:
        info = client.futures_exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        step = float(f["stepSize"])
                        minq = float(f["minQty"])
                        _symbol_info[symbol] = {"step": step, "minQty": minq}
                        return _symbol_info[symbol]
    except:
        pass
    return {"step": 1.0, "minQty": 1.0}

def round_step(qty, step):
    precision = int(round(-math.log(step, 10), 0)) if step < 1 else 0
    return round(math.floor(qty / step) * step, precision)

def calc_quantity(symbol, price):
    info    = get_symbol_info(symbol)
    raw_qty = ORDER_VALUE_USDT / price
    qty     = round_step(raw_qty, info["step"])
    return max(qty, info["minQty"])

# ── Fear & Greed Index ────────────────────────────────────────
_fng_cache = {"value": 50, "label": "Neutral", "last_fetch": 0}

def get_fear_greed():
    now = time.time()
    if now - _fng_cache["last_fetch"] > 600:  # refresh 10 menit
        try:
            res  = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
            data = res.json()["data"][0]
            _fng_cache["value"]      = int(data["value"])
            _fng_cache["label"]      = data["value_classification"]
            _fng_cache["last_fetch"] = now
        except:
            pass
    return _fng_cache["value"], _fng_cache["label"]

# ── USDT Dominance ────────────────────────────────────────────
_dom_cache = {"usdt": 5.0, "usdt_prev": 5.0, "last_fetch": 0}

def get_usdt_dominance():
    now = time.time()
    if now - _dom_cache["last_fetch"] > 300:
        try:
            res  = requests.get("https://api.coingecko.com/api/v3/global", timeout=8)
            data = res.json()["data"]["market_cap_percentage"]
            _dom_cache["usdt_prev"]  = _dom_cache["usdt"]
            _dom_cache["usdt"]       = round(data.get("usdt", 5), 2)
            _dom_cache["last_fetch"] = now
        except:
            pass
    trend = "up" if _dom_cache["usdt"] > _dom_cache["usdt_prev"] else \
            ("down" if _dom_cache["usdt"] < _dom_cache["usdt_prev"] else "flat")
    return _dom_cache["usdt"], trend

# ── Long/Short Ratio ──────────────────────────────────────────
def get_long_short_ratio(symbol):
    """Rasio akun long vs short — dari Binance Futures."""
    try:
        url    = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={symbol}&period=5m&limit=2"
        res    = requests.get(url, timeout=5)
        data   = res.json()
        latest = float(data[0]["longShortRatio"])
        prev   = float(data[1]["longShortRatio"])
        trend  = "more_longs" if latest > prev else "more_shorts"
        return latest, trend
    except:
        return 1.0, "neutral"

# ── Order Book Imbalance ──────────────────────────────────────
def get_orderbook_imbalance(symbol):
    """
    Hitung tekanan beli vs jual dari top 20 order book.
    Return: imbalance (-1.0 s/d 1.0), positif = lebih banyak buy
    """
    try:
        ob     = client.futures_order_book(symbol=symbol, limit=20)
        bid_vol = sum(float(b[1]) for b in ob["bids"])
        ask_vol = sum(float(a[1]) for a in ob["asks"])
        total   = bid_vol + ask_vol
        if total == 0: return 0.0
        imbalance = (bid_vol - ask_vol) / total  # -1 sampai +1
        return round(imbalance, 3)
    except:
        return 0.0

# ── Whale Detection ───────────────────────────────────────────
def detect_whale(df):
    """
    Cek apakah ada candle dengan volume > 3x rata-rata (whale masuk).
    Return: "buy_whale", "sell_whale", atau "none"
    """
    last    = df.iloc[-1]
    avg_vol = df["vol_ma"].iloc[-1]
    if pd.isna(avg_vol) or avg_vol == 0:
        return "none"
    ratio = last["volume"] / avg_vol
    if ratio >= 3.0:
        if last["close"] > last["open"]:
            return "buy_whale"
        else:
            return "sell_whale"
    elif ratio >= 1.8:
        if last["close"] > last["open"]:
            return "mild_buy"
        else:
            return "mild_sell"
    return "none"

# ── Funding Rate ──────────────────────────────────────────────
def get_funding_rate(symbol):
    """Funding rate positif = pasar terlalu banyak long (potensi SHORT)."""
    try:
        data = client.futures_funding_rate(symbol=symbol, limit=1)
        rate = float(data[0]["fundingRate"])
        return round(rate * 100, 4)  # dalam persen
    except:
        return 0.0

# ── Open Interest ─────────────────────────────────────────────
def get_open_interest_change(symbol):
    """
    Bandingkan OI sekarang vs sebelumnya.
    OI naik + harga naik = bullish konfirmasi
    OI naik + harga turun = bearish konfirmasi
    """
    try:
        url  = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m&limit=3"
        res  = requests.get(url, timeout=5)
        data = res.json()
        if len(data) < 2: return "neutral"
        oi_now  = float(data[0]["sumOpenInterest"])
        oi_prev = float(data[1]["sumOpenInterest"])
        if oi_now > oi_prev * 1.01:   return "increasing"
        if oi_now < oi_prev * 0.99:   return "decreasing"
        return "stable"
    except:
        return "neutral"

# ── News Sentiment ────────────────────────────────────────────
_news_cache = {"sentiment": "neutral", "last_fetch": 0}

def get_news_sentiment():
    now = time.time()
    if now - _news_cache["last_fetch"] > 180:
        try:
            url  = "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC"
            res  = requests.get(url, timeout=5)
            data = res.json()
            neg_kw = ["crash","hack","ban","bear","fear","lawsuit","fraud","dump","warning","collapse","scam"]
            pos_kw = ["bullish","rally","surge","adoption","institutional","ath","breakout","pump"]
            neg = pos = 0
            for post in data.get("results", [])[:10]:
                t = post.get("title","").lower()
                if any(w in t for w in neg_kw): neg += 1
                if any(w in t for w in pos_kw): pos += 1
            _news_cache["sentiment"]  = "negative" if neg >= 2 else ("positive" if pos >= 2 else "neutral")
            _news_cache["last_fetch"] = now
        except:
            pass
    return _news_cache["sentiment"]

# ── OHLCV ─────────────────────────────────────────────────────
def get_ohlcv(symbol):
    try:
        klines = client.futures_klines(symbol=symbol, interval=INTERVAL, limit=100)
        df     = pd.DataFrame(klines, columns=[
            "time","open","high","low","close","volume",
            "close_time","quote_vol","trades","taker_buy_base","taker_buy_quote","ignore"
        ])
        for col in ["open","high","low","close","volume"]:
            df[col] = df[col].astype(float)
        return df
    except:
        return None

# ── Analisa Teknikal ──────────────────────────────────────────
def analyze(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]       = ta.momentum.RSIIndicator(c, window=14).rsi()
    macd            = ta.trend.MACD(c)
    df["macd"]      = macd.macd()
    df["macd_sig"]  = macd.macd_signal()
    df["macd_hist"] = macd.macd_diff()
    df["ema20"]     = ta.trend.EMAIndicator(c, window=20).ema_indicator()
    df["ema50"]     = ta.trend.EMAIndicator(c, window=50).ema_indicator()
    bb              = ta.volatility.BollingerBands(c, window=20, window_dev=2)
    df["bb_upper"]  = bb.bollinger_hband()
    df["bb_lower"]  = bb.bollinger_lband()
    stoch           = ta.momentum.StochasticOscillator(h, l, c, window=14, smooth_window=3)
    df["stoch_k"]   = stoch.stoch()
    df["stoch_d"]   = stoch.stoch_signal()
    df["atr"]       = ta.volatility.AverageTrueRange(h, l, c, window=14).average_true_range()
    df["vol_ma"]    = v.rolling(window=20).mean()
    return df

# ── Master Scoring ────────────────────────────────────────────
def score_signal(df, symbol, usdt_d, usdt_trend, fng_value,
                 ls_ratio, ls_trend, ob_imbalance, whale,
                 funding_rate, oi_change, news):
    last = df.iloc[-1]
    prev = df.iloc[-2]
    ls = ss = 0

    # ── Teknikal ─────────────────────────────────────────────
    rsi = last["rsi"]
    if rsi < 30:   ls += 25
    elif rsi < 40: ls += 15
    elif rsi < 50: ls += 5
    if rsi > 70:   ss += 25
    elif rsi > 60: ss += 15
    elif rsi > 50: ss += 5

    if last["macd"] > last["macd_sig"] and prev["macd"] <= prev["macd_sig"]: ls += 20
    elif last["macd"] > last["macd_sig"]: ls += 10
    if last["macd"] < last["macd_sig"] and prev["macd"] >= prev["macd_sig"]: ss += 20
    elif last["macd"] < last["macd_sig"]: ss += 10

    if last["ema20"] > last["ema50"]:   ls += 15
    elif last["ema20"] < last["ema50"]: ss += 15

    price = last["close"]
    if price <= last["bb_lower"]:            ls += 15
    elif price <= last["bb_lower"] * 1.005:  ls += 8
    if price >= last["bb_upper"]:            ss += 15
    elif price >= last["bb_upper"] * 0.995:  ss += 8

    if last["stoch_k"] < 25 and last["stoch_k"] > last["stoch_d"]: ls += 15
    elif last["stoch_k"] < 40: ls += 7
    if last["stoch_k"] > 75 and last["stoch_k"] < last["stoch_d"]: ss += 15
    elif last["stoch_k"] > 60: ss += 7

    if last["macd_hist"] > 0 and prev["macd_hist"] > 0 and last["macd_hist"] > prev["macd_hist"]: ls += 10
    if last["macd_hist"] < 0 and prev["macd_hist"] < 0 and last["macd_hist"] < prev["macd_hist"]: ss += 10

    # ── Volume ───────────────────────────────────────────────
    if last["volume"] > last["vol_ma"] * 1.3:   ls += 8; ss += 8
    elif last["volume"] > last["vol_ma"]:        ls += 4; ss += 4

    # ── USDT Dominance ───────────────────────────────────────
    if usdt_trend == "up":    ls -= 15; ss += 10
    elif usdt_trend == "down": ls += 10; ss -= 5

    # ── Fear & Greed ─────────────────────────────────────────
    if fng_value <= 25:    ls += 15   # Extreme Fear = oversold, potensi LONG
    elif fng_value <= 40:  ls += 8
    elif fng_value >= 75:  ss += 15   # Extreme Greed = overbought, potensi SHORT
    elif fng_value >= 60:  ss += 8

    # ── Long/Short Ratio ─────────────────────────────────────
    if ls_ratio > 1.5:    ls += 10   # banyak yang long = trend up
    elif ls_ratio < 0.7:  ss += 10   # banyak yang short = trend down
    if ls_trend == "more_longs":  ls += 5
    if ls_trend == "more_shorts": ss += 5

    # ── Order Book Imbalance ─────────────────────────────────
    if ob_imbalance > 0.2:    ls += 15   # lebih banyak bid (buy wall)
    elif ob_imbalance > 0.1:  ls += 8
    if ob_imbalance < -0.2:   ss += 15  # lebih banyak ask (sell wall)
    elif ob_imbalance < -0.1: ss += 8

    # ── Whale Detection ──────────────────────────────────────
    if whale == "buy_whale":   ls += 20
    elif whale == "mild_buy":  ls += 10
    if whale == "sell_whale":  ss += 20
    elif whale == "mild_sell": ss += 10

    # ── Funding Rate ─────────────────────────────────────────
    if funding_rate > 0.05:    ss += 10   # terlalu banyak long, potensi SHORT
    elif funding_rate > 0.02:  ss += 5
    if funding_rate < -0.05:   ls += 10   # terlalu banyak short, potensi LONG
    elif funding_rate < -0.02: ls += 5

    # ── Open Interest ────────────────────────────────────────
    if oi_change == "increasing":
        if last["close"] > last["open"]: ls += 10   # OI naik + harga naik = bullish
        else:                            ss += 10   # OI naik + harga turun = bearish
    elif oi_change == "decreasing":
        ls -= 5; ss -= 5   # OI turun = posisi ditutup, kurang konfirmasi

    # ── News ─────────────────────────────────────────────────
    if news == "positive":  ls += 10
    if news == "negative":  ls -= 20; ss += 15

    return max(0, ls), max(0, ss)

# ── Exchange helpers ──────────────────────────────────────────
def get_open_position_exchange(symbol):
    try:
        for p in client.futures_position_information(symbol=symbol):
            amt = float(p["positionAmt"])
            if amt != 0: return amt
        return 0
    except:
        return 0

def open_position(symbol, side, score):
    try:
        price      = float(client.futures_symbol_ticker(symbol=symbol)["price"])
        qty        = calc_quantity(symbol, price)
        order_side = SIDE_BUY if side == "LONG" else SIDE_SELL
        client.futures_create_order(
            symbol=symbol, side=order_side,
            type=ORDER_TYPE_MARKET, quantity=qty
        )
        entry = float(client.futures_symbol_ticker(symbol=symbol)["price"])
        tp    = entry * (1 + TP_PCT) if side == "LONG" else entry * (1 - TP_PCT)
        sl    = entry * (1 - SL_PCT) if side == "LONG" else entry * (1 + SL_PCT)
        open_positions[symbol] = {"side": side, "entry": entry, "qty": qty}
        print(f"  ✅ [{symbol}] {side} @ {entry:.4f} | qty={qty} | TP: {tp:.4f} | SL: {sl:.4f} | Skor: {score}")
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal buka: {e}")

def close_position(symbol, reason=""):
    try:
        amt = get_open_position_exchange(symbol)
        if amt == 0:
            open_positions.pop(symbol, None)
            return
        side = SIDE_SELL if amt > 0 else SIDE_BUY
        client.futures_create_order(
            symbol=symbol, side=side,
            type=ORDER_TYPE_MARKET, quantity=abs(amt), reduceOnly=True
        )
        # Hitung P&L
        if symbol in open_positions:
            pos        = open_positions[symbol]
            entry      = pos["entry"]
            qty        = pos["qty"]
            exit_price = float(client.futures_symbol_ticker(symbol=symbol)["price"])
            if pos["side"] == "LONG":
                pnl = (exit_price - entry) * qty
            else:
                pnl = (entry - exit_price) * qty
            pnl_pct = (pnl / (entry * qty)) * 100
            emoji   = "🟢" if pnl >= 0 else "🔴"
            print(f"  💰 [{symbol}] CLOSED — {reason}")
            print(f"     {emoji} P&L: {'+' if pnl >= 0 else ''}{pnl:.4f} USDT ({'+' if pnl_pct >= 0 else ''}{pnl_pct:.2f}%)")
            trade_log.append({
                "symbol": symbol, "side": pos["side"],
                "entry": entry, "exit": exit_price,
                "qty": qty, "pnl": round(pnl, 4),
                "pnl_pct": round(pnl_pct, 2), "reason": reason
            })
        open_positions.pop(symbol, None)
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal tutup: {e}")

def check_tp_sl(symbol, price):
    if symbol not in open_positions: return False
    pos   = open_positions[symbol]
    entry = pos["entry"]
    side  = pos["side"]
    if side == "LONG":
        if price >= entry * (1 + TP_PCT): close_position(symbol, f"✨ TAKE PROFIT +{TP_PCT*100}%"); return True
        if price <= entry * (1 - SL_PCT): close_position(symbol, f"🛑 STOP LOSS -{SL_PCT*100}%");  return True
    else:
        if price <= entry * (1 - TP_PCT): close_position(symbol, f"✨ TAKE PROFIT +{TP_PCT*100}%"); return True
        if price >= entry * (1 + SL_PCT): close_position(symbol, f"🛑 STOP LOSS -{SL_PCT*100}%");  return True
    return False

def print_summary():
    if not trade_log: return
    total_pnl  = sum(t["pnl"] for t in trade_log)
    wins       = sum(1 for t in trade_log if t["pnl"] > 0)
    losses     = sum(1 for t in trade_log if t["pnl"] <= 0)
    winrate    = (wins / len(trade_log)) * 100 if trade_log else 0
    print(f"\n  📈 SUMMARY — {len(trade_log)} trade")
    print(f"     Menang : {wins} | Kalah: {losses} | WR: {winrate:.1f}%")
    print(f"     Total P&L: {'+' if total_pnl >= 0 else ''}{total_pnl:.4f} USDT")

# ── Main Loop ─────────────────────────────────────────────────
def run_bot():
    print("🤖 Bot Multi-Pair v4 dimulai!")
    print(f"   Pairs        : Top 20 Futures")
    print(f"   Order Value  : ${ORDER_VALUE_USDT} USDT per posisi")
    print(f"   Min Score    : {MIN_SCORE}")
    print(f"   TP / SL      : +{TP_PCT*100}% / -{SL_PCT*100}% (RR 1:2)")
    print(f"   Max Posisi   : {MAX_POSITIONS}")
    print(f"   Analisa      : TA + USDT.D + Fear&Greed + L/S Ratio")
    print(f"                  + OrderBook + Whale + FundingRate + OI + News\n")

    print("  ⏳ Loading symbol info...")
    for sym in TOP_SYMBOLS:
        get_symbol_info(sym)
    print("  ✅ Symbol info loaded!\n")

    cycle = 0
    while True:
        cycle += 1
        print(f"\n{'='*62}")
        print(f"  🔄 Siklus #{cycle} — {time.strftime('%H:%M:%S')}")
        print(f"{'='*62}")

        # Data global (fetch sekali per siklus)
        usdt_d, usdt_trend = get_usdt_dominance()
        fng_val, fng_label = get_fear_greed()
        news               = get_news_sentiment()

        print(f"  📊 USDT.D : {usdt_d}% ({usdt_trend})")
        print(f"  😱 F&G    : {fng_val} — {fng_label}")
        print(f"  📰 News   : {news}")
        print(f"  📂 Posisi ({len(open_positions)}): {list(open_positions.keys()) or '-'}")

        candidates = []

        for symbol in TOP_SYMBOLS:
            df = get_ohlcv(symbol)
            if df is None or len(df) < 55:
                continue

            df    = analyze(df)
            price = df["close"].iloc[-1]

            # Cek TP/SL posisi terbuka
            if symbol in open_positions:
                closed = check_tp_sl(symbol, price)
                if not closed and news == "negative":
                    close_position(symbol, "📰 Berita buruk")
                continue

            if news == "negative":
                continue

            # Fetch data on-chain & market untuk scoring
            ls_ratio, ls_trend = get_long_short_ratio(symbol)
            ob_imbalance       = get_orderbook_imbalance(symbol)
            whale              = detect_whale(df)
            funding_rate       = get_funding_rate(symbol)
            oi_change          = get_open_interest_change(symbol)

            long_s, short_s = score_signal(
                df, symbol, usdt_d, usdt_trend, fng_val,
                ls_ratio, ls_trend, ob_imbalance, whale,
                funding_rate, oi_change, news
            )

            if long_s  >= MIN_SCORE: candidates.append((long_s,  symbol, "LONG"))
            if short_s >= MIN_SCORE: candidates.append((short_s, symbol, "SHORT"))

        candidates.sort(key=lambda x: x[0], reverse=True)

        if candidates:
            print(f"\n  🎯 Top Kandidat:")
            for score, sym, side in candidates[:5]:
                print(f"     {sym:12} {side:5} skor={score}")
        else:
            print(f"\n  ⏳ Belum ada sinyal kuat (min skor {MIN_SCORE})")

        # Entry top kandidat sampai MAX_POSITIONS
        slot    = MAX_POSITIONS - len(open_positions)
        entered = 0
        for score, symbol, side in candidates:
            if entered >= slot: break
            if symbol not in open_positions:
                open_position(symbol, side, score)
                entered += 1

        # Print ringkasan trade
        print_summary()

        print(f"\n  ⏱️  Menunggu 60 detik...")
        time.sleep(60)

if __name__ == "__main__":
    run_bot()
