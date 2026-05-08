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
LEVERAGE         = 10
TP_PCT           = 0.012    # TP 1.2%   → RR 1:2
SL_PCT           = 0.006    # SL 0.6%
TRAIL_PCT        = 0.005    # Trailing Stop 0.5% dari peak
MIN_SCORE        = 60       # Lebih ketat — kurangi noise
SCORE_GAP        = 15       # Selisih min antara LONG vs SHORT score (anti konflik)
MAX_POSITIONS    = 3
ORDER_VALUE_USDT = 55

TOP_SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","MATICUSDT",
    "LINKUSDT","LTCUSDT","UNIUSDT","ATOMUSDT","ETCUSDT",
    "FILUSDT","NEARUSDT","APTUSDT","ARBUSDT","OPUSDT"
]

# { symbol: {side, entry, qty, peak, trail_sl} }
open_positions = {}
trade_log      = []

# ── Leverage Setup ────────────────────────────────────────────
def set_leverage(symbol):
    try:
        client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except:
        pass

# ── Symbol Info ───────────────────────────────────────────────
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

# ── Fear & Greed ──────────────────────────────────────────────
_fng_cache = {"value": 50, "label": "Neutral", "last_fetch": 0}

def get_fear_greed():
    now = time.time()
    if now - _fng_cache["last_fetch"] > 300:
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

# ── News — refresh tiap 60 detik ─────────────────────────────
_news_cache = {"sentiment": "neutral", "headlines": [], "last_fetch": 0}

def get_news_sentiment():
    now = time.time()
    if now - _news_cache["last_fetch"] > 60:   # ← tiap 1 menit
        try:
            url  = "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC"
            res  = requests.get(url, timeout=5)
            data = res.json()
            neg_kw = ["crash","hack","ban","bear","fear","lawsuit","fraud","dump",
                      "warning","collapse","scam","sell","decline","plunge","seized"]
            pos_kw = ["bullish","rally","surge","adoption","institutional","ath",
                      "breakout","pump","buy","approved","launched","partnership"]
            neg = pos = 0
            headlines = []
            for post in data.get("results", [])[:10]:
                t = post.get("title","")
                tl = t.lower()
                if any(w in tl for w in neg_kw): neg += 1; headlines.append(f"🔴 {t[:60]}")
                elif any(w in tl for w in pos_kw): pos += 1; headlines.append(f"🟢 {t[:60]}")
            _news_cache["sentiment"]  = "negative" if neg >= 2 else ("positive" if pos >= 2 else "neutral")
            _news_cache["headlines"]  = headlines[:3]
            _news_cache["last_fetch"] = now
        except:
            pass
    return _news_cache["sentiment"], _news_cache["headlines"]

# ── Market Data ───────────────────────────────────────────────
def get_long_short_ratio(symbol):
    try:
        url  = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={symbol}&period=5m&limit=3"
        res  = requests.get(url, timeout=5)
        data = res.json()
        r0   = float(data[0]["longShortRatio"])
        r1   = float(data[1]["longShortRatio"])
        trend = "more_longs" if r0 > r1 else "more_shorts"
        return r0, trend
    except:
        return 1.0, "neutral"

def get_orderbook_imbalance(symbol):
    try:
        ob      = client.futures_order_book(symbol=symbol, limit=20)
        bid_vol = sum(float(b[1]) for b in ob["bids"])
        ask_vol = sum(float(a[1]) for a in ob["asks"])
        total   = bid_vol + ask_vol
        return round((bid_vol - ask_vol) / total, 3) if total else 0.0
    except:
        return 0.0

def detect_whale(df):
    last    = df.iloc[-1]
    avg_vol = df["vol_ma"].iloc[-1]
    if pd.isna(avg_vol) or avg_vol == 0: return "none"
    ratio = last["volume"] / avg_vol
    if ratio >= 3.0:
        return "buy_whale"  if last["close"] > last["open"] else "sell_whale"
    elif ratio >= 1.8:
        return "mild_buy"   if last["close"] > last["open"] else "mild_sell"
    return "none"

def get_funding_rate(symbol):
    try:
        data = client.futures_funding_rate(symbol=symbol, limit=1)
        return round(float(data[0]["fundingRate"]) * 100, 4)
    except:
        return 0.0

def get_open_interest_change(symbol):
    try:
        url  = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m&limit=3"
        res  = requests.get(url, timeout=5)
        data = res.json()
        if len(data) < 2: return "neutral"
        oi0 = float(data[0]["sumOpenInterest"])
        oi1 = float(data[1]["sumOpenInterest"])
        if oi0 > oi1 * 1.01: return "increasing"
        if oi0 < oi1 * 0.99: return "decreasing"
        return "stable"
    except:
        return "neutral"

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
    df["ema200"]    = ta.trend.EMAIndicator(c, window=200).ema_indicator()
    bb              = ta.volatility.BollingerBands(c, window=20, window_dev=2)
    df["bb_upper"]  = bb.bollinger_hband()
    df["bb_lower"]  = bb.bollinger_lband()
    df["bb_mid"]    = bb.bollinger_mavg()
    stoch           = ta.momentum.StochasticOscillator(h, l, c, window=14, smooth_window=3)
    df["stoch_k"]   = stoch.stoch()
    df["stoch_d"]   = stoch.stoch_signal()
    df["atr"]       = ta.volatility.AverageTrueRange(h, l, c, window=14).average_true_range()
    df["vol_ma"]    = v.rolling(window=20).mean()
    # Taker buy ratio — seberapa banyak market buy vs market sell
    df["taker_buy_base"] = df["taker_buy_base"].astype(float)
    df["buy_ratio"] = df["taker_buy_base"] / df["volume"].replace(0, 1)
    return df

# ── Master Scoring dengan filter konflik ─────────────────────
def score_signal(df, usdt_d, usdt_trend, fng_value,
                 ls_ratio, ls_trend, ob_imbalance, whale,
                 funding_rate, oi_change, news):
    last = df.iloc[-1]
    prev = df.iloc[-2]
    ls = ss = 0

    # ── RSI ───────────────────────────────────────────────────
    rsi = last["rsi"]
    if rsi < 25:   ls += 30
    elif rsi < 35: ls += 20
    elif rsi < 45: ls += 8
    if rsi > 75:   ss += 30
    elif rsi > 65: ss += 20
    elif rsi > 55: ss += 8

    # ── MACD — crossover lebih berbobot ───────────────────────
    if last["macd"] > last["macd_sig"] and prev["macd"] <= prev["macd_sig"]:
        ls += 25   # fresh crossover bullish
    elif last["macd"] > last["macd_sig"] and last["macd_hist"] > prev["macd_hist"]:
        ls += 12   # momentum makin kuat
    if last["macd"] < last["macd_sig"] and prev["macd"] >= prev["macd_sig"]:
        ss += 25   # fresh crossover bearish
    elif last["macd"] < last["macd_sig"] and last["macd_hist"] < prev["macd_hist"]:
        ss += 12

    # ── EMA trend ─────────────────────────────────────────────
    price = last["close"]
    if last["ema20"] > last["ema50"]:   ls += 12
    elif last["ema20"] < last["ema50"]: ss += 12
    # Konfirmasi EMA200 (trend jangka panjang)
    if not pd.isna(last["ema200"]):
        if price > last["ema200"]: ls += 8
        else:                      ss += 8

    # ── Bollinger Bands ───────────────────────────────────────
    if price <= last["bb_lower"]:            ls += 18
    elif price <= last["bb_lower"] * 1.003:  ls += 10
    if price >= last["bb_upper"]:            ss += 18
    elif price >= last["bb_upper"] * 0.997:  ss += 10

    # ── Stochastic ────────────────────────────────────────────
    k, d = last["stoch_k"], last["stoch_d"]
    if k < 20 and k > d:   ls += 18
    elif k < 30:           ls += 8
    if k > 80 and k < d:   ss += 18
    elif k > 70:           ss += 8

    # ── Taker Buy Ratio (agresivitas pembeli) ─────────────────
    buy_ratio = last["buy_ratio"]
    if buy_ratio > 0.6:    ls += 12   # >60% transaksi adalah market buy
    elif buy_ratio > 0.55: ls += 6
    if buy_ratio < 0.4:    ss += 12   # >60% transaksi adalah market sell
    elif buy_ratio < 0.45: ss += 6

    # ── Volume ────────────────────────────────────────────────
    if last["volume"] > last["vol_ma"] * 1.5:   ls += 10; ss += 10
    elif last["volume"] > last["vol_ma"] * 1.2:  ls += 5;  ss += 5

    # ── USDT Dominance ────────────────────────────────────────
    if usdt_trend == "up":    ls -= 20; ss += 12
    elif usdt_trend == "down": ls += 12; ss -= 8

    # ── Fear & Greed ─────────────────────────────────────────
    if fng_value <= 20:    ls += 18   # Extreme Fear = bottom potensi
    elif fng_value <= 35:  ls += 10
    elif fng_value >= 80:  ss += 18   # Extreme Greed = top potensi
    elif fng_value >= 65:  ss += 10

    # ── Long/Short Ratio ──────────────────────────────────────
    if ls_ratio > 1.8:   ls += 10
    elif ls_ratio > 1.3: ls += 5
    if ls_ratio < 0.6:   ss += 10
    elif ls_ratio < 0.8: ss += 5
    if ls_trend == "more_longs":  ls += 5
    if ls_trend == "more_shorts": ss += 5

    # ── Order Book Imbalance ──────────────────────────────────
    if ob_imbalance > 0.25:   ls += 18
    elif ob_imbalance > 0.12: ls += 10
    if ob_imbalance < -0.25:  ss += 18
    elif ob_imbalance < -0.12: ss += 10

    # ── Whale ─────────────────────────────────────────────────
    if whale == "buy_whale":   ls += 25
    elif whale == "mild_buy":  ls += 12
    if whale == "sell_whale":  ss += 25
    elif whale == "mild_sell": ss += 12

    # ── Funding Rate ──────────────────────────────────────────
    if funding_rate > 0.08:    ss += 15
    elif funding_rate > 0.04:  ss += 8
    if funding_rate < -0.08:   ls += 15
    elif funding_rate < -0.04: ls += 8

    # ── Open Interest ─────────────────────────────────────────
    if oi_change == "increasing":
        if last["close"] > last["open"]: ls += 12
        else:                            ss += 12
    elif oi_change == "decreasing":
        ls -= 5; ss -= 5

    # ── News ──────────────────────────────────────────────────
    if news == "positive":  ls += 15; ss -= 10
    if news == "negative":  ls -= 25; ss += 18

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
        set_leverage(symbol)
        price      = float(client.futures_symbol_ticker(symbol=symbol)["price"])
        qty        = calc_quantity(symbol, price)
        order_side = SIDE_BUY if side == "LONG" else SIDE_SELL
        client.futures_create_order(
            symbol=symbol, side=order_side,
            type=ORDER_TYPE_MARKET, quantity=qty
        )
        entry      = float(client.futures_symbol_ticker(symbol=symbol)["price"])
        tp         = entry * (1 + TP_PCT) if side == "LONG" else entry * (1 - TP_PCT)
        sl         = entry * (1 - SL_PCT) if side == "LONG" else entry * (1 + SL_PCT)
        trail_sl   = entry * (1 - TRAIL_PCT) if side == "LONG" else entry * (1 + TRAIL_PCT)
        open_positions[symbol] = {
            "side": side, "entry": entry, "qty": qty,
            "peak": entry, "trail_sl": trail_sl
        }
        print(f"  ✅ [{symbol}] {side} @ {entry:.4f} | qty={qty} | TP:{tp:.4f} | SL:{sl:.4f} | Skor:{score}")
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
        if symbol in open_positions:
            pos        = open_positions[symbol]
            entry      = pos["entry"]
            qty        = pos["qty"]
            exit_price = float(client.futures_symbol_ticker(symbol=symbol)["price"])
            pnl        = (exit_price - entry) * qty if pos["side"] == "LONG" else (entry - exit_price) * qty
            pnl_pct    = (pnl / (entry * qty)) * 100
            emoji      = "🟢" if pnl >= 0 else "🔴"
            print(f"  💰 [{symbol}] CLOSED — {reason}")
            print(f"     {emoji} P&L: {'+' if pnl>=0 else ''}{pnl:.4f} USDT ({'+' if pnl_pct>=0 else ''}{pnl_pct:.2f}%)")
            trade_log.append({
                "symbol": symbol, "side": pos["side"],
                "entry": entry, "exit": exit_price,
                "pnl": round(pnl, 4), "pnl_pct": round(pnl_pct, 2)
            })
        open_positions.pop(symbol, None)
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal tutup: {e}")

# ── Trailing Stop + TP/SL check ───────────────────────────────
def check_exit(symbol, price):
    if symbol not in open_positions: return False
    pos   = open_positions[symbol]
    entry = pos["entry"]
    side  = pos["side"]
    peak  = pos["peak"]

    # Update peak & trailing stop
    if side == "LONG":
        if price > peak:
            pos["peak"]     = price
            pos["trail_sl"] = price * (1 - TRAIL_PCT)
        # Cek TP dulu
        if price >= entry * (1 + TP_PCT):
            close_position(symbol, f"✨ TAKE PROFIT +{TP_PCT*100}%"); return True
        # Cek Trailing Stop
        if price <= pos["trail_sl"] and price < peak:
            close_position(symbol, f"🔄 TRAILING STOP (peak:{peak:.4f})"); return True
        # Cek SL hard
        if price <= entry * (1 - SL_PCT):
            close_position(symbol, f"🛑 STOP LOSS -{SL_PCT*100}%"); return True
    else:  # SHORT
        if price < peak:
            pos["peak"]     = price
            pos["trail_sl"] = price * (1 + TRAIL_PCT)
        if price <= entry * (1 - TP_PCT):
            close_position(symbol, f"✨ TAKE PROFIT +{TP_PCT*100}%"); return True
        if price >= pos["trail_sl"] and price > peak:
            close_position(symbol, f"🔄 TRAILING STOP (peak:{peak:.4f})"); return True
        if price >= entry * (1 + SL_PCT):
            close_position(symbol, f"🛑 STOP LOSS -{SL_PCT*100}%"); return True
    return False

def print_summary():
    if not trade_log: return
    total_pnl = sum(t["pnl"] for t in trade_log)
    wins      = sum(1 for t in trade_log if t["pnl"] > 0)
    losses    = len(trade_log) - wins
    winrate   = (wins / len(trade_log)) * 100
    print(f"\n  📈 SUMMARY — {len(trade_log)} trade | WR: {winrate:.1f}% | W:{wins} L:{losses}")
    print(f"     💵 Total P&L: {'+' if total_pnl>=0 else ''}{total_pnl:.4f} USDT")

# ── Main Loop ─────────────────────────────────────────────────
def run_bot():
    print("🤖 Bot Multi-Pair v5 dimulai!")
    print(f"   Leverage     : {LEVERAGE}x")
    print(f"   Order Value  : ${ORDER_VALUE_USDT} USDT (margin) = ${ORDER_VALUE_USDT*LEVERAGE} exposure")
    print(f"   Min Score    : {MIN_SCORE} | Score Gap: {SCORE_GAP}")
    print(f"   TP / SL      : +{TP_PCT*100}% / -{SL_PCT*100}% (RR 1:2)")
    print(f"   Trailing Stop: {TRAIL_PCT*100}% dari peak")
    print(f"   Max Posisi   : {MAX_POSITIONS}")
    print(f"   News refresh : tiap 60 detik\n")

    print("  ⏳ Loading symbol info...")
    for sym in TOP_SYMBOLS:
        get_symbol_info(sym)
    print("  ✅ Ready!\n")

    cycle = 0
    while True:
        cycle += 1
        print(f"\n{'='*62}")
        print(f"  🔄 Siklus #{cycle} — {time.strftime('%H:%M:%S')}")
        print(f"{'='*62}")

        usdt_d, usdt_trend = get_usdt_dominance()
        fng_val, fng_label = get_fear_greed()
        news, headlines    = get_news_sentiment()

        print(f"  📊 USDT.D : {usdt_d}% ({usdt_trend})")
        print(f"  😱 F&G    : {fng_val} — {fng_label}")
        print(f"  📰 News   : {news}")
        for h in headlines:
            print(f"     {h}")
        print(f"  📂 Posisi ({len(open_positions)}): {list(open_positions.keys()) or '-'}")

        candidates = []

        for symbol in TOP_SYMBOLS:
            df = get_ohlcv(symbol)
            if df is None or len(df) < 60:
                continue

            df    = analyze(df)
            price = df["close"].iloc[-1]

            if symbol in open_positions:
                closed = check_exit(symbol, price)
                if not closed and news == "negative":
                    close_position(symbol, "📰 Berita buruk")
                continue

            if news == "negative":
                continue

            ls_ratio, ls_trend = get_long_short_ratio(symbol)
            ob_imbalance       = get_orderbook_imbalance(symbol)
            whale              = detect_whale(df)
            funding_rate       = get_funding_rate(symbol)
            oi_change          = get_open_interest_change(symbol)

            long_s, short_s = score_signal(
                df, usdt_d, usdt_trend, fng_val,
                ls_ratio, ls_trend, ob_imbalance, whale,
                funding_rate, oi_change, news
            )

            # ── Filter anti-konflik sinyal ────────────────────
            gap = abs(long_s - short_s)
            if gap < SCORE_GAP:
                continue   # sinyal terlalu ambigu, skip

            if long_s  >= MIN_SCORE and long_s  > short_s:
                candidates.append((long_s,  symbol, "LONG"))
            if short_s >= MIN_SCORE and short_s > long_s:
                candidates.append((short_s, symbol, "SHORT"))

        candidates.sort(key=lambda x: x[0], reverse=True)

        if candidates:
            print(f"\n  🎯 Top Kandidat:")
            for score, sym, side in candidates[:5]:
                print(f"     {sym:12} {side:5} skor={score}")
        else:
            print(f"\n  ⏳ Belum ada sinyal kuat (min:{MIN_SCORE}, gap:{SCORE_GAP})")

        slot    = MAX_POSITIONS - len(open_positions)
        entered = 0
        for score, symbol, side in candidates:
            if entered >= slot: break
            if symbol not in open_positions:
                open_position(symbol, side, score)
                entered += 1

        print_summary()
        print(f"\n  ⏱️  Menunggu 60 detik...")
        time.sleep(60)

if __name__ == "__main__":
    run_bot()
