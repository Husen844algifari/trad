"""
Bot Trading v6 — High-WR Multi-Layer Confluence System
=====================================================
Layers sebelum entry (SEMUA harus konfirmasi):
  1. Market Regime Filter  — hanya trade searah trend besar
  2. SMC: FVG + Liquidity  — entry di area institusional
  3. Order Book Imbalance  — tekanan beli/jual real-time
  4. Cumulative Delta      — akumulasi aggressor buy/sell
  5. Whale / On-Chain      — deteksi smart money
  6. Long/Short + Funding  — sentiment futures
  7. Fear & Greed + News   — makro sentiment
  8. Teknikal (RSI/MACD/BB/Stoch/EMA) — konfirmasi timing
Setelah entry:
  - Hard TP/SL + Trailing Stop
  - Leverage 10x
  - News emergency exit tiap 60 detik
"""

import os, time, math, requests
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════
LEVERAGE          = 10
ORDER_USDT        = 55       # margin per trade
TP_PCT            = 0.012    # 1.2%  → RR 1:2
SL_PCT            = 0.006    # 0.6%
TRAIL_PCT         = 0.004    # trailing 0.4% dari peak
MIN_CONFLUENCE    = 5        # minimal layer yang konfirmasi (dari 8)
MAX_POSITIONS     = 3
INTERVAL_MAIN     = Client.KLINE_INTERVAL_5MINUTE
INTERVAL_HTF      = Client.KLINE_INTERVAL_1HOUR   # higher time frame

TOP_50_SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","MATICUSDT",
    "LINKUSDT","LTCUSDT","UNIUSDT","ATOMUSDT","ETCUSDT",
    "FILUSDT","NEARUSDT","APTUSDT","ARBUSDT","OPUSDT",
    "INJUSDT","SUIUSDT","SEIUSDT","TIAUSDT","WLDUSDT",
    "RUNEUSDT","AAVEUSDT","MKRUSDT","SNXUSDT","CRVUSDT",
    " 1000PEPEUSDT","FLOKIUSDT","BONKUSDT","WIFUSDT","JUPUSDT",
    "STRKUSDT","ALTUSDT","PYTHUSDT","JSTOUSDT","CAKEUSDT",
    "GALAUSDT","SANDUSDT","MANAUSDT","AXSUSDT","ENJUSDT",
    "ZILUSDT","IOTAUSDT","ONTUSDT","ZENUSDT","CKBUSDT"
]

open_positions = {}   # {symbol: {side,entry,qty,peak,trail_sl}}
trade_log      = []

# ════════════════════════════════════════════════════
#  UTILS
# ════════════════════════════════════════════════════
_sym_info = {}
def get_sym_info(symbol):
    if symbol in _sym_info: return _sym_info[symbol]
    try:
        for s in client.futures_exchange_info()["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        _sym_info[symbol] = {"step": float(f["stepSize"]), "minQty": float(f["minQty"])}
                        return _sym_info[symbol]
    except: pass
    return {"step":1.0,"minQty":1.0}

def round_step(qty, step):
    p = int(round(-math.log(step,10),0)) if step < 1 else 0
    return round(math.floor(qty/step)*step, p)

def calc_qty(symbol, price):
    info = get_sym_info(symbol)
    return max(round_step(ORDER_USDT/price, info["step"]), info["minQty"])

def set_leverage(symbol):
    try: client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except: pass

def get_price(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

# ════════════════════════════════════════════════════
#  OHLCV
# ════════════════════════════════════════════════════
def get_ohlcv(symbol, interval=INTERVAL_MAIN, limit=200):
    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            "time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        return df
    except: return None

# ════════════════════════════════════════════════════
#  LAYER 1 — Market Regime (HTF trend)
# ════════════════════════════════════════════════════
def market_regime(symbol):
    """
    Pakai 1H chart untuk tentukan regime: BULL / BEAR / RANGE
    EMA20 > EMA50 > EMA200 = BULL, sebaliknya BEAR, sisanya RANGE
    Return: ('BULL'|'BEAR'|'RANGE', bias)
    bias: +1=long_ok, -1=short_ok, 0=skip
    """
    df = get_ohlcv(symbol, interval=INTERVAL_HTF, limit=210)
    if df is None or len(df) < 60: return "RANGE", 0
    c = df["close"]
    ema20  = ta.trend.EMAIndicator(c, window=20).ema_indicator().iloc[-1]
    ema50  = ta.trend.EMAIndicator(c, window=50).ema_indicator().iloc[-1]
    try:
        ema200 = ta.trend.EMAIndicator(c, window=200).ema_indicator().iloc[-1]
    except:
        ema200 = ema50
    price = c.iloc[-1]
    if ema20 > ema50 and price > ema50:
        return "BULL", 1
    elif ema20 < ema50 and price < ema50:
        return "BEAR", -1
    return "RANGE", 0

# ════════════════════════════════════════════════════
#  LAYER 2 — SMC: FVG + Liquidity Sweep
# ════════════════════════════════════════════════════
def find_fvg(df):
    """
    Fair Value Gap: gap antara candle i-2 high dan candle i low (bullish FVG)
    atau candle i-2 low dan candle i high (bearish FVG)
    Return: ('bull_fvg'|'bear_fvg'|'none', fvg_level)
    """
    if len(df) < 5: return "none", 0
    results = []
    for i in range(2, min(6, len(df))):
        idx = -(i)
        c0 = df.iloc[idx-2]   # candle paling kiri
        c2 = df.iloc[idx]     # candle paling kanan
        # Bullish FVG: low candle kanan > high candle kiri
        if c2["low"] > c0["high"]:
            results.append(("bull_fvg", (c0["high"] + c2["low"]) / 2))
        # Bearish FVG: high candle kanan < low candle kiri
        elif c2["high"] < c0["low"]:
            results.append(("bear_fvg", (c0["low"] + c2["high"]) / 2))
    # Return FVG yang paling dekat dengan harga saat ini
    if not results: return "none", 0
    price = df["close"].iloc[-1]
    results.sort(key=lambda x: abs(x[1] - price))
    return results[0]

def liquidity_sweep(df):
    """
    Deteksi liquidity grab: harga spike ke swing high/low lalu balik arah.
    Return: 'bull_sweep' (ambil likuiditas bawah lalu naik)
            'bear_sweep' (ambil likuiditas atas lalu turun)
            'none'
    """
    if len(df) < 20: return "none"
    recent   = df.tail(20)
    last     = df.iloc[-1]
    prev     = df.iloc[-2]
    swing_lo = recent["low"].min()
    swing_hi = recent["high"].max()

    # Bull sweep: wick turun ke swing low lalu close di atas
    if prev["low"] <= swing_lo * 1.002 and last["close"] > last["open"]:
        return "bull_sweep"
    # Bear sweep: wick naik ke swing high lalu close di bawah
    if prev["high"] >= swing_hi * 0.998 and last["close"] < last["open"]:
        return "bear_sweep"
    return "none"

# ════════════════════════════════════════════════════
#  LAYER 3 — Order Book Imbalance
# ════════════════════════════════════════════════════
def orderbook_imbalance(symbol):
    """
    Bandingkan total volume bid vs ask top 50 level.
    Return: float -1.0 s/d +1.0 (positif = buy pressure)
    """
    try:
        ob      = client.futures_order_book(symbol=symbol, limit=50)
        bids    = sum(float(b[1]) for b in ob["bids"])
        asks    = sum(float(a[1]) for a in ob["asks"])
        total   = bids + asks
        return round((bids - asks) / total, 3) if total else 0.0
    except: return 0.0

# ════════════════════════════════════════════════════
#  LAYER 4 — Cumulative Delta (aggressor buy vs sell)
# ════════════════════════════════════════════════════
def cumulative_delta(df, lookback=20):
    """
    Delta per candle = taker_buy_volume - taker_sell_volume
    Cumulative delta naik = net buyer aggression
    Return: ('bull'|'bear'|'neutral', cum_delta_slope)
    """
    if len(df) < lookback: return "neutral", 0
    recent = df.tail(lookback).copy()
    recent["sell_vol"] = recent["volume"] - recent["tbbase"]
    recent["delta"]    = recent["tbbase"] - recent["sell_vol"]
    recent["cum_delta"]= recent["delta"].cumsum()
    slope = recent["cum_delta"].iloc[-1] - recent["cum_delta"].iloc[0]
    # Normalize by avg volume
    avg_vol = recent["volume"].mean()
    norm_slope = slope / avg_vol if avg_vol > 0 else 0
    if norm_slope > 0.1:   return "bull", norm_slope
    elif norm_slope < -0.1: return "bear", norm_slope
    return "neutral", norm_slope

# ════════════════════════════════════════════════════
#  LAYER 5 — Whale / Volume Spike Detection
# ════════════════════════════════════════════════════
def whale_detection(df):
    last    = df.iloc[-1]
    vol_ma  = df["vol_ma"].iloc[-1]
    if pd.isna(vol_ma) or vol_ma == 0: return "none", 1.0
    ratio = last["volume"] / vol_ma
    if ratio >= 4.0:
        direction = "buy_whale" if last["close"] > last["open"] else "sell_whale"
    elif ratio >= 2.5:
        direction = "mild_buy" if last["close"] > last["open"] else "mild_sell"
    else:
        direction = "none"
    return direction, ratio

def get_large_trades(symbol):
    """Cek apakah ada recent trades sangat besar (> 5x avg)."""
    try:
        trades  = client.futures_recent_trades(symbol=symbol, limit=50)
        qtys    = [float(t["qty"]) for t in trades]
        avg_qty = sum(qtys) / len(qtys) if qtys else 1
        large   = [t for t in trades if float(t["qty"]) > avg_qty * 5]
        if not large: return "none"
        buy_large  = sum(1 for t in large if not t["isBuyerMaker"])
        sell_large = sum(1 for t in large if t["isBuyerMaker"])
        if buy_large > sell_large:  return "large_buy"
        if sell_large > buy_large:  return "large_sell"
        return "mixed"
    except: return "none"

# ════════════════════════════════════════════════════
#  LAYER 6 — Futures Sentiment
# ════════════════════════════════════════════════════
def get_ls_ratio(symbol):
    try:
        url  = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={symbol}&period=5m&limit=3"
        data = requests.get(url, timeout=5).json()
        r0   = float(data[0]["longShortRatio"])
        r1   = float(data[1]["longShortRatio"])
        return r0, ("more_longs" if r0 > r1 else "more_shorts")
    except: return 1.0, "neutral"

def get_funding(symbol):
    try:
        data = client.futures_funding_rate(symbol=symbol, limit=1)
        return round(float(data[0]["fundingRate"]) * 100, 4)
    except: return 0.0

def get_oi_change(symbol):
    try:
        url  = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m&limit=3"
        data = requests.get(url, timeout=5).json()
        if len(data) < 2: return "stable"
        oi0, oi1 = float(data[0]["sumOpenInterest"]), float(data[1]["sumOpenInterest"])
        if oi0 > oi1 * 1.015: return "rising_fast"
        if oi0 > oi1 * 1.005: return "rising"
        if oi0 < oi1 * 0.985: return "falling_fast"
        if oi0 < oi1 * 0.995: return "falling"
        return "stable"
    except: return "stable"

# ════════════════════════════════════════════════════
#  LAYER 7 — Macro Sentiment (cache-based)
# ════════════════════════════════════════════════════
_macro = {"fng": 50, "fng_label": "Neutral",
          "usdt_d": 5.0, "usdt_prev": 5.0,
          "news": "neutral", "headlines": [],
          "last_fng": 0, "last_dom": 0, "last_news": 0}

def refresh_macro():
    now = time.time()
    # Fear & Greed tiap 5 menit
    if now - _macro["last_fng"] > 300:
        try:
            d = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()["data"][0]
            _macro["fng"] = int(d["value"])
            _macro["fng_label"] = d["value_classification"]
            _macro["last_fng"] = now
        except: pass
    # USDT Dominance tiap 5 menit
    if now - _macro["last_dom"] > 300:
        try:
            d = requests.get("https://api.coingecko.com/api/v3/global", timeout=8).json()
            _macro["usdt_prev"] = _macro["usdt_d"]
            _macro["usdt_d"]    = round(d["data"]["market_cap_percentage"].get("usdt", 5), 2)
            _macro["last_dom"]  = now
        except: pass
    # News tiap 60 detik
    if now - _macro["last_news"] > 60:
        try:
            url  = "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC"
            data = requests.get(url, timeout=5).json()
            neg_kw = ["crash","hack","ban","bear","fear","lawsuit","fraud","dump",
                      "warning","collapse","scam","decline","plunge","seized","fud"]
            pos_kw = ["bullish","rally","surge","adoption","institutional","ath",
                      "breakout","pump","approved","launched","partnership","record"]
            neg = pos = 0
            hl = []
            for post in data.get("results", [])[:10]:
                t = post.get("title","")
                tl = t.lower()
                if any(w in tl for w in neg_kw): neg += 1; hl.append(f"🔴 {t[:55]}")
                elif any(w in tl for w in pos_kw): pos += 1; hl.append(f"🟢 {t[:55]}")
            _macro["news"]       = "negative" if neg >= 2 else ("positive" if pos >= 2 else "neutral")
            _macro["headlines"]  = hl[:3]
            _macro["last_news"]  = now
        except: pass

# ════════════════════════════════════════════════════
#  LAYER 8 — Technical Analysis
# ════════════════════════════════════════════════════
def technical_analysis(df):
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
    df["bb_hi"]     = bb.bollinger_hband()
    df["bb_lo"]     = bb.bollinger_lband()
    df["bb_mid"]    = bb.bollinger_mavg()
    stoch           = ta.momentum.StochasticOscillator(h, l, c, window=14, smooth_window=3)
    df["stk"]       = stoch.stoch()
    df["std"]       = stoch.stoch_signal()
    df["atr"]       = ta.volatility.AverageTrueRange(h, l, c, window=14).average_true_range()
    df["vol_ma"]    = v.rolling(20).mean()
    df["buy_ratio"] = df["tbbase"] / df["volume"].replace(0, 1)
    return df

def ta_signal(df):
    """
    Return: ('LONG'|'SHORT'|'NONE', confidence 0-100)
    Confidence dihitung dari berapa banyak indikator yang setuju.
    """
    last = df.iloc[-1]
    prev = df.iloc[-2]
    long_votes = short_votes = 0

    # RSI
    rsi = last["rsi"]
    if rsi < 30:       long_votes  += 2
    elif rsi < 42:     long_votes  += 1
    if rsi > 70:       short_votes += 2
    elif rsi > 58:     short_votes += 1

    # MACD fresh crossover = 2 votes, masih bullish = 1
    if last["macd"] > last["macd_sig"] and prev["macd"] <= prev["macd_sig"]:
        long_votes += 2
    elif last["macd"] > last["macd_sig"]:
        long_votes += 1
    if last["macd"] < last["macd_sig"] and prev["macd"] >= prev["macd_sig"]:
        short_votes += 2
    elif last["macd"] < last["macd_sig"]:
        short_votes += 1

    # EMA stack
    price = last["close"]
    if last["ema20"] > last["ema50"] and price > last["ema20"]: long_votes  += 2
    elif last["ema20"] > last["ema50"]:                          long_votes  += 1
    if last["ema20"] < last["ema50"] and price < last["ema20"]: short_votes += 2
    elif last["ema20"] < last["ema50"]:                          short_votes += 1

    # EMA200
    if not pd.isna(last["ema200"]):
        if price > last["ema200"]: long_votes  += 1
        else:                      short_votes += 1

    # Bollinger
    if price <= last["bb_lo"]:           long_votes  += 2
    elif price < last["bb_mid"]:         long_votes  += 1
    if price >= last["bb_hi"]:           short_votes += 2
    elif price > last["bb_mid"]:         short_votes += 1

    # Stochastic
    k, d = last["stk"], last["std"]
    if k < 20 and k > d:   long_votes  += 2
    elif k < 35:            long_votes  += 1
    if k > 80 and k < d:   short_votes += 2
    elif k > 65:            short_votes += 1

    # Taker buy ratio
    br = last["buy_ratio"]
    if br > 0.62:   long_votes  += 1
    elif br < 0.38: short_votes += 1

    total = long_votes + short_votes
    if total == 0: return "NONE", 0

    if long_votes > short_votes:
        conf = int((long_votes / total) * 100)
        return ("LONG", conf) if conf >= 60 else ("NONE", conf)
    elif short_votes > long_votes:
        conf = int((short_votes / total) * 100)
        return ("SHORT", conf) if conf >= 60 else ("NONE", conf)
    return "NONE", 50

# ════════════════════════════════════════════════════
#  CONFLUENCE ENGINE
# ════════════════════════════════════════════════════
def evaluate_symbol(symbol, df):
    """
    Jalankan semua 8 layer. Return: (side, confluence_count, details_dict)
    side = 'LONG' | 'SHORT' | None
    Harus minimal MIN_CONFLUENCE layer setuju + tidak ada layer yang
    secara keras bertentangan.
    """
    details = {}

    # L1: Market Regime
    regime, bias = market_regime(symbol)
    details["regime"] = regime

    # L2: SMC
    fvg_type, fvg_level = find_fvg(df)
    sweep               = liquidity_sweep(df)
    details["fvg"]      = fvg_type
    details["sweep"]    = sweep

    # L3: Order Book
    ob_imb = orderbook_imbalance(symbol)
    details["ob_imb"] = ob_imb

    # L4: Cumulative Delta
    cd_dir, cd_slope = cumulative_delta(df)
    details["cum_delta"] = cd_dir

    # L5: Whale
    whale_dir, whale_ratio = whale_detection(df)
    large_trade            = get_large_trades(symbol)
    details["whale"]       = whale_dir
    details["large_trade"] = large_trade

    # L6: Futures Sentiment
    ls_ratio, ls_trend = get_ls_ratio(symbol)
    funding            = get_funding(symbol)
    oi                 = get_oi_change(symbol)
    details["ls"]      = f"{ls_ratio:.2f}({ls_trend})"
    details["funding"] = funding
    details["oi"]      = oi

    # L7: Macro
    fng    = _macro["fng"]
    usdt_d = _macro["usdt_d"]
    usdt_trend = "up" if usdt_d > _macro["usdt_prev"] else ("down" if usdt_d < _macro["usdt_prev"] else "flat")
    news   = _macro["news"]
    details["fng"]    = fng
    details["news"]   = news

    # L8: Technical
    ta_dir, ta_conf = ta_signal(df)
    details["ta"]    = f"{ta_dir}({ta_conf}%)"

    # ── Score LONG ────────────────────────────────────────────
    long_layers  = 0
    short_layers = 0
    hard_block_long  = False
    hard_block_short = False

    # L1
    if bias == 1:  long_layers  += 1
    elif bias == -1: short_layers += 1
    # RANGE = tidak block tapi juga tidak bantu

    # L2 FVG
    if fvg_type == "bull_fvg": long_layers  += 1
    elif fvg_type == "bear_fvg": short_layers += 1
    # L2 Sweep
    if sweep == "bull_sweep":  long_layers  += 1
    elif sweep == "bear_sweep": short_layers += 1

    # L3 Order Book
    if ob_imb > 0.15:   long_layers  += 1
    elif ob_imb < -0.15: short_layers += 1

    # L4 Cumulative Delta
    if cd_dir == "bull":  long_layers  += 1
    elif cd_dir == "bear": short_layers += 1

    # L5 Whale
    if whale_dir in ("buy_whale","mild_buy"):    long_layers  += 1
    elif whale_dir in ("sell_whale","mild_sell"): short_layers += 1
    if large_trade == "large_buy":  long_layers  += 0.5
    elif large_trade == "large_sell": short_layers += 0.5

    # L6 Futures
    if ls_ratio > 1.5 and ls_trend == "more_longs": long_layers  += 0.5
    elif ls_ratio < 0.7 and ls_trend == "more_shorts": short_layers += 0.5
    if funding > 0.08:   short_layers += 1; hard_block_long  = True  # terlalu banyak long
    elif funding < -0.08: long_layers  += 1; hard_block_short = True
    if oi in ("rising_fast",):
        price = df["close"].iloc[-1]
        if df["close"].iloc[-1] > df["open"].iloc[-1]: long_layers  += 1
        else:                                           short_layers += 1

    # L7 Macro
    if fng <= 20:   long_layers  += 1       # Extreme Fear = bottom signal
    elif fng <= 35: long_layers  += 0.5
    elif fng >= 80: short_layers += 1       # Extreme Greed = top signal
    elif fng >= 65: short_layers += 0.5
    if usdt_trend == "up":    hard_block_long = True   # USDT.D naik = risk-off
    elif usdt_trend == "down": long_layers += 0.5
    if news == "negative":   hard_block_long = True; short_layers += 1
    elif news == "positive":  long_layers += 0.5

    # L8 Technical (bobot tinggi = 2 layers)
    if ta_dir == "LONG":
        long_layers  += 2
    elif ta_dir == "SHORT":
        short_layers += 2

    # ── Decision ──────────────────────────────────────────────
    details["long_layers"]  = long_layers
    details["short_layers"] = short_layers

    # Harus ada selisih jelas (tidak ambigu)
    gap = abs(long_layers - short_layers)
    if gap < 2: return None, 0, details

    if long_layers >= MIN_CONFLUENCE and long_layers > short_layers and not hard_block_long:
        return "LONG", long_layers, details
    if short_layers >= MIN_CONFLUENCE and short_layers > long_layers and not hard_block_short:
        return "SHORT", short_layers, details

    return None, 0, details

# ════════════════════════════════════════════════════
#  POSITION MANAGEMENT
# ════════════════════════════════════════════════════
def get_exchange_amt(symbol):
    try:
        for p in client.futures_position_information(symbol=symbol):
            amt = float(p["positionAmt"])
            if amt != 0: return amt
        return 0
    except: return 0

def open_trade(symbol, side, confluence):
    try:
        set_leverage(symbol)
        price = get_price(symbol)
        qty   = calc_qty(symbol, price)
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_BUY if side=="LONG" else SIDE_SELL,
            type=ORDER_TYPE_MARKET, quantity=qty)
        entry = get_price(symbol)
        tp    = entry*(1+TP_PCT) if side=="LONG" else entry*(1-TP_PCT)
        sl    = entry*(1-SL_PCT) if side=="LONG" else entry*(1+SL_PCT)
        open_positions[symbol] = {
            "side":side,"entry":entry,"qty":qty,
            "peak":entry,
            "trail_sl": entry*(1-TRAIL_PCT) if side=="LONG" else entry*(1+TRAIL_PCT)
        }
        print(f"  ✅ [{symbol}] {side} @{entry:.4f} qty={qty} | TP:{tp:.4f} SL:{sl:.4f} | confluence={confluence:.1f}")
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal entry: {e}")

def close_trade(symbol, reason=""):
    try:
        amt = get_exchange_amt(symbol)
        if amt == 0: open_positions.pop(symbol,None); return
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt>0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET, quantity=abs(amt), reduceOnly=True)
        if symbol in open_positions:
            pos   = open_positions[symbol]
            exit_ = get_price(symbol)
            pnl   = (exit_-pos["entry"])*pos["qty"] if pos["side"]=="LONG" else (pos["entry"]-exit_)*pos["qty"]
            pct   = pnl/(pos["entry"]*pos["qty"])*100
            emoji = "🟢" if pnl>=0 else "🔴"
            print(f"  💰 [{symbol}] CLOSED — {reason}")
            print(f"     {emoji} P&L: {pnl:+.4f} USDT ({pct:+.2f}%)")
            trade_log.append({"symbol":symbol,"side":pos["side"],"pnl":round(pnl,4)})
        open_positions.pop(symbol,None)
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal close: {e}")

def manage_position(symbol):
    if symbol not in open_positions: return False
    pos   = open_positions[symbol]
    price = get_price(symbol)
    if price == 0: return False
    entry = pos["entry"]
    side  = pos["side"]

    if side == "LONG":
        if price > pos["peak"]:
            pos["peak"]     = price
            pos["trail_sl"] = price*(1-TRAIL_PCT)
        if price >= entry*(1+TP_PCT):
            close_trade(symbol, f"✨ TP +{TP_PCT*100}%"); return True
        if price <= pos["trail_sl"] and price < pos["peak"]*0.999:
            close_trade(symbol, f"🔄 Trailing Stop"); return True
        if price <= entry*(1-SL_PCT):
            close_trade(symbol, f"🛑 SL -{SL_PCT*100}%"); return True
    else:
        if price < pos["peak"]:
            pos["peak"]     = price
            pos["trail_sl"] = price*(1+TRAIL_PCT)
        if price <= entry*(1-TP_PCT):
            close_trade(symbol, f"✨ TP +{TP_PCT*100}%"); return True
        if price >= pos["trail_sl"] and price > pos["peak"]*1.001:
            close_trade(symbol, f"🔄 Trailing Stop"); return True
        if price >= entry*(1+SL_PCT):
            close_trade(symbol, f"🛑 SL -{SL_PCT*100}%"); return True
    return False

# ════════════════════════════════════════════════════
#  SUMMARY
# ════════════════════════════════════════════════════
def print_summary():
    if not trade_log: return
    total = sum(t["pnl"] for t in trade_log)
    wins  = sum(1 for t in trade_log if t["pnl"]>0)
    n     = len(trade_log)
    wr    = wins/n*100
    print(f"\n  📊 SUMMARY {n} trades | WR:{wr:.1f}% W:{wins} L:{n-wins} | P&L:{total:+.4f} USDT")

# ════════════════════════════════════════════════════
#  VALIDATE SYMBOLS (hapus yang tidak ada di exchange)
# ════════════════════════════════════════════════════
def validate_symbols():
    try:
        info    = client.futures_exchange_info()
        valid   = {s["symbol"] for s in info["symbols"] if s["status"]=="TRADING"}
        cleaned = [s for s in TOP_50_SYMBOLS if s in valid]
        print(f"  ✅ {len(cleaned)}/{len(TOP_50_SYMBOLS)} symbols valid di Futures")
        return cleaned
    except:
        return TOP_50_SYMBOLS

# ════════════════════════════════════════════════════
#  MAIN LOOP
# ════════════════════════════════════════════════════
def run_bot():
    print("🤖 Bot v6 — Multi-Layer Confluence System")
    print(f"   Leverage   : {LEVERAGE}x | Order: ${ORDER_USDT} USDT margin")
    print(f"   TP/SL      : +{TP_PCT*100}% / -{SL_PCT*100}% (RR 1:2)")
    print(f"   Trailing   : {TRAIL_PCT*100}% dari peak")
    print(f"   Min Layers : {MIN_CONFLUENCE}/8 harus konfirmasi")
    print(f"   Max Pos    : {MAX_POSITIONS}")
    print(f"   Layers     : Regime|FVG|Sweep|OB|CumDelta|Whale|Funding|FnG|News|TA\n")

    print("  ⏳ Validating symbols...")
    symbols = validate_symbols()
    print("  ⏳ Loading symbol info...")
    for s in symbols: get_sym_info(s)
    print("  ✅ Ready!\n")

    cycle = 0
    while True:
        cycle += 1
        refresh_macro()

        print(f"\n{'='*64}")
        print(f"  🔄 Siklus #{cycle} — {time.strftime('%H:%M:%S')}")
        print(f"{'='*64}")
        print(f"  😱 F&G:{_macro['fng']}({_macro['fng_label']}) | USDT.D:{_macro['usdt_d']}% | News:{_macro['news']}")
        for h in _macro["headlines"]: print(f"     {h}")
        print(f"  📂 Posisi ({len(open_positions)}): {list(open_positions.keys()) or '-'}")

        # Manage existing positions first
        for sym in list(open_positions.keys()):
            closed = manage_position(sym)
            if not closed and _macro["news"] == "negative":
                close_trade(sym, "📰 Emergency exit — bad news")

        # Scan for new entries
        candidates = []
        if len(open_positions) < MAX_POSITIONS and _macro["news"] != "negative":
            for symbol in symbols:
                if symbol in open_positions: continue
                df = get_ohlcv(symbol)
                if df is None or len(df) < 60: continue
                df   = technical_analysis(df)
                side, confluence, details = evaluate_symbol(symbol, df)
                if side:
                    candidates.append((confluence, symbol, side, details))

        candidates.sort(key=lambda x: x[0], reverse=True)

        if candidates:
            print(f"\n  🎯 Top Kandidat (confluence layers):")
            for conf, sym, side, det in candidates[:5]:
                print(f"     {sym:14} {side:5} layers={conf:.1f} | TA:{det['ta']} OB:{det['ob_imb']:+.2f} Δ:{det['cum_delta']} 🐋:{det['whale']}")
        else:
            print(f"\n  ⏳ Tidak ada setup valid saat ini")

        # Enter top candidates
        for conf, symbol, side, _ in candidates:
            if len(open_positions) >= MAX_POSITIONS: break
            if symbol not in open_positions:
                open_trade(symbol, side, conf)

        print_summary()
        print(f"\n  ⏱️  60 detik...")
        time.sleep(60)

if __name__ == "__main__":
    run_bot()
