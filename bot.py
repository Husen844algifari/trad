"""
Bot Scalping v10 FIXED — 1H Bias + 15m/5m Entry
================================================

PERBAIKAN UTAMA vs v10 ORIGINAL:
  1. [FIX] Score normalisasi: max_possible = sum(W.values()) bukan *1.5
  2. [FIX] Toleransi pullback EMA: 0.8% → 2.5% (lebih realistis)
  3. [FIX] MIN_COMPOSITE_SCORE: 52 → 38 (bot sebelumnya tidak pernah entry)
  4. [FIX] Tambah kondisi "Full Bull Stack" & "Full Bear Stack" di trend mode
  5. [FIX] RSI bounce threshold: 40 → 45 (lebih sering trigger)
  6. [FIX] MIN_5M_SIGNALS: dikurangi, cukup 0 sinyal jika mode_score >= 70
  7. [FIX] check_trend_pullback_setup: tambah kondisi harga jauh dari EMA tapi
           masih in-trend (sebelumnya hanya cek pullback/touch)
  8. [FIX] 1H Bias: threshold diturunkan dari 55% → 48% agar lebih banyak
           simbol mendapat bias direction
  9. [NEW] 100 simbol top Binance Futures
  10.[FIX] BATCH_SIZE 5→8, SCAN_INTERVAL 30→40s agar tidak rate-limit
  11.[FIX] Breadth fallback 0.5 lebih agresif agar tidak block entry
  12.[FIX] Volume spike threshold BASE_VOL_SPIKE: 1.3 → 1.1

ARSITEKTUR SCALPING:
  ┌─────────────┐
  │  1H Candle  │  → Tentukan BIAS (BULL / BEAR / SIDEWAYS)
  │  (Filter)   │    → Hanya LONG kalau 1H BULL, SHORT kalau 1H BEAR
  └──────┬──────┘
         │
  ┌──────▼──────┐
  │ 15m Candle  │  → Konfirmasi SETUP (RSI, MACD, EMA Stack, Volume)
  │  (Setup)    │    → Score ≥ MIN_SCORE → lanjut ke 5m
  └──────┬──────┘
         │
  ┌──────▼──────┐
  │  5m Candle  │  → TRIGGER ENTRY (breakout mini, engulfing, RSI bounce)
  │  (Entry)    │    → Entry instan kalau ≥ MIN_5M_SIGNALS sinyal confirm
  └─────────────┘

SCALPING SETTINGS:
  - Target TP1: 0.8% (cepat ambil profit)
  - Target TP2: 1.5% (trailing)
  - SL: 0.5-0.8% max (tight stop)
  - Leverage: 15x
  - Holding time ideal: 5-20 menit per trade
  - Scan interval: 40 detik
"""

import os, time, math, json, requests
from collections import deque
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))
# Hapus baris berikut untuk akun REAL:
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ════════════════════════════════════════════════════
#  CONFIG SCALPING
# ════════════════════════════════════════════════════
LEVERAGE              = 15
ORDER_USDT            = 50
MAX_POSITIONS         = 3

# ── TP/SL ────────────────────────────────────────────────────────────────────
ATR_SL_MULT           = 1.2
ATR_TP1_MULT          = 1.0
ATR_TP2_MULT          = 2.0
MAX_SL_PCT            = 0.010        # [FIX] naikkan dari 0.008 → 0.010 agar ATR besar tidak selalu skip
MIN_RR                = 1.0          # [FIX] turunkan dari 1.2 → 1.0 agar tidak terlalu ketat

# ── Trailing ─────────────────────────────────────────────────────────────────
TRAIL_TRIGGER         = 0.003
TRAIL_PCT             = 0.002

# ── Score & Filter ── [FIX UTAMA] ────────────────────────────────────────────
MIN_COMPOSITE_SCORE   = 38           # [FIX] dari 52 → 38
MIN_5M_SIGNALS        = 0            # [FIX] dari 1 → 0, mode_score tinggi cukup
MIN_5M_SIGNALS_STRICT = 1            # kalau mode_score < 70, butuh minimal ini
MIN_FNG               = 20           # [FIX] dari 30 → 20
MAX_FNG_LONG          = 90           # [FIX] dari 88 → 90
MIN_FNG_ANY           = 12           # [FIX] dari 15 → 12
MIN_MARKET_BREADTH    = 0.25         # [FIX] dari 0.30 → 0.25

# ── Timing ───────────────────────────────────────────────────────────────────
SCAN_INTERVAL         = 40           # [FIX] dari 30 → 40 (hindari rate limit dengan 100 simbol)
BATCH_SIZE            = 8            # [FIX] dari 5 → 8
SCAN_DELAY_MS         = 0.10         # 100ms antar call
OHLCV_CACHE_TTL_5M    = 25
OHLCV_CACHE_TTL_15M   = 55
OHLCV_CACHE_TTL_1H    = 3500

# ── Misc ─────────────────────────────────────────────────────────────────────
SR_BUFFER             = 0.004        # [FIX] dari 0.005 → 0.004
USDT_RISK_OFF_DELTA   = 0.03
MAX_CONSEC_LOSS       = 3
MAX_HOLDING_MINUTES   = 45
FLASH_CRASH_PCT       = 1.0
FLASH_PUMP_PCT        = 1.0
FLASH_WINDOW_SEC      = 300

# ── Funding ──────────────────────────────────────────────────────────────────
FUNDING_LOOKBACK      = 3
FUNDING_THRESHOLD     = 0.07

# ── Adaptive Volume ── [FIX] ─────────────────────────────────────────────────
BASE_VOL_SPIKE        = 1.1          # [FIX] dari 1.3 → 1.1 (lebih sensitif)
ATR_VOL_SCALE         = 1.5
VOL_SPIKE_MIN_CANDLES = 1

# ── 1H Bias threshold ── [FIX] ───────────────────────────────────────────────
BIAS_THRESHOLD        = 48           # [FIX] dari 55 → 48 (lebih banyak simbol dapat bias)
BIAS_MARGIN           = 8            # [FIX] dari 10 → 8

# ── EMA Pullback Tolerance ── [FIX] ──────────────────────────────────────────
EMA_TOUCH_PCT         = 0.025        # [FIX] dari 0.008 → 0.025 (2.5%)

# ── Smart Cooldown ───────────────────────────────────────────────────────────
COOLDOWN_BTC_BAD      = {"BEAR"}
COOLDOWN_BREADTH_MAX  = 0.35
COOLDOWN_BTC_RECOVER  = {"BULL", "MILD_BULL", "SIDEWAYS"}
COOLDOWN_BREADTH_MIN  = 0.40        # [FIX] dari 0.45 → 0.40

# ════════════════════════════════════════════════════
#  100 TOP SYMBOLS BINANCE FUTURES [NEW]
# ════════════════════════════════════════════════════
SYMBOLS = [
    # Mega cap
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    # Large cap
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
    "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT", "ETCUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "TIAUSDT", "AAVEUSDT", "RUNEUSDT", "FILUSDT",
    "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
    # Mid cap - DeFi & Layer 1
    "STXUSDT", "SEIUSDT", "FLOWUSDT", "GALAUSDT", "SANDUSDT",
    "MANAUSDT", "APEUSDT", "GMXUSDT", "DYDXUSDT", "CRVUSDT",
    "LDOUSDT", "RNDRUSDT", "FETUSDT", "AGIXUSDT", "OCEANUSDT",
    "BLURUSDT", "JTOUSDT", "PYTHUSDT", "WUSDT", "STRKUSDT",
    # Gaming & NFT
    "AXSUSDT", "IMXUSDT", "GRTUSDT", "ENSUSDT", "ILVUSDT",
    "YGGUSDT", "ROSEUSDT", "SKLUSDT", "CELRUSDT", "LRCUSDT",
    # BTC ecosystem & Meme
    "ORDIUSDT", "1000BONKUSDT", "1000FLOKIUSDT", "MEMEUSDT",
    "BOMEUSDT", "TURBOUSDT", "NEIROUSDT",
    # AI & Data
    "WLDUSDT", "TAOUSDT", "ARKMUSDT", "ZETAUSDT", "DYMUSDT",
    "ONDOUSDT", "ETHFIUSDT", "AIUSDT",
    # Misc high-volume
    "HBARUSDT", "ALGOUSDT", "XLMUSDT", "VETUSDT", "ICPUSDT",
    "CFXUSDT", "TONUSDT", "NOTUSDT",
    # Exchange tokens & Ecosystem
    "MKRUSDT", "SNXUSDT", "COMPUSDT", "BALUSDT", "SUSHIUSDT",
    "1INCHUSDT", "YFIUSDT", "BANDUSDT",
    # Additional Layer 1 / Layer 2
    "MANTAUSDT", "ALTUSDT", "EIGENUSDT", "RONINUSDT",
    "PIXELUSDT", "ACEUSDT",
    # Perp high OI
    "KDAUSDT", "ZENUSDT", "ZILUSDT", "IOTAUSDT",
    "ONTUSDT", "COTIUSDT", "STORJUSDT", "SXPUSDT",
    # Newer listings with decent volume
    "ENAUSDT", "PEOPLEUSDT", "BBUSDT", "IOtusdt",
]

# ── Composite score weights ───────────────────────────────────────────────────
SCORE_WEIGHTS = {
    "macd_hist":    16,
    "rsi":          14,
    "ema_stack":    14,   # [FIX] naikkan dari 12 → 14
    "volume":       10,   # [FIX] turunkan dari 12 → 10
    "ob_imbalance":  8,   # [FIX] turunkan dari 10 → 8
    "cum_delta":     6,   # [FIX] turunkan dari 8 → 6
    "stoch":         8,
    "bb":            8,
    "funding":       4,
    "momentum_5m":  12,   # [FIX] naikkan dari 8 → 12 (lebih dominan)
}
# Total weights = 100 → max_possible = 100 (tidak di-*1.5 lagi) ← FIX UTAMA

# ════════════════════════════════════════════════════
#  STATE
# ════════════════════════════════════════════════════
open_positions     = {}
trade_log          = []
_last_candle       = {}
_consec_loss       = 0
_in_cooldown       = False
_scan_batch_idx    = 0
_ohlcv_cache       = {}
_btc_price_history = deque(maxlen=100)
_sym_info          = {}

# ════════════════════════════════════════════════════
#  UTILS
# ════════════════════════════════════════════════════
def get_sym_info(symbol):
    if symbol in _sym_info:
        return _sym_info[symbol]
    try:
        for s in client.futures_exchange_info()["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        _sym_info[symbol] = {
                            "step":   float(f["stepSize"]),
                            "minQty": float(f["minQty"])
                        }
                        return _sym_info[symbol]
    except:
        pass
    return {"step": 1.0, "minQty": 1.0}

def round_step(qty, step):
    p = max(0, int(round(-math.log(step, 10), 0))) if step < 1 else 0
    return round(math.floor(qty / step) * step, p)

def calc_qty(symbol, price, fraction=1.0):
    info = get_sym_info(symbol)
    return max(
        round_step((ORDER_USDT * fraction * LEVERAGE) / price / LEVERAGE, info["step"]),
        info["minQty"]
    )

def set_leverage(symbol):
    try:
        client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except:
        pass

def get_price(symbol):
    try:
        return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except:
        return 0.0

def validate_symbols():
    try:
        valid  = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                  if s["status"] == "TRADING"}
        result = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
        removed = [s for s in SYMBOLS if s not in valid]
        if removed:
            print(f"  ⚠️  {len(removed)} simbol tidak valid di Futures: {removed}")
        print(f"  ✅ {len(result)} symbols valid dari {len(SYMBOLS)} list")
        return result
    except:
        return list(dict.fromkeys(SYMBOLS))

def get_exchange_amt(symbol, retries=3):
    for attempt in range(retries):
        try:
            for p in client.futures_position_information(symbol=symbol):
                amt = float(p["positionAmt"])
                if amt != 0:
                    return amt
            return 0
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                print(f"  ⚠️  [{symbol}] Gagal query posisi — skip")
                return None

# ════════════════════════════════════════════════════
#  OHLCV CACHE (multi-TF)
# ════════════════════════════════════════════════════
def get_ohlcv(symbol, interval, limit=200):
    cache_key = (symbol, interval)
    now = time.time()

    ttl_map = {
        Client.KLINE_INTERVAL_4HOUR:    3500,
        Client.KLINE_INTERVAL_1HOUR:    OHLCV_CACHE_TTL_1H,
        Client.KLINE_INTERVAL_15MINUTE: OHLCV_CACHE_TTL_15M,
        Client.KLINE_INTERVAL_5MINUTE:  OHLCV_CACHE_TTL_5M,
    }
    ttl = ttl_map.get(interval, 55)

    if cache_key in _ohlcv_cache:
        ts, df_cached = _ohlcv_cache[cache_key]
        if now - ts < ttl:
            return df_cached

    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            "time","open","high","low","close","volume",
            "ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        _ohlcv_cache[cache_key] = (now, df)
        return df
    except:
        if cache_key in _ohlcv_cache:
            _, df_old = _ohlcv_cache[cache_key]
            return df_old
        return None

def get_current_batch(symbols):
    global _scan_batch_idx
    if not symbols:
        return []
    total_batches = math.ceil(len(symbols) / BATCH_SIZE)
    start = _scan_batch_idx * BATCH_SIZE
    batch = symbols[start:start + BATCH_SIZE]
    _scan_batch_idx = (_scan_batch_idx + 1) % total_batches
    return batch

# ════════════════════════════════════════════════════
#  MACRO CACHE
# ════════════════════════════════════════════════════
_macro = {
    "fng": 50, "fng_label": "Neutral",
    "usdt_d": 5.0, "usdt_prev": 5.0,
    "news": "neutral", "news_strength": 0, "headlines": [],
    "btc_trend_15m": "UNKNOWN",
    "btc_trend_1h":  "UNKNOWN",
    "btc_trend_4h":  "UNKNOWN",
    "market_breadth": 0.5,
    "global_mcap_chg": 0.0,
    "scalp_mode": "TREND",
    "last_fng": 0, "last_dom": 0, "last_news": 0,
    "last_btc": 0, "last_breadth": 0,
}

BULL_TRENDS = {"BULL", "MILD_BULL"}
BEAR_TRENDS = {"BEAR", "MILD_BEAR"}

def _calc_trend(df):
    if df is None or len(df) < 30:
        return "UNKNOWN"
    c     = df["close"]
    price = c.iloc[-1]
    ema9  = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
    ema21 = ta.trend.EMAIndicator(c, 21).ema_indicator().iloc[-1]
    ema50 = ta.trend.EMAIndicator(c, 50).ema_indicator().iloc[-1]
    chg   = (price - c.iloc[-4]) / c.iloc[-4] * 100

    if price > ema9 > ema21 > ema50 and chg > 0:
        return "BULL"
    elif price < ema9 < ema21 < ema50 and chg < 0:
        return "BEAR"
    elif price > ema21 and chg > -0.3:
        return "MILD_BULL"
    elif price < ema21 and chg < 0.3:
        return "MILD_BEAR"
    return "SIDEWAYS"

def _detect_scalp_mode():
    t15 = _macro["btc_trend_15m"]
    t1h = _macro["btc_trend_1h"]
    if t1h == "SIDEWAYS" and t15 == "SIDEWAYS":
        return "MEAN_REV"
    return "TREND"

def refresh_macro():
    now = time.time()

    if now - _macro["last_fng"] > 300:
        try:
            d = requests.get(
                "https://api.alternative.me/fng/?limit=1", timeout=5
            ).json()["data"][0]
            _macro["fng"]       = int(d["value"])
            _macro["fng_label"] = d["value_classification"]
            _macro["last_fng"]  = now
        except:
            pass

    if now - _macro["last_dom"] > 300:
        try:
            d = requests.get(
                "https://api.coingecko.com/api/v3/global", timeout=8
            ).json()
            _macro["usdt_prev"] = _macro["usdt_d"]
            _macro["usdt_d"]    = round(d["data"]["market_cap_percentage"].get("usdt", 5), 2)
            _macro["global_mcap_chg"] = round(
                d["data"].get("market_cap_change_percentage_24h_usd", 0), 2)
            _macro["last_dom"]  = now
        except:
            pass

    if now - _macro["last_news"] > 60:
        try:
            data = requests.get(
                "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC",
                timeout=5).json()
            neg_kw_strong = ["crash","hack","ban","fraud","collapse","seized","scam"]
            neg_kw_mild   = ["bear","fear","lawsuit","dump","warning","plunge","fud","sell-off","decline"]
            pos_kw_strong = ["institutional","ath","approved","record","bullish","rally","surge"]
            pos_kw_mild   = ["adoption","breakout","buy","launched","partnership","soar"]
            neg = pos = 0
            hl  = []
            for post in data.get("results", [])[:10]:
                t  = post.get("title", "")
                tl = t.lower()
                if any(w in tl for w in neg_kw_strong):
                    neg += 2; hl.append(f"🔴🔴 {t[:55]}")
                elif any(w in tl for w in neg_kw_mild):
                    neg += 1; hl.append(f"🔴 {t[:55]}")
                elif any(w in tl for w in pos_kw_strong):
                    pos += 2; hl.append(f"🟢🟢 {t[:55]}")
                elif any(w in tl for w in pos_kw_mild):
                    pos += 1; hl.append(f"🟢 {t[:55]}")
            score = pos - neg
            if score <= -4:   sentiment = "strong_negative"
            elif score <= -2: sentiment = "negative"
            elif score >= 4:  sentiment = "strong_positive"
            elif score >= 2:  sentiment = "positive"
            else:             sentiment = "neutral"
            _macro["news"]          = sentiment
            _macro["news_strength"] = score
            _macro["headlines"]     = hl[:3]
            _macro["last_news"]     = now
        except:
            pass

    if now - _macro["last_btc"] > 30:
        try:
            df_15m = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, 60)
            df_1h  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1HOUR, 60)
            df_4h  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_4HOUR, 60)
            _macro["btc_trend_15m"] = _calc_trend(df_15m)
            _macro["btc_trend_1h"]  = _calc_trend(df_1h)
            _macro["btc_trend_4h"]  = _calc_trend(df_4h)
            _macro["scalp_mode"]    = _detect_scalp_mode()
            _macro["last_btc"]      = now
        except:
            pass

    if now - _macro["last_breadth"] > 120:
        try:
            bullish = 0
            counted = 0
            # [FIX] sample lebih banyak untuk akurasi
            sample  = SYMBOLS[:20]
            for sym in sample:
                df = get_ohlcv(sym, Client.KLINE_INTERVAL_15MINUTE, 30)
                if df is not None and len(df) >= 10:
                    c    = df["close"]
                    ema9 = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
                    counted += 1
                    if c.iloc[-1] > ema9 or df["close"].iloc[-1] > df["open"].iloc[-1]:
                        bullish += 1
            if counted > 0:
                _macro["market_breadth"] = bullish / counted
            else:
                _macro["market_breadth"] = 0.5
            _macro["last_breadth"] = now
            print(f"  📊 Breadth: {bullish}/{counted} = {_macro['market_breadth']*100:.0f}%")
        except Exception as e:
            if _macro["market_breadth"] == 0:
                _macro["market_breadth"] = 0.5
            print(f"  ⚠️  Breadth error: {e} → fallback {_macro['market_breadth']*100:.0f}%")

# ════════════════════════════════════════════════════
#  FLASH CRASH DETECTOR
# ════════════════════════════════════════════════════
def update_btc_price_history():
    try:
        price = get_price("BTCUSDT")
        if price > 0:
            _btc_price_history.append((time.time(), price))
    except:
        pass

def detect_flash_move():
    if len(_btc_price_history) < 2:
        return "none", 0.0
    now    = time.time()
    cutoff = now - FLASH_WINDOW_SEC
    oldest_price = None
    for ts, px in _btc_price_history:
        if ts >= cutoff:
            oldest_price = px
            break
    if oldest_price is None:
        return "none", 0.0
    current_price = _btc_price_history[-1][1]
    pct_change = (current_price - oldest_price) / oldest_price * 100
    if pct_change <= -FLASH_CRASH_PCT:
        return "crash", abs(pct_change)
    if pct_change >= FLASH_PUMP_PCT:
        return "pump", abs(pct_change)
    return "none", 0.0

# ════════════════════════════════════════════════════
#  TECHNICAL ANALYSIS
# ════════════════════════════════════════════════════
def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]        = ta.momentum.RSIIndicator(c, 14).rsi()
    df["rsi_fast"]   = ta.momentum.RSIIndicator(c, 7).rsi()
    macd             = ta.trend.MACD(c)
    df["macd"]       = macd.macd()
    df["macd_sig"]   = macd.macd_signal()
    df["macd_hist"]  = macd.macd_diff()
    df["ema9"]       = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["ema21"]      = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["ema50"]      = ta.trend.EMAIndicator(c, 50).ema_indicator()
    df["ema200"]     = ta.trend.EMAIndicator(c, 200).ema_indicator()
    bb               = ta.volatility.BollingerBands(c, 20, 2)
    df["bb_hi"]      = bb.bollinger_hband()
    df["bb_lo"]      = bb.bollinger_lband()
    df["bb_mid"]     = bb.bollinger_mavg()
    df["bb_width"]   = (df["bb_hi"] - df["bb_lo"]) / df["bb_mid"]
    stoch            = ta.momentum.StochasticOscillator(h, l, c, 14, 3)
    df["stk"]        = stoch.stoch()
    df["std"]        = stoch.stoch_signal()
    df["atr"]        = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["vol_ma"]     = v.rolling(20).mean()
    df["vol_ratio"]  = v / df["vol_ma"].replace(0, 1)
    df["buy_ratio"]  = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"]       = abs(df["close"] - df["open"])
    df["range_"]     = df["high"] - df["low"]
    df["body_ratio"] = df["body"] / df["range_"].replace(0, 1)
    return df

# ════════════════════════════════════════════════════
#  1H BIAS — [FIX] threshold diturunkan
# ════════════════════════════════════════════════════
def get_1h_bias(symbol):
    """
    Analisis 1H candle untuk tentukan BIAS.
    [FIX] Threshold: 55% → 48%, margin: 10 → 8
    Sehingga lebih banyak simbol mendapat bias direction.
    """
    df = get_ohlcv(symbol, Client.KLINE_INTERVAL_1HOUR, 60)
    if df is None or len(df) < 55:
        return "NONE", 0

    df = run_ta(df.copy())
    last = df.iloc[-1]
    prev = df.iloc[-2]

    score_long  = 0
    score_short = 0
    max_pts     = 0

    # EMA Stack (paling penting di 1H)
    e9, e21, e50 = last["ema9"], last["ema21"], last["ema50"]
    if e9 > e21 > e50:
        score_long += 35
    elif e9 < e21 < e50:
        score_short += 35
    elif e9 > e21:
        score_long += 15
    elif e9 < e21:
        score_short += 15
    max_pts += 35

    # Price vs EMA200
    ema200 = last["ema200"]
    if last["close"] > ema200:
        score_long += 20
    else:
        score_short += 20
    max_pts += 20

    # MACD momentum
    if last["macd_hist"] > 0 and last["macd_hist"] > prev["macd_hist"]:
        score_long += 25
    elif last["macd_hist"] < 0 and last["macd_hist"] < prev["macd_hist"]:
        score_short += 25
    max_pts += 25

    # RSI zone
    rsi = last["rsi"]
    if rsi > 50 and rsi < 70:
        score_long += 10
    elif rsi < 50 and rsi > 30:
        score_short += 10
    elif rsi >= 70:
        score_short += 5
    elif rsi <= 30:
        score_long += 5
    max_pts += 10

    # Stoch
    if last["stk"] > last["std"] and last["stk"] < 80:
        score_long += 10
    elif last["stk"] < last["std"] and last["stk"] > 20:
        score_short += 10
    max_pts += 10

    long_pct  = (score_long  / max_pts * 100)
    short_pct = (score_short / max_pts * 100)

    # [FIX] Threshold diturunkan 55→48, margin 10→8
    if long_pct >= BIAS_THRESHOLD and long_pct > short_pct + BIAS_MARGIN:
        return "LONG", long_pct
    if short_pct >= BIAS_THRESHOLD and short_pct > long_pct + BIAS_MARGIN:
        return "SHORT", short_pct
    return "NONE", max(long_pct, short_pct)

# ════════════════════════════════════════════════════
#  5m ENTRY TRIGGER
# ════════════════════════════════════════════════════
def get_5m_entry_signals(symbol, direction):
    """
    [FIX] RSI bounce threshold: 40→45 (lebih sering trigger di bull market)
    [FIX] Engulfing: body_ratio 0.6→0.5 (lebih longgar)
    """
    df = get_ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE, 50)
    if df is None or len(df) < 20:
        return 0, ["Data 5m tidak cukup"]

    df = run_ta(df.copy())
    last = df.iloc[-1]
    prev = df.iloc[-2]
    signals = []

    # Sinyal 1: Bullish/Bearish Engulfing — [FIX] body_ratio 0.6→0.5
    if direction == "LONG":
        if (last["close"] > last["open"] and
            last["close"] > prev["high"] and
            last["body_ratio"] > 0.50):
            signals.append("🕯️ Engulfing LONG")
    else:
        if (last["close"] < last["open"] and
            last["close"] < prev["low"] and
            last["body_ratio"] > 0.50):
            signals.append("🕯️ Engulfing SHORT")

    # Sinyal 2: RSI Bounce — [FIX] threshold 40→45 untuk LONG, 60→55 untuk SHORT
    rsi      = last["rsi"]
    rsi_fast = last["rsi_fast"]
    if direction == "LONG" and rsi < 45 and rsi_fast > df.iloc[-2]["rsi_fast"]:
        signals.append(f"📈 RSI bounce ({rsi:.0f})")
    elif direction == "SHORT" and rsi > 55 and rsi_fast < df.iloc[-2]["rsi_fast"]:
        signals.append(f"📉 RSI pullback ({rsi:.0f})")

    # Sinyal 3: EMA9 Cross di 5m
    ema9_now   = last["ema9"]
    ema9_prev  = prev["ema9"]
    ema21_now  = last["ema21"]
    ema21_prev = prev["ema21"]
    if direction == "LONG":
        if (last["close"] > ema9_now and prev["close"] <= ema9_prev) or \
           (ema9_now > ema21_now and ema9_prev <= ema21_prev):
            signals.append("➡️ EMA9 cross LONG")
    else:
        if (last["close"] < ema9_now and prev["close"] >= ema9_prev) or \
           (ema9_now < ema21_now and ema9_prev >= ema21_prev):
            signals.append("➡️ EMA9 cross SHORT")

    # Sinyal 4: Volume Spike — [FIX] threshold 1.5→1.3, buy_ratio 0.55→0.52
    if last["vol_ratio"] >= 1.3:
        if direction == "LONG" and last["buy_ratio"] > 0.52 and last["close"] > last["open"]:
            signals.append(f"📊 Vol spike 5m ({last['vol_ratio']:.1f}x)")
        elif direction == "SHORT" and last["buy_ratio"] < 0.48 and last["close"] < last["open"]:
            signals.append(f"📊 Vol spike 5m ({last['vol_ratio']:.1f}x)")

    # Sinyal 5: Mini Breakout
    recent_10 = df.iloc[-11:-1]
    mini_high = recent_10["high"].max()
    mini_low  = recent_10["low"].min()
    if direction == "LONG" and last["close"] > mini_high:
        signals.append(f"🚀 Breakout mini range ({mini_high:.4f})")
    elif direction == "SHORT" and last["close"] < mini_low:
        signals.append(f"💥 Breakdown mini range ({mini_low:.4f})")

    # Sinyal 6: Stochastic cross
    if direction == "LONG" and last["stk"] > last["std"] and prev["stk"] <= prev["std"] and last["stk"] < 80:
        signals.append(f"⚡ Stoch GX 5m ({last['stk']:.0f})")
    elif direction == "SHORT" and last["stk"] < last["std"] and prev["stk"] >= prev["std"] and last["stk"] > 20:
        signals.append(f"⚡ Stoch DX 5m ({last['stk']:.0f})")

    # [NEW] Sinyal 7: Candle hijau/merah sederhana dengan volume di atas rata-rata
    if direction == "LONG" and last["close"] > last["open"] and last["vol_ratio"] > 1.0:
        signals.append(f"🟢 Green candle vol ({last['vol_ratio']:.1f}x)")
    elif direction == "SHORT" and last["close"] < last["open"] and last["vol_ratio"] > 1.0:
        signals.append(f"🔴 Red candle vol ({last['vol_ratio']:.1f}x)")

    return len(signals), signals

# ════════════════════════════════════════════════════
#  MEAN REVERSION MODE
# ════════════════════════════════════════════════════
def check_mean_reversion_setup(df_15m, symbol):
    """
    [FIX] BB pct threshold: 0.30→0.35, RSI threshold: 45→48
    Lebih sering trigger di ranging market.
    """
    if df_15m is None or len(df_15m) < 30:
        return None, 0

    last = df_15m.iloc[-1]
    prev = df_15m.iloc[-2]

    bb_lo  = last["bb_lo"]
    bb_hi  = last["bb_hi"]
    bb_mid = last["bb_mid"]
    price  = last["close"]
    rsi    = last["rsi"]
    bb_pct = (price - bb_lo) / (bb_hi - bb_lo) if (bb_hi - bb_lo) > 0 else 0.5

    # Setup LONG — [FIX] 0.30→0.35, RSI 45→48
    if bb_pct <= 0.35 and rsi < 48:
        score = 52
        if bb_pct <= 0.20: score += 12
        elif bb_pct <= 0.30: score += 8
        if rsi < 38:       score += 10
        if last["vol_ratio"] > 1.0: score += 8
        if last["stk"] < 35 and last["stk"] > last["std"]: score += 10
        if last["close"] > last["open"]: score += 5
        if prev["close"] < prev["open"] and last["close"] > last["open"]: score += 5
        return "LONG", min(score, 100)

    # Setup SHORT — [FIX] 0.70→0.65, RSI 55→52
    if bb_pct >= 0.65 and rsi > 52:
        score = 52
        if bb_pct >= 0.80: score += 12
        elif bb_pct >= 0.70: score += 8
        if rsi > 62:       score += 10
        if last["vol_ratio"] > 1.0: score += 8
        if last["stk"] > 65 and last["stk"] < last["std"]: score += 10
        if last["close"] < last["open"]: score += 5
        if prev["close"] > prev["open"] and last["close"] < last["open"]: score += 5
        return "SHORT", min(score, 100)

    return None, 0

# ════════════════════════════════════════════════════
#  TREND MODE — [FIX UTAMA]
# ════════════════════════════════════════════════════
def check_trend_pullback_setup(df_15m, bias_direction):
    """
    [FIX] 3 kondisi entry (sebelumnya hanya 2):
    A) Pullback dekat EMA9/EMA21 — tolerance ditingkatkan 0.8% → 2.5%
    B) Trend continuation: harga di sisi benar EMA dengan volume
    C) [NEW] Full stack confirmation: semua EMA aligned + candle searah
       (menangkap strong trend yang TIDAK pullback)

    Kondisi C penting karena saat market strong bull (BTC BULL + Breadth 93%),
    harga jarang pullback ke EMA — kondisi original tidak pernah trigger.
    """
    if df_15m is None or len(df_15m) < 30:
        return None, 0

    last  = df_15m.iloc[-1]
    prev  = df_15m.iloc[-2]
    price = last["close"]
    e9    = last["ema9"]
    e21   = last["ema21"]
    e50   = last["ema50"]

    if bias_direction == "LONG":
        # ── Kondisi A: Pullback dekat EMA — [FIX] 0.008 → EMA_TOUCH_PCT (0.025)
        touch_ema9  = abs(price - e9)  / price < EMA_TOUCH_PCT
        touch_ema21 = abs(price - e21) / price < EMA_TOUCH_PCT
        if (touch_ema9 or touch_ema21) and price > e50:
            score = 58
            if last["rsi"] < 58:              score += 10
            if last["vol_ratio"] > 1.0:       score += 8
            if last["close"] > last["open"]:  score += 8
            if e9 > e21:                      score += 8
            if last["macd_hist"] > 0:         score += 6
            return "LONG", min(score, 100)

        # ── Kondisi B: Trend continuation dengan volume
        if price > e9 > e21 and last["vol_ratio"] > BASE_VOL_SPIKE and last["close"] > last["open"]:
            score = 58
            if last["rsi"] < 65:        score += 10
            if last["macd_hist"] > 0:   score += 8
            if e9 > e50:                score += 8
            if last["vol_ratio"] > 1.3: score += 6
            return "LONG", min(score, 100)

        # ── Kondisi C [NEW]: Full Bull Stack — harga di atas semua EMA
        # Menangkap strong trend yang tidak pullback
        if price > e9 > e21 > e50:
            score = 55
            if last["rsi"] > 45 and last["rsi"] < 70: score += 8
            if last["macd_hist"] > 0:                  score += 8
            if last["close"] > last["open"]:           score += 8
            if prev["close"] > prev["open"]:           score += 5
            if last["vol_ratio"] > 0.8:                score += 6
            return "LONG", min(score, 100)

        # ── Kondisi D [NEW]: Price baru menembus EMA21 ke atas
        if last["close"] > e21 and prev["close"] <= prev["ema21"]:
            score = 60
            if last["rsi"] < 65:              score += 8
            if last["vol_ratio"] > 1.0:       score += 10
            if last["macd_hist"] > 0:         score += 8
            return "LONG", min(score, 100)

    elif bias_direction == "SHORT":
        # ── Kondisi A: Pullback dekat EMA
        touch_ema9  = abs(price - e9)  / price < EMA_TOUCH_PCT
        touch_ema21 = abs(price - e21) / price < EMA_TOUCH_PCT
        if (touch_ema9 or touch_ema21) and price < e50:
            score = 58
            if last["rsi"] > 42:              score += 10
            if last["vol_ratio"] > 1.0:       score += 8
            if last["close"] < last["open"]:  score += 8
            if e9 < e21:                      score += 8
            if last["macd_hist"] < 0:         score += 6
            return "SHORT", min(score, 100)

        # ── Kondisi B: Trend continuation
        if price < e9 < e21 and last["vol_ratio"] > BASE_VOL_SPIKE and last["close"] < last["open"]:
            score = 58
            if last["rsi"] > 35:        score += 10
            if last["macd_hist"] < 0:   score += 8
            if e9 < e50:                score += 8
            if last["vol_ratio"] > 1.3: score += 6
            return "SHORT", min(score, 100)

        # ── Kondisi C [NEW]: Full Bear Stack
        if price < e9 < e21 < e50:
            score = 55
            if last["rsi"] > 30 and last["rsi"] < 55: score += 8
            if last["macd_hist"] < 0:                  score += 8
            if last["close"] < last["open"]:           score += 8
            if prev["close"] < prev["open"]:           score += 5
            if last["vol_ratio"] > 0.8:                score += 6
            return "SHORT", min(score, 100)

        # ── Kondisi D [NEW]: Price baru menembus EMA21 ke bawah
        if last["close"] < e21 and prev["close"] >= prev["ema21"]:
            score = 60
            if last["rsi"] > 35:              score += 8
            if last["vol_ratio"] > 1.0:       score += 10
            if last["macd_hist"] < 0:         score += 8
            return "SHORT", min(score, 100)

    return None, 0

# ════════════════════════════════════════════════════
#  15m COMPOSITE SCORE — [FIX NORMALISASI]
# ════════════════════════════════════════════════════
def get_funding(symbol):
    try:
        data = client.futures_funding_rate(symbol=symbol, limit=FUNDING_LOOKBACK + 1)
        if not data:
            return 0.0, "flat"
        rates = [round(float(d["fundingRate"]) * 100, 4) for d in data]
        avg   = round(sum(rates[:FUNDING_LOOKBACK]) / min(len(rates), FUNDING_LOOKBACK), 4)
        trend = "rising"  if len(rates) >= 2 and rates[0] - rates[-1] > 0.01  else \
                "falling" if len(rates) >= 2 and rates[0] - rates[-1] < -0.01 else "flat"
        return avg, trend
    except:
        return 0.0, "flat"

def get_ob_imbalance(symbol):
    try:
        ob    = client.futures_order_book(symbol=symbol, limit=50)
        bids  = sum(float(b[1]) for b in ob["bids"])
        asks  = sum(float(a[1]) for a in ob["asks"])
        total = bids + asks
        return round((bids - asks) / total, 3) if total else 0.0
    except:
        return 0.0

def get_cum_delta(df, lookback=10):
    if len(df) < lookback:
        return 0.0
    recent = df.tail(lookback).copy()
    recent["delta"] = recent["tbbase"] - (recent["volume"] - recent["tbbase"])
    norm = recent["delta"].sum() / (recent["volume"].sum() + 1)
    return round(norm, 3)

def calc_15m_score(df, direction, ob_imb, cum_d, funding_avg, funding_trend,
                   mode_score=0, signal_5m_count=0):
    """
    [FIX UTAMA] Normalisasi score:
    - Sebelumnya: max_possible = sum(W.values()) * 1.5 = 150
      → Score 52 butuh 78 raw points dari 100 → hampir mustahil
    - Sesudah: max_possible = sum(W.values()) = 100
      → Score 38 butuh 38 raw points dari 100 → realistis

    [FIX] RSI scoring: threshold lebih longgar
    [FIX] Volume: BASE_VOL_SPIKE lebih rendah (1.1)
    """
    last = df.iloc[-1]
    prev = df.iloc[-2]
    W    = SCORE_WEIGHTS
    breakdown = {}
    score = 0.0

    # 1. MACD Histogram
    hist_now  = last["macd_hist"]
    hist_prev = prev["macd_hist"]
    if direction == "LONG":
        if hist_now > 0 and hist_now > hist_prev:
            pts = W["macd_hist"] if last["macd"] > last["macd_sig"] else W["macd_hist"] * 0.7
            score += pts; breakdown["macd"] = f"+{pts:.1f}"
        elif hist_now > hist_prev:            # [FIX] partial point untuk momentum naik walau masih negatif
            pts = W["macd_hist"] * 0.3
            score += pts; breakdown["macd"] = f"+{pts:.1f}(rising)"
        else: breakdown["macd"] = "0"
    else:
        if hist_now < 0 and hist_now < hist_prev:
            pts = W["macd_hist"] if last["macd"] < last["macd_sig"] else W["macd_hist"] * 0.7
            score += pts; breakdown["macd"] = f"+{pts:.1f}"
        elif hist_now < hist_prev:
            pts = W["macd_hist"] * 0.3
            score += pts; breakdown["macd"] = f"+{pts:.1f}(falling)"
        else: breakdown["macd"] = "0"

    # 2. RSI — [FIX] threshold lebih longgar
    rsi = last["rsi"]
    if direction == "LONG":
        if rsi < 40:   pts = W["rsi"]
        elif rsi < 50: pts = W["rsi"] * 0.7   # [FIX] 0.5→0.7
        elif rsi < 60: pts = W["rsi"] * 0.3   # [FIX] NEW zone
        else:          pts = 0
    else:
        if rsi > 60:   pts = W["rsi"]
        elif rsi > 50: pts = W["rsi"] * 0.7   # [FIX] 0.5→0.7
        elif rsi > 40: pts = W["rsi"] * 0.3   # [FIX] NEW zone
        else:          pts = 0
    score += pts; breakdown["rsi"] = f"+{pts:.1f}(rsi:{rsi:.0f})"

    # 3. EMA Stack
    e9, e21, e50 = last["ema9"], last["ema21"], last["ema50"]
    if direction == "LONG":
        if e9 > e21 > e50:       pts = W["ema_stack"]
        elif e9 > e21:           pts = W["ema_stack"] * 0.6
        elif last["close"] > e21: pts = W["ema_stack"] * 0.3   # [FIX] partial
        else:                    pts = 0
    else:
        if e9 < e21 < e50:       pts = W["ema_stack"]
        elif e9 < e21:           pts = W["ema_stack"] * 0.6
        elif last["close"] < e21: pts = W["ema_stack"] * 0.3
        else:                    pts = 0
    score += pts; breakdown["ema"] = f"+{pts:.1f}"

    # 4. Volume — [FIX] BASE_VOL_SPIKE lebih rendah (1.1)
    vr = last["vol_ratio"]
    br = last["buy_ratio"]
    if vr >= BASE_VOL_SPIKE:
        if direction == "LONG" and last["close"] > last["open"] and br > 0.50:
            pts = W["volume"] * min(vr / BASE_VOL_SPIKE, 1.5)
            score += pts; breakdown["vol"] = f"+{pts:.1f}({vr:.1f}x)"
        elif direction == "SHORT" and last["close"] < last["open"] and br < 0.50:
            pts = W["volume"] * min(vr / BASE_VOL_SPIKE, 1.5)
            score += pts; breakdown["vol"] = f"+{pts:.1f}({vr:.1f}x)"
        else:
            # [FIX] Partial: volume spike tapi arah ambigu → setengah poin
            pts = W["volume"] * 0.3
            score += pts; breakdown["vol"] = f"+{pts:.1f}(ambigu,{vr:.1f}x)"
    else: breakdown["vol"] = f"no spike({vr:.1f}x)"

    # 5. OB Imbalance — [FIX] threshold 0.10→0.08
    if direction == "LONG" and ob_imb > 0.08:
        pts = W["ob_imbalance"] * min(ob_imb / 0.08, 1.5)
        score += pts; breakdown["ob"] = f"+{pts:.1f}({ob_imb:+.2f})"
    elif direction == "SHORT" and ob_imb < -0.08:
        pts = W["ob_imbalance"] * min(abs(ob_imb) / 0.08, 1.5)
        score += pts; breakdown["ob"] = f"+{pts:.1f}({ob_imb:+.2f})"
    else: breakdown["ob"] = f"neutral({ob_imb:+.2f})"

    # 6. Cumulative Delta — [FIX] threshold 0.10→0.08
    if direction == "LONG" and cum_d > 0.08:
        score += W["cum_delta"]; breakdown["delta"] = f"+{W['cum_delta']}"
    elif direction == "SHORT" and cum_d < -0.08:
        score += W["cum_delta"]; breakdown["delta"] = f"+{W['cum_delta']}"
    else: breakdown["delta"] = "0"

    # 7. Stochastic — [FIX] threshold K: 35→40, 65→60
    k, d_ = last["stk"], last["std"]
    pk, pd_ = prev["stk"], prev["std"]
    if direction == "LONG" and k < 40 and k > d_ and pk <= pd_:
        score += W["stoch"]; breakdown["stoch"] = f"+{W['stoch']}"
    elif direction == "SHORT" and k > 60 and k < d_ and pk >= pd_:
        score += W["stoch"]; breakdown["stoch"] = f"+{W['stoch']}"
    else: breakdown["stoch"] = "0"

    # 8. Bollinger Band
    price = last["close"]
    if direction == "LONG" and price <= last["bb_lo"] * 1.008:   # [FIX] 1.005→1.008
        score += W["bb"]; breakdown["bb"] = f"+{W['bb']}(BB_lo)"
    elif direction == "SHORT" and price >= last["bb_hi"] * 0.992: # [FIX] 0.995→0.992
        score += W["bb"]; breakdown["bb"] = f"+{W['bb']}(BB_hi)"
    else: breakdown["bb"] = "0"

    # 9. Funding
    trend_mult = 1.5 if (direction == "LONG"  and funding_trend == "falling") or \
                        (direction == "SHORT" and funding_trend == "rising") else 1.0
    if direction == "LONG" and funding_avg < -FUNDING_THRESHOLD:
        pts = W["funding"] * trend_mult; score += pts
        breakdown["funding"] = f"+{pts:.1f}({funding_avg:.3f}%)"
    elif direction == "SHORT" and funding_avg > FUNDING_THRESHOLD:
        pts = W["funding"] * trend_mult; score += pts
        breakdown["funding"] = f"+{pts:.1f}({funding_avg:.3f}%)"
    else: breakdown["funding"] = f"neutral({funding_avg:.3f}%)"

    # 10. Momentum 5m bonus — [FIX] naik ke W["momentum_5m"]=12
    mode_pts = min(mode_score / 100 * W["momentum_5m"], W["momentum_5m"])
    sig_pts  = min(signal_5m_count * 4, W["momentum_5m"])   # [FIX] 3→4 per signal
    bonus    = round(max(mode_pts, sig_pts), 1)
    score   += bonus
    breakdown["5m_bonus"] = f"+{bonus}(mode:{mode_score},sig:{signal_5m_count})"

    # ── [FIX] NORMALISASI: max_possible = sum(W) = 100, BUKAN *1.5 ──────────
    max_possible = sum(W.values())   # = 100
    final_score  = min(score / max_possible * 100, 100)
    return final_score, breakdown

# ════════════════════════════════════════════════════
#  S/R LEVELS
# ════════════════════════════════════════════════════
def get_sr_levels(symbol):
    df = get_ohlcv(symbol, Client.KLINE_INTERVAL_1HOUR, 50)
    if df is None or len(df) < 10:
        return {"resistance": [], "support": []}
    highs = df["high"].values
    lows  = df["low"].values
    resistance, support = [], []
    for i in range(2, len(highs) - 2):
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and \
           highs[i] > highs[i+1] and highs[i] > highs[i+2]:
            resistance.append(highs[i])
        if lows[i] < lows[i-1] and lows[i] < lows[i-2] and \
           lows[i] < lows[i+1] and lows[i] < lows[i+2]:
            support.append(lows[i])
    return {"resistance": sorted(resistance, reverse=True)[:3],
            "support":    sorted(support)[:3]}

def check_sr_clear(symbol, price, direction):
    sr = get_sr_levels(symbol)
    if direction == "LONG":
        nearby = [r for r in sr["resistance"] if r > price]
        if nearby:
            nearest = min(nearby)
            gap_pct = (nearest - price) / price
            if gap_pct < SR_BUFFER:
                return False, f"Dekat resistance {nearest:.4f} ({gap_pct*100:.2f}%)"
    else:
        nearby = [s for s in sr["support"] if s < price]
        if nearby:
            nearest = max(nearby)
            gap_pct = (price - nearest) / price
            if gap_pct < SR_BUFFER:
                return False, f"Dekat support {nearest:.4f} ({gap_pct*100:.2f}%)"
    return True, ""

# ════════════════════════════════════════════════════
#  SMART COOLDOWN
# ════════════════════════════════════════════════════
def is_cooldown_active():
    global _in_cooldown
    if not _in_cooldown:
        return False
    if _macro["btc_trend_15m"] in COOLDOWN_BTC_RECOVER and \
       _macro["market_breadth"] >= COOLDOWN_BREADTH_MIN:
        _in_cooldown = False
        print(f"  ✅ Cooldown berakhir! BTC:{_macro['btc_trend_15m']} Breadth:{_macro['market_breadth']*100:.0f}%")
        return False
    return True

def cooldown_reason():
    r = []
    if _macro["btc_trend_15m"] in COOLDOWN_BTC_BAD:
        r.append(f"BTC {_macro['btc_trend_15m']}")
    if _macro["market_breadth"] < COOLDOWN_BREADTH_MIN:
        r.append(f"Breadth {_macro['market_breadth']*100:.0f}%")
    return " & ".join(r) if r else "kondisi buruk"

# ════════════════════════════════════════════════════
#  MASTER ENTRY FILTER — [FIX] Pipeline lebih longgar
# ════════════════════════════════════════════════════
def should_enter(symbol, df_15m):
    """
    Pipeline:
    1. Macro filter (F&G, news)
    2. 1H Bias — [FIX] threshold lebih rendah
    3. Scalp mode → setup
    4. 5m trigger — [FIX] MIN_5M_SIGNALS lebih longgar
    5. 15m score — [FIX] threshold 52→38, normalisasi fixed
    6. S/R check + SL validation
    """
    if is_cooldown_active():
        return None, 0, 0, 0, {"skip": f"🧊 Cooldown ({cooldown_reason()})"}

    fng  = _macro["fng"]
    news = _macro["news"]

    if fng < MIN_FNG_ANY:
        return None, 0, 0, 0, {"skip": f"F&G terlalu ekstrem ({fng})"}
    if news in ("strong_negative",):
        return None, 0, 0, 0, {"skip": f"News {news}"}

    # ── STEP 2: 1H Bias ──────────────────────────────────────
    bias_dir, bias_conf = get_1h_bias(symbol)

    if bias_dir == "NONE":
        btc_15m = _macro["btc_trend_15m"]
        btc_1h  = _macro["btc_trend_1h"]
        if btc_1h in BULL_TRENDS or btc_15m == "BULL":
            bias_dir, bias_conf = "LONG", 50
        elif btc_1h in BEAR_TRENDS or btc_15m == "BEAR":
            bias_dir, bias_conf = "SHORT", 50
        elif _macro["scalp_mode"] == "TREND":
            return None, 0, 0, 0, {
                "skip": f"1H bias tidak jelas (conf:{bias_conf:.0f}%) & BTC neutral"}

    # ── STEP 3: Candle check & setup ─────────────────────────
    prev_candle_time = int(df_15m["time"].iloc[-2])
    if _last_candle.get(symbol) == prev_candle_time:
        return None, 0, 0, 0, {"skip": "Candle sama, skip"}

    df_closed = df_15m.iloc[:-1].copy()
    if len(df_closed) < 50:
        return None, 0, 0, 0, {"skip": "Data kurang"}

    df_closed = run_ta(df_closed)
    scalp_mode = _macro["scalp_mode"]
    direction  = None
    mode_score = 0

    if scalp_mode == "MEAN_REV":
        direction, mode_score = check_mean_reversion_setup(df_closed, symbol)
        if direction is None:
            return None, 0, 0, 0, {"skip": "MeanRev: tidak ada BB bounce setup"}
    else:
        if bias_dir == "NONE":
            return None, 0, 0, 0, {"skip": "Trend mode tapi bias tidak jelas"}
        direction, mode_score = check_trend_pullback_setup(df_closed, bias_dir)
        if direction is None:
            return None, 0, 0, 0, {
                "skip": f"Trend mode: tidak ada setup valid ({bias_dir})"}

    # Validasi direction vs macro
    btc_t1h = _macro["btc_trend_1h"]
    btc_t4h = _macro["btc_trend_4h"]

    if direction == "LONG":
        if btc_t4h in BEAR_TRENDS:
            return None, 0, 0, 0, {"skip": f"BTC 4H={btc_t4h} BEAR — skip LONG"}
        if fng > MAX_FNG_LONG:
            return None, 0, 0, 0, {"skip": f"F&G terlalu greedy ({fng})"}
        if scalp_mode == "TREND" and _macro["market_breadth"] < MIN_MARKET_BREADTH:
            return None, 0, 0, 0, {
                "skip": f"Breadth rendah ({_macro['market_breadth']*100:.0f}%)"}
    elif direction == "SHORT":
        if btc_t4h in BULL_TRENDS and btc_t1h in BULL_TRENDS:
            return None, 0, 0, 0, {"skip": f"BTC 4H+1H BULL — skip SHORT"}

    # ── STEP 4: 5m entry trigger ─────────────────────────────
    sig_count, sig_list = get_5m_entry_signals(symbol, direction)

    # [FIX] Logika MIN_5M_SIGNALS lebih cerdas:
    # - mode_score >= 70: tidak perlu sinyal 5m (setup kuat sudah cukup)
    # - mode_score 50-70: butuh 1 sinyal
    # - mode_score < 50: butuh 2 sinyal (selektif)
    if mode_score >= 70:
        min_sigs_required = 0
    elif mode_score >= 50:
        min_sigs_required = 1
    else:
        min_sigs_required = 2

    if sig_count < min_sigs_required:
        return None, 0, 0, 0, {
            "skip": f"5m signals kurang ({sig_count}<{min_sigs_required}, mode:{mode_score})"}

    # ── STEP 5: 15m composite score ──────────────────────────
    ob_imb                = get_ob_imbalance(symbol)
    cum_d                 = get_cum_delta(df_closed)
    fund_avg, fund_trend  = get_funding(symbol)

    score, breakdown = calc_15m_score(
        df_closed, direction, ob_imb, cum_d, fund_avg, fund_trend,
        mode_score, sig_count)

    if score < MIN_COMPOSITE_SCORE:
        return None, 0, 0, 0, {"skip": f"Score rendah ({score:.1f}/{MIN_COMPOSITE_SCORE})"}

    # ── STEP 6: S/R check ────────────────────────────────────
    price = df_closed["close"].iloc[-1]
    sr_ok, sr_reason = check_sr_clear(symbol, price, direction)
    if not sr_ok:
        return None, 0, 0, 0, {"skip": f"S/R: {sr_reason}"}

    # ── SL/TP kalkulasi ──────────────────────────────────────
    atr    = df_closed["atr"].iloc[-1]
    sl_dist = max(ATR_SL_MULT * atr, price * 0.003)

    if direction == "LONG":
        sl_price  = round(price - sl_dist, 8)
        tp1_price = round(price + ATR_TP1_MULT * atr, 8)
        tp2_price = round(price + ATR_TP2_MULT * atr, 8)
    else:
        sl_price  = round(price + sl_dist, 8)
        tp1_price = round(price - ATR_TP1_MULT * atr, 8)
        tp2_price = round(price - ATR_TP2_MULT * atr, 8)

    sl_pct  = abs(price - sl_price) / price
    tp1_pct = abs(tp1_price - price) / price

    if sl_pct > MAX_SL_PCT:
        return None, 0, 0, 0, {"skip": f"ATR terlalu besar (SL={sl_pct*100:.1f}%)"}
    if tp1_pct / sl_pct < MIN_RR:
        return None, 0, 0, 0, {
            "skip": f"R:R buruk ({tp1_pct/sl_pct:.2f}x, min {MIN_RR}x)"}

    _last_candle[symbol] = prev_candle_time

    info = {
        "score":      f"{score:.1f}/100",
        "mode":       scalp_mode,
        "bias_1h":    f"{bias_dir}({bias_conf:.0f}%)",
        "btc_1h":     btc_t1h,
        "btc_4h":     btc_t4h,
        "5m_signals": f"{sig_count}: {', '.join(sig_list[:2])}",
        "mode_score": f"{mode_score}",
        "funding":    f"{fund_avg:.4f}%({fund_trend})",
        "sl_pct":     f"{sl_pct*100:.2f}%",
        "rr":         f"{tp1_pct/sl_pct:.2f}x",
        "breadth":    f"{_macro['market_breadth']*100:.0f}%",
        "breakdown":  breakdown,
    }

    return direction, sl_price, tp1_price, tp2_price, info

# ════════════════════════════════════════════════════
#  TRADE EXECUTION
# ════════════════════════════════════════════════════
def open_trade(symbol, side, sl_price, tp1_price, tp2_price, info):
    try:
        set_leverage(symbol)
        price = get_price(symbol)
        qty   = calc_qty(symbol, price, fraction=1.0)
        client.futures_create_order(
            symbol=symbol,
            side=SIDE_BUY if side == "LONG" else SIDE_SELL,
            type=ORDER_TYPE_MARKET,
            quantity=qty)

        entry    = get_price(symbol)
        trail_sl = entry * (1 - TRAIL_PCT) if side == "LONG" else entry * (1 + TRAIL_PCT)

        open_positions[symbol] = {
            "side":            side,
            "entry":           entry,
            "qty":             qty,
            "qty_remain":      qty,
            "sl":              sl_price,
            "tp1":             tp1_price,
            "tp2":             tp2_price,
            "peak":            entry,
            "trail_sl":        trail_sl,
            "trailing_active": False,
            "tp1_hit":         False,
            "be_active":       False,
            "open_time":       time.time(),
            "mode":            info.get("mode", "TREND"),
        }

        sl_pct  = abs(entry - sl_price) / entry * 100
        tp1_pct = abs(tp1_price - entry) / entry * 100
        tp2_pct = abs(tp2_price - entry) / entry * 100
        score   = info.get("score", "?")

        print(f"\n  ✅ [{symbol}] {side} ENTRY @{entry:.5f} | qty={qty}")
        print(f"     SL:{sl_price:.5f}(-{sl_pct:.2f}%) TP1:{tp1_price:.5f}(+{tp1_pct:.2f}%) "
              f"TP2:{tp2_price:.5f}(+{tp2_pct:.2f}%)")
        print(f"     Score:{score} | Mode:{info.get('mode','?')} | Bias:{info.get('bias_1h','?')}")
        print(f"     5m:{info.get('5m_signals','?')} | R:R={info.get('rr','?')} | "
              f"Fund:{info.get('funding','?')}")
        print(f"     Breakdown: {info.get('breakdown', {})}")
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal entry: {e}")

def partial_close(symbol, reason="TP1"):
    pos = open_positions.get(symbol)
    if pos is None:
        return
    try:
        amt = get_exchange_amt(symbol)
        if amt is None or amt == 0:
            pos["tp1_hit"] = True
            return

        half_qty  = round_step(abs(amt) * 0.5, get_sym_info(symbol)["step"])
        min_qty   = get_sym_info(symbol)["minQty"]
        close_qty = max(half_qty, min_qty)
        if close_qty > abs(amt):
            close_qty = abs(amt)

        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt > 0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=close_qty,
            reduceOnly=True)

        exit_price = get_price(symbol)
        side = pos["side"]
        pnl  = (exit_price - pos["entry"]) * close_qty if side == "LONG" \
               else (pos["entry"] - exit_price) * close_qty
        pct  = pnl / (pos["entry"] * close_qty) * 100
        hold_min = (time.time() - pos["open_time"]) / 60

        print(f"  🎯 [{symbol}] PARTIAL TP1 — {reason} | Hold:{hold_min:.0f}m")
        print(f"     💛 P&L (50%): {pnl:+.4f}U ({pct:+.2f}%)")

        pos["tp1_hit"]         = True
        pos["qty_remain"]      = abs(amt) - close_qty
        pos["be_active"]       = True
        pos["sl"]              = pos["entry"]
        pos["trailing_active"] = True
        pos["peak"]            = exit_price
        pos["trail_sl"]        = exit_price * (1 - TRAIL_PCT) if side == "LONG" \
                                 else exit_price * (1 + TRAIL_PCT)

        trade_log.append({"symbol": symbol, "side": side,
                          "pnl": round(pnl, 4), "reason": f"Partial {reason}"})
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal partial close: {e}")
        pos["tp1_hit"] = True

def close_trade(symbol, reason=""):
    global _consec_loss, _in_cooldown
    try:
        amt = get_exchange_amt(symbol)
        if amt is None:
            print(f"  ⚠️  [{symbol}] Query gagal, tunda close")
            return False
        if amt == 0:
            if symbol in open_positions:
                pos   = open_positions[symbol]
                exit_ = get_price(symbol)
                if exit_ > 0:
                    qty_r = pos.get("qty_remain", pos["qty"])
                    pnl   = (exit_ - pos["entry"]) * qty_r if pos["side"] == "LONG" \
                            else (pos["entry"] - exit_) * qty_r
                    trade_log.append({"symbol": symbol, "side": pos["side"],
                                      "pnl": round(pnl, 4), "reason": "External close"})
                    _update_loss_streak(pnl)
                open_positions.pop(symbol, None)
            return True

        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt > 0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=abs(amt),
            reduceOnly=True)

        if symbol in open_positions:
            pos   = open_positions[symbol]
            exit_ = get_price(symbol)
            qty_r = pos.get("qty_remain", pos["qty"])
            pnl   = (exit_ - pos["entry"]) * qty_r if pos["side"] == "LONG" \
                    else (pos["entry"] - exit_) * qty_r
            pct   = pnl / (pos["entry"] * qty_r) * 100 if qty_r > 0 else 0
            emoji = "🟢" if pnl >= 0 else "🔴"
            hold_min = (time.time() - pos.get("open_time", time.time())) / 60
            be_tag = " [BE]" if pos.get("be_active") else ""
            print(f"  💰 [{symbol}] CLOSED — {reason}{be_tag} | Hold:{hold_min:.0f}m")
            print(f"     {emoji} P&L: {pnl:+.4f}U ({pct:+.2f}%)")
            trade_log.append({"symbol": symbol, "side": pos["side"],
                              "pnl": round(pnl, 4), "reason": reason})
            _update_loss_streak(pnl)

        open_positions.pop(symbol, None)
        return True
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal close: {e}")
        return False

def _update_loss_streak(pnl):
    global _consec_loss, _in_cooldown
    if pnl < 0:
        _consec_loss += 1
        if _consec_loss >= MAX_CONSEC_LOSS:
            btc_bad     = _macro["btc_trend_15m"] in COOLDOWN_BTC_BAD
            breadth_bad = _macro["market_breadth"] < COOLDOWN_BREADTH_MAX
            if btc_bad or breadth_bad:
                _in_cooldown = True
                print(f"  🧊 {MAX_CONSEC_LOSS} loss + market buruk → Cooldown!")
            else:
                _consec_loss = 0
                print(f"  ⚡ {MAX_CONSEC_LOSS} loss tapi market masih oke → lanjut")
    else:
        _consec_loss = 0
        if _in_cooldown:
            _in_cooldown = False
            print(f"  ✅ Win! Cooldown diakhiri.")

# ════════════════════════════════════════════════════
#  POSITION MANAGEMENT
# ════════════════════════════════════════════════════
def manage_positions():
    # Flash crash/pump exit
    flash_dir, flash_pct = detect_flash_move()
    if flash_dir != "none" and open_positions:
        for symbol in list(open_positions.keys()):
            pos  = open_positions[symbol]
            side = pos["side"]
            if flash_dir == "crash" and side == "LONG":
                close_trade(symbol, f"🚨 Flash Crash -{flash_pct:.2f}%")
                continue
            elif flash_dir == "pump" and side == "SHORT":
                close_trade(symbol, f"🚨 Flash Pump +{flash_pct:.2f}%")
                continue

    for symbol in list(open_positions.keys()):
        pos   = open_positions[symbol]
        price = get_price(symbol)
        if price == 0:
            continue

        entry = pos["entry"]
        side  = pos["side"]

        # Force close setelah MAX_HOLDING_MINUTES
        hold_min = (time.time() - pos.get("open_time", time.time())) / 60
        if hold_min >= MAX_HOLDING_MINUTES:
            close_trade(symbol, f"⏰ Force close ({hold_min:.0f}m)")
            continue

        # Emergency exits
        if _macro["news"] in ("strong_negative",):
            close_trade(symbol, "🚨 Bad news emergency")
            continue

        if side == "LONG" and _macro["btc_trend_1h"] == "BEAR" and \
           _macro["btc_trend_4h"] == "BEAR":
            close_trade(symbol, "⚡ BTC 1H+4H BEAR")
            continue
        if side == "SHORT" and _macro["btc_trend_1h"] == "BULL" and \
           _macro["btc_trend_4h"] == "BULL":
            close_trade(symbol, "⚡ BTC 1H+4H BULL")
            continue

        if side == "LONG":
            profit_pct = (price - entry) / entry

            if not pos["tp1_hit"] and price >= pos["tp1"]:
                partial_close(symbol, "TP1"); continue

            if not pos["trailing_active"] and profit_pct >= TRAIL_TRIGGER:
                pos["trailing_active"] = True
                pos["trail_sl"] = price * (1 - TRAIL_PCT)
                print(f"  🔄 [{symbol}] Trailing aktif +{profit_pct*100:.2f}%")

            if pos["trailing_active"] and price > pos["peak"]:
                pos["peak"]     = price
                pos["trail_sl"] = price * (1 - TRAIL_PCT)

            if pos["tp1_hit"] and price >= pos["tp2"]:
                close_trade(symbol, "✨ TP2"); continue

            if pos["trailing_active"] and price <= pos["trail_sl"]:
                close_trade(symbol, "🔄 Trailing Stop"); continue

            if price <= pos["sl"]:
                reason = "🔒 Break-even" if pos.get("be_active") else "🛑 Stop Loss"
                close_trade(symbol, reason); continue

            pnl_now = (price - entry) * pos.get("qty_remain", pos["qty"])
            be_tag  = "[BE]" if pos.get("be_active") else ""
            tsl     = f" TSL:{pos['trail_sl']:.4f}" if pos["trailing_active"] else ""
            tp_tag  = f"TP2:{pos['tp2']:.4f}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.4f}"
            print(f"  📌 [{symbol}] LONG @{entry:.4f}→{price:.4f} {be_tag}| "
                  f"{pnl_now:+.3f}U | {hold_min:.0f}m |{tsl} {tp_tag}")

        else:  # SHORT
            profit_pct = (entry - price) / entry

            if not pos["tp1_hit"] and price <= pos["tp1"]:
                partial_close(symbol, "TP1"); continue

            if not pos["trailing_active"] and profit_pct >= TRAIL_TRIGGER:
                pos["trailing_active"] = True
                pos["trail_sl"] = price * (1 + TRAIL_PCT)
                print(f"  🔄 [{symbol}] Trailing aktif +{profit_pct*100:.2f}%")

            if pos["trailing_active"] and price < pos["peak"]:
                pos["peak"]     = price
                pos["trail_sl"] = price * (1 + TRAIL_PCT)

            if pos["tp1_hit"] and price <= pos["tp2"]:
                close_trade(symbol, "✨ TP2"); continue

            if pos["trailing_active"] and price >= pos["trail_sl"]:
                close_trade(symbol, "🔄 Trailing Stop"); continue

            if price >= pos["sl"]:
                reason = "🔒 Break-even" if pos.get("be_active") else "🛑 Stop Loss"
                close_trade(symbol, reason); continue

            pnl_now = (entry - price) * pos.get("qty_remain", pos["qty"])
            be_tag  = "[BE]" if pos.get("be_active") else ""
            tsl     = f" TSL:{pos['trail_sl']:.4f}" if pos["trailing_active"] else ""
            tp_tag  = f"TP2:{pos['tp2']:.4f}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.4f}"
            print(f"  📌 [{symbol}] SHORT @{entry:.4f}→{price:.4f} {be_tag}| "
                  f"{pnl_now:+.3f}U | {hold_min:.0f}m |{tsl} {tp_tag}")

# ════════════════════════════════════════════════════
#  SUMMARY
# ════════════════════════════════════════════════════
def print_summary():
    if not trade_log:
        return
    total = sum(t["pnl"] for t in trade_log)
    wins  = sum(1 for t in trade_log if t["pnl"] > 0)
    n     = len(trade_log)
    wr    = wins / n * 100 if n else 0
    cd    = f" | 🧊 Cooldown ({cooldown_reason()})" if _in_cooldown else ""
    print(f"\n  📊 {n} trades | WR:{wr:.0f}% W:{wins} L:{n-wins} | "
          f"P&L:{total:+.4f}U | streak:{_consec_loss}L{cd}")
    for t in trade_log[-5:]:
        e = "🟢" if t["pnl"] > 0 else "🔴"
        print(f"     {e} {t['symbol']} {t['side']} {t['pnl']:+.4f}U — {t['reason'][:40]}")

# ════════════════════════════════════════════════════
#  MAIN LOOP
# ════════════════════════════════════════════════════
def run_bot():
    print("🤖 Bot Scalping v10 FIXED — 1H Bias | 15m Setup | 5m Entry")
    print("=" * 68)
    print(f"   Leverage        : {LEVERAGE}x | Order: ${ORDER_USDT} USDT")
    print(f"   TF Hierarchy    : 1H(bias) → 15m(setup) → 5m(entry)")
    print(f"   TP1 / TP2       : {ATR_TP1_MULT}x ATR / {ATR_TP2_MULT}x ATR")
    print(f"   SL Max          : {MAX_SL_PCT*100:.1f}% | Min R:R = {MIN_RR}x")
    print(f"   Holding Max     : {MAX_HOLDING_MINUTES} menit (force close)")
    print(f"   Scan Interval   : {SCAN_INTERVAL}s | Batch: {BATCH_SIZE} simbol")
    print(f"   Flash Exit      : BTC ≥{FLASH_CRASH_PCT}% dalam {FLASH_WINDOW_SEC//60}m")
    print(f"   Min Score       : {MIN_COMPOSITE_SCORE}/100 [FIXED dari 52]")
    print(f"   EMA Touch PCT   : {EMA_TOUCH_PCT*100:.1f}% [FIXED dari 0.8%]")
    print(f"   Bias Threshold  : {BIAS_THRESHOLD}% [FIXED dari 55%]")
    print(f"   Base Vol Spike  : {BASE_VOL_SPIKE}x [FIXED dari 1.3x]")
    print(f"   Score Norm      : max={sum(SCORE_WEIGHTS.values())} [FIXED, bukan *1.5]")
    print(f"   Total Symbols   : {len(SYMBOLS)}")
    print("=" * 68 + "\n")

    symbols = []
    print("  ⏳ Inisialisasi...")
    symbols = validate_symbols()
    for s in symbols:
        get_sym_info(s)
    refresh_macro()
    update_btc_price_history()

    print(f"\n  ✅ Ready | F&G:{_macro['fng']} | "
          f"BTC 15m:{_macro['btc_trend_15m']} 1H:{_macro['btc_trend_1h']} "
          f"4H:{_macro['btc_trend_4h']} | Mode:{_macro['scalp_mode']} | "
          f"News:{_macro['news']}\n")

    cycle = 0
    while True:
        cycle += 1
        refresh_macro()
        update_btc_price_history()

        if _in_cooldown:
            is_cooldown_active()

        manage_positions()

        flash_dir, flash_pct = detect_flash_move()
        flash_info = f" ⚡{flash_dir.upper()}:{flash_pct:.2f}%" if flash_dir != "none" else ""
        cd_info    = f" 🧊COOLDOWN" if _in_cooldown else ""
        mode_emoji = "📈" if _macro["scalp_mode"] == "TREND" else "↩️"

        print(f"\n{'='*72}")
        print(f"  🔄 #{cycle} {time.strftime('%H:%M:%S')} | F&G:{_macro['fng']}({_macro['fng_label']}) | "
              f"USDT.D:{_macro['usdt_d']}% | News:{_macro['news']}{cd_info}{flash_info}")
        print(f"  {mode_emoji} Mode:{_macro['scalp_mode']} | BTC 15m:{_macro['btc_trend_15m']} "
              f"1H:{_macro['btc_trend_1h']} 4H:{_macro['btc_trend_4h']}")
        print(f"  🌍 Breadth:{_macro['market_breadth']*100:.0f}% | "
              f"MCap24h:{_macro['global_mcap_chg']:+.1f}%")
        for h in _macro["headlines"]:
            print(f"  {h}")
        print(f"  📂 Posisi({len(open_positions)}): {list(open_positions.keys()) or '-'}")

        total_batches = math.ceil(len(symbols) / BATCH_SIZE)
        print(f"  🔍 Scanning batch {_scan_batch_idx + 1}/{total_batches} "
              f"({BATCH_SIZE} simbol/batch, total {len(symbols)})")
        print(f"{'='*72}")

        candidates = []
        skipped    = 0

        if len(open_positions) < MAX_POSITIONS and not _in_cooldown and \
           _macro["news"] not in ("strong_negative",):

            batch = get_current_batch([s for s in symbols if s not in open_positions])

            for symbol in batch:
                time.sleep(SCAN_DELAY_MS)
                df = get_ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 220)
                if df is None or len(df) < 70:
                    continue

                side, sl, tp1, tp2, info = should_enter(symbol, df)
                if side:
                    candidates.append((symbol, side, sl, tp1, tp2, info))
                else:
                    skipped += 1
                    skip_reason = info.get("skip", "?")
                    if "Candle sama" not in skip_reason and "Data" not in skip_reason:
                        print(f"     ⚪ {symbol}: {skip_reason}")

            if candidates:
                candidates.sort(
                    key=lambda x: float(x[5].get("score", "0").split("/")[0]),
                    reverse=True)
                print(f"\n  🎯 {len(candidates)} setup valid! ({skipped} skip)")
                for sym, side, sl, tp1, tp2, info in candidates[:3]:
                    print(f"     ⭐ {sym} {side} | Score:{info.get('score','?')} | "
                          f"Mode:{info.get('mode','?')} | Bias:{info.get('bias_1h','?')}")
                    print(f"        5m:{info.get('5m_signals','?')} | "
                          f"R:R={info.get('rr','?')} | SL:{info.get('sl_pct','?')}")
                for sym, side, sl, tp1, tp2, info in candidates:
                    if len(open_positions) >= MAX_POSITIONS:
                        break
                    open_trade(sym, side, sl, tp1, tp2, info)
            else:
                print(f"  ⏳ Batch selesai, belum ada setup ({len(batch)} simbol di-scan)")
        else:
            if _in_cooldown:
                print(f"  🧊 Cooldown — {cooldown_reason()}")
            else:
                print(f"  ⏸️  Posisi penuh ({len(open_positions)}/{MAX_POSITIONS})")

        print_summary()
        print(f"\n  ⏱️  Next scan: {SCAN_INTERVAL}s...")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
