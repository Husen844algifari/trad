"""
Bot Trading v9 — Fixed Edition
================================
Perbaikan dari v9 original:

FIX 1 — RATE LIMIT (28 simbol × 60 detik):
  - Scan dibagi batch kecil (BATCH_SIZE simbol per cycle)
  - Round-robin: tiap cycle scan batch berikutnya, bukan semua sekaligus
  - OHLCV di-cache selama 1 candle period (14 detik buffer)
    → kalau candle belum tutup, pakai data cached, hemat API call
  - Delay kecil antar simbol (SCAN_DELAY_MS) untuk hindari burst

FIX 2 — VOLUME SPIKE NOISE (threshold flat 1.5x tidak cukup):
  - Threshold volume spike kini adaptif: base 1.5x dinaikkan
    proporsional ke ATR% simbol (simbol volatile butuh spike lebih besar)
  - Formula: min_spike = BASE_VOL_SPIKE + (atr_pct / ATR_VOL_SCALE)
  - Contoh: BTCUSDT ATR 0.5% → min_spike 1.5x; DOGEUSDT ATR 3% → min_spike ~2.5x
  - Tambahan: volume spike harus konsisten di ≥2 dari 3 candle terakhir
    (bukan cukup 1 candle saja) untuk filter noise lebih ketat

FIX 3 — FUNDING RATE (1 data, threshold terlalu kecil):
  - Ambil 8 data terakhir (1 sesi penuh = 8 jam × 1 data/jam di beberapa exchange)
    → gunakan rata-rata 3 funding rate terakhir untuk smooth outlier
  - Threshold dinaikkan: 0.05% → 0.08% (lebih kebal noise)
  - Tambah "funding trend": kalau funding makin naik (positif semakin besar),
    ini sinyal crowding LONG yang lebih kuat → penalti lebih besar

FIX 4 — BTC EMERGENCY EXIT (lambat karena tunggu 1H+4H):
  - Tambah Flash Crash Detector: cek pergerakan BTC % dalam window pendek
    tanpa perlu tunggu candle 1H/4H closed
  - FLASH_CRASH_PCT: kalau BTC turun ≥ X% dalam 5 menit terakhir → emergency exit LONG
  - FLASH_PUMP_PCT: kalau BTC naik ≥ X% dalam 5 menit → emergency exit SHORT
  - Flash detector jalan di manage_positions() setiap iterasi (independen dari macro refresh)
  - 1H+4H filter tetap ada untuk exit normal, flash detector hanya untuk kondisi ekstrem
"""

import os, time, math, json, requests
from collections import deque
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
import ta
import pandas as pd

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))
# Hapus baris berikut untuk akun REAL:
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════
LEVERAGE              = 10
ORDER_USDT            = 55
ATR_SL_MULT           = 2.0
ATR_TP1_MULT          = 2.0
ATR_TP2_MULT          = 4.0
TRAIL_TRIGGER         = 0.005
TRAIL_PCT             = 0.003
MIN_COMPOSITE_SCORE   = 62
MIN_FNG               = 45
MAX_FNG_LONG          = 85
MIN_FNG_ANY           = 20
MAX_POSITIONS         = 2
SCAN_INTERVAL         = 60
MAX_CONSEC_LOSS       = 2
MIN_MARKET_BREADTH    = 0.45
SR_BUFFER             = 0.008
USDT_RISK_OFF_DELTA   = 0.03

# ── FIX 1: Batch scan & OHLCV cache ──────────────────────────────────────────
BATCH_SIZE            = 7        # scan N simbol per cycle (28 simbol → 4 cycle = ~4 menit untuk full scan)
SCAN_DELAY_MS         = 0.12     # detik jeda antar symbol API call (120ms = ~8 call/detik, aman di bawah limit)
OHLCV_CACHE_TTL       = 55       # detik: cache OHLCV 15m selama 55 detik (1 candle = 60 detik)
_ohlcv_cache          = {}       # {(symbol, interval): (timestamp_fetch, df)}
_ohlcv_errors         = {}       # {(symbol, interval): error_msg} — dikumpul, ditampilkan di header
_scan_batch_idx       = 0        # index batch saat ini (round-robin)

# ── FIX 2: Adaptive volume spike ─────────────────────────────────────────────
BASE_VOL_SPIKE        = 1.5      # minimum base (sama dengan v9 original)
ATR_VOL_SCALE         = 1.2      # tiap 1% ATR → +0.83 threshold spike
                                 # contoh: ATR 2% → threshold = 1.5 + (2/1.2) = 3.17x
VOL_SPIKE_MIN_CANDLES = 2        # wajib ada spike di ≥2 dari 3 candle terakhir

# ── FIX 3: Funding rate average ──────────────────────────────────────────────
FUNDING_LOOKBACK      = 3        # rata-rata N funding rate terakhir
FUNDING_THRESHOLD     = 0.08     # threshold naik: 0.05% → 0.08%
FUNDING_TREND_WEIGHT  = 1.5      # multiplier kalau funding trend memburuk (makin crowded)

# ── FIX 4: Flash crash / pump detector ───────────────────────────────────────
FLASH_CRASH_PCT       = 1.2      # % drop BTC dalam 5 menit → emergency exit LONG
FLASH_PUMP_PCT        = 1.2      # % pump BTC dalam 5 menit → emergency exit SHORT
FLASH_WINDOW_SEC      = 300      # window 5 menit
_btc_price_history    = deque(maxlen=100)  # (timestamp, price) untuk flash detector

# Smart Cooldown
COOLDOWN_BTC_BAD      = {"BEAR", "MILD_BEAR"}
COOLDOWN_BREADTH_MAX  = 0.40
COOLDOWN_BTC_RECOVER  = {"BULL", "MILD_BULL"}
COOLDOWN_BREADTH_MIN  = 0.50

# Composite score weights (total = 100)
SCORE_WEIGHTS = {
    "macd_hist":    18,
    "rsi":          15,
    "ema_stack":    14,
    "volume":       13,
    "ob_imbalance": 12,
    "cum_delta":    10,
    "stoch":         8,
    "bb":            6,
    "funding":       4,
}

SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","DOTUSDT",
    "MATICUSDT","LTCUSDT","ATOMUSDT","UNIUSDT","ETCUSDT",
    "NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","INJUSDT",
    "SUIUSDT","TIAUSDT","AAVEUSDT","RUNEUSDT","FILUSDT",
    "1000PEPEUSDT","WIFUSDT","JUPUSDT",
]

open_positions  = {}
trade_log       = []
_last_candle    = {}
_consec_loss    = 0
_in_cooldown    = False

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
                        _sym_info[symbol] = {
                            "step": float(f["stepSize"]),
                            "minQty": float(f["minQty"])
                        }
                        return _sym_info[symbol]
    except: pass
    return {"step": 1.0, "minQty": 1.0}

def round_step(qty, step):
    p = max(0, int(round(-math.log(step, 10), 0))) if step < 1 else 0
    return round(math.floor(qty / step) * step, p)

def calc_qty(symbol, price, fraction=1.0):
    info = get_sym_info(symbol)
    return max(round_step((ORDER_USDT * fraction) / price, info["step"]), info["minQty"])

def set_leverage(symbol):
    try: client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except: pass

def get_price(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def validate_symbols():
    try:
        valid  = {s["symbol"] for s in client.futures_exchange_info()["symbols"] if s["status"] == "TRADING"}
        result = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
        print(f"  ✅ {len(result)} symbols valid")
        return result
    except:
        return list(dict.fromkeys(SYMBOLS))

def get_exchange_amt(symbol, retries=3):
    for attempt in range(retries):
        try:
            for p in client.futures_position_information(symbol=symbol):
                amt = float(p["positionAmt"])
                if amt != 0: return amt
            return 0
        except Exception as e:
            if attempt < retries - 1: time.sleep(1)
            else:
                print(f"  ⚠️  [{symbol}] Gagal query posisi — skip")
                return None

# ════════════════════════════════════════════════════
#  FIX 1: OHLCV dengan cache + batch scan
# ════════════════════════════════════════════════════
def get_ohlcv(symbol, interval, limit=200):
    """
    Fetch OHLCV dengan cache.
    Kalau data untuk (symbol, interval) masih segar (< OHLCV_CACHE_TTL detik),
    kembalikan cache tanpa API call baru.
    Cache TTL = 55 detik untuk interval 15m (candle baru tiap 60 detik).
    Untuk interval 1H/4H TTL bisa lebih panjang, tapi kita pakai TTL sama
    karena fungsi ini juga dipanggil untuk macro (BTC trend).
    """
    cache_key = (symbol, interval)
    now = time.time()

    # Tentukan TTL berdasarkan interval
    if interval in (Client.KLINE_INTERVAL_4HOUR,):
        ttl = 3500   # 4H candle: cache hampir 1 jam
    elif interval in (Client.KLINE_INTERVAL_1HOUR,):
        ttl = 3500   # 1H candle: cache ~58 menit
    else:
        ttl = OHLCV_CACHE_TTL  # 15m candle: cache 55 detik

    if cache_key in _ohlcv_cache:
        ts, df_cached = _ohlcv_cache[cache_key]
        if now - ts < ttl:
            return df_cached  # pakai cache, tidak ada API call

    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            "time", "open", "high", "low", "close", "volume",
            "ct", "qv", "trades", "tbbase", "tbquote", "ignore"])
        for c in ["open", "high", "low", "close", "volume", "tbbase", "tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = pd.to_numeric(df["time"])
        _ohlcv_cache[cache_key] = (now, df)
        # Reset error counter kalau berhasil
        _ohlcv_errors.pop(cache_key, None)
        return df
    except Exception as e:
        # Catat error untuk ditampilkan di header cycle (bukan di sini langsung)
        # supaya tidak merusak urutan log
        err_msg = str(e)[:80]
        _ohlcv_errors[cache_key] = err_msg
        # Kembalikan cache lama kalau ada (lebih baik dari None)
        if cache_key in _ohlcv_cache:
            _, df_old = _ohlcv_cache[cache_key]
            return df_old
        return None

def get_current_batch(symbols):
    """
    FIX 1: Ambil subset simbol untuk di-scan di cycle ini (round-robin batch).
    Contoh: 28 simbol, BATCH_SIZE=7 → 4 batch, tiap cycle scan 7 simbol berbeda.
    Full scan selesai dalam 4 cycle (~4 menit).
    Simbol yang sudah punya open position tetap di-check di manage_positions(),
    jadi tidak ada risiko melewatkan exit signal.
    """
    global _scan_batch_idx
    if not symbols:
        return []
    total_batches = math.ceil(len(symbols) / BATCH_SIZE)
    start = _scan_batch_idx * BATCH_SIZE
    end   = start + BATCH_SIZE
    batch = symbols[start:end]
    _scan_batch_idx = (_scan_batch_idx + 1) % total_batches
    return batch

# ════════════════════════════════════════════════════
#  SUPPORT & RESISTANCE (4H Swing Points)
# ════════════════════════════════════════════════════
def get_sr_levels(symbol, lookback=30):
    df = get_ohlcv(symbol, Client.KLINE_INTERVAL_4HOUR, lookback + 5)
    if df is None or len(df) < 10:
        return {"resistance": [], "support": []}

    highs = df["high"].values
    lows  = df["low"].values
    resistance = []
    support    = []

    for i in range(2, len(highs) - 2):
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and \
           highs[i] > highs[i+1] and highs[i] > highs[i+2]:
            resistance.append(highs[i])
        if lows[i] < lows[i-1] and lows[i] < lows[i-2] and \
           lows[i] < lows[i+1] and lows[i] < lows[i+2]:
            support.append(lows[i])

    return {"resistance": sorted(resistance, reverse=True)[:5],
            "support":    sorted(support)[:5]}

def check_sr_clear(symbol, price, direction):
    sr = get_sr_levels(symbol)

    if direction == "LONG":
        nearby_res = [r for r in sr["resistance"] if r > price]
        if nearby_res:
            nearest = min(nearby_res)
            gap_pct  = (nearest - price) / price
            if gap_pct < SR_BUFFER:
                return False, f"Terlalu dekat resistance {nearest:.4f} (gap {gap_pct*100:.2f}%)"

    elif direction == "SHORT":
        nearby_sup = [s for s in sr["support"] if s < price]
        if nearby_sup:
            nearest = max(nearby_sup)
            gap_pct  = (price - nearest) / price
            if gap_pct < SR_BUFFER:
                return False, f"Terlalu dekat support {nearest:.4f} (gap {gap_pct*100:.2f}%)"

    return True, ""

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
    "last_fng": 0, "last_dom": 0, "last_news": 0,
    "last_btc": 0, "last_breadth": 0, "last_mcap": 0
}

def _calc_btc_trend(df):
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

def refresh_macro():
    now = time.time()

    if now - _macro["last_fng"] > 300:
        try:
            d = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()["data"][0]
            _macro["fng"]       = int(d["value"])
            _macro["fng_label"] = d["value_classification"]
            _macro["last_fng"]  = now
        except: pass

    if now - _macro["last_dom"] > 300:
        try:
            d = requests.get("https://api.coingecko.com/api/v3/global", timeout=8).json()
            _macro["usdt_prev"] = _macro["usdt_d"]
            _macro["usdt_d"]    = round(d["data"]["market_cap_percentage"].get("usdt", 5), 2)
            chg_pct = d["data"].get("market_cap_change_percentage_24h_usd", 0)
            _macro["global_mcap_chg"] = round(chg_pct, 2)
            _macro["last_dom"]  = now
        except: pass

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
                if any(w in tl for w in neg_kw_strong): neg += 2; hl.append(f"🔴🔴 {t[:55]}")
                elif any(w in tl for w in neg_kw_mild): neg += 1; hl.append(f"🔴 {t[:55]}")
                elif any(w in tl for w in pos_kw_strong): pos += 2; hl.append(f"🟢🟢 {t[:55]}")
                elif any(w in tl for w in pos_kw_mild):   pos += 1; hl.append(f"🟢 {t[:55]}")

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
        except: pass

    if now - _macro["last_btc"] > 60:
        try:
            df_15m = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, 60)
            df_1h  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1HOUR, 60)
            df_4h  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_4HOUR, 60)
            _macro["btc_trend_15m"] = _calc_btc_trend(df_15m)
            _macro["btc_trend_1h"]  = _calc_btc_trend(df_1h)
            _macro["btc_trend_4h"]  = _calc_btc_trend(df_4h)
            _macro["last_btc"]      = now
        except: pass

    if now - _macro["last_breadth"] > 300:
        try:
            bullish = 0
            sample  = SYMBOLS[:15]
            for sym in sample:
                df = get_ohlcv(sym, Client.KLINE_INTERVAL_15MINUTE, 10)
                if df is not None and len(df) >= 5:
                    c    = df["close"]
                    ema9 = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
                    if c.iloc[-1] > ema9 and df["close"].iloc[-1] > df["open"].iloc[-1]:
                        bullish += 1
            _macro["market_breadth"] = bullish / len(sample)
            _macro["last_breadth"]   = now
        except: pass

# ════════════════════════════════════════════════════
#  FIX 4: Flash Crash / Pump Detector
# ════════════════════════════════════════════════════
def update_btc_price_history():
    """
    Rekam harga BTC saat ini ke history ring buffer.
    Dipanggil setiap cycle (tiap 60 detik).
    """
    try:
        price = get_price("BTCUSDT")
        if price > 0:
            _btc_price_history.append((time.time(), price))
    except:
        pass

def detect_flash_move():
    """
    FIX 4: Deteksi pergerakan BTC ekstrem dalam FLASH_WINDOW_SEC terakhir
    tanpa perlu tunggu candle 1H/4H closed.

    Returns:
        ("crash", pct)  → harga turun tajam → emergency exit LONG
        ("pump",  pct)  → harga naik tajam  → emergency exit SHORT
        ("none",  0.0)  → normal
    """
    if len(_btc_price_history) < 2:
        return "none", 0.0

    now = time.time()
    cutoff = now - FLASH_WINDOW_SEC

    # Ambil price tertua dalam window
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
#  HELPER: BTC trend gabungan
# ════════════════════════════════════════════════════
BULL_TRENDS = {"BULL", "MILD_BULL"}
BEAR_TRENDS = {"BEAR", "MILD_BEAR"}

def btc_multi_tf_ok_for(direction):
    t15 = _macro["btc_trend_15m"]
    t1h = _macro["btc_trend_1h"]
    t4h = _macro["btc_trend_4h"]

    if direction == "LONG":
        if t4h in BEAR_TRENDS:
            return False, f"BTC 4H={t4h} bearish — skip LONG"
        if t1h in BEAR_TRENDS:
            return False, f"BTC 1H={t1h} bearish — skip LONG"
        if t1h == "SIDEWAYS" and t15 in BEAR_TRENDS:
            return False, f"BTC 1H=SIDEWAYS + 15m={t15} — skip LONG"
    elif direction == "SHORT":
        if t4h in BULL_TRENDS:
            return False, f"BTC 4H={t4h} bullish — skip SHORT"
        if t1h in BULL_TRENDS:
            return False, f"BTC 1H={t1h} bullish — skip SHORT"
        if t1h == "SIDEWAYS" and t15 in BULL_TRENDS:
            return False, f"BTC 1H=SIDEWAYS + 15m={t15} — skip SHORT"

    return True, ""

# ════════════════════════════════════════════════════
#  SMART COOLDOWN
# ════════════════════════════════════════════════════
def check_cooldown_recover():
    btc_ok     = _macro["btc_trend_15m"] in COOLDOWN_BTC_RECOVER
    breadth_ok = _macro["market_breadth"] >= COOLDOWN_BREADTH_MIN
    return btc_ok and breadth_ok

def is_cooldown_active():
    global _in_cooldown
    if not _in_cooldown:
        return False
    if check_cooldown_recover():
        _in_cooldown = False
        print(f"  ✅ Cooldown dibatalkan! BTC:{_macro['btc_trend_15m']} "
              f"Breadth:{_macro['market_breadth']*100:.0f}%")
        return False
    return True

def cooldown_reason():
    reasons = []
    if _macro["btc_trend_15m"] in COOLDOWN_BTC_BAD:
        reasons.append(f"BTC {_macro['btc_trend_15m']}")
    if _macro["market_breadth"] < COOLDOWN_BREADTH_MIN:
        reasons.append(f"breadth {_macro['market_breadth']*100:.0f}%")
    return " & ".join(reasons) if reasons else "kondisi belum jelas"

# ════════════════════════════════════════════════════
#  REGIME (1H per symbol)
# ════════════════════════════════════════════════════
def get_regime(symbol):
    df = get_ohlcv(symbol, Client.KLINE_INTERVAL_1HOUR, 60)
    if df is None or len(df) < 55: return "RANGE"
    c     = df["close"]
    price = c.iloc[-1]
    ema20 = ta.trend.EMAIndicator(c, 20).ema_indicator().iloc[-1]
    ema50 = ta.trend.EMAIndicator(c, 50).ema_indicator().iloc[-1]
    if ema20 > ema50 and price > ema50: return "BULL"
    if ema20 < ema50 and price < ema50: return "BEAR"
    return "RANGE"

# ════════════════════════════════════════════════════
#  TECHNICAL ANALYSIS (15m)
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
#  FIX 3: Funding Rate (average + trend)
# ════════════════════════════════════════════════════
def get_funding(symbol):
    """
    FIX 3: Ambil FUNDING_LOOKBACK data terakhir, hitung:
    - avg_funding: rata-rata funding rate
    - funding_trend: apakah funding makin naik/turun (crowding detector)

    Kalau trend funding memburuk (makin positif untuk LONG, makin negatif untuk SHORT),
    naikkan bobot penalti lewat FUNDING_TREND_WEIGHT.

    Returns: (avg_funding: float, funding_trend: str)
    """
    try:
        data = client.futures_funding_rate(symbol=symbol, limit=FUNDING_LOOKBACK + 1)
        if not data:
            return 0.0, "flat"

        rates = [round(float(d["fundingRate"]) * 100, 4) for d in data]

        # Rata-rata N terakhir (bukan hanya 1)
        avg = round(sum(rates[:FUNDING_LOOKBACK]) / min(len(rates), FUNDING_LOOKBACK), 4)

        # Trend: bandingkan rate terbaru vs rate terlama dalam window
        if len(rates) >= 2:
            delta = rates[0] - rates[-1]  # rates[0] = paling baru
            if delta > 0.01:
                trend = "rising"    # makin crowded LONG
            elif delta < -0.01:
                trend = "falling"   # makin crowded SHORT
            else:
                trend = "flat"
        else:
            trend = "flat"

        return avg, trend
    except:
        return 0.0, "flat"

# ════════════════════════════════════════════════════
#  FIX 2: Adaptive Volume Spike
# ════════════════════════════════════════════════════
def calc_adaptive_vol_threshold(df):
    """
    FIX 2: Hitung threshold volume spike yang adaptif berdasarkan ATR% simbol.

    Logika:
    - Simbol volatile (ATR% tinggi) = noise lebih besar = butuh spike lebih besar
    - Formula: threshold = BASE_VOL_SPIKE + (atr_pct / ATR_VOL_SCALE)
    - ATR% = ATR / price * 100 (dalam %)

    Contoh:
    - BTCUSDT: ATR 0.4% → threshold = 1.5 + 0.4/1.2 = 1.83x (hampir sama dengan v9)
    - DOGEUSDT: ATR 3.5% → threshold = 1.5 + 3.5/1.2 = 4.42x (jauh lebih ketat)
    - ETHUSDT: ATR 1.2% → threshold = 1.5 + 1.2/1.2 = 2.50x (menengah)
    """
    try:
        last = df.iloc[-1]
        atr_pct = (last["atr"] / last["close"]) * 100
        threshold = BASE_VOL_SPIKE + (atr_pct / ATR_VOL_SCALE)
        # Cap threshold antara 1.5x dan 5x agar tidak ekstrem
        return round(max(BASE_VOL_SPIKE, min(threshold, 5.0)), 2)
    except:
        return BASE_VOL_SPIKE

def check_volume_spike_confirm(df, direction):
    """
    FIX 2: Volume spike harus ada di ≥ VOL_SPIKE_MIN_CANDLES dari 3 candle terakhir.
    Threshold spike sekarang adaptif (tidak flat 1.5x untuk semua simbol).

    Lebih ketat dari v9 yang:
    - Threshold flat 1.5x untuk semua simbol
    - Cukup 1 dari 3 candle
    """
    min_spike = calc_adaptive_vol_threshold(df)
    recent    = df.iloc[-3:]
    count     = 0
    best_info = ""

    for _, row in recent.iterrows():
        vr = row["vol_ratio"]
        br = row["buy_ratio"]
        if vr >= min_spike:
            if direction == "LONG" and row["close"] > row["open"] and br > 0.52:
                count += 1
                best_info = f"vol {vr:.1f}x buy {br:.2f}"
            elif direction == "SHORT" and row["close"] < row["open"] and br < 0.48:
                count += 1
                best_info = f"vol {vr:.1f}x sell {1-br:.2f}"

    if count >= VOL_SPIKE_MIN_CANDLES:
        return True, f"{best_info} (threshold:{min_spike:.1f}x, {count}/3 candles)"
    return False, f"spike kurang ({count}/{VOL_SPIKE_MIN_CANDLES} candles, threshold:{min_spike:.1f}x)"

def calc_composite_score(df, regime, ob_imb, cum_d, funding_avg, funding_trend):
    """
    FIX 3: Terima funding_avg + funding_trend (bukan satu angka).
    Kalau funding trend memburuk (rising untuk long crowding),
    penalti dikalikan FUNDING_TREND_WEIGHT.
    """
    last = df.iloc[-1]
    prev = df.iloc[-2]
    W    = SCORE_WEIGHTS
    breakdown = {}
    long_score = short_score = 0.0

    # 1. MACD Histogram momentum
    hist_now  = last["macd_hist"]
    hist_prev = prev["macd_hist"]
    if hist_now > 0 and hist_now > hist_prev:
        pts = W["macd_hist"] if (last["macd"] > last["macd_sig"] and prev["macd"] <= prev["macd_sig"]) \
              else W["macd_hist"] * 0.7
        long_score += pts
        breakdown["macd"] = f"+{pts:.1f}L"
    elif hist_now < 0 and hist_now < hist_prev:
        pts = W["macd_hist"] if (last["macd"] < last["macd_sig"] and prev["macd"] >= prev["macd_sig"]) \
              else W["macd_hist"] * 0.7
        short_score += pts
        breakdown["macd"] = f"+{pts:.1f}S"
    else:
        breakdown["macd"] = "0"

    # 2. RSI
    rsi = last["rsi"]
    if rsi < 35:
        pts = W["rsi"] if rsi < 30 else W["rsi"] * 0.6
        long_score += pts; breakdown["rsi"] = f"+{pts:.1f}L"
    elif rsi > 65:
        pts = W["rsi"] if rsi > 70 else W["rsi"] * 0.6
        short_score += pts; breakdown["rsi"] = f"+{pts:.1f}S"
    else:
        breakdown["rsi"] = "0"

    # 3. EMA Stack
    e9, e21, e50 = last["ema9"], last["ema21"], last["ema50"]
    if e9 > e21 > e50:
        long_score += W["ema_stack"]; breakdown["ema"] = f"+{W['ema_stack']}L"
    elif e9 < e21 < e50:
        short_score += W["ema_stack"]; breakdown["ema"] = f"+{W['ema_stack']}S"
    else:
        if e9 > e21: long_score += W["ema_stack"] * 0.4
        elif e9 < e21: short_score += W["ema_stack"] * 0.4
        breakdown["ema"] = "partial"

    # 4. Volume + taker ratio (threshold sudah di-handle di check_volume_spike_confirm)
    vr = last["vol_ratio"]
    br = last["buy_ratio"]
    if vr >= BASE_VOL_SPIKE:
        if last["close"] > last["open"] and br > 0.55:
            pts = W["volume"] * min(vr / BASE_VOL_SPIKE, 1.5)
            long_score += pts; breakdown["vol"] = f"+{pts:.1f}L({vr:.1f}x)"
        elif last["close"] < last["open"] and br < 0.45:
            pts = W["volume"] * min(vr / BASE_VOL_SPIKE, 1.5)
            short_score += pts; breakdown["vol"] = f"+{pts:.1f}S({vr:.1f}x)"
        else:
            breakdown["vol"] = f"spike({vr:.1f}x) tapi arah ambigu"
    else:
        breakdown["vol"] = f"no spike({vr:.1f}x)"

    # 5. Order Book Imbalance
    if ob_imb > 0.15:
        pts = W["ob_imbalance"] * min(ob_imb / 0.15, 1.5)
        long_score += pts; breakdown["ob"] = f"+{pts:.1f}L({ob_imb:+.2f})"
    elif ob_imb < -0.15:
        pts = W["ob_imbalance"] * min(abs(ob_imb) / 0.15, 1.5)
        short_score += pts; breakdown["ob"] = f"+{pts:.1f}S({ob_imb:+.2f})"
    else:
        breakdown["ob"] = f"neutral({ob_imb:+.2f})"

    # 6. Cumulative Delta
    if cum_d > 0.15:
        long_score += W["cum_delta"]; breakdown["delta"] = f"+{W['cum_delta']}L"
    elif cum_d < -0.15:
        short_score += W["cum_delta"]; breakdown["delta"] = f"+{W['cum_delta']}S"
    else:
        breakdown["delta"] = "0"

    # 7. Stochastic
    k, d   = last["stk"], last["std"]
    pk, pd_ = prev["stk"], prev["std"]
    if k < 25 and k > d and pk <= pd_:
        long_score += W["stoch"]; breakdown["stoch"] = f"+{W['stoch']}L"
    elif k > 75 and k < d and pk >= pd_:
        short_score += W["stoch"]; breakdown["stoch"] = f"+{W['stoch']}S"
    else:
        breakdown["stoch"] = "0"

    # 8. Bollinger Band
    price = last["close"]
    if price <= last["bb_lo"] * 1.002:
        long_score += W["bb"]; breakdown["bb"] = f"+{W['bb']}L"
    elif price >= last["bb_hi"] * 0.998:
        short_score += W["bb"]; breakdown["bb"] = f"+{W['bb']}S"
    else:
        breakdown["bb"] = "0"

    # 9. Funding Rate — FIX 3: pakai avg + trend
    funding = funding_avg
    trend_mult = FUNDING_TREND_WEIGHT if funding_trend == "rising" else 1.0
    trend_mult_short = FUNDING_TREND_WEIGHT if funding_trend == "falling" else 1.0

    if funding < -FUNDING_THRESHOLD:
        pts = W["funding"] * trend_mult_short  # funding negatif + falling = crowded short → squeeze LONG
        long_score += pts
        breakdown["funding"] = f"+{pts:.1f}L(avg:{funding:.3f}% {funding_trend})"
    elif funding > FUNDING_THRESHOLD:
        pts = W["funding"] * trend_mult  # funding positif + rising = crowded long → squeeze SHORT
        short_score += pts
        breakdown["funding"] = f"+{pts:.1f}S(avg:{funding:.3f}% {funding_trend})"
    else:
        breakdown["funding"] = f"neutral(avg:{funding:.3f}% {funding_trend})"

    # Regime adjustment
    if regime == "BULL":
        long_score  *= 1.1
        short_score *= 0.8
    elif regime == "BEAR":
        short_score *= 1.1
        long_score  *= 0.8

    max_possible = sum(W.values()) * 1.5
    long_pct  = min(long_score  / max_possible * 100, 100)
    short_pct = min(short_score / max_possible * 100, 100)

    if long_pct >= MIN_COMPOSITE_SCORE and long_pct > short_pct + 10:
        return "LONG", long_pct, breakdown
    if short_pct >= MIN_COMPOSITE_SCORE and short_pct > long_pct + 10:
        return "SHORT", short_pct, breakdown
    return "NONE", max(long_pct, short_pct), breakdown

def get_ob_imbalance(symbol):
    try:
        ob    = client.futures_order_book(symbol=symbol, limit=50)
        bids  = sum(float(b[1]) for b in ob["bids"])
        asks  = sum(float(a[1]) for a in ob["asks"])
        total = bids + asks
        return round((bids - asks) / total, 3) if total else 0.0
    except: return 0.0

def get_cum_delta(df, lookback=10):
    if len(df) < lookback: return 0.0
    recent = df.tail(lookback).copy()
    recent["delta"] = recent["tbbase"] - (recent["volume"] - recent["tbbase"])
    norm = recent["delta"].sum() / (recent["volume"].sum() + 1)
    return round(norm, 3)

def detect_whale(df):
    last   = df.iloc[-1]
    vol_ma = df["vol_ma"].iloc[-1]
    if pd.isna(vol_ma) or vol_ma == 0: return "none", 1.0
    ratio = last["volume"] / vol_ma
    if ratio >= 3.5:
        return ("buy_whale" if last["close"] > last["open"] else "sell_whale"), ratio
    elif ratio >= 2.0:
        return ("mild_buy" if last["close"] > last["open"] else "mild_sell"), ratio
    return "none", ratio

# ════════════════════════════════════════════════════
#  MASTER ENTRY FILTER
# ════════════════════════════════════════════════════
def should_enter(symbol, df):
    info = {}

    if is_cooldown_active():
        return None, 0, 0, 0, {"skip": f"🧊 Cooldown ({cooldown_reason()})"}

    fng      = _macro["fng"]
    news     = _macro["news"]
    usdt_up  = _macro["usdt_d"] > _macro["usdt_prev"] + USDT_RISK_OFF_DELTA
    mcap_bad = _macro["global_mcap_chg"] < -2.0

    if fng < MIN_FNG_ANY:
        return None, 0, 0, 0, {"skip": f"F&G ekstrem rendah ({fng}) — skip semua"}
    if fng < MIN_FNG:
        return None, 0, 0, 0, {"skip": f"F&G terlalu rendah ({fng})"}
    if news in ("strong_negative", "negative"):
        return None, 0, 0, 0, {"skip": f"News {news} (skor:{_macro['news_strength']})"}
    if usdt_up:
        return None, 0, 0, 0, {"skip": f"USDT.D naik risk-off ({_macro['usdt_prev']}→{_macro['usdt_d']})"}

    info["btc_15m"] = _macro["btc_trend_15m"]
    info["btc_1h"]  = _macro["btc_trend_1h"]
    info["btc_4h"]  = _macro["btc_trend_4h"]

    breadth = _macro["market_breadth"]
    info["breadth"] = f"{breadth*100:.0f}%"

    regime = get_regime(symbol)
    info["regime"] = regime

    prev_candle_time = int(df["time"].iloc[-2])
    df_closed = df.iloc[:-1].copy()
    if len(df_closed) < 60:
        return None, 0, 0, 0, {"skip": "Data tidak cukup"}
    if _last_candle.get(symbol) == prev_candle_time:
        return None, 0, 0, 0, {"skip": "Sudah dianalisa candle ini"}

    df_closed = run_ta(df_closed)
    ob_imb    = get_ob_imbalance(symbol)
    cum_d     = get_cum_delta(df_closed)

    # FIX 3: funding sekarang tuple (avg, trend)
    funding_avg, funding_trend = get_funding(symbol)

    ta_dir, score, breakdown = calc_composite_score(
        df_closed, regime, ob_imb, cum_d, funding_avg, funding_trend)
    info["score"]     = f"{score:.1f}/100"
    info["breakdown"] = breakdown

    if ta_dir == "NONE":
        return None, 0, 0, 0, {"skip": f"Score rendah ({score:.1f}/100)"}

    if ta_dir == "LONG" and fng > MAX_FNG_LONG:
        return None, 0, 0, 0, {"skip": f"F&G terlalu greedy ({fng}) — euphoria risk LONG"}
    if mcap_bad and ta_dir == "LONG":
        return None, 0, 0, 0, {"skip": f"Global mcap turun {_macro['global_mcap_chg']:.1f}% — skip LONG"}

    btc_ok, btc_reason = btc_multi_tf_ok_for(ta_dir)
    if not btc_ok:
        return None, 0, 0, 0, {"skip": btc_reason}

    if ta_dir == "LONG" and breadth < MIN_MARKET_BREADTH:
        return None, 0, 0, 0, {"skip": f"Market breadth rendah ({breadth*100:.0f}%)"}
    if ta_dir == "SHORT" and breadth > 0.65:
        return None, 0, 0, 0, {"skip": f"Market breadth tinggi ({breadth*100:.0f}%), skip SHORT"}

    if ta_dir == "LONG" and regime == "BEAR":
        return None, 0, 0, 0, {"skip": "Regime BEAR — tidak LONG"}
    if ta_dir == "SHORT" and regime == "BULL":
        return None, 0, 0, 0, {"skip": "Regime BULL — tidak SHORT"}

    # FIX 2: volume spike check dengan threshold adaptif
    vol_ok, vol_info = check_volume_spike_confirm(df_closed, ta_dir)
    if not vol_ok:
        return None, 0, 0, 0, {"skip": f"Tidak ada volume spike ({vol_info})"}
    info["vol_spike"] = vol_info

    sr_ok, sr_reason = check_sr_clear(symbol, df_closed["close"].iloc[-1], ta_dir)
    if not sr_ok:
        return None, 0, 0, 0, {"skip": f"S/R: {sr_reason}"}
    info["sr"] = "clear"

    whale_dir, whale_ratio = detect_whale(df_closed)
    info["whale"] = f"{whale_dir}({whale_ratio:.1f}x)"
    if ta_dir == "LONG" and whale_dir == "sell_whale":
        return None, 0, 0, 0, {"skip": "Whale sell aktif"}
    if ta_dir == "SHORT" and whale_dir == "buy_whale":
        return None, 0, 0, 0, {"skip": "Whale buy aktif"}

    # FIX 3: gunakan threshold yang dinaikkan
    info["funding"] = f"{funding_avg:.4f}%({funding_trend})"
    if ta_dir == "LONG" and funding_avg > FUNDING_THRESHOLD * 1.25:
        return None, 0, 0, 0, {"skip": f"Funding terlalu positif ({funding_avg:.4f}%) — long squeeze risk"}
    if ta_dir == "SHORT" and funding_avg < -FUNDING_THRESHOLD * 1.25:
        return None, 0, 0, 0, {"skip": f"Funding terlalu negatif ({funding_avg:.4f}%) — short squeeze risk"}

    bb_width = df_closed["bb_width"].iloc[-1]
    info["bb_width"] = round(bb_width, 4)
    if bb_width < 0.01:
        return None, 0, 0, 0, {"skip": "BB terlalu sempit, choppy"}

    atr   = df_closed["atr"].iloc[-1]
    price = df_closed["close"].iloc[-1]
    if ta_dir == "LONG":
        sl_price  = round(price - ATR_SL_MULT  * atr, 8)
        tp1_price = round(price + ATR_TP1_MULT * atr, 8)
        tp2_price = round(price + ATR_TP2_MULT * atr, 8)
    else:
        sl_price  = round(price + ATR_SL_MULT  * atr, 8)
        tp1_price = round(price - ATR_TP1_MULT * atr, 8)
        tp2_price = round(price - ATR_TP2_MULT * atr, 8)

    sl_pct = abs(price - sl_price) / price
    if sl_pct > 0.04:
        return None, 0, 0, 0, {"skip": f"ATR terlalu besar (SL={sl_pct*100:.1f}%)"}

    _last_candle[symbol] = prev_candle_time
    info["atr_sl_pct"] = f"{sl_pct*100:.2f}%"
    info["ob"]   = ob_imb
    info["ta"]   = ta_dir
    return ta_dir, sl_price, tp1_price, tp2_price, info

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
            type=ORDER_TYPE_MARKET, quantity=qty)

        entry    = get_price(symbol)
        trail_sl = entry * (1 - TRAIL_PCT) if side == "LONG" else entry * (1 + TRAIL_PCT)

        open_positions[symbol] = {
            "side":      side,
            "entry":     entry,
            "qty":       qty,
            "qty_remain": qty,
            "sl":        sl_price,
            "tp1":       tp1_price,
            "tp2":       tp2_price,
            "peak":      entry,
            "trail_sl":  trail_sl,
            "trailing_active": False,
            "tp1_hit":   False,
            "be_active": False,
        }
        sl_pct  = abs(entry - sl_price) / entry * 100
        tp1_pct = abs(tp1_price - entry) / entry * 100
        tp2_pct = abs(tp2_price - entry) / entry * 100
        score   = info.get("score", "?")
        print(f"  ✅ [{symbol}] {side} @{entry:.5f} qty={qty}")
        print(f"     SL:{sl_price:.5f}(-{sl_pct:.2f}%) TP1:{tp1_price:.5f}(+{tp1_pct:.2f}%) TP2:{tp2_price:.5f}(+{tp2_pct:.2f}%)")
        print(f"     Score:{score} BTC:{info.get('btc_15m','?')}/{info.get('btc_1h','?')}/{info.get('btc_4h','?')}")
        print(f"     Vol:{info.get('vol_spike','?')} Funding:{info.get('funding','?')}")
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal entry: {e}")

def partial_close(symbol, reason="TP1"):
    global _consec_loss
    pos = open_positions.get(symbol)
    if pos is None: return

    try:
        amt = get_exchange_amt(symbol)
        if amt is None or amt == 0:
            pos["tp1_hit"] = True
            return

        # Guard: pastikan close_qty tidak melebihi posisi aktual
        half_qty    = round_step(abs(amt) * 0.5, get_sym_info(symbol)["step"])
        min_qty     = get_sym_info(symbol)["minQty"]
        close_qty   = max(half_qty, min_qty)

        # FIX: kalau close_qty > abs(amt), tutup semua daripada over-close
        if close_qty > abs(amt):
            close_qty = abs(amt)

        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt > 0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=close_qty,
            reduceOnly=True)

        exit_price = get_price(symbol)
        side  = pos["side"]
        pnl   = (exit_price - pos["entry"]) * close_qty if side == "LONG" \
                else (pos["entry"] - exit_price) * close_qty
        pct   = pnl / (pos["entry"] * close_qty) * 100

        print(f"  🎯 [{symbol}] PARTIAL TP1 — {reason}")
        print(f"     💛 P&L (50%): {pnl:+.4f} USDT ({pct:+.2f}%)")

        pos["tp1_hit"]         = True
        pos["qty_remain"]      = abs(amt) - close_qty
        pos["be_active"]       = True
        pos["sl"]              = pos["entry"]
        pos["trailing_active"] = True
        pos["peak"]            = exit_price
        pos["trail_sl"]        = exit_price * (1 - TRAIL_PCT) if side == "LONG" \
                                 else exit_price * (1 + TRAIL_PCT)

        print(f"     🔒 Break-even SL @{pos['entry']:.5f} | Trailing aktif")
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
                    pnl = (exit_ - pos["entry"]) * qty_r if pos["side"] == "LONG" \
                          else (pos["entry"] - exit_) * qty_r
                    pct = pnl / (pos["entry"] * qty_r) * 100 if qty_r > 0 else 0
                    print(f"  ⚠️  [{symbol}] Sudah tutup di exchange — Est P&L: {pnl:+.4f}U ({pct:+.2f}%)")
                    trade_log.append({"symbol": symbol, "side": pos["side"],
                                      "pnl": round(pnl, 4), "reason": "External close"})
                    _update_loss_streak(pnl)
                open_positions.pop(symbol, None)
            return True

        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt > 0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET, quantity=abs(amt), reduceOnly=True)

        if symbol in open_positions:
            pos   = open_positions[symbol]
            exit_ = get_price(symbol)
            qty_r = pos.get("qty_remain", pos["qty"])
            pnl   = (exit_ - pos["entry"]) * qty_r if pos["side"] == "LONG" \
                    else (pos["entry"] - exit_) * qty_r
            pct   = pnl / (pos["entry"] * qty_r) * 100 if qty_r > 0 else 0
            emoji = "🟢" if pnl >= 0 else "🔴"
            be_tag = " [BE]" if pos.get("be_active") else ""
            print(f"  💰 [{symbol}] CLOSED — {reason}{be_tag}")
            print(f"     {emoji} P&L (sisa): {pnl:+.4f} USDT ({pct:+.2f}%)")
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
                reasons = []
                if btc_bad:     reasons.append(f"BTC {_macro['btc_trend_15m']}")
                if breadth_bad: reasons.append(f"breadth {_macro['market_breadth']*100:.0f}%")
                print(f"  🧊 {MAX_CONSEC_LOSS} loss + market buruk ({', '.join(reasons)}) → Cooldown!")
            else:
                print(f"  ⚡ {MAX_CONSEC_LOSS} loss tapi market masih oke → lanjut trading")
                _consec_loss = 0
    else:
        _consec_loss = 0
        if _in_cooldown:
            _in_cooldown = False
            print(f"  ✅ Win! Cooldown diakhiri.")

# ════════════════════════════════════════════════════
#  POSITION MANAGEMENT
# ════════════════════════════════════════════════════
def manage_positions():
    """
    FIX 4: Flash crash/pump detector ditambah di sini.
    Dijalankan SETIAP cycle sebelum check normal SL/TP.
    Tidak perlu tunggu candle 1H/4H closed.
    """
    # ── FIX 4: Cek flash crash/pump dulu ─────────────────────
    flash_dir, flash_pct = detect_flash_move()
    if flash_dir != "none" and open_positions:
        for symbol in list(open_positions.keys()):
            pos  = open_positions[symbol]
            side = pos["side"]
            if flash_dir == "crash" and side == "LONG":
                close_trade(symbol,
                    f"🚨 Flash Crash BTC -{flash_pct:.2f}% dalam {FLASH_WINDOW_SEC//60}m — exit LONG")
                continue
            elif flash_dir == "pump" and side == "SHORT":
                close_trade(symbol,
                    f"🚨 Flash Pump BTC +{flash_pct:.2f}% dalam {FLASH_WINDOW_SEC//60}m — exit SHORT")
                continue

    for symbol in list(open_positions.keys()):
        pos   = open_positions[symbol]
        price = get_price(symbol)
        if price == 0:
            print(f"  ⚠️  [{symbol}] Tidak bisa get price, skip")
            continue

        entry = pos["entry"]
        side  = pos["side"]

        # Emergency exits (news + BTC multi-TF — tetap ada untuk kondisi non-flash)
        if _macro["news"] in ("strong_negative",):
            close_trade(symbol, "🚨 Emergency — strong bad news")
            continue

        # FIX 4: Untuk exit berbasis trend (bukan flash), tetap butuh 1H+4H confirm
        # supaya tidak exit karena 15m noise
        if side == "LONG" and _macro["btc_trend_1h"] == "BEAR" and _macro["btc_trend_4h"] == "BEAR":
            close_trade(symbol, "⚡ BTC 1H+4H BEAR — emergency exit LONG")
            continue

        if side == "SHORT" and _macro["btc_trend_1h"] == "BULL" and _macro["btc_trend_4h"] == "BULL":
            close_trade(symbol, "⚡ BTC 1H+4H BULL — emergency exit SHORT")
            continue

        if side == "LONG":
            profit_pct = (price - entry) / entry

            if not pos["tp1_hit"] and price >= pos["tp1"]:
                partial_close(symbol, "TP1")
                continue

            if not pos["trailing_active"] and profit_pct >= TRAIL_TRIGGER:
                pos["trailing_active"] = True
                pos["trail_sl"] = price * (1 - TRAIL_PCT)
                print(f"  🔄 [{symbol}] Trailing aktif @{price:.5f} (+{profit_pct*100:.2f}%)")

            if pos["trailing_active"] and price > pos["peak"]:
                pos["peak"]     = price
                pos["trail_sl"] = price * (1 - TRAIL_PCT)

            if pos["tp1_hit"] and price >= pos["tp2"]:
                close_trade(symbol, "✨ TP2 (sisa 50%)")
                continue

            if pos["trailing_active"] and price <= pos["trail_sl"]:
                close_trade(symbol, "🔄 Trailing Stop")
                continue

            if price <= pos["sl"]:
                reason = "🔒 Break-even Stop" if pos.get("be_active") else "🛑 STOP LOSS"
                close_trade(symbol, reason)
                continue

            pnl_now = (price - entry) * pos.get("qty_remain", pos["qty"])
            be_tag  = " [BE]" if pos.get("be_active") else ""
            tp_tag  = f" TP2:{pos['tp2']:.4f}" if pos["tp1_hit"] else f" TP1:{pos['tp1']:.4f}"
            tsl     = f" TSL:{pos['trail_sl']:.4f}" if pos["trailing_active"] else ""
            print(f"  📌 [{symbol}] LONG @{entry:.4f}→{price:.4f}{be_tag} | {pnl_now:+.3f}U{tsl}{tp_tag}")

        else:  # SHORT
            profit_pct = (entry - price) / entry

            if not pos["tp1_hit"] and price <= pos["tp1"]:
                partial_close(symbol, "TP1")
                continue

            if not pos["trailing_active"] and profit_pct >= TRAIL_TRIGGER:
                pos["trailing_active"] = True
                pos["trail_sl"] = price * (1 + TRAIL_PCT)
                print(f"  🔄 [{symbol}] Trailing aktif @{price:.5f} (+{profit_pct*100:.2f}%)")

            if pos["trailing_active"] and price < pos["peak"]:
                pos["peak"]     = price
                pos["trail_sl"] = price * (1 + TRAIL_PCT)

            if pos["tp1_hit"] and price <= pos["tp2"]:
                close_trade(symbol, "✨ TP2 (sisa 50%)")
                continue

            if pos["trailing_active"] and price >= pos["trail_sl"]:
                close_trade(symbol, "🔄 Trailing Stop")
                continue

            if price >= pos["sl"]:
                reason = "🔒 Break-even Stop" if pos.get("be_active") else "🛑 STOP LOSS"
                close_trade(symbol, reason)
                continue

            pnl_now = (entry - price) * pos.get("qty_remain", pos["qty"])
            be_tag  = " [BE]" if pos.get("be_active") else ""
            tp_tag  = f" TP2:{pos['tp2']:.4f}" if pos["tp1_hit"] else f" TP1:{pos['tp1']:.4f}"
            tsl     = f" TSL:{pos['trail_sl']:.4f}" if pos["trailing_active"] else ""
            print(f"  📌 [{symbol}] SHORT @{entry:.4f}→{price:.4f}{be_tag} | {pnl_now:+.3f}U{tsl}{tp_tag}")

# ════════════════════════════════════════════════════
#  SUMMARY
# ════════════════════════════════════════════════════
def print_summary():
    if not trade_log: return
    total = sum(t["pnl"] for t in trade_log)
    wins  = sum(1 for t in trade_log if t["pnl"] > 0)
    n     = len(trade_log)
    wr    = wins / n * 100 if n else 0
    cd    = f" | 🧊 Cooldown ({cooldown_reason()})" if _in_cooldown else ""
    print(f"\n  📊 {n} trades | WR:{wr:.0f}% W:{wins} L:{n-wins} | P&L:{total:+.4f}U | streak:{_consec_loss}L{cd}")
    for t in trade_log[-3:]:
        e = "🟢" if t["pnl"] > 0 else "🔴"
        print(f"     {e} {t['symbol']} {t['side']} {t['pnl']:+.4f}U — {t['reason'][:40]}")

# ════════════════════════════════════════════════════
#  MAIN LOOP
# ════════════════════════════════════════════════════
def run_bot():
    print("🤖 Bot v9-Fixed — Batch Scan + Adaptive Vol + Avg Funding + Flash Exit")
    print(f"   Leverage      : {LEVERAGE}x | Order: ${ORDER_USDT} USDT")
    print(f"   Batch Scan    : {BATCH_SIZE} simbol/cycle (hemat ~{28//BATCH_SIZE}x API call per cycle)")
    print(f"   OHLCV Cache   : 15m={OHLCV_CACHE_TTL}s | 1H/4H=~1jam (hindari refetch sia-sia)")
    print(f"   Volume Spike  : adaptif (base {BASE_VOL_SPIKE}x + ATR%/{ATR_VOL_SCALE}), wajib ≥{VOL_SPIKE_MIN_CANDLES}/3 candles")
    print(f"   Funding       : avg {FUNDING_LOOKBACK} data terakhir, threshold {FUNDING_THRESHOLD}%")
    print(f"   Flash Exit    : BTC ≥{FLASH_CRASH_PCT}% dalam {FLASH_WINDOW_SEC//60}m → exit instan")
    print(f"   Min Score     : {MIN_COMPOSITE_SCORE}/100")
    print(f"   BTC Filter    : 15m + 1H + 4H harus searah")
    print(f"   S/R Check     : 4H swing high/low, buffer {SR_BUFFER*100:.1f}%")
    print(f"   Smart Cooldown: {MAX_CONSEC_LOSS} loss + market buruk → pause\n")

    print("  ⏳ Setup...")
    symbols = validate_symbols()
    for s in symbols: get_sym_info(s)
    refresh_macro()
    update_btc_price_history()  # seed flash detector dengan harga awal
    print(f"  ✅ {len(symbols)} symbols | F&G:{_macro['fng']} | "
          f"BTC 15m:{_macro['btc_trend_15m']} 1H:{_macro['btc_trend_1h']} 4H:{_macro['btc_trend_4h']} | "
          f"Batch:{BATCH_SIZE} simbol/cycle | News:{_macro['news']}\n")

    cycle = 0
    while True:
        cycle += 1
        refresh_macro()
        update_btc_price_history()  # FIX 4: rekam harga BTC tiap cycle

        if _in_cooldown:
            is_cooldown_active()

        manage_positions()

        cd_info = f" 🧊 COOLDOWN ({cooldown_reason()})" if _in_cooldown else ""

        # FIX 4: Tampilkan status flash detector
        flash_dir, flash_pct = detect_flash_move()
        flash_info = f" ⚡{flash_dir.upper()}:{flash_pct:.2f}%" if flash_dir != "none" else ""

        # Kumpulkan error fetch yang terjadi sejak cycle terakhir
        # (diisi oleh get_ohlcv, bukan di-print langsung supaya log tidak berantakan)
        fetch_errors = list(_ohlcv_errors.items())
        _ohlcv_errors.clear()  # reset setelah ditampilkan

        # FIX LOG: Semua print header dikumpulkan dulu, baru dicetak sekaligus
        total_batches = math.ceil(len(symbols) / BATCH_SIZE)
        next_batch_num = (_scan_batch_idx % total_batches) + 1  # batch yang akan di-scan cycle ini

        print(f"\n{'='*72}")
        print(f"  🔄 #{cycle} {time.strftime('%H:%M:%S')} | F&G:{_macro['fng']}({_macro['fng_label']}) | "
              f"USDT:{_macro['usdt_d']}% | News:{_macro['news']}(skor:{_macro['news_strength']}){cd_info}{flash_info}")
        print(f"  📈 BTC 15m:{_macro['btc_trend_15m']} | 1H:{_macro['btc_trend_1h']} | 4H:{_macro['btc_trend_4h']}")
        print(f"  🌍 Breadth:{_macro['market_breadth']*100:.0f}% | MCap24h:{_macro['global_mcap_chg']:+.1f}%")
        for h in _macro["headlines"]: print(f"  {h}")
        print(f"  📂 Posisi({len(open_positions)}): {list(open_positions.keys()) or '-'}")
        print(f"  🔍 Akan scan batch {next_batch_num}/{total_batches} "
              f"(full scan selesai tiap ~{total_batches} cycle ≈ {total_batches * SCAN_INTERVAL // 60}m)")

        # Tampilkan error fetch dari cycle sebelumnya di sini (bukan di tengah scan)
        if fetch_errors:
            unique_syms = list(dict.fromkeys(k[0] for k, v in fetch_errors))
            print(f"  ⚠️  Fetch gagal ({len(fetch_errors)}x): {', '.join(unique_syms[:5])}"
                  + (f" +{len(unique_syms)-5} lagi" if len(unique_syms) > 5 else ""))
            # Tampilkan 1 contoh error message untuk debug
            first_key, first_err = fetch_errors[0]
            print(f"     └─ Contoh error [{first_key[0]}]: {first_err}")

        print(f"{'='*72}")

        skipped    = 0
        candidates = []

        if len(open_positions) < MAX_POSITIONS and _macro["news"] not in ("strong_negative", "negative") \
           and not _in_cooldown:

            # FIX 1: Scan hanya batch saat ini, bukan semua simbol
            batch = get_current_batch([s for s in symbols if s not in open_positions])

            for symbol in batch:
                # FIX 1: Jeda kecil antar API call untuk hindari burst
                time.sleep(SCAN_DELAY_MS)

                df = get_ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 220)
                if df is None or len(df) < 70: continue
                side, sl, tp1, tp2, info = should_enter(symbol, df)
                if side:
                    candidates.append((symbol, side, sl, tp1, tp2, info))
                else:
                    skipped += 1

            if candidates:
                candidates.sort(
                    key=lambda x: float(x[5].get("score", "0").split("/")[0]),
                    reverse=True
                )
                print(f"\n  🎯 {len(candidates)} setup valid | {skipped} di-skip")
                for sym, side, sl, tp1, tp2, info in candidates[:3]:
                    print(f"     ⭐ {sym} {side} | Score:{info.get('score','?')} | "
                          f"Vol:{info.get('vol_spike','?')} | Funding:{info.get('funding','?')}")
                for sym, side, sl, tp1, tp2, info in candidates:
                    if len(open_positions) >= MAX_POSITIONS: break
                    open_trade(sym, side, sl, tp1, tp2, info)
            else:
                print(f"  ⏳ {len(batch)} simbol di-scan (batch), belum ada setup valid")
        else:
            if _in_cooldown:
                print(f"  🧊 Cooldown — {cooldown_reason()}")
            else:
                print(f"  ⏸️  Posisi penuh atau kondisi tidak aman")

        print_summary()
        print(f"\n  ⏱️  {SCAN_INTERVAL}s...")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run_bot()
