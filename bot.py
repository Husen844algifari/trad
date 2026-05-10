"""
Bot Scalping v13 — ULTRA LIGHTNING ⚡⚡⚡
==========================================

FILOSOFI v13:
  ⚡ "MINUS SATU ANGKA → CUT. PROFIT SATU ANGKA → TRAIL DARI SANA. TIDAK ADA KOMPROMI."

  PERUBAHAN UTAMA dari v12:
  ─────────────────────────
  🔴 ZERO-LOSS TOLERANCE: minus 1 tick (0.01%) → CUT SEKETIKA, tanpa basa-basi
  🟢 INSTANT TRAIL FROM PROFIT: profit 1 tick → trailing stop langsung pasang di entry price
  📈 DYNAMIC TRAIL FOLLOW: trail naik mengikuti harga, TIDAK diam di entry
  🔄 PRE-SCAN PARALEL: selalu ada 5 kandidat siap dalam antrian (tidak pernah kosong)
  🚀 MAX 3 POSISI: fokus kualitas, bukan kuantitas
  📊 LAPORAN LENGKAP: setiap close tampil detail (profit/loss, TP count, trailing berapa kali)
  ⚡ MONITOR 1 DETIK: posisi dicek setiap 1 detik untuk respons ultra-cepat
  🎯 PRE-WARM CANDIDATES: background scan terus-menerus, kandidat selalu fresh

  FIX v13.1:
  ──────────
  🐛 FIX: get_best_candidate() TTL naik dari 12s → 45s (kandidat tidak expire sebelum dipakai)
  🐛 FIX: pre_scan_engine() staleness check naik dari 10s → 40s (konsisten dengan TTL)
  🐛 FIX: PRE_SCAN_INTERVAL turun dari 8s → 5s (lebih sering isi queue)
  🐛 FIX: re-validasi harga sebelum open_trade() — skip jika drift >0.3%

ARSITEKTUR v13:
  ┌─────────────────────────────────────────────────────────┐
  │  PRE-SCAN ENGINE (background, parallel 25 threads)      │
  │  Selalu maintain 5 kandidat terbaik di antrian          │
  │  Re-scan setiap 5 detik, prioritas hot symbols          │
  └───────────────────┬─────────────────────────────────────┘
                      │
  ┌───────────────────▼─────────────────────────────────────┐
  │  ULTRA-FAST POSITION MONITOR (1 detik interval)         │
  │  Tick resolution: deteksi pergerakan per-tick           │
  │  Zero-loss: minus 0.01% → cut dalam <2 detik           │
  │  Instant trail: profit 0.01% → trail aktif di entry    │
  │  Dynamic trail: makin profit → trail makin ketat        │
  └───────────────────┬─────────────────────────────────────┘
                      │
  ┌───────────────────▼─────────────────────────────────────┐
  │  CLOSE ENGINE + LAPORAN DETAIL                          │
  │  Setiap close: tampilkan ringkasan lengkap              │
  │  Trail count, TP hits, holding time, PnL realtime       │
  │  Langsung ambil kandidat dari antrian → entry baru      │
  └─────────────────────────────────────────────────────────┘

PROFIT FLOW v13:
  Entry @ 100
  ├─ Minus 0.01%? (99.99) → CUT SEKARANG ─────────────────── [LOSS]
  ├─ Profit 0.01%? (100.01) → Trail aktif di entry (100.00)
  │   ├─ Naik ke 100.10 → Trail naik ke 100.00 (masih di entry)
  │   ├─ Naik ke 100.30 → Trail naik ke 100.24 (trail 0.06%)
  │   ├─ Naik ke 100.50 → Trail naik ke 100.46 (trail 0.04%)
  │   └─ Naik ke 100.60 → Trail naik ke 100.57 (trail 0.03% super ketat)
  └─ TP1 @ 100.35% → Close 60% + trail 40% sisa

TRAIL MECHANICS:
  Profit 0.01-0.30%: Trail di ENTRY PRICE (zero-loss guarantee)
  Profit 0.30-0.50%: Trail mengikuti 0.06% di bawah peak
  Profit 0.50-0.80%: Trail mengikuti 0.04% di bawah peak
  Profit 0.80%+:     Trail mengikuti 0.03% di bawah peak (super ketat)
"""

import os, time, math, json, threading, queue
import requests
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
import ta
import pandas as pd
import numpy as np

load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))

# ══════════════════════════════════════════════════════════
# TESTNET / REAL — pilih salah satu

# Untuk real: comment baris atas
# ══════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════
#  ██╗   ██╗ ██╗██████╗     ███████╗███████╗████████╗
#  ██║   ██║███║╚════╝╚═╗   ██╔════╝██╔════╝╚══██╔══╝
#  ██║   ██║╚██║ █████╗  ║   ███████╗█████╗     ██║
#  ╚██╗ ██╔╝ ██║ ╚═══██╗ ║   ╚════██║██╔══╝     ██║
#   ╚████╔╝  ██║██████╔╝ ║   ███████║███████╗   ██║
#    ╚═══╝   ╚═╝╚═════╝ ╝   ╚══════╝╚══════╝   ╚═╝
#  ULTRA LIGHTNING MODE
# ════════════════════════════════════════════════════

# ── CORE SETTINGS ────────────────────────────────────────────────
LEVERAGE              = 10
ORDER_USDT            = 1           # $10 per trade
MAX_POSITIONS         = 1            # ✅ MAX 3 POSISI (fokus kualitas!)
PRE_SCAN_CANDIDATES   = 5            # selalu siapkan 5 kandidat di antrian

# ── ZERO-LOSS TOLERANCE (FITUR BARU v13) ─────────────────────────
# Minus satu angka (tick) → CUT SEKETIKA
ZERO_LOSS_TICK_PCT    = 0.0001       # 0.01% = ~1 tick, kalau minus ini langsung cut
# Berapa detik window zero-loss aktif (semua waktu, bukan hanya 2 candle)
ZERO_LOSS_ALWAYS      = True         # ✅ aktif SELAMANYA, bukan hanya awal

# ── INSTANT TRAIL FROM PROFIT (FITUR BARU v13) ───────────────────
# Profit 1 tick → trailing stop LANGSUNG pasang di entry (zero-loss lock)
PROFIT_TRIGGER_TRAIL  = 0.0001       # 0.01% profit → trail aktif
TRAIL_ANCHOR_ENTRY    = True         # ✅ trail awal = di ENTRY PRICE (bukan lebih rendah)

# Dynamic trail: makin profit → makin ketat
# Format: (profit_pct_threshold, trail_distance_pct)
TRAIL_PHASES = [
    (0.0000, 0.0000),   # 0.00-0.01% profit: trail TEPAT di entry (zero loss)
    (0.0001, 0.0000),   # 0.01-0.30% profit: trail DI entry price
    (0.0030, 0.0006),   # 0.30-0.50% profit: trail 0.06% di bawah peak
    (0.0050, 0.0004),   # 0.50-0.80% profit: trail 0.04% di bawah peak
    (0.0080, 0.0003),   # 0.80%+ profit:     trail 0.03% (ultra ketat)
]

# ── PROFIT TARGETS ───────────────────────────────────────────────
TP1_PCT               = 0.0035       # 0.35% → partial close 60%
TP2_PCT               = 0.0060       # 0.60% → target penuh
SL_PCT                = 0.0020       # 0.20% hard SL (backup dari zero-loss)

TP1_CLOSE_RATIO       = 0.60         # 60% close di TP1
TP2_CLOSE_RATIO       = 0.40         # sisa trailing ke TP2

# ── KECEPATAN v13 ─────────────────────────────────────────────────
SCAN_INTERVAL         = 10           # main loop scan tiap 10 detik
POSITION_MONITOR_SEC  = 1            # ✅ monitor posisi tiap 1 DETIK (ultra fast!)
PRE_SCAN_INTERVAL     = 5            # 🔧 FIX: background pre-scan tiap 5 detik (dari 8)
SCAN_DELAY_MS         = 0.020        # 20ms antar API call (lebih cepat)
BATCH_SIZE            = 25           # 25 symbol per batch
MAX_HOLDING_MIN       = 6            # force close setelah 6 menit
SYMBOL_COOLDOWN_SEC   = 20           # cooldown 20 detik setelah close
RE_SCAN_DELAY_SEC     = 0.3          # delay 0.3 detik setelah close

# ── CANDIDATE TTL ─────────────────────────────────────────────────
# 🔧 FIX: TTL kandidat — naik supaya tidak expire sebelum dipakai main loop
CANDIDATE_TTL_VALID   = 45           # detik: kandidat dianggap valid untuk entry
CANDIDATE_TTL_PRESCAN = 40           # detik: staleness check di pre_scan_engine
CANDIDATE_TTL_STALE   = 60           # detik: hapus dari cache sama sekali

# ── PRICE DRIFT GUARD ─────────────────────────────────────────────
# 🔧 FIX: re-validasi harga sebelum open_trade, skip jika harga sudah gerak jauh
PRICE_DRIFT_MAX_PCT   = 0.003        # 0.3% — kalau harga sudah drift, skip kandidat

# ── CACHE ─────────────────────────────────────────────────────────
OHLCV_CACHE_TTL_5M    = 5
OHLCV_CACHE_TTL_15M   = 40
OHLCV_CACHE_TTL_1H    = 1800

# ── FILTER ────────────────────────────────────────────────────────
MIN_SCORE_TREND       = 50
MIN_SCORE_MEANREV     = 60
MIN_ENTRY_SIGNALS     = 2
MIN_VOL_SPIKE         = 1.2
MIN_FNG               = 20
MAX_FNG_LONG          = 90
MIN_BREADTH           = 0.28
MAX_SL_PCT            = 0.004

# ── SYMBOLS ──────────────────────────────────────────────────────
SYMBOLS = [
    # ── Tier 1 — Mega Cap ─────────────────────────────────────────
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",

    # ── Tier 2 — Large Cap ────────────────────────────────────────
    "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT", "ETCUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "TIAUSDT", "AAVEUSDT", "RUNEUSDT", "FILUSDT",
    "STXUSDT", "TONUSDT", "ICPUSDT", "HBARUSDT", "FLOWUSDT",

    # ── Tier 3 — Mid Cap ──────────────────────────────────────────
    "1000PEPEUSDT", "WIFUSDT", "JUPUSDT", "SEIUSDT", "PYTHUSDT",
    "FETUSDT", "RENDERUSDT", "WLDUSDT", "STRKUSDT", "ALTUSDT",
    "DYMUSDT", "MANTAUSDT", "ZETAUSDT", "RONINUSDT", "NOTUSDT",
    "EIGENUSDT", "CATIUSDT", "1000BONKUSDT", "MOVEUSDT", "MEUSDT",

    # ── DeFi ──────────────────────────────────────────────────────
    "CRVUSDT", "MKRUSDT", "COMPUSDT", "SUSHIUSDT", "SNXUSDT",
    "1INCHUSDT", "BALUSDT", "DYDXUSDT", "GMXUSDT", "PENDLEUSDT",
    "JTOUSDT", "RAYUSDT", "RDNTUSDT", "LQTYUSDT", "ANKRUSDT",

    # ── L1 / L2 ───────────────────────────────────────────────────
    "ALGOUSDT", "FTMUSDT", "EGLDUSDT", "THETAUSDT", "KSMUSDT",
    "KAVAUSDT", "BANDUSDT", "COTIUSDT", "SKLUSDT", "CELRUSDT",
    "CTSIUSDT", "IOTAUSDT", "ONEUSDT", "ZILUSDT", "ONTUSDT",

    # ── Gaming / NFT / Metaverse ──────────────────────────────────
    "AXSUSDT", "SANDUSDT", "MANAUSDT", "ENJUSDT", "GALAUSDT",
    "IMXUSDT", "BLURUSDT", "MASKUSDT", "HIGHUSDT", "BEAMXUSDT",
    "MEMEUSDT", "ORDIUSDT", "YGGUSDT", "SLPUSDT", "WAXPUSDT",

    # ── Infrastructure / Data / AI ────────────────────────────────
    "ARUSDT", "OCEANUSDT", "GRTUSDT", "AGIXUSDT", "NMRUSDT",
    "CTXCUSDT", "POLYXUSDT", "TRUUSDT", "BLZUSDT", "SXPUSDT",
    "VIDTUSDT", "DUSKUSDT", "MDTUSDT", "REQUSDT", "POWRUSDT",

    # ── Meme / Viral ──────────────────────────────────────────────
    "SHIBUSDT", "FLOKIUSDT", "BONKUSDT", "JASMYUSDT", "LUNCUSDT",
    "CFXUSDT", "COMBOUSDT", "AGLDUSDT", "IDUSDT", "GASUSDT",
    "1000RATSUSDT", "1000SATSUSDT", "TURBOUSDT", "BOMEUSDT", "POPCATUSDT",

    # ── Exchange / CeFi ───────────────────────────────────────────
    "CAKEUSDT", "GMTUSDT", "ACHUSDT", "HOOKUSDT", "MAGICUSDT",
    "HFTUSDT", "CYBERUSDT", "ARKUSDT", "PIVXUSDT", "STEEMUSDT",

    # ── Cross-chain / Bridge / Oracle ─────────────────────────────
    "API3USDT", "UMAUSDT", "BANDUSDT", "DIAUSDT", "OXTUSDT",
    "STMXUSDT", "IDEXUSDT", "BADGERUSDT", "ALPACAUSDT", "BNTUSDT",

    # ── Misc High-Volume ──────────────────────────────────────────
    "VETUSDT", "XTZUSDT", "NEOUSDT", "DASHUSDT", "ZECUSDT",
    "WAVESUSDT", "IOSTUSDT", "STORJUSDT", "CVCUSDT", "OGNUSDT",
]

# ════════════════════════════════════════════════════
#  STATE GLOBAL
# ════════════════════════════════════════════════════
open_positions      = {}
trade_log           = []
_ohlcv_cache        = {}
_sym_info           = {}
_sym_cooldown       = {}
_btc_price_history  = deque(maxlen=300)
_scan_batch_idx     = 0
_lock               = threading.Lock()
_executor           = ThreadPoolExecutor(max_workers=25)

# Pre-scan candidate queue — selalu terisi, siap pakai
_candidate_queue    = queue.PriorityQueue()  # (−score, symbol, direction, info)
_candidate_lock     = threading.Lock()
_candidate_cache    = {}   # symbol → (ts, direction, info) untuk deduplikasi

# Event queue untuk trigger re-scan instan setelah close
_rescan_queue       = queue.Queue()
_hot_symbols        = deque(maxlen=20)

_macro = {
    "fng": 50, "fng_label": "Neutral",
    "btc_trend_5m": "UNKNOWN",
    "btc_trend_15m": "UNKNOWN",
    "btc_trend_1h": "UNKNOWN",
    "market_breadth": 0.5,
    "news": "neutral",
    "scalp_mode": "TREND",
    "last_fng": 0, "last_btc": 0, "last_breadth": 0, "last_news": 0,
}

_stats = {
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "total_pnl": 0.0,
    "best_trade": 0.0,
    "worst_trade": 0.0,
    "tp1_hits": 0,
    "tp2_hits": 0,
    "sl_hits": 0,
    "zero_loss_cuts": 0,   # berapa kali zero-loss cut aktif
    "trail_stops": 0,       # berapa kali trailing stop kena
    "force_closes": 0,
    "rescans": 0,
    "session_start": time.time(),
}

BULL_TRENDS = {"BULL", "MILD_BULL"}
BEAR_TRENDS = {"BEAR", "MILD_BEAR"}


# ════════════════════════════════════════════════════
#  UTILS
# ════════════════════════════════════════════════════
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

def calc_qty(symbol, price):
    info = get_sym_info(symbol)
    raw  = (ORDER_USDT * LEVERAGE) / price
    return max(round_step(raw, info["step"]), info["minQty"])

def set_leverage(symbol):
    try: client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
    except: pass

def get_price(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return 0.0

def get_exchange_amt(symbol):
    try:
        for p in client.futures_position_information(symbol=symbol):
            amt = float(p["positionAmt"])
            if amt != 0: return amt
        return 0
    except: return None

def is_symbol_cooling_down(symbol):
    if symbol not in _sym_cooldown: return False
    return (time.time() - _sym_cooldown[symbol]) < SYMBOL_COOLDOWN_SEC

def set_symbol_cooldown(symbol):
    _sym_cooldown[symbol] = time.time()

def validate_symbols():
    try:
        valid = {s["symbol"] for s in client.futures_exchange_info()["symbols"]
                 if s["status"] == "TRADING"}
        result = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
        print(f"  ✅ {len(result)}/{len(SYMBOLS)} symbols valid")
        return result
    except:
        return list(dict.fromkeys(SYMBOLS))


# ════════════════════════════════════════════════════
#  OHLCV CACHE
# ════════════════════════════════════════════════════
def get_ohlcv(symbol, interval, limit=100):
    cache_key = (symbol, interval)
    now = time.time()
    ttl_map = {
        Client.KLINE_INTERVAL_5MINUTE:  OHLCV_CACHE_TTL_5M,
        Client.KLINE_INTERVAL_15MINUTE: OHLCV_CACHE_TTL_15M,
        Client.KLINE_INTERVAL_1HOUR:    OHLCV_CACHE_TTL_1H,
    }
    ttl = ttl_map.get(interval, 30)
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
            return _ohlcv_cache[cache_key][1]
        return None


# ════════════════════════════════════════════════════
#  TECHNICAL ANALYSIS
# ════════════════════════════════════════════════════
def run_ta(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    df["rsi"]       = ta.momentum.RSIIndicator(c, 14).rsi()
    df["rsi_fast"]  = ta.momentum.RSIIndicator(c, 7).rsi()
    macd            = ta.trend.MACD(c, 12, 26, 9)
    df["macd"]      = macd.macd()
    df["macd_sig"]  = macd.macd_signal()
    df["macd_hist"] = macd.macd_diff()
    df["ema5"]      = ta.trend.EMAIndicator(c, 5).ema_indicator()
    df["ema9"]      = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["ema21"]     = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["ema50"]     = ta.trend.EMAIndicator(c, 50).ema_indicator()
    bb              = ta.volatility.BollingerBands(c, 20, 2)
    df["bb_hi"]     = bb.bollinger_hband()
    df["bb_lo"]     = bb.bollinger_lband()
    df["bb_mid"]    = bb.bollinger_mavg()
    df["bb_width"]  = (df["bb_hi"] - df["bb_lo"]) / df["bb_mid"]
    stoch           = ta.momentum.StochasticOscillator(h, l, c, 14, 3)
    df["stk"]       = stoch.stoch()
    df["std"]       = stoch.stoch_signal()
    df["atr"]       = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["vol_ma"]    = v.rolling(20).mean()
    df["vol_ratio"] = v / df["vol_ma"].replace(0, 1)
    df["buy_ratio"] = df["tbbase"] / df["volume"].replace(0, 1)
    df["body"]      = abs(df["close"] - df["open"])
    df["range_"]    = df["high"] - df["low"]
    df["body_ratio"]= df["body"] / df["range_"].replace(0, 1)
    df["bb_squeeze"]= df["bb_width"] < df["bb_width"].rolling(20).mean() * 0.85
    return df

def _calc_trend(df):
    if df is None or len(df) < 25: return "UNKNOWN"
    c     = df["close"]
    price = c.iloc[-1]
    ema9  = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
    ema21 = ta.trend.EMAIndicator(c, 21).ema_indicator().iloc[-1]
    ema50 = ta.trend.EMAIndicator(c, 50).ema_indicator().iloc[-1]
    chg   = (price - c.iloc[-4]) / c.iloc[-4] * 100
    if price > ema9 > ema21 > ema50 and chg > 0:   return "BULL"
    elif price < ema9 < ema21 < ema50 and chg < 0: return "BEAR"
    elif price > ema21 and chg > -0.2:             return "MILD_BULL"
    elif price < ema21 and chg < 0.2:              return "MILD_BEAR"
    return "SIDEWAYS"


# ════════════════════════════════════════════════════
#  MACRO REFRESH
# ════════════════════════════════════════════════════
def refresh_macro():
    now = time.time()
    if now - _macro["last_fng"] > 300:
        try:
            d = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()["data"][0]
            _macro["fng"]       = int(d["value"])
            _macro["fng_label"] = d["value_classification"]
            _macro["last_fng"]  = now
        except: pass

    if now - _macro["last_btc"] > 10:
        try:
            df_5m  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 60)
            df_15m = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, 60)
            df_1h  = get_ohlcv("BTCUSDT", Client.KLINE_INTERVAL_1HOUR, 60)
            _macro["btc_trend_5m"]  = _calc_trend(df_5m)
            _macro["btc_trend_15m"] = _calc_trend(df_15m)
            _macro["btc_trend_1h"]  = _calc_trend(df_1h)
            _macro["last_btc"]      = now
            t5m  = _macro["btc_trend_5m"]
            t15m = _macro["btc_trend_15m"]
            if t15m in ("BULL","BEAR") or t5m in ("BULL","BEAR"):
                _macro["scalp_mode"] = "TREND"
            else:
                _macro["scalp_mode"] = "MEAN_REV"
        except: pass

    if now - _macro["last_breadth"] > 60:
        try:
            bullish = 0
            sample  = SYMBOLS[:20]
            for sym in sample:
                df = get_ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 10)
                if df is not None and len(df) >= 5:
                    c  = df["close"]
                    e9 = ta.trend.EMAIndicator(c, 9).ema_indicator().iloc[-1]
                    if c.iloc[-1] > e9: bullish += 1
            _macro["market_breadth"] = bullish / len(sample)
            _macro["last_breadth"]   = now
        except: pass

    if now - _macro.get("last_news", 0) > 120:
        try:
            data = requests.get(
                "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC",
                timeout=5).json()
            neg_kw = ["crash","hack","ban","fraud","collapse","seized","scam","plunge"]
            pos_kw = ["institutional","ath","approved","record","bullish","rally","surge"]
            neg = pos = 0
            for post in data.get("results", [])[:8]:
                tl = post.get("title","").lower()
                if any(w in tl for w in neg_kw): neg += 1
                if any(w in tl for w in pos_kw): pos += 1
            score = pos - neg
            if score <= -3:   _macro["news"] = "strong_negative"
            elif score <= -1: _macro["news"] = "negative"
            elif score >= 3:  _macro["news"] = "strong_positive"
            else:             _macro["news"] = "neutral"
            _macro["last_news"] = now
        except: pass

def update_btc_price():
    try:
        px = get_price("BTCUSDT")
        if px > 0: _btc_price_history.append((time.time(), px))
    except: pass

def detect_flash_move():
    if len(_btc_price_history) < 2: return "none", 0.0
    cutoff  = time.time() - 180
    oldest  = next((px for ts, px in _btc_price_history if ts >= cutoff), None)
    if oldest is None: return "none", 0.0
    current = _btc_price_history[-1][1]
    pct = (current - oldest) / oldest * 100
    if pct <= -0.8: return "crash", abs(pct)
    if pct >= 0.8:  return "pump",  abs(pct)
    return "none", 0.0


# ════════════════════════════════════════════════════
#  ORDER BOOK IMBALANCE
# ════════════════════════════════════════════════════
def get_ob_imbalance(symbol):
    try:
        ob    = client.futures_order_book(symbol=symbol, limit=20)
        bids  = sum(float(b[1]) for b in ob["bids"][:10])
        asks  = sum(float(a[1]) for a in ob["asks"][:10])
        total = bids + asks
        return round((bids - asks) / total, 3) if total else 0.0
    except: return 0.0


# ════════════════════════════════════════════════════
#  ENTRY SCORE ENGINE
# ════════════════════════════════════════════════════
def get_hft_entry_score(symbol, df_5m, direction):
    if df_5m is None or len(df_5m) < 30:
        return 0, []
    last  = df_5m.iloc[-1]
    prev  = df_5m.iloc[-2]
    prev2 = df_5m.iloc[-3]
    sigs  = []
    score = 0

    rsi      = last["rsi"]
    rsi_fast = last["rsi_fast"]
    rsi_prev = prev["rsi_fast"]

    if direction == "LONG":
        if rsi < 45 and rsi_fast > rsi_prev:
            score += 20; sigs.append(f"📈RSI bounce({rsi:.0f}↑)")
        elif rsi < 55 and rsi_fast > rsi_prev:
            score += 10; sigs.append(f"📈RSI ok({rsi:.0f})")
        elif rsi > 70:
            score -= 10
    else:
        if rsi > 55 and rsi_fast < rsi_prev:
            score += 20; sigs.append(f"📉RSI reject({rsi:.0f}↓)")
        elif rsi > 45 and rsi_fast < rsi_prev:
            score += 10; sigs.append(f"📉RSI ok({rsi:.0f})")
        elif rsi < 30:
            score -= 10

    h_now   = last["macd_hist"]
    h_prev  = prev["macd_hist"]
    h_prev2 = prev2["macd_hist"]

    if direction == "LONG":
        if h_now > 0 and h_now > h_prev > h_prev2:
            score += 20; sigs.append("✅MACD hist naik")
        elif h_now > h_prev and h_now > 0:
            score += 12; sigs.append("✅MACD pos")
        elif h_prev < 0 and h_now >= 0:
            score += 15; sigs.append("⚡MACD cross 0")
    else:
        if h_now < 0 and h_now < h_prev < h_prev2:
            score += 20; sigs.append("✅MACD hist turun")
        elif h_now < h_prev and h_now < 0:
            score += 12; sigs.append("✅MACD neg")
        elif h_prev > 0 and h_now <= 0:
            score += 15; sigs.append("⚡MACD cross 0")

    vr = last["vol_ratio"]
    br = last["buy_ratio"]
    if vr >= MIN_VOL_SPIKE:
        if direction == "LONG" and br > 0.55 and last["close"] > last["open"]:
            pts = min(20, int(vr * 8)); score += pts
            sigs.append(f"🔥Vol{vr:.1f}x(buy{br:.0%})")
        elif direction == "SHORT" and br < 0.45 and last["close"] < last["open"]:
            pts = min(20, int(vr * 8)); score += pts
            sigs.append(f"🔥Vol{vr:.1f}x(sell{1-br:.0%})")
        else:
            score += 5
    elif vr > 1.0:
        score += 3

    recent_5  = df_5m.iloc[-6:-1]
    micro_hi  = recent_5["high"].max()
    micro_lo  = recent_5["low"].min()
    price     = last["close"]

    if direction == "LONG" and price > micro_hi and last["body_ratio"] > 0.5:
        score += 15; sigs.append(f"🚀Break>{micro_hi:.5g}")
    elif direction == "SHORT" and price < micro_lo and last["body_ratio"] > 0.5:
        score += 15; sigs.append(f"💥Break<{micro_lo:.5g}")
    elif direction == "LONG" and price > (micro_hi + micro_lo) / 2:
        score += 7
    elif direction == "SHORT" and price < (micro_hi + micro_lo) / 2:
        score += 7

    e5  = last["ema5"]
    e9  = last["ema9"]
    e21 = last["ema21"]
    p5  = prev["ema5"]
    p9  = prev["ema9"]

    if direction == "LONG":
        if price > e5 > e9 > e21:
            score += 15; sigs.append("📐EMA bull stack")
        elif price > e5 > e9:
            score += 10; sigs.append("📐EMA5>9")
        elif (e5 > e9 and p5 <= p9):
            score += 8; sigs.append("📐EMA cross↑")
    else:
        if price < e5 < e9 < e21:
            score += 15; sigs.append("📐EMA bear stack")
        elif price < e5 < e9:
            score += 10; sigs.append("📐EMA5<9")
        elif (e5 < e9 and p5 >= p9):
            score += 8; sigs.append("📐EMA cross↓")

    squeeze = bool(last["bb_squeeze"])
    if direction == "LONG":
        if price <= last["bb_lo"] * 1.002:
            score += 10; sigs.append("🎯BB bounce lo")
        elif squeeze and last["close"] > last["open"] and last["close"] > prev["high"]:
            score += 10; sigs.append("💥BB squeeze↑")
        elif price < last["bb_mid"]:
            score += 3
    else:
        if price >= last["bb_hi"] * 0.998:
            score += 10; sigs.append("🎯BB bounce hi")
        elif squeeze and last["close"] < last["open"] and last["close"] < prev["low"]:
            score += 10; sigs.append("💥BB squeeze↓")
        elif price > last["bb_mid"]:
            score += 3

    if direction == "LONG" and last["close"] > last["open"] and \
       last["close"] > prev["high"] and last["body_ratio"] > 0.65:
        score += 8; sigs.append("🕯️Engulf↑")
    elif direction == "SHORT" and last["close"] < last["open"] and \
         last["close"] < prev["low"] and last["body_ratio"] > 0.65:
        score += 8; sigs.append("🕯️Engulf↓")

    k, d_ = last["stk"], last["std"]
    pk    = prev["stk"]
    pd_   = prev["std"]
    if direction == "LONG" and k > d_ and pk <= pd_ and k < 75:
        score += 5; sigs.append(f"⚡Stoch GX({k:.0f})")
    elif direction == "SHORT" and k < d_ and pk >= pd_ and k > 25:
        score += 5; sigs.append(f"⚡Stoch DX({k:.0f})")

    return max(0, min(score, 100)), sigs


def determine_direction(df_5m, df_15m=None):
    if df_5m is None or len(df_5m) < 20: return None
    last   = df_5m.iloc[-1]
    prev   = df_5m.iloc[-2]
    price  = last["close"]
    e5, e9 = last["ema5"], last["ema9"]
    long_pts = short_pts = 0

    if price > e5 > e9:  long_pts  += 3
    elif price < e5 < e9: short_pts += 3
    if last["macd_hist"] > prev["macd_hist"]: long_pts  += 2
    else:                                     short_pts += 2
    rsi = last["rsi"]
    if rsi < 50:   long_pts  += 1
    elif rsi > 50: short_pts += 1
    if last["buy_ratio"] > 0.52 and last["close"] > last["open"]:  long_pts  += 2
    elif last["buy_ratio"] < 0.48 and last["close"] < last["open"]: short_pts += 2
    if df_15m is not None and len(df_15m) >= 20:
        l15 = df_15m.iloc[-1]
        if l15["ema9"] > l15["ema21"]: long_pts  += 2
        else:                          short_pts += 2
    btc_t = _macro.get("btc_trend_5m", "UNKNOWN")
    if btc_t in BULL_TRENDS:  long_pts  += 2
    elif btc_t in BEAR_TRENDS: short_pts += 2

    if long_pts > short_pts and long_pts >= 5:  return "LONG"
    if short_pts > long_pts and short_pts >= 5: return "SHORT"
    return None


# ════════════════════════════════════════════════════
#  ENTRY FILTER
# ════════════════════════════════════════════════════
def should_enter_hft(symbol):
    if is_symbol_cooling_down(symbol):
        return None, "cooldown"
    fng  = _macro["fng"]
    news = _macro["news"]
    if fng < MIN_FNG:              return None, f"F&G={fng}"
    if news == "strong_negative":  return None, "bad_news"

    flash_dir, _ = detect_flash_move()
    if flash_dir != "none":        return None, f"flash_{flash_dir}"

    df_5m  = get_ohlcv(symbol, Client.KLINE_INTERVAL_5MINUTE, 80)
    df_15m = get_ohlcv(symbol, Client.KLINE_INTERVAL_15MINUTE, 60)
    if df_5m is None or len(df_5m) < 30: return None, "no_data"

    df_5m  = run_ta(df_5m.copy())
    if df_15m is not None and len(df_15m) >= 20:
        df_15m = run_ta(df_15m.copy())

    direction = determine_direction(df_5m, df_15m)
    if direction is None: return None, "no_direction"

    btc_5m  = _macro["btc_trend_5m"]
    btc_15m = _macro["btc_trend_15m"]
    if direction == "LONG"  and btc_5m in BEAR_TRENDS and btc_15m in BEAR_TRENDS:
        return None, f"skip LONG — BTC {btc_5m}"
    if direction == "SHORT" and btc_5m in BULL_TRENDS and btc_15m in BULL_TRENDS:
        return None, f"skip SHORT — BTC {btc_5m}"
    if direction == "LONG"  and fng > MAX_FNG_LONG:
        return None, f"overbought F&G={fng}"

    score, sigs = get_hft_entry_score(symbol, df_5m, direction)
    min_score   = MIN_SCORE_MEANREV if _macro["scalp_mode"] == "MEAN_REV" else MIN_SCORE_TREND
    if score < min_score: return None, f"score={score:.0f}<{min_score}"
    if len(sigs) < MIN_ENTRY_SIGNALS: return None, f"signals={len(sigs)}"

    atr     = df_5m["atr"].iloc[-1]
    price   = df_5m["close"].iloc[-1]
    atr_pct = atr / price
    if atr_pct > MAX_SL_PCT * 2: return None, f"ATR besar({atr_pct*100:.2f}%)"

    ob_imb = get_ob_imbalance(symbol)
    if direction == "LONG"  and ob_imb < -0.25: return None, f"OB imbal SHORT({ob_imb:.2f})"
    if direction == "SHORT" and ob_imb > 0.25:  return None, f"OB imbal LONG({ob_imb:.2f})"

    entry = price
    if direction == "LONG":
        sl  = round(entry * (1 - SL_PCT), 8)
        tp1 = round(entry * (1 + TP1_PCT), 8)
        tp2 = round(entry * (1 + TP2_PCT), 8)
    else:
        sl  = round(entry * (1 + SL_PCT), 8)
        tp1 = round(entry * (1 - TP1_PCT), 8)
        tp2 = round(entry * (1 - TP2_PCT), 8)

    return direction, {
        "score":          score,
        "signals":        sigs,
        "direction":      direction,
        "sl":             sl,
        "tp1":            tp1,
        "tp2":            tp2,
        "ob_imb":         ob_imb,
        "atr_pct":        atr_pct,
        "scalp_mode":     _macro["scalp_mode"],
        "btc_trend":      _macro["btc_trend_5m"],
        "entry_approx":   entry,   # 🔧 FIX: simpan harga saat scan untuk drift check
    }


# ════════════════════════════════════════════════════
#  PARALLEL SCANNER
# ════════════════════════════════════════════════════
def scan_symbol_safe(symbol):
    try:
        time.sleep(SCAN_DELAY_MS)
        direction, info = should_enter_hft(symbol)
        if direction: return symbol, direction, info
    except: pass
    return None

def scan_batch_parallel(symbols):
    candidates = []
    futures = {_executor.submit(scan_symbol_safe, sym): sym for sym in symbols}
    for future in as_completed(futures, timeout=8):
        result = future.result()
        if result: candidates.append(result)
    return candidates


# ════════════════════════════════════════════════════
#  ⚡ PRE-SCAN ENGINE (FITUR BARU v13)
#  Background thread yang SELALU menjaga kandidat siap
# ════════════════════════════════════════════════════
def pre_scan_engine(symbols_active):
    """
    Background engine yang terus-menerus scan semua symbol
    dan menjaga PRE_SCAN_CANDIDATES terbaik selalu siap di antrian.
    Ketika posisi close, kandidat langsung tersedia tanpa tunggu.
    """
    global _scan_batch_idx
    total_batches = math.ceil(len(symbols_active) / BATCH_SIZE)

    while True:
        try:
            # Tentukan symbol yang belum di posisi dan tidak cooldown
            available = [s for s in symbols_active
                         if s not in open_positions and not is_symbol_cooling_down(s)]

            if not available:
                time.sleep(PRE_SCAN_INTERVAL)
                continue

            # Prioritas: hot symbols dulu
            hot = [s for s in list(_hot_symbols) if s in available]
            rest = [s for s in available if s not in hot]

            # Rotate batch
            batch_start = _scan_batch_idx * BATCH_SIZE
            batch = (hot + rest[batch_start:batch_start + BATCH_SIZE])[:BATCH_SIZE]
            _scan_batch_idx = (_scan_batch_idx + 1) % total_batches

            candidates = scan_batch_parallel(batch)

            if candidates:
                candidates.sort(key=lambda x: x[2].get("score", 0), reverse=True)

                # Update candidate cache dengan deduplikasi
                with _candidate_lock:
                    now = time.time()
                    for sym, direction, info in candidates:
                        _candidate_cache[sym] = (now, direction, info)

                    # 🔧 FIX: Rebuild priority queue dari cache terbaru
                    # Staleness check naik dari 10s → CANDIDATE_TTL_PRESCAN (40s)
                    fresh = [(sym, ts, direction, info)
                             for sym, (ts, direction, info) in _candidate_cache.items()
                             if now - ts < CANDIDATE_TTL_PRESCAN
                             and sym not in open_positions]
                    fresh.sort(key=lambda x: x[3].get("score", 0), reverse=True)

                    # Clear dan rebuild queue
                    while not _candidate_queue.empty():
                        try: _candidate_queue.get_nowait()
                        except: pass

                    for sym, ts, direction, info in fresh[:PRE_SCAN_CANDIDATES * 2]:
                        _candidate_queue.put((-info.get("score", 0), sym, direction, info))

                n_queue = _candidate_queue.qsize()
                top_sym = fresh[0][0] if fresh else "—"
                top_sc  = fresh[0][3].get("score", 0) if fresh else 0
                print(f"  🔮 Pre-scan: {len(candidates)} found | Queue:{n_queue} | "
                      f"Top:{top_sym}({top_sc:.0f})")

        except Exception as e:
            print(f"  ❌ Pre-scan error: {e}")

        time.sleep(PRE_SCAN_INTERVAL)


def get_best_candidate(exclude_symbols=None):
    """
    Ambil kandidat terbaik dari antrian pre-scan.
    Exclude symbol yang sudah di posisi atau cooldown.

    🔧 FIX: TTL naik dari 12s → CANDIDATE_TTL_VALID (45s)
    supaya kandidat tidak expire sebelum main loop sempat pakai.
    """
    exclude = set(exclude_symbols or []) | set(open_positions.keys())
    with _candidate_lock:
        now = time.time()
        # Bersihkan stale candidates dari cache (pakai TTL paling longgar)
        stale = [k for k, (ts, _, _) in _candidate_cache.items()
                 if now - ts > CANDIDATE_TTL_STALE]
        for k in stale:
            del _candidate_cache[k]

        # Cari kandidat terbaik yang masih valid
        fresh = [(sym, ts, direction, info)
                 for sym, (ts, direction, info) in _candidate_cache.items()
                 if sym not in exclude and not is_symbol_cooling_down(sym)
                 and now - ts < CANDIDATE_TTL_VALID]   # 🔧 FIX: 45s bukan 12s
        if not fresh:
            return None
        fresh.sort(key=lambda x: x[3].get("score", 0), reverse=True)
        return fresh[0][0], fresh[0][2], fresh[0][3]  # (symbol, direction, info)


# ════════════════════════════════════════════════════
#  ⚡ TRAIL MECHANICS v13
# ════════════════════════════════════════════════════
def calc_trail_sl(pos, current_price):
    """
    Hitung trailing stop berdasarkan profit saat ini.

    Rules:
    - Profit 0.01%+: trail TEPAT di entry (zero-loss lock)
    - Profit 0.30%+: trail 0.06% di bawah peak
    - Profit 0.50%+: trail 0.04% di bawah peak
    - Profit 0.80%+: trail 0.03% di bawah peak (ultra ketat)

    Returns: new_trail_sl (atau None kalau belum profit)
    """
    side  = pos["side"]
    entry = pos["entry"]

    if side == "LONG":
        profit_pct = (current_price - entry) / entry
    else:
        profit_pct = (entry - current_price) / entry

    # Belum profit sama sekali → trail belum aktif
    if profit_pct < PROFIT_TRIGGER_TRAIL:
        return None, 0

    # Tentukan trail distance berdasarkan profit level
    trail_dist = 0.0
    for threshold, dist in reversed(TRAIL_PHASES):
        if profit_pct >= threshold:
            trail_dist = dist
            break

    # Hitung trailing SL
    peak = pos.get("peak", entry)
    if side == "LONG":
        if trail_dist == 0.0:
            # Fase awal: trail TEPAT di entry (zero-loss)
            new_trail_sl = entry
        else:
            new_trail_sl = peak * (1 - trail_dist)
            # Tidak boleh di bawah entry setelah trail aktif
            new_trail_sl = max(new_trail_sl, entry)
    else:
        if trail_dist == 0.0:
            new_trail_sl = entry
        else:
            new_trail_sl = peak * (1 + trail_dist)
            new_trail_sl = min(new_trail_sl, entry)

    return new_trail_sl, trail_dist


# ════════════════════════════════════════════════════
#  TRADE EXECUTION
# ════════════════════════════════════════════════════
def open_trade(symbol, direction, info):
    with _lock:
        if symbol in open_positions: return
        if len(open_positions) >= MAX_POSITIONS: return
    try:
        set_leverage(symbol)
        price = get_price(symbol)
        if price == 0: return
        qty = calc_qty(symbol, price)

        client.futures_create_order(
            symbol=symbol,
            side=SIDE_BUY if direction == "LONG" else SIDE_SELL,
            type=ORDER_TYPE_MARKET,
            quantity=qty)

        entry = get_price(symbol)

        # ─── ZERO-LOSS THRESHOLD ───────────────────────────────────
        if direction == "LONG":
            zero_loss_price = entry * (1 - ZERO_LOSS_TICK_PCT)   # 0.01% minus → cut
            sl_hard         = round(entry * (1 - SL_PCT), 8)
            tp1             = round(entry * (1 + TP1_PCT), 8)
            tp2             = round(entry * (1 + TP2_PCT), 8)
        else:
            zero_loss_price = entry * (1 + ZERO_LOSS_TICK_PCT)
            sl_hard         = round(entry * (1 + SL_PCT), 8)
            tp1             = round(entry * (1 - TP1_PCT), 8)
            tp2             = round(entry * (1 - TP2_PCT), 8)

        with _lock:
            open_positions[symbol] = {
                "side":             direction,
                "entry":            entry,
                "qty":              qty,
                "qty_remain":       qty,
                "sl":               sl_hard,
                "tp1":              tp1,
                "tp2":              tp2,
                "peak":             entry,       # peak price sejauh ini
                "trail_sl":         None,        # None = trail belum aktif
                "trail_active":     False,       # trail belum aktif
                "trail_updates":    0,           # berapa kali trail di-update
                "trail_max_profit": 0.0,         # max profit yang dicapai
                "tp1_hit":          False,
                "be_active":        False,
                "open_time":        time.time(),
                "score":            info.get("score", 0),
                "signals":          info.get("signals", []),
                "zero_loss_price":  zero_loss_price,  # threshold zero-loss
                "partial_pnl":      0.0,          # PnL dari partial close
                "tp1_count":        0,
                "tp2_count":        0,
                "trail_tp_count":   0,            # berapa kali trailing stop kena TP
            }

        sl_p  = abs(entry - sl_hard) / entry * 100
        tp1_p = abs(tp1 - entry)     / entry * 100
        tp2_p = abs(tp2 - entry)     / entry * 100
        sig_str = " | ".join(info.get("signals", [])[:3])

        print(f"\n  {'🟢' if direction=='LONG' else '🔴'} ENTRY [{symbol}] {direction} @{entry:.6g}")
        print(f"     💀 ZeroLoss:{ZERO_LOSS_TICK_PCT*100:.2f}% | 🛑 SL:{sl_p:.2f}% | 🎯 TP1:{tp1_p:.2f}% | ✨ TP2:{tp2_p:.2f}%")
        print(f"     📈 Trail: aktif saat profit {PROFIT_TRIGGER_TRAIL*100:.2f}% (tepat di entry)")
        print(f"     Score:{info['score']:.0f} | {sig_str}")
        _stats["total_trades"] += 1

        # Update candidate cache: hapus symbol ini (sudah dipakai)
        with _candidate_lock:
            _candidate_cache.pop(symbol, None)

    except Exception as e:
        print(f"  ❌ [{symbol}] Entry error: {e}")


def partial_close_tp1(symbol):
    pos = open_positions.get(symbol)
    if pos is None or pos.get("tp1_hit"): return
    try:
        amt = get_exchange_amt(symbol)
        if amt is None or amt == 0:
            pos["tp1_hit"] = True; return

        close_qty = round_step(abs(amt) * TP1_CLOSE_RATIO, get_sym_info(symbol)["step"])
        close_qty = max(close_qty, get_sym_info(symbol)["minQty"])
        if close_qty > abs(amt): close_qty = abs(amt)

        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt > 0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=close_qty,
            reduceOnly=True)

        exit_p  = get_price(symbol)
        side    = pos["side"]
        pnl     = (exit_p - pos["entry"]) * close_qty if side == "LONG" \
                  else (pos["entry"] - exit_p) * close_qty
        hold_s  = time.time() - pos["open_time"]
        pct     = abs(exit_p - pos["entry"]) / pos["entry"] * 100

        print(f"\n  🎯 [{symbol}] TP1 PARTIAL ({hold_s:.0f}s)")
        print(f"     Entry:{pos['entry']:.6g} → Exit:{exit_p:.6g} ({pct:+.3f}%)")
        print(f"     Qty:{close_qty} | PnL: +{pnl:.4f}U | Sisa: {abs(amt)-close_qty:.4g}")

        pos["tp1_hit"]    = True
        pos["tp1_count"]  = pos.get("tp1_count", 0) + 1
        pos["qty_remain"] = abs(amt) - close_qty
        pos["be_active"]  = True
        pos["sl"]         = pos["entry"]    # SL ke breakeven
        pos["partial_pnl"] += pnl
        pos["peak"]       = exit_p
        # Trail setelah TP1 langsung phase 2 (0.06%)
        pos["trail_sl"]   = exit_p * (1 - 0.0006) if side == "LONG" \
                            else exit_p * (1 + 0.0006)
        pos["trail_active"] = True

        _stats["tp1_hits"]  += 1
        _stats["wins"]      += 1
        _stats["total_pnl"] += pnl
        if pnl > _stats["best_trade"]: _stats["best_trade"] = pnl

        trade_log.append({
            "symbol": symbol, "side": side,
            "pnl": round(pnl, 4), "reason": "TP1 Partial",
            "hold_sec": int(hold_s), "entry": pos["entry"], "exit": exit_p,
        })
        _hot_symbols.appendleft(symbol)
        print_stats_inline()
    except Exception as e:
        print(f"  ❌ [{symbol}] TP1 error: {e}")
        pos["tp1_hit"] = True


def close_trade(symbol, reason=""):
    """
    Close posisi dan tampilkan laporan lengkap.
    """
    try:
        amt = get_exchange_amt(symbol)
        if amt is None: return False
        if amt == 0:
            with _lock: open_positions.pop(symbol, None)
            set_symbol_cooldown(symbol)
            _trigger_entry_from_queue()
            return True

        info      = get_sym_info(symbol)
        close_qty = round_step(abs(amt), info["step"])
        close_qty = max(close_qty, info["minQty"])
        close_qty = min(close_qty, round_step(abs(amt), info["step"]))  # tidak melebihi posisi

        if close_qty <= 0:
            print(f"  ⚠️ [{symbol}] close_qty=0 setelah round, skip close")
            with _lock: open_positions.pop(symbol, None)
            set_symbol_cooldown(symbol)
            return False

        client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL if amt > 0 else SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=close_qty,
            reduceOnly=True)

        with _lock:
            pos = open_positions.pop(symbol, None)

        if pos:
            exit_p      = get_price(symbol)
            qty_r       = pos.get("qty_remain", pos["qty"])
            side        = pos["side"]
            pnl_close   = (exit_p - pos["entry"]) * qty_r if side == "LONG" \
                          else (pos["entry"] - exit_p) * qty_r
            partial_pnl = pos.get("partial_pnl", 0.0)
            total_pnl   = pnl_close + partial_pnl
            pct         = (exit_p - pos["entry"]) / pos["entry"] * 100 if side == "LONG" \
                          else (pos["entry"] - exit_p) / pos["entry"] * 100
            hold_s      = time.time() - pos["open_time"]
            hold_str    = f"{int(hold_s//60)}m{int(hold_s%60)}s"
            emoji       = "💰" if total_pnl >= 0 else "💸"
            be_tag      = "[BE]" if pos.get("be_active") else ""
            trail_cnt   = pos.get("trail_updates", 0)
            tp1_c       = pos.get("tp1_count", 0)
            tp2_c       = pos.get("tp2_count", 0) + (1 if "TP2" in reason else 0)

            # ═══ LAPORAN DETAIL CLOSE ═══════════════════════════════
            print(f"\n  {'═'*62}")
            print(f"  {emoji} CLOSE [{symbol}] {side} — {reason}{be_tag}")
            print(f"  {'═'*62}")
            print(f"  📍 Entry  : {pos['entry']:.6g}")
            print(f"  📍 Exit   : {exit_p:.6g} ({pct:+.3f}%)")
            print(f"  ⏱️  Durasi : {hold_str}")
            print(f"  💵 PnL Close : {pnl_close:+.4f}U")
            if partial_pnl != 0:
                print(f"  💵 PnL Partial (TP1) : {partial_pnl:+.4f}U")
            print(f"  {'─'*30}")
            print(f"  💰 TOTAL PnL : {total_pnl:+.4f}U  {'✅ PROFIT' if total_pnl > 0 else '❌ LOSS'}")
            print(f"  📊 TP1 hits  : {tp1_c}x")
            print(f"  📊 TP2 hits  : {tp2_c}x")
            print(f"  📈 Trail updates: {trail_cnt}x (trail jalan {trail_cnt} kali)")
            print(f"  🏆 Score entry: {pos.get('score',0):.0f}")
            sigs = pos.get("signals", [])
            if sigs:
                print(f"  🔍 Signals : {' | '.join(sigs[:3])}")
            print(f"  {'═'*62}\n")

            trade_log.append({
                "symbol": symbol, "side": side,
                "entry": pos["entry"], "exit": exit_p,
                "pnl": round(total_pnl, 4), "reason": reason,
                "hold_sec": int(hold_s),
                "tp1_count": tp1_c,
                "tp2_count": tp2_c,
                "trail_updates": trail_cnt,
                "score": pos.get("score", 0),
            })
            _stats["total_pnl"] += total_pnl

            if total_pnl >= 0:
                _stats["wins"] += 1
                if total_pnl > _stats["best_trade"]: _stats["best_trade"] = total_pnl
            else:
                _stats["losses"] += 1
                if total_pnl < _stats["worst_trade"]: _stats["worst_trade"] = total_pnl

            if "TP2"      in reason: _stats["tp2_hits"]     += 1
            if "SL"       in reason or "Stop" in reason: _stats["sl_hits"] += 1
            if "Force"    in reason: _stats["force_closes"] += 1
            if "ZeroLoss" in reason: _stats["zero_loss_cuts"] += 1
            if "Trail"    in reason: _stats["trail_stops"]   += 1

            print_stats_inline()
            set_symbol_cooldown(symbol)
            _hot_symbols.appendleft(symbol)

            # ⚡ LANGSUNG ambil kandidat dari pre-scan queue
            _trigger_entry_from_queue()

        return True
    except Exception as e:
        print(f"  ❌ [{symbol}] Close error: {e}")
        return False


def _trigger_entry_from_queue():
    """
    Segera setelah close, cek apakah ada slot dan kandidat siap.
    Ambil dari pre-scan queue (zero latency, sudah dianalisis).
    """
    time.sleep(RE_SCAN_DELAY_SEC)
    slots_free = MAX_POSITIONS - len(open_positions)
    if slots_free <= 0: return

    flash_dir, _ = detect_flash_move()
    if flash_dir != "none": return
    if _macro["news"] == "strong_negative": return

    _stats["rescans"] += 1
    for _ in range(slots_free):
        if len(open_positions) >= MAX_POSITIONS: break
        result = get_best_candidate()
        if result is None:
            print(f"  ⏳ Queue kosong — menunggu pre-scan mengisi kandidat...")
            break
        sym, direction, info = result

        # 🔧 FIX: Re-validasi harga sebelum entry — skip jika drift >PRICE_DRIFT_MAX_PCT
        current_price   = get_price(sym)
        cached_price    = info.get("entry_approx", current_price)
        if cached_price > 0 and current_price > 0:
            drift = abs(current_price - cached_price) / cached_price
            if drift > PRICE_DRIFT_MAX_PCT:
                print(f"  ⚠️ [{sym}] Harga drift {drift*100:.2f}% — kandidat di-skip, hapus dari cache")
                with _candidate_lock:
                    _candidate_cache.pop(sym, None)
                continue

        sig_str = " | ".join(info.get("signals", [])[:3])
        print(f"  ⚡ QUEUE ENTRY: {sym} {direction} Score:{info['score']:.0f} | {sig_str}")
        # Run di thread terpisah supaya tidak blocking monitor
        threading.Thread(target=open_trade, args=(sym, direction, info), daemon=True).start()


# ════════════════════════════════════════════════════
#  ⚡ POSITION MONITOR v13 (1 detik, zero-loss + instant trail)
# ════════════════════════════════════════════════════
def manage_positions():
    if not open_positions: return
    flash_dir, flash_pct = detect_flash_move()

    for symbol in list(open_positions.keys()):
        pos = open_positions.get(symbol)
        if pos is None: continue

        price = get_price(symbol)
        if price == 0: continue

        side  = pos["side"]
        entry = pos["entry"]
        hold_min = (time.time() - pos["open_time"]) / 60

        # ── FORCE CLOSE: timeout ─────────────────────────
        if hold_min >= MAX_HOLDING_MIN:
            close_trade(symbol, f"⏰Force({hold_min:.0f}m)")
            continue

        # ── FLASH CRASH EXIT ──────────────────────────────
        if flash_dir == "crash" and side == "LONG":
            close_trade(symbol, f"⚡FlashCrash-{flash_pct:.1f}%")
            continue
        elif flash_dir == "pump" and side == "SHORT":
            close_trade(symbol, f"⚡FlashPump+{flash_pct:.1f}%")
            continue

        # ════════════════════════════════════════════════
        # ⚡ ZERO-LOSS TOLERANCE (FITUR UTAMA v13)
        # Minus satu angka (0.01%) → CUT SEKARANG
        # ════════════════════════════════════════════════
        if ZERO_LOSS_ALWAYS and not pos.get("tp1_hit"):
            zlp = pos["zero_loss_price"]
            if side == "LONG" and price <= zlp:
                loss_pct = (entry - price) / entry * 100
                close_trade(symbol, f"🔴ZeroLoss(-{loss_pct:.3f}%)")
                continue
            elif side == "SHORT" and price >= zlp:
                loss_pct = (price - entry) / entry * 100
                close_trade(symbol, f"🔴ZeroLoss(-{loss_pct:.3f}%)")
                continue

        if side == "LONG":
            profit_pct = (price - entry) / entry

            # ── Update peak ──────────────────────────────
            if price > pos["peak"]:
                pos["peak"] = price
                if profit_pct > pos.get("trail_max_profit", 0):
                    pos["trail_max_profit"] = profit_pct

            # ── INSTANT TRAIL ACTIVATION ─────────────────
            # Profit 0.01% → trail langsung aktif di entry
            if profit_pct >= PROFIT_TRIGGER_TRAIL:
                new_trail_sl, trail_dist = calc_trail_sl(pos, price)
                if new_trail_sl is not None:
                    old_trail = pos.get("trail_sl")
                    # Trail hanya naik, TIDAK turun
                    if old_trail is None or new_trail_sl > old_trail:
                        pos["trail_sl"]      = new_trail_sl
                        pos["trail_active"]  = True
                        pos["trail_updates"] += 1

            # ── TP1: partial close ──────────────────────
            if not pos["tp1_hit"] and price >= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            # ── TP2 full close ──────────────────────────
            if pos["tp1_hit"] and price >= pos["tp2"]:
                pos["tp2_count"] = pos.get("tp2_count", 0) + 1
                close_trade(symbol, "✨TP2")
                continue

            # ── Trailing stop hit ───────────────────────
            if pos.get("trail_active") and pos["trail_sl"] is not None:
                if price <= pos["trail_sl"]:
                    be_tag = "[BE]" if pos.get("be_active") else ""
                    if pos.get("be_active"):
                        close_trade(symbol, f"🔒TrailBE{be_tag}")
                    else:
                        close_trade(symbol, f"🔄TrailStop(trail×{pos['trail_updates']}){be_tag}")
                    continue

            # ── Hard SL (backup kalau trail belum aktif) ─
            if price <= pos["sl"]:
                close_trade(symbol, "🛑SL")
                continue

            # ── Status print ────────────────────────────
            trail_info = ""
            if pos.get("trail_active") and pos["trail_sl"] is not None:
                trail_pct  = (price - pos["trail_sl"]) / price * 100
                trail_info = f" | TSL:{pos['trail_sl']:.6g}({trail_pct:.2f}%gap) ×{pos['trail_updates']}"
            else:
                trail_info = f" | Trail:WAITING(profit<{PROFIT_TRIGGER_TRAIL*100:.2f}%)"
            pnl_pct = profit_pct * 100
            pnl_u   = profit_pct * entry * pos.get("qty_remain", pos["qty"])
            tp_next = f"TP2:{pos['tp2']:.6g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.6g}"
            print(f"  📌 [{symbol}] L@{entry:.5g}→{price:.5g} ({pnl_pct:+.3f}%) "
                  f"| {pnl_u:+.3f}U | {hold_min:.1f}m{trail_info} | {tp_next}")

        else:  # SHORT
            profit_pct = (entry - price) / entry

            if price < pos["peak"]:
                pos["peak"] = price
                if profit_pct > pos.get("trail_max_profit", 0):
                    pos["trail_max_profit"] = profit_pct

            if profit_pct >= PROFIT_TRIGGER_TRAIL:
                new_trail_sl, trail_dist = calc_trail_sl(pos, price)
                if new_trail_sl is not None:
                    old_trail = pos.get("trail_sl")
                    if old_trail is None or new_trail_sl < old_trail:
                        pos["trail_sl"]      = new_trail_sl
                        pos["trail_active"]  = True
                        pos["trail_updates"] += 1

            if not pos["tp1_hit"] and price <= pos["tp1"]:
                partial_close_tp1(symbol)
                continue

            if pos["tp1_hit"] and price <= pos["tp2"]:
                pos["tp2_count"] = pos.get("tp2_count", 0) + 1
                close_trade(symbol, "✨TP2")
                continue

            if pos.get("trail_active") and pos["trail_sl"] is not None:
                if price >= pos["trail_sl"]:
                    be_tag = "[BE]" if pos.get("be_active") else ""
                    if pos.get("be_active"):
                        close_trade(symbol, f"🔒TrailBE{be_tag}")
                    else:
                        close_trade(symbol, f"🔄TrailStop(trail×{pos['trail_updates']}){be_tag}")
                    continue

            if price >= pos["sl"]:
                close_trade(symbol, "🛑SL")
                continue

            trail_info = ""
            if pos.get("trail_active") and pos["trail_sl"] is not None:
                trail_pct  = (pos["trail_sl"] - price) / price * 100
                trail_info = f" | TSL:{pos['trail_sl']:.6g}({trail_pct:.2f}%gap) ×{pos['trail_updates']}"
            else:
                trail_info = f" | Trail:WAITING"
            pnl_pct = profit_pct * 100
            pnl_u   = profit_pct * entry * pos.get("qty_remain", pos["qty"])
            tp_next = f"TP2:{pos['tp2']:.6g}" if pos["tp1_hit"] else f"TP1:{pos['tp1']:.6g}"
            print(f"  📌 [{symbol}] S@{entry:.5g}→{price:.5g} ({pnl_pct:+.3f}%) "
                  f"| {pnl_u:+.3f}U | {hold_min:.1f}m{trail_info} | {tp_next}")


# ════════════════════════════════════════════════════
#  POSITION MONITOR THREAD (1 detik — ultra fast)
# ════════════════════════════════════════════════════
def position_monitor_thread():
    while True:
        try:
            if open_positions:
                manage_positions()
        except Exception as e:
            print(f"  ❌ Monitor error: {e}")
        time.sleep(POSITION_MONITOR_SEC)


# ════════════════════════════════════════════════════
#  STATS
# ════════════════════════════════════════════════════
def print_stats_inline():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    pnl  = _stats["total_pnl"]
    sess = (time.time() - _stats["session_start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    bar  = ("█" * _stats["wins"] + "░" * _stats["losses"])[-20:]
    emoji = "💚" if pnl >= 0 else "🔴"
    print(f"     ┌─ 📊 {n}T | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']} | {emoji}PnL:{pnl:+.4f}U | {tph:.0f}T/h")
    print(f"     └─ TP1:{_stats['tp1_hits']} TP2:{_stats['tp2_hits']} SL:{_stats['sl_hits']} 🔴ZL:{_stats['zero_loss_cuts']} Trail:{_stats['trail_stops']} | Best:{_stats['best_trade']:+.3f} [{bar}]")

def print_stats():
    n    = _stats["wins"] + _stats["losses"]
    wr   = _stats["wins"] / n * 100 if n else 0
    sess = (time.time() - _stats["session_start"]) / 3600
    tph  = n / sess if sess > 0 else 0
    pnl  = _stats["total_pnl"]
    emoji = "💚" if pnl >= 0 else "🔴"

    print(f"\n  {'═'*64}")
    print(f"  ⚡ SESSION {sess*60:.0f}m | {tph:.0f} trades/jam | Pre-scans: {_stats['rescans']}")
    print(f"  🎯 {n} trades | WR:{wr:.0f}% | W:{_stats['wins']} L:{_stats['losses']}")
    print(f"  {emoji} Total P&L : {pnl:+.4f} USDT")
    print(f"  📈 Best:{_stats['best_trade']:+.4f}U │ 📉 Worst:{_stats['worst_trade']:+.4f}U")
    print(f"  🎯TP1:{_stats['tp1_hits']} │ ✨TP2:{_stats['tp2_hits']} │ 🛑SL:{_stats['sl_hits']}")
    print(f"  🔴ZeroLoss:{_stats['zero_loss_cuts']} │ 🔄Trail:{_stats['trail_stops']} │ ⏰Force:{_stats['force_closes']}")
    if trade_log:
        print(f"  {'─'*64}")
        print(f"  📋 Last 5 Trades:")
        for t in trade_log[-5:]:
            e    = "🟢" if t["pnl"] > 0 else "🔴"
            secs = t.get("hold_sec", 0)
            hold = f"{secs//60}m{secs%60}s"
            tp1c = t.get("tp1_count", 0)
            tp2c = t.get("tp2_count", 0)
            trc  = t.get("trail_updates", 0)
            print(f"     {e} {t['symbol']:<14} {t['side']} {t['pnl']:+.4f}U ({hold})")
            print(f"        └─ TP1:{tp1c}x TP2:{tp2c}x Trail:{trc}x — {t['reason'][:30]}")
    print(f"  {'═'*64}")


# ════════════════════════════════════════════════════
#  MAIN LOOP — ULTRA LIGHTNING v13
# ════════════════════════════════════════════════════
def run_bot():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  ⚡⚡⚡ BOT SCALPING v13.1 — ULTRA LIGHTNING MODE (FIXED)    ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Leverage:{LEVERAGE}x │ Per trade:${ORDER_USDT} │ Max posisi:{MAX_POSITIONS} (fokus!)           ║")
    print(f"║  TP1:{TP1_PCT*100:.2f}%(60%) │ TP2:{TP2_PCT*100:.2f}% │ SL:{SL_PCT*100:.2f}%                  ║")
    print(f"║  🔴 ZeroLoss: minus {ZERO_LOSS_TICK_PCT*100:.2f}% (1 tick) → CUT SEKETIKA!     ║")
    print(f"║  📈 Trail: aktif saat profit {PROFIT_TRIGGER_TRAIL*100:.2f}% (anchor di entry) ║")
    print(f"║  📈 Trail dinamis: 0→entry lock, 0.3%→0.06%, 0.5%→0.04%  ║")
    print(f"║  🔮 Pre-scan: {PRE_SCAN_CANDIDATES} kandidat selalu siap di antrian           ║")
    print(f"║  ⚡ Monitor: setiap {POSITION_MONITOR_SEC} detik (ultra fast!)                  ║")
    print(f"║  🐛 FIX: Candidate TTL {CANDIDATE_TTL_VALID}s | PreScan {PRE_SCAN_INTERVAL}s | Drift {PRICE_DRIFT_MAX_PCT*100:.1f}%  ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    print("\n  ⏳ Validasi symbols...")
    symbols_active = validate_symbols()

    print(f"  📦 Pre-load symbol info...")
    with ThreadPoolExecutor(max_workers=15) as ex:
        list(ex.map(get_sym_info, symbols_active[:50]))

    print(f"  🌐 Refresh macro...")
    refresh_macro()
    update_btc_price()

    print(f"\n  ✅ {len(symbols_active)} symbols | BTC:{_macro['btc_trend_5m']} | "
          f"Mode:{_macro['scalp_mode']} | F&G:{_macro['fng']}")

    # ── Warm-up pre-scan queue ────────────────────────────────────
    print(f"  🔮 Warming up candidate queue (initial scan)...")
    sample = symbols_active[:30]
    init_candidates = scan_batch_parallel(sample)
    if init_candidates:
        init_candidates.sort(key=lambda x: x[2].get("score", 0), reverse=True)
        with _candidate_lock:
            for sym, direction, info in init_candidates:
                _candidate_cache[sym] = (time.time(), direction, info)
        print(f"  🎯 {len(init_candidates)} kandidat awal siap!")

    print(f"  🚀 Start dalam 3 detik...\n")
    time.sleep(3)

    # ── Start dedicated threads ────────────────────────────────────
    # 1. Position monitor thread (1 detik — ultra fast)
    pm_thread = threading.Thread(target=position_monitor_thread, daemon=True)
    pm_thread.start()
    print("  🔧 Position monitor (1s): START ✅")

    # 2. Background pre-scan engine (maintain candidate queue)
    ps_thread = threading.Thread(
        target=pre_scan_engine,
        args=(symbols_active,),
        daemon=True)
    ps_thread.start()
    print("  🔧 Pre-scan engine: START ✅")

    cycle = 0
    while True:
        cycle += 1
        refresh_macro()
        update_btc_price()

        flash_dir, flash_pct = detect_flash_move()
        flash_info = f"⚡{flash_dir.upper()}:{flash_pct:.1f}%" if flash_dir != "none" else ""
        mode_e = "📈" if _macro["scalp_mode"] == "TREND" else "↩️"
        q_size = _candidate_queue.qsize()

        print(f"\n{'═'*67}")
        print(f"  🔄 #{cycle} {time.strftime('%H:%M:%S')} | F&G:{_macro['fng']}({_macro['fng_label']}) | "
              f"BTC5m:{_macro['btc_trend_5m']} {flash_info}")
        print(f"  {mode_e} Mode:{_macro['scalp_mode']} | Breadth:{_macro['market_breadth']*100:.0f}% | "
              f"News:{_macro['news']} | 🔮Queue:{q_size}")
        print(f"  📂 Posisi({len(open_positions)}/{MAX_POSITIONS}): "
              f"{list(open_positions.keys()) or '—'}")

        # Tampilkan kandidat terbaik yang siap
        with _candidate_lock:
            now_ts = time.time()
            fresh = [(sym, ts, d, i) for sym, (ts, d, i) in _candidate_cache.items()
                     if sym not in open_positions and now_ts - ts < CANDIDATE_TTL_VALID]
            fresh.sort(key=lambda x: x[3].get("score", 0), reverse=True)
            if fresh:
                print(f"  🎯 Top candidates:")
                for sym, ts, d, i in fresh[:3]:
                    age  = now_ts - ts
                    sigs = " | ".join(i.get("signals", [])[:2])
                    print(f"     ⭐ {sym:14} {d} Score:{i.get('score',0):.0f} ({age:.0f}s ago) | {sigs}")

        slots_free = MAX_POSITIONS - len(open_positions)

        if slots_free > 0 and _macro["news"] != "strong_negative" and \
           flash_dir == "none" and _macro["market_breadth"] >= MIN_BREADTH:

            entries_opened = 0
            for _ in range(slots_free):
                if len(open_positions) >= MAX_POSITIONS: break
                result = get_best_candidate()
                if result:
                    sym, direction, info = result

                    # 🔧 FIX: Re-validasi harga — skip jika drift >PRICE_DRIFT_MAX_PCT
                    current_price = get_price(sym)
                    cached_price  = info.get("entry_approx", current_price)
                    if cached_price > 0 and current_price > 0:
                        drift = abs(current_price - cached_price) / cached_price
                        if drift > PRICE_DRIFT_MAX_PCT:
                            print(f"  ⚠️ [{sym}] Harga drift {drift*100:.2f}% — skip, hapus cache")
                            with _candidate_lock:
                                _candidate_cache.pop(sym, None)
                            continue

                    sig_str = " | ".join(info.get("signals", [])[:3])
                    print(f"  ⚡ ENTRY from queue: {sym} {direction} Score:{info['score']:.0f} | {sig_str}")
                    open_trade(sym, direction, info)
                    entries_opened += 1
                else:
                    print(f"  ⏳ Queue kosong — menunggu pre-scan...")
                    break

            if entries_opened == 0 and slots_free > 0:
                print(f"  ⏳ Tidak ada kandidat fresh saat ini")
        else:
            if slots_free == 0:
                print(f"  ⏸️  Posisi penuh ({MAX_POSITIONS}/{MAX_POSITIONS})")
            elif flash_dir != "none":
                print(f"  ⚡ Flash {flash_dir} — skip entry")
            elif _macro["market_breadth"] < MIN_BREADTH:
                print(f"  ⚠️  Breadth rendah ({_macro['market_breadth']*100:.0f}%) — skip")
            else:
                print(f"  🚫 Bad news — skip entry")

        if cycle % 20 == 0:
            print_stats()

        print(f"\n  ⏱️  Next scan: {SCAN_INTERVAL}s | "
              f"ZeroLoss cuts: {_stats['zero_loss_cuts']} | "
              f"Trail stops: {_stats['trail_stops']}")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
