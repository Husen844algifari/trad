"""
╔══════════════════════════════════════════════════════════════╗
║         CRYPTO FUTURES BOT v5 — SMART MULTI-FILTER          ║
║  Analisa: Teknikal 9 indikator + Dominance + News + Regime   ║
║  Strategi: Trend-following, konfirmasi multi-layer, anti-chop║
╚══════════════════════════════════════════════════════════════╝
"""

import os, time, math, requests
from dotenv    import load_dotenv
from binance.client import Client
from binance.enums  import *
import ta
import pandas as pd
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
load_dotenv()
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))
client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"

# ── Money Management ─────────────────────────────────────────
ORDER_VALUE_USDT = 100        # modal per posisi (USDT)
LEVERAGE         = 5          # leverage
MAX_POSITIONS    = 8          # max posisi paralel

# ── TP / SL dinamis berdasarkan ATR (bukan fixed %) ──────────
TP_ATR_MULT   = 2.5           # TP = entry ± (ATR × 2.5)
SL_ATR_MULT   = 1.0           # SL = entry ∓ (ATR × 1.0)  → R:R 1:2.5
MIN_TP_PCT    = 0.008         # TP minimal 0.8%
MIN_SL_PCT    = 0.004         # SL minimal 0.4%

# ── Entry Filter ─────────────────────────────────────────────
MIN_SCORE       = 60          # skor minimum masuk (dari maks ~150)
MIN_CONFIRM     = 5           # jumlah indikator harus setuju (dari 9)
INTERVAL_FAST   = Client.KLINE_INTERVAL_5MINUTE
INTERVAL_SLOW   = Client.KLINE_INTERVAL_15MINUTE   # konfirmasi trend HTF

# ── Trailing Stop ────────────────────────────────────────────
TRAILING_STOP   = True
TRAILING_FACTOR = 1.2         # trail SL di ATR × 1.2 dari peak/trough

# ── Market Regime ────────────────────────────────────────────
# BTC.D > 58 → altcoin lemah, prioritas BTC/ETH saja
# USDT.D naik → risk-off, bias SHORT atau skip LONG
# Fear & Greed bisa dimanfaatkan untuk konfirmasi arah

TOP_SYMBOLS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","MATICUSDT",
    "LINKUSDT","LTCUSDT","UNIUSDT","ATOMUSDT","ETCUSDT",
    "FILUSDT","NEARUSDT","APTUSDT","ARBUSDT","OPUSDT",
]

# Minimum notional per exchange rule
MIN_NOTIONAL_OVERRIDE = {"BTCUSDT": 100, "ETHUSDT": 50, "BNBUSDT": 50}

open_positions: dict = {}

# ═══════════════════════════════════════════════════════════════
# P&L TRACKER
# ═══════════════════════════════════════════════════════════════
class PnLTracker:
    def __init__(self):
        self.trades       = []
        self.total_profit = 0.0
        self.total_loss   = 0.0
        self.wins         = 0
        self.losses       = 0
        self.biggest_win  = 0.0
        self.biggest_loss = 0.0
        self.streak       = 0
        self.start        = datetime.now()

    def record(self, symbol, side, entry, exit_p, qty, notional, reason):
        pnl_pct  = (exit_p - entry)/entry if side=="LONG" else (entry - exit_p)/entry
        pnl_usdt = pnl_pct * notional
        is_win   = pnl_usdt > 0
        trade = dict(symbol=symbol, side=side, entry=entry, exit=exit_p,
                     qty=qty, notional=notional, pnl_pct=pnl_pct*100,
                     pnl_usdt=pnl_usdt, reason=reason, win=is_win,
                     time=datetime.now().strftime("%H:%M:%S"))
        self.trades.append(trade)
        if is_win:
            self.total_profit += pnl_usdt
            self.wins         += 1
            self.biggest_win   = max(self.biggest_win, pnl_usdt)
            self.streak        = self.streak+1 if self.streak >= 0 else 1
        else:
            self.total_loss   += abs(pnl_usdt)
            self.losses       += 1
            self.biggest_loss  = max(self.biggest_loss, abs(pnl_usdt))
            self.streak        = self.streak-1 if self.streak <= 0 else -1
        return trade

    @property
    def net(self):        return self.total_profit - self.total_loss
    @property
    def total(self):      return self.wins + self.losses
    @property
    def winrate(self):    return (self.wins/self.total*100) if self.total else 0
    @property
    def pf(self):         return (self.total_profit/self.total_loss) if self.total_loss else float("inf")

    def print_result(self, t):
        sign = "+" if t["win"] else ""
        icon = "✅ PROFIT" if t["win"] else "❌ LOSS"
        print(f"\n  {'═'*56}")
        print(f"  {'📈' if t['win'] else '📉'}  TRADE CLOSED — {t['symbol']} [{t['side']}]")
        print(f"  {'═'*56}")
        print(f"  {icon}")
        print(f"  Entry    : ${t['entry']:.6f}")
        print(f"  Exit     : ${t['exit']:.6f}")
        print(f"  Qty      : {t['qty']}")
        print(f"  P&L      : {sign}{t['pnl_pct']:.3f}%  ({sign}${t['pnl_usdt']:.2f} USDT)")
        print(f"  Alasan   : {t['reason']}")
        print(f"  Waktu    : {t['time']}")
        print(f"  {'─'*56}")
        pf_s = f"{self.pf:.2f}" if self.pf != float("inf") else "∞"
        ns   = "+" if self.net >= 0 else ""
        print(f"  📊 Trades : {self.total} ({self.wins}W/{self.losses}L)  WR: {self.winrate:.1f}%  PF: {pf_s}")
        print(f"  💰 Net    : {ns}${self.net:.2f}  │  Best: +${self.biggest_win:.2f}  Worst: -${self.biggest_loss:.2f}")
        if self.streak >  1: print(f"  🔥 Win streak  {self.streak}")
        if self.streak < -1: print(f"  ❄️  Loss streak {abs(self.streak)}")
        print(f"  {'═'*56}\n")

pnl = PnLTracker()

# ═══════════════════════════════════════════════════════════════
# SYMBOL INFO & QTY
# ═══════════════════════════════════════════════════════════════
_sym_cache: dict = {}

def get_symbol_info(symbol):
    if symbol in _sym_cache: return _sym_cache[symbol]
    try:
        for s in client.futures_exchange_info()["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        _sym_cache[symbol] = {"step": float(f["stepSize"]), "minQty": float(f["minQty"])}
                        return _sym_cache[symbol]
    except: pass
    return {"step": 1.0, "minQty": 1.0}

def round_step(qty, step):
    p = int(round(-math.log(step,10),0)) if step < 1 else 0
    return round(math.floor(qty/step)*step, p)

def get_notional(symbol):
    base = max(ORDER_VALUE_USDT, MIN_NOTIONAL_OVERRIDE.get(symbol, 20))
    return base * LEVERAGE

def calc_qty(symbol, price):
    info = get_symbol_info(symbol)
    qty  = round_step(get_notional(symbol)/price, info["step"])
    return max(qty, info["minQty"])

def set_leverage_all():
    for sym in TOP_SYMBOLS:
        try: client.futures_change_leverage(symbol=sym, leverage=LEVERAGE)
        except: pass

# ═══════════════════════════════════════════════════════════════
# MARKET DATA — OHLCV (dual timeframe)
# ═══════════════════════════════════════════════════════════════
def get_ohlcv(symbol, interval, limit=150):
    try:
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(klines, columns=[
            "time","open","high","low","close","volume",
            "close_time","quote_vol","trades","taker_buy_base","taker_buy_quote","ignore"
        ])
        for c in ["open","high","low","close","volume"]: df[c] = df[c].astype(float)
        df["taker_buy_base"] = df["taker_buy_base"].astype(float)
        return df
    except: return None

# ═══════════════════════════════════════════════════════════════
# TECHNICAL ANALYSIS — 9 INDIKATOR
# ═══════════════════════════════════════════════════════════════
def analyze(df):
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]

    # 1. RSI
    df["rsi"]       = ta.momentum.RSIIndicator(c, 14).rsi()

    # 2. MACD
    macd            = ta.trend.MACD(c, 12, 26, 9)
    df["macd"]      = macd.macd()
    df["macd_sig"]  = macd.macd_signal()
    df["macd_hist"] = macd.macd_diff()

    # 3. EMA Stack (trend direction)
    df["ema8"]      = ta.trend.EMAIndicator(c, 8).ema_indicator()
    df["ema21"]     = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["ema55"]     = ta.trend.EMAIndicator(c, 55).ema_indicator()
    df["ema200"]    = ta.trend.EMAIndicator(c, 100).ema_indicator()  # proxy EMA200

    # 4. Bollinger Bands
    bb              = ta.volatility.BollingerBands(c, 20, 2)
    df["bb_upper"]  = bb.bollinger_hband()
    df["bb_mid"]    = bb.bollinger_mavg()
    df["bb_lower"]  = bb.bollinger_lband()
    df["bb_width"]  = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]
    df["bb_pct"]    = bb.bollinger_pband()   # 0=bawah, 1=atas

    # 5. Stochastic RSI
    stoch_rsi       = ta.momentum.StochRSIIndicator(c, 14, 3, 3)
    df["srsi_k"]    = stoch_rsi.stochrsi_k()
    df["srsi_d"]    = stoch_rsi.stochrsi_d()

    # 6. ATR (volatility & dynamic SL/TP)
    df["atr"]       = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()
    df["atr_pct"]   = df["atr"] / c   # ATR as % of price

    # 7. Volume analysis
    df["vol_ma"]    = v.rolling(20).mean()
    df["vol_ratio"] = v / df["vol_ma"]

    # 8. OBV (On-Balance Volume) — divergence proxy
    df["obv"]       = ta.volume.OnBalanceVolumeIndicator(c, v).on_balance_volume()
    df["obv_ema"]   = df["obv"].ewm(span=21).mean()

    # 9. Taker Buy Ratio (agression buyer vs seller dari exchange)
    total_vol       = v.replace(0, 1)
    df["tbr"]       = df["taker_buy_base"] / total_vol  # >0.5 = buyer dominan

    # Candlestick patterns (simple)
    df["body"]      = (c - df["open"]).abs()
    df["upper_wick"]= df["high"] - df[["close","open"]].max(axis=1)
    df["lower_wick"]= df[["close","open"]].min(axis=1) - df["low"]

    return df

# ═══════════════════════════════════════════════════════════════
# MARKET REGIME — BTC.D + USDT.D + Fear & Greed
# ═══════════════════════════════════════════════════════════════
_dom_cache  = {"btc":50.0,"usdt":5.0,"btc_p":50.0,"usdt_p":5.0,"ts":0}
_fg_cache   = {"value":50,"class":"Neutral","ts":0}
_news_cache = {"sentiment":"neutral","ts":0}

def fetch_dominance():
    try:
        r    = requests.get("https://api.coingecko.com/api/v3/global", timeout=8)
        data = r.json()["data"]["market_cap_percentage"]
        return round(data.get("btc",50),2), round(data.get("usdt",5),2)
    except: return 50.0, 5.0

def get_dominance():
    now = time.time()
    if now - _dom_cache["ts"] > 300:
        btc, usdt               = fetch_dominance()
        _dom_cache["btc_p"]     = _dom_cache["btc"]
        _dom_cache["usdt_p"]    = _dom_cache["usdt"]
        _dom_cache["btc"]       = btc
        _dom_cache["usdt"]      = usdt
        _dom_cache["ts"]        = now
    d = _dom_cache
    btc_trend  = "up" if d["btc"]  > d["btc_p"]  else ("down" if d["btc"]  < d["btc_p"]  else "flat")
    usdt_trend = "up" if d["usdt"] > d["usdt_p"] else ("down" if d["usdt"] < d["usdt_p"] else "flat")
    return d["btc"], d["usdt"], btc_trend, usdt_trend

def get_fear_greed():
    """Fear & Greed Index (0=extreme fear, 100=extreme greed)."""
    now = time.time()
    if now - _fg_cache["ts"] > 3600:
        try:
            r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=6)
            d = r.json()["data"][0]
            _fg_cache["value"] = int(d["value"])
            _fg_cache["class"] = d["value_classification"]
            _fg_cache["ts"]    = now
        except: pass
    return _fg_cache["value"], _fg_cache["class"]

def get_news():
    """Sentiment berita crypto terkini."""
    now = time.time()
    if now - _news_cache["ts"] > 180:
        try:
            r    = requests.get(
                "https://cryptopanic.com/api/v1/posts/?auth_token=demo&public=true&currencies=BTC",
                timeout=5)
            data = r.json()
            neg_kw = ["crash","hack","ban","bear","fear","lawsuit","fraud","dump","warning","collapse","scam","liquidat"]
            pos_kw = ["bullish","rally","surge","adoption","institutional","ath","breakout","pump","buy","approve","etf"]
            neg = pos = 0
            for post in data.get("results",[])[:15]:
                t = post.get("title","").lower()
                neg += sum(1 for w in neg_kw if w in t)
                pos += sum(1 for w in pos_kw if w in t)
            if   neg >= 3: sent = "very_negative"
            elif neg >= 2: sent = "negative"
            elif pos >= 3: sent = "very_positive"
            elif pos >= 2: sent = "positive"
            else:          sent = "neutral"
            _news_cache["sentiment"] = sent
            _news_cache["ts"]        = now
        except: pass
    return _news_cache["sentiment"]

def get_market_regime(btc_d, usdt_d, btc_trend, usdt_trend, fg_val):
    """
    Tentukan kondisi pasar secara keseluruhan.
    Returns: ("BULL"|"BEAR"|"NEUTRAL"|"RISK_OFF"), bias_arah
    """
    score = 0
    # BTC dominance tinggi = altcoin lesu
    if btc_d > 60:   score -= 1   # risk-off untuk alt
    elif btc_d < 52: score += 1   # alt season kemungkinan

    # USDT dominance naik = orang kabur ke cash = BEAR
    if usdt_trend == "up":   score -= 2
    elif usdt_trend == "down": score += 2

    # BTC trend
    if btc_trend == "up":   score += 1
    elif btc_trend == "down": score -= 1

    # Fear & Greed
    if fg_val <= 20:   score -= 2   # extreme fear → kemungkinan reversal LONG
    elif fg_val <= 35: score -= 1
    elif fg_val >= 80: score += 2   # extreme greed → hati-hati LONG, short mungkin
    elif fg_val >= 65: score += 1

    if score >= 2:    return "BULL",     "LONG"
    elif score <= -2: return "BEAR",     "SHORT"
    elif score == 0:  return "NEUTRAL",  "BOTH"
    elif score > 0:   return "MILD_BULL","LONG"
    else:             return "MILD_BEAR","SHORT"

# ═══════════════════════════════════════════════════════════════
# MULTI-TIMEFRAME TREND CONFIRMATION
# ═══════════════════════════════════════════════════════════════
_htf_cache: dict = {}

def get_htf_bias(symbol):
    """
    Ambil bias trend dari timeframe 15m (HTF).
    Returns: "LONG", "SHORT", atau "NEUTRAL"
    """
    now = time.time()
    if symbol in _htf_cache and now - _htf_cache[symbol]["ts"] < 300:
        return _htf_cache[symbol]["bias"]
    df = get_ohlcv(symbol, INTERVAL_SLOW, limit=60)
    if df is None or len(df) < 55:
        return "NEUTRAL"
    df = analyze(df)
    last = df.iloc[-1]
    bias_score = 0
    if last["ema8"] > last["ema21"] > last["ema55"]:   bias_score += 2  # uptrend kuat
    elif last["ema8"] > last["ema21"]:                  bias_score += 1
    if last["ema8"] < last["ema21"] < last["ema55"]:   bias_score -= 2  # downtrend kuat
    elif last["ema8"] < last["ema21"]:                  bias_score -= 1
    if last["macd"] > last["macd_sig"]:                 bias_score += 1
    else:                                               bias_score -= 1
    if last["close"] > last["ema200"]:                  bias_score += 1
    else:                                               bias_score -= 1

    bias = "LONG" if bias_score >= 2 else ("SHORT" if bias_score <= -2 else "NEUTRAL")
    _htf_cache[symbol] = {"bias": bias, "ts": now}
    return bias

# ═══════════════════════════════════════════════════════════════
# SCORING ENGINE — 9 LAYER
# ═══════════════════════════════════════════════════════════════
def score_and_confirm(df, htf_bias, regime_bias, btc_d, usdt_trend, news):
    """
    Returns: (long_score, short_score, long_confirms, short_confirms, reasons_dict)
    Setiap layer: skor & +1 confirm jika setuju arah.
    Butuh MIN_CONFIRM konfirmasi untuk lanjut entry.
    """
    last = df.iloc[-1]
    prev = df.iloc[-2]
    p2   = df.iloc[-3]
    ls = ss = 0
    lc = sc = 0   # confirm count
    lr = {}; sr = {}

    price = last["close"]
    atr   = last["atr"]

    # ── 1. EMA STACK (trend utama) ─────────────────────────────
    if last["ema8"] > last["ema21"] > last["ema55"] > last["ema200"]:
        ls += 25; lc += 1; lr["ema"] = "stack bullish ✓"
    elif last["ema8"] > last["ema21"] > last["ema55"]:
        ls += 15; lr["ema"] = "ema8>21>55"
    elif last["ema8"] > last["ema21"]:
        ls += 8

    if last["ema8"] < last["ema21"] < last["ema55"] < last["ema200"]:
        ss += 25; sc += 1; sr["ema"] = "stack bearish ✓"
    elif last["ema8"] < last["ema21"] < last["ema55"]:
        ss += 15; sr["ema"] = "ema8<21<55"
    elif last["ema8"] < last["ema21"]:
        ss += 8

    # ── 2. RSI ────────────────────────────────────────────────
    rsi = last["rsi"]
    if 30 <= rsi <= 50:   ls += 20; lc += 1; lr["rsi"] = f"RSI={rsi:.1f} oversold zone ✓"
    elif rsi < 30:        ls += 30; lc += 1; lr["rsi"] = f"RSI={rsi:.1f} extreme oversold ✓"
    elif 50 < rsi <= 60:  ls += 8

    if 50 <= rsi <= 70:   ss += 20; sc += 1; sr["rsi"] = f"RSI={rsi:.1f} overbought zone ✓"
    elif rsi > 70:        ss += 30; sc += 1; sr["rsi"] = f"RSI={rsi:.1f} extreme overbought ✓"
    elif 40 < rsi < 50:   ss += 8

    # ── 3. MACD ───────────────────────────────────────────────
    # Crossover = sinyal kuat
    if last["macd"] > last["macd_sig"] and prev["macd"] <= prev["macd_sig"]:
        ls += 25; lc += 1; lr["macd"] = "MACD crossover UP ✓"
    elif last["macd"] > last["macd_sig"] and last["macd_hist"] > prev["macd_hist"]:
        ls += 15; lc += 1; lr["macd"] = "MACD bullish momentum ✓"
    elif last["macd"] > last["macd_sig"]:
        ls += 8

    if last["macd"] < last["macd_sig"] and prev["macd"] >= prev["macd_sig"]:
        ss += 25; sc += 1; sr["macd"] = "MACD crossover DOWN ✓"
    elif last["macd"] < last["macd_sig"] and last["macd_hist"] < prev["macd_hist"]:
        ss += 15; sc += 1; sr["macd"] = "MACD bearish momentum ✓"
    elif last["macd"] < last["macd_sig"]:
        ss += 8

    # ── 4. STOCHASTIC RSI ─────────────────────────────────────
    sk, sd = last["srsi_k"], last["srsi_d"]
    pk, pd_ = prev["srsi_k"], prev["srsi_d"]
    if sk < 0.2 and sk > sd and pk <= pd_:      # oversold + crossover
        ls += 20; lc += 1; lr["srsi"] = f"StochRSI crossover up @{sk:.2f} ✓"
    elif sk < 0.3 and sk > sd:
        ls += 12; lr["srsi"] = f"StochRSI bullish @{sk:.2f}"

    if sk > 0.8 and sk < sd and pk >= pd_:      # overbought + crossover
        ss += 20; sc += 1; sr["srsi"] = f"StochRSI crossover dn @{sk:.2f} ✓"
    elif sk > 0.7 and sk < sd:
        ss += 12; sr["srsi"] = f"StochRSI bearish @{sk:.2f}"

    # ── 5. BOLLINGER BANDS ────────────────────────────────────
    bp = last["bb_pct"]     # 0=bawah band, 1=atas band
    bw = last["bb_width"]
    # Hanya valid jika BB width cukup (bukan squeeze)
    if bw > 0.01:
        if price <= last["bb_lower"]:            ls += 20; lc += 1; lr["bb"] = "BB lower touch ✓"
        elif bp < 0.2:                           ls += 12; lr["bb"] = f"BB low zone {bp:.2f}"
        if price >= last["bb_upper"]:            ss += 20; sc += 1; sr["bb"] = "BB upper touch ✓"
        elif bp > 0.8:                           ss += 12; sr["bb"] = f"BB high zone {bp:.2f}"
    # BB squeeze breakout
    prev_bw = df["bb_width"].iloc[-5:-1].mean()
    if bw > prev_bw * 1.5:      # BB melebar = breakout
        if price > last["bb_mid"]: ls += 10; lr["bb_break"] = "BB breakout UP"
        else:                       ss += 10; sr["bb_break"] = "BB breakout DOWN"

    # ── 6. VOLUME & TAKER BUY RATIO ───────────────────────────
    vr  = last["vol_ratio"]
    tbr = last["tbr"]
    if vr >= 1.5:
        if tbr > 0.55:     ls += 20; lc += 1; lr["vol"] = f"Vol spike+buyer dominan tbr={tbr:.2f} ✓"
        elif tbr < 0.45:   ss += 20; sc += 1; sr["vol"] = f"Vol spike+seller dominan tbr={tbr:.2f} ✓"
        else:              ls += 8; ss += 8   # volume tinggi tapi netral
    elif vr >= 1.2:
        if tbr > 0.52:     ls += 10
        elif tbr < 0.48:   ss += 10

    # ── 7. OBV DIVERGENCE ─────────────────────────────────────
    # Harga turun tapi OBV naik = bullish divergence
    price_5 = df["close"].iloc[-6]
    obv_now  = last["obv"]
    obv_5    = df["obv"].iloc[-6]
    if price < price_5 and obv_now > obv_5:   ls += 15; lc += 1; lr["obv"] = "Bullish OBV divergence ✓"
    if price > price_5 and obv_now < obv_5:   ss += 15; sc += 1; sr["obv"] = "Bearish OBV divergence ✓"

    # ── 8. CANDLESTICK PATTERN (2-3 candle) ──────────────────
    # Hammer / Bullish engulfing
    body     = last["body"]
    lw       = last["lower_wick"]
    uw       = last["upper_wick"]
    prev_body = prev["body"]
    # Hammer: lower wick > 2× body, close > open
    if lw > body*2 and last["close"] > last["open"] and vr > 1.0:
        ls += 15; lc += 1; lr["candle"] = "Hammer/Pin bar bullish ✓"
    # Bullish engulfing
    if (last["close"] > last["open"] and prev["close"] < prev["open"]
            and body > prev_body * 1.5):
        ls += 15; lc += 1; lr["candle"] = "Bullish engulfing ✓"
    # Shooting star / Bearish engulfing
    if uw > body*2 and last["close"] < last["open"] and vr > 1.0:
        ss += 15; sc += 1; sr["candle"] = "Shooting star bearish ✓"
    if (last["close"] < last["open"] and prev["close"] > prev["open"]
            and body > prev_body * 1.5):
        ss += 15; sc += 1; sr["candle"] = "Bearish engulfing ✓"

    # ── 9. MARKET REGIME (DOMINANCE + F&G + NEWS) ────────────
    # USDT.D naik → risk off → penalti LONG, bonus SHORT
    if usdt_trend == "up":
        ls -= 20; ss += 15; sr["regime"] = "USDT.D naik risk-off ✓"
    elif usdt_trend == "down":
        ls += 15; ss -= 10; lr["regime"] = "USDT.D turun risk-on ✓"

    # BTC dominance tinggi = alt bias lebih lemah
    if btc_d > 60:
        ls = int(ls * 0.85); ss = int(ss * 1.1)

    # News sentiment
    if news == "very_negative":  ls -= 25; ss += 20; sc += 1
    elif news == "negative":     ls -= 15; ss += 10
    elif news == "very_positive": ls += 20; ss -= 15; lc += 1
    elif news == "positive":     ls += 10; ss -= 5

    # HTF bias filter — sinyal berlawanan HTF dapat penalti besar
    if htf_bias == "LONG":
        ls += 15; ss -= 20; lc += 1; lr["htf"] = "HTF 15m = LONG ✓"
    elif htf_bias == "SHORT":
        ss += 15; ls -= 20; sc += 1; sr["htf"] = "HTF 15m = SHORT ✓"

    # Regime bias
    if regime_bias == "LONG":
        ls += 10; ss -= 10
    elif regime_bias == "SHORT":
        ss += 10; ls -= 10

    # Loss streak → lebih konservatif
    if pnl.streak <= -2:
        factor = max(0.7, 1.0 - abs(pnl.streak) * 0.05)
        ls = int(ls * factor); ss = int(ss * factor)

    return max(0,ls), max(0,ss), max(0,lc), max(0,sc), lr, sr

# ═══════════════════════════════════════════════════════════════
# CHOP / NOISE FILTER
# ═══════════════════════════════════════════════════════════════
def is_valid_market(df):
    """
    Cek apakah kondisi market layak untuk entry.
    Return (ok, reason)
    """
    last = df.iloc[-1]

    # ATR terlalu tinggi = market terlalu volatile/choppy
    if last["atr_pct"] > 0.035:
        return False, f"ATR% terlalu tinggi ({last['atr_pct']*100:.2f}%) — skip"

    # BB squeeze ekstrem = menunggu breakout, jangan masuk dulu
    if last["bb_width"] < 0.004:
        return False, f"BB squeeze ({last['bb_width']:.4f}) — tunggu breakout"

    # Volume sangat rendah = market mati, sinyal tidak valid
    if last["vol_ratio"] < 0.4:
        return False, f"Volume sangat rendah ({last['vol_ratio']:.2f}x) — skip"

    return True, "OK"

# ═══════════════════════════════════════════════════════════════
# DYNAMIC TP/SL BERDASARKAN ATR
# ═══════════════════════════════════════════════════════════════
def calc_tp_sl(entry, side, atr):
    """TP dan SL berdasarkan ATR, bukan persentase fixed."""
    tp_dist = max(atr * TP_ATR_MULT, entry * MIN_TP_PCT)
    sl_dist = max(atr * SL_ATR_MULT, entry * MIN_SL_PCT)
    if side == "LONG":
        return entry + tp_dist, entry - sl_dist
    else:
        return entry - tp_dist, entry + sl_dist

# ═══════════════════════════════════════════════════════════════
# EXCHANGE HELPERS
# ═══════════════════════════════════════════════════════════════
def get_price(symbol):
    try: return float(client.futures_symbol_ticker(symbol=symbol)["price"])
    except: return None

def get_pos_amt(symbol):
    try:
        for p in client.futures_position_information(symbol=symbol):
            amt = float(p["positionAmt"])
            if amt != 0: return amt
        return 0
    except: return 0

def open_position(symbol, side, score, confirms, atr, reasons):
    try:
        price = get_price(symbol)
        if not price: return
        qty   = calc_qty(symbol, price)
        client.futures_create_order(
            symbol=symbol,
            side  = SIDE_BUY if side=="LONG" else SIDE_SELL,
            type  = ORDER_TYPE_MARKET,
            quantity=qty
        )
        entry     = get_price(symbol) or price
        tp, sl    = calc_tp_sl(entry, side, atr)
        notional  = get_notional(symbol)
        pot_tp    = notional * abs(tp-entry)/entry
        pot_sl    = notional * abs(sl-entry)/entry

        open_positions[symbol] = {
            "side": side, "entry": entry, "qty": qty,
            "tp": tp, "sl": sl, "atr": atr,
            "notional": notional,
            "highest": entry, "lowest": entry,
            "trailing_on": False,
            "open_time": time.time(), "score": score,
        }

        rr   = abs(tp-entry)/abs(sl-entry)
        tp_p = abs(tp-entry)/entry*100
        sl_p = abs(sl-entry)/entry*100

        print(f"\n  🚀 POSISI DIBUKA — {symbol} [{side}]")
        print(f"  {'─'*56}")
        print(f"  Skor: {score}  │  Konfirmasi: {confirms}/{MIN_CONFIRM}  │  ATR: {atr:.5f}")
        print(f"  Entry   : ${entry:.6f}  │  Qty: {qty}")
        print(f"  Margin  : ${ORDER_VALUE_USDT}  │  Leverage: {LEVERAGE}x  │  Notional: ${notional:.0f}")
        print(f"  TP      : ${tp:.6f}  (+{tp_p:.2f}%)  → +${pot_tp:.2f} USDT")
        print(f"  SL      : ${sl:.6f}  (-{sl_p:.2f}%)  → -${pot_sl:.2f} USDT")
        print(f"  R:R     : 1:{rr:.2f}")
        print(f"  Alasan  :")
        for k, v in list(reasons.items())[:6]:
            print(f"    • {v}")
        print(f"  {'─'*56}")
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal buka: {e}")

def close_position(symbol, reason="", exit_price=None):
    try:
        amt = get_pos_amt(symbol)
        if amt == 0:
            open_positions.pop(symbol, None); return
        client.futures_create_order(
            symbol=symbol,
            side  = SIDE_SELL if amt > 0 else SIDE_BUY,
            type  = ORDER_TYPE_MARKET,
            quantity=abs(amt), reduceOnly=True
        )
        exit_p = exit_price or get_price(symbol) or 0
        pos    = open_positions.get(symbol, {})
        if pos:
            t = pnl.record(symbol, pos["side"], pos["entry"], exit_p,
                           pos["qty"], pos.get("notional", ORDER_VALUE_USDT*LEVERAGE), reason)
            pnl.print_result(t)
        open_positions.pop(symbol, None)
    except Exception as e:
        print(f"  ❌ [{symbol}] Gagal tutup: {e}")

def check_tp_sl(symbol, price):
    if symbol not in open_positions: return False
    pos   = open_positions[symbol]
    entry = pos["entry"]
    side  = pos["side"]
    atr   = pos["atr"]

    # ── Trailing Stop (berbasis ATR) ──────────────────────────
    if TRAILING_STOP:
        trail_dist = atr * TRAILING_FACTOR
        if side == "LONG":
            if price > pos["highest"]:
                open_positions[symbol]["highest"] = price
            # Aktifkan trailing setelah profit > 1 ATR
            if (price - entry) >= atr and not pos["trailing_on"]:
                open_positions[symbol]["trailing_on"] = True
            if pos["trailing_on"]:
                new_sl = pos["highest"] - trail_dist
                if new_sl > pos["sl"]:
                    open_positions[symbol]["sl"] = new_sl
        else:
            if price < pos["lowest"]:
                open_positions[symbol]["lowest"] = price
            if (entry - price) >= atr and not pos["trailing_on"]:
                open_positions[symbol]["trailing_on"] = True
            if pos["trailing_on"]:
                new_sl = pos["lowest"] + trail_dist
                if new_sl < pos["sl"]:
                    open_positions[symbol]["sl"] = new_sl

    # ── TP / SL hit ───────────────────────────────────────────
    if side == "LONG":
        if price >= pos["tp"]:
            close_position(symbol, "✨ TAKE PROFIT", price); return True
        if price <= pos["sl"]:
            lbl = "🔒 TRAILING STOP" if pos["trailing_on"] else "🛑 STOP LOSS"
            close_position(symbol, lbl, price); return True
    else:
        if price <= pos["tp"]:
            close_position(symbol, "✨ TAKE PROFIT", price); return True
        if price >= pos["sl"]:
            lbl = "🔒 TRAILING STOP" if pos["trailing_on"] else "🛑 STOP LOSS"
            close_position(symbol, lbl, price); return True
    return False

# ═══════════════════════════════════════════════════════════════
# LIVE POSITION DISPLAY
# ═══════════════════════════════════════════════════════════════
def show_positions():
    if not open_positions: return
    print(f"\n  📂 Posisi Aktif ({len(open_positions)}/{MAX_POSITIONS}):")
    for sym, pos in open_positions.items():
        price = get_price(sym)
        if not price: continue
        notional  = pos.get("notional", ORDER_VALUE_USDT*LEVERAGE)
        if pos["side"] == "LONG":
            upct = (price - pos["entry"]) / pos["entry"] * 100
        else:
            upct = (pos["entry"] - price) / pos["entry"] * 100
        uusdt = upct/100 * notional
        sign  = "+" if upct >= 0 else ""
        icon  = "🟢" if upct >= 0 else "🔴"
        trail = " [🔒TRAIL]" if pos["trailing_on"] else ""
        hold  = (time.time() - pos["open_time"]) / 60
        print(f"     {icon} {sym:12} {pos['side']:5} │ uPnL: {sign}{upct:.3f}% ({sign}${uusdt:.2f}) │ hold: {hold:.0f}m{trail}")

# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════
def run_bot():
    print("╔══════════════════════════════════════════════════════╗")
    print("║       CRYPTO FUTURES BOT v5 — SMART MULTI-FILTER    ║")
    print("╚══════════════════════════════════════════════════════╝")
    print(f"  Modal/Posisi   : ${ORDER_VALUE_USDT} USDT × {LEVERAGE}x = ${ORDER_VALUE_USDT*LEVERAGE} notional")
    print(f"  TP             : ATR × {TP_ATR_MULT} (min {MIN_TP_PCT*100:.1f}%)")
    print(f"  SL             : ATR × {SL_ATR_MULT} (min {MIN_SL_PCT*100:.1f}%) → R:R ~1:{TP_ATR_MULT/SL_ATR_MULT:.1f}")
    print(f"  Min Score      : {MIN_SCORE}  │  Min Konfirmasi: {MIN_CONFIRM}/9")
    print(f"  Max Posisi     : {MAX_POSITIONS}  │  Trailing: {'ON' if TRAILING_STOP else 'OFF'}")
    print(f"  Filter         : 9-indikator + HTF 15m + Dominance + F&G + News")
    print(f"  {'═'*54}\n")

    print("  ⏳ Setting leverage & loading symbol info...")
    set_leverage_all()
    for sym in TOP_SYMBOLS: get_symbol_info(sym)
    print("  ✅ Ready!\n")

    cycle = 0
    while True:
        cycle += 1
        ts = time.strftime("%H:%M:%S")
        print(f"\n{'═'*60}")
        print(f"  🔄 Siklus #{cycle} — {ts}")
        print(f"{'═'*60}")

        # ── Market Regime ─────────────────────────────────────
        btc_d, usdt_d, btc_trend, usdt_trend = get_dominance()
        fg_val, fg_class                      = get_fear_greed()
        news                                  = get_news()
        regime, regime_bias                   = get_market_regime(btc_d, usdt_d, btc_trend, usdt_trend, fg_val)

        print(f"  📊 BTC.D : {btc_d}% ({btc_trend})  │  USDT.D: {usdt_d}% ({usdt_trend})")
        print(f"  😱 F&G   : {fg_val} — {fg_class}")
        print(f"  📰 News  : {news}")
        print(f"  🌍 Regime: {regime}  │  Bias: {regime_bias}")

        if pnl.total > 0:
            ns = "+" if pnl.net >= 0 else ""
            print(f"  💼 Sesi  : {pnl.total} trades │ WR: {pnl.winrate:.1f}% │ Net: {ns}${pnl.net:.2f} USDT")

        # Blokir semua entry baru jika berita sangat buruk
        block_entry = (news == "very_negative")
        if block_entry:
            print(f"  🚫 ENTRY DIBLOKIR — Berita sangat negatif")

        # ── Cek & tutup posisi aktif ──────────────────────────
        for symbol in list(open_positions.keys()):
            price = get_price(symbol)
            if not price: continue
            closed = check_tp_sl(symbol, price)
            if not closed and news in ("very_negative","negative"):
                # Kalau berita buruk & posisi LONG → tutup
                if open_positions.get(symbol,{}).get("side") == "LONG":
                    close_position(symbol, "📰 News negatif — exit LONG", price)

        # Tampilkan posisi aktif
        show_positions()

        # ── Scan kandidat baru ────────────────────────────────
        candidates = []

        if not block_entry:
            for symbol in TOP_SYMBOLS:
                if symbol in open_positions: continue
                if len(open_positions) >= MAX_POSITIONS: break

                # Ambil data 5m
                df = get_ohlcv(symbol, INTERVAL_FAST, limit=150)
                if df is None or len(df) < 60: continue

                df = analyze(df)

                # Market quality filter
                ok, reason = is_valid_market(df)
                if not ok:
                    # print(f"     ⏭ {symbol}: {reason}")  # uncomment untuk debug
                    continue

                # HTF bias
                htf_bias = get_htf_bias(symbol)

                # Scoring
                last = df.iloc[-1]
                ls, ss, lc, sc, lr, sr = score_and_confirm(
                    df, htf_bias, regime_bias, btc_d, usdt_trend, news
                )

                atr = last["atr"]

                # Masuk kandidat hanya jika skor DAN konfirmasi cukup
                if ls >= MIN_SCORE and lc >= MIN_CONFIRM:
                    candidates.append((ls, lc, symbol, "LONG",  atr, lr))
                if ss >= MIN_SCORE and sc >= MIN_CONFIRM:
                    candidates.append((ss, sc, symbol, "SHORT", atr, sr))

        # Sort by score DESC, then confirms DESC
        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

        if candidates:
            print(f"\n  🎯 Top Kandidat ({len(candidates)} sinyal):")
            for sc_val, cf, sym, side, atr_, _ in candidates[:6]:
                print(f"     {sym:12} {side:5} │ skor={sc_val:3d} │ konfirmasi={cf}/{MIN_CONFIRM}")
        else:
            print(f"\n  ⏳ Belum ada sinyal memenuhi syarat (min skor {MIN_SCORE}, konfirmasi {MIN_CONFIRM})")

        # ── Buka posisi terbaik ───────────────────────────────
        slot    = MAX_POSITIONS - len(open_positions)
        entered = 0
        for sc_val, cf, symbol, side, atr_, reasons in candidates:
            if entered >= slot: break
            if symbol in open_positions: continue
            open_position(symbol, side, sc_val, cf, atr_, reasons)
            entered += 1

        print(f"\n  ⏱️  Menunggu 60 detik...\n")
        time.sleep(60)

if __name__ == "__main__":
    run_bot()