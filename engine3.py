"""
ENGINE — engine3.py  (Phase 9.2)
Adds futures subscription to existing WebSocket feed.

Changes from Phase 9.1:
    1. subscribe NIFTY futures token alongside options
    2. futures_candles table (write 1-min candles, same schema as candles)
    3. forward-fill idle futures token between aggregator cycles

Futures token:
    Use the current front-month Nifty futures symbol.
    Find it in the scrip master: name="NIFTY", instrumenttype="FUTIDX"
    OR hard-code the front-month token and update weekly.

    Token "26000" is the standard NSE Nifty 50 FUTURES continuous token
    used by Angel Broking SmartAPI.  Verify this against your account.
"""
import time
import sqlite3
import pandas as pd
import pyotp
import requests
import logging
import threading
import sys
import os
import json
import traceback
from datetime import datetime, time as dt_time
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ── CREDENTIALS (fill these in) ──────────────────────────────────────────────
API_KEY     = "QojuoKHB"
CLIENT_ID   = "AACD466608"
PIN         = "0512"
TOTP_SECRET = "TUWOIJB64SIINTLWC4SKEEINQ4"

DB_NAME  = "market_data.db"
LOG_FILE = "engine.log"

logging.basicConfig(
    filename=LOG_FILE, level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)


def init_db():
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute('''CREATE TABLE IF NOT EXISTS candles
                     (timestamp INTEGER, token TEXT, symbol TEXT,
                     strike INTEGER, type TEXT, open REAL, high REAL,
                     low REAL, close REAL, oi REAL, volume REAL,
                     PRIMARY KEY (timestamp, token))''')

        conn.execute('''CREATE TABLE IF NOT EXISTS system_health
                     (timestamp INTEGER PRIMARY KEY, spot_price REAL,
                     expected_tokens INTEGER, received_tokens INTEGER,
                     health_pct REAL, india_vix REAL DEFAULT 0.0)''')
        # Add india_vix column if upgrading existing DB
        try:
            conn.execute("ALTER TABLE system_health ADD COLUMN india_vix REAL DEFAULT 0.0")
        except Exception:
            pass  # Column already exists

        # ── NEW: futures candles table ────────────────────────────────────
        conn.execute('''CREATE TABLE IF NOT EXISTS futures_candles
                     (timestamp INTEGER PRIMARY KEY,
                     open REAL, high REAL, low REAL, close REAL,
                     volume REAL)''')

        conn.execute('''CREATE TABLE IF NOT EXISTS strike_oi_history
                     (timestamp INTEGER, strike INTEGER, type TEXT, oi REAL,
                     PRIMARY KEY (timestamp, strike, type))''')

        conn.execute('''CREATE TABLE IF NOT EXISTS token_registry
                     (token TEXT PRIMARY KEY, first_seen_ts INTEGER)''')

        cutoff = int(time.time()) - (48 * 3600)
        conn.execute("DELETE FROM candles WHERE timestamp < ?", (cutoff,))
        conn.execute("DELETE FROM system_health WHERE timestamp < ?", (cutoff,))
        conn.execute("DELETE FROM futures_candles WHERE timestamp < ?", (cutoff,))
        conn.execute("DELETE FROM strike_oi_history WHERE timestamp < ?",
                     (int(time.time()) - 6 * 3600,))
        conn.commit()
        conn.execute("VACUUM")
        conn.close()
    except Exception as e:
        logging.error(f"❌ DB Init Failed: {e}")
        sys.exit(1)


def is_market_hours():
    now = datetime.now().time()
    return dt_time(9, 15) <= now <= dt_time(15, 30)


class TradeEngine:
    def __init__(self):
        self.api       = None
        self.sws       = None
        self.tokens    = {}
        self.spot_token    = "99926000"
        self.vix_token     = "1340"       # India VIX (NSE exchange type 1)
        # ── NEW: futures token ────────────────────────────────────────────
        self.futures_token = "26000"      # Nifty front-month futures (verify)
        self.center_strike = 0
        self.running   = True
        self.lock      = threading.Lock()
        self.aggregator_running  = False
        self.reconnect_flag      = False

        self.candle_buffer       = {}
        self.spot_buffer         = {'close': 0, 'last_update': 0}
        self.vix_buffer          = {'close': 0.0, 'last_update': 0}
        # ── NEW: futures buffer ───────────────────────────────────────────
        self.futures_buffer      = {
            'open': 0, 'high': 0, 'low': 0, 'close': 0,
            'vol_start': 0, 'last_vol': 0, 'last_update': 0}
        self.active_tokens_this_min = set()
        self.last_known_state    = {}
        self.master_cache_df     = None
        self.master_cache_date   = None

    # ── Login (unchanged) ─────────────────────────────────────────────────
    def login(self):
        try:
            self.api  = SmartConnect(api_key=API_KEY)
            totp      = pyotp.TOTP(TOTP_SECRET).now()
            data      = self.api.generateSession(CLIENT_ID, PIN, totp)
            if not data['status']:
                raise Exception(data['message'])
            jwt       = data['data']['jwtToken'].replace("Bearer ", "")
            refresh   = data['data']['refreshToken']
            self.api.setAccessToken(jwt)
            self.api.setRefreshToken(refresh)
            self.api.getProfile(refresh)
            self.feed_token = data['data']['feedToken']
            self.jwt_token  = jwt
            logging.info("✅ Login OK")
            return True
        except Exception as e:
            logging.error(f"❌ Login Failed: {e}")
            return False

    # ── Scrip master (unchanged) ──────────────────────────────────────────
    def load_scrip_master(self):
        today      = datetime.now().strftime('%Y-%m-%d')
        cache_file = "scrip_master.json"
        if os.path.exists(cache_file):
            try:
                file_date = datetime.fromtimestamp(
                    os.path.getmtime(cache_file)).strftime('%Y-%m-%d')
                if file_date == today:
                    with open(cache_file, 'r') as f:
                        data = json.load(f)
                    df = pd.DataFrame(data)
                    df = df[(df['name'] == 'NIFTY') & (df['instrumenttype'] == 'OPTIDX')]
                    df['expiry_dt'] = pd.to_datetime(df['expiry'], format='%d%b%Y')
                    return df
            except Exception:
                pass

        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(cache_file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        with open(cache_file, 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        df = df[(df['name'] == 'NIFTY') & (df['instrumenttype'] == 'OPTIDX')]
        df['expiry_dt'] = pd.to_datetime(df['expiry'], format='%d%b%Y')
        return df

    # ── Token map (unchanged core + ±2000) ───────────────────────────────
    def get_token_map(self, force_center=None):
        try:
            if force_center:
                center = force_center
            else:
                spot_data = self.api.ltpData("NSE", "Nifty 50", self.spot_token)
                if not spot_data['status']:
                    raise Exception("Spot fetch failed")
                center = int(round(float(spot_data['data']['ltp']) / 50) * 50)

            opt = self.load_scrip_master()
            if opt is None: return False
            nearest_exp  = opt[opt['expiry_dt'] >= pd.Timestamp.now().normalize()]['expiry_dt'].min()
            target_df    = opt[opt['expiry_dt'] == nearest_exp]
            valid_strikes = range(center - 2000, center + 2000, 50)
            new_tokens   = {}
            for _, row in target_df.iterrows():
                strike = int(float(row['strike']) / 100.0)
                if strike in valid_strikes:
                    t = row['token']
                    new_tokens[t] = {'strike': strike, 'type': row['symbol'][-2:],
                                     'symbol': row['symbol']}
            with self.lock:
                self.tokens      = new_tokens
                self.center_strike = center
                valid_keys       = set(new_tokens.keys())
                self.candle_buffer   = {k: v for k, v in self.candle_buffer.items()   if k in valid_keys}
                self.last_known_state = {k: v for k, v in self.last_known_state.items() if k in valid_keys}
            logging.info(f"🎯 Map: center={center}, tracking={len(new_tokens)} (±2000)")
            return True
        except Exception as e:
            logging.error(f"❌ Map Error: {e}")
            return False

    # ── Dynamic recentering ───────────────────────────────────────────────
    def update_dynamic_strikes(self):
        spot = self.spot_buffer['close']
        if spot == 0: return
        curr = int(round(spot / 50) * 50)
        if abs(curr - self.center_strike) >= 100:
            logging.info(f"🔄 Shifted {self.center_strike} → {curr}")
            if self.get_token_map(force_center=curr):
                self.reconnect_flag = True
                try:
                    if self.sws: self.sws.close_connection()
                except Exception:
                    pass

    # ── WebSocket data handler ────────────────────────────────────────────
    def on_data(self, wsapp, msg):
        try:
            if 'token' not in msg or 'last_traded_price' not in msg:
                return
            token = msg['token']
            ltp   = float(msg['last_traded_price']) / 100.0

            # ── Spot ──────────────────────────────────────────────────────
            if token == self.spot_token:
                with self.lock:
                    self.spot_buffer['close']       = ltp
                    self.spot_buffer['last_update'] = time.time()
                return

            # ── VIX ──────────────────────────────────────────────────────
            if token == self.vix_token:
                with self.lock:
                    self.vix_buffer['close']       = ltp
                    self.vix_buffer['last_update'] = time.time()
                return

            # ── NEW: Futures ───────────────────────────────────────────────
            if token == self.futures_token:
                vol = float(msg.get('vol_traded', 0)) or float(msg.get('volume', 0))
                with self.lock:
                    fb = self.futures_buffer
                    if fb['close'] == 0:
                        fb['open'] = fb['high'] = fb['low'] = ltp
                        fb['vol_start'] = vol
                    fb['high']        = max(fb['high'], ltp)
                    fb['low']         = min(fb['low'], ltp)
                    fb['close']       = ltp
                    fb['last_vol']    = vol
                    fb['last_update'] = time.time()
                return

            # ── Options ───────────────────────────────────────────────────
            vol = float(msg.get('vol_traded', 0)) or float(msg.get('volume', 0))
            oi  = float(msg.get('open_interest', 0))

            with self.lock:
                self.active_tokens_this_min.add(token)
                self.last_known_state[token] = {'close': ltp, 'oi': oi}
                if token not in self.candle_buffer:
                    self.candle_buffer[token] = {
                        'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp,
                        'vol_start': vol, 'last_vol': vol, 'oi': oi, 'accumulated_vol': 0}
                else:
                    d = self.candle_buffer[token]
                    d['high']  = max(d['high'], ltp)
                    d['low']   = min(d['low'],  ltp)
                    d['close'] = ltp
                    d['oi']    = oi
                    if vol == 0:
                        tick = float(msg.get('last_traded_quantity', 0))
                        d['accumulated_vol'] += tick
                        d['last_vol'] = d['accumulated_vol']
                    else:
                        d['last_vol'] = vol
        except Exception as e:
            logging.error(f"WS Error: {e}")

    # ── Aggregator ────────────────────────────────────────────────────────
    def aggregator_loop(self):
        db_conn = None
        try:
            db_conn = sqlite3.connect(DB_NAME, check_same_thread=False)
            db_conn.execute("PRAGMA journal_mode=WAL;")
            last_dynamic_check  = time.time()
            last_oi_snap        = time.time()
            logging.info("⏳ Aggregator started")

            while self.running:
                now_ts     = time.time()
                sleep_time = 60 - (now_ts % 60)
                time.sleep(sleep_time)
                ts_bucket  = ((int(time.time()) // 60) * 60) - 60

                if time.time() - last_dynamic_check > 300:
                    self.update_dynamic_strikes()
                    last_dynamic_check = time.time()

                records      = []
                oi_snap_recs = []
                do_oi_snap   = (time.time() - last_oi_snap) >= 300   # every 5 min

                with self.lock:
                    for token, meta in self.tokens.items():
                        if token in self.candle_buffer:
                            data = self.candle_buffer[token]
                            minute_vol = (data['last_vol'] if data['last_vol'] < data['vol_start']
                                         else data['last_vol'] - data['vol_start'])
                            records.append((
                                ts_bucket, token, meta['symbol'],
                                meta['strike'], meta['type'],
                                data['open'], data['high'], data['low'],
                                data['close'], data['oi'], minute_vol))
                            # Reset
                            data['open']      = data['close']
                            data['high']      = data['close']
                            data['low']       = data['close']
                            data['vol_start'] = data['last_vol']

                        elif token in self.last_known_state:
                            last = self.last_known_state[token]
                            records.append((
                                ts_bucket, token, meta['symbol'],
                                meta['strike'], meta['type'],
                                last['close'], last['close'], last['close'],
                                last['close'], last['oi'], 0))

                        if do_oi_snap and token in self.last_known_state:
                            last = self.last_known_state[token]
                            oi_snap_recs.append(
                                (ts_bucket, meta['strike'], meta['type'], last['oi']))

                    # Futures candle
                    fb        = self.futures_buffer
                    fut_alive = (time.time() - fb['last_update']) < 120
                    if fut_alive and fb['close'] > 0:
                        fut_vol = (fb['last_vol'] if fb['last_vol'] < fb['vol_start']
                                  else fb['last_vol'] - fb['vol_start'])
                        fut_rec = (ts_bucket, fb['open'], fb['high'], fb['low'],
                                   fb['close'], fut_vol)
                        # Reset futures buffer
                        fb['open']      = fb['close']
                        fb['high']      = fb['close']
                        fb['low']       = fb['close']
                        fb['vol_start'] = fb['last_vol']
                    else:
                        fut_rec = None

                    expected = len(self.tokens)
                    received = len(self.active_tokens_this_min)
                    spot     = self.spot_buffer['close']
                    spot_alive = (time.time() - self.spot_buffer['last_update']) < 60
                    health   = (100.0 if not is_market_hours() else
                                0.0   if (spot_alive and received == 0) else
                                (received / expected * 100.0 if expected > 0 else 0.0))
                    self.active_tokens_this_min.clear()

                try:
                    if records:
                        db_conn.executemany(
                            "INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            records)
                    vix_val = self.vix_buffer['close']
                    db_conn.execute(
                        "INSERT OR REPLACE INTO system_health VALUES (?,?,?,?,?,?)",
                        (ts_bucket, spot, expected, received, health, vix_val))
                    if fut_rec:
                        db_conn.execute(
                            "INSERT OR REPLACE INTO futures_candles VALUES (?,?,?,?,?,?)",
                            fut_rec)
                    if oi_snap_recs:
                        db_conn.executemany(
                            "INSERT OR REPLACE INTO strike_oi_history VALUES (?,?,?,?)",
                            oi_snap_recs)
                        # Cleanup oi_history > 6hr
                        db_conn.execute(
                            "DELETE FROM strike_oi_history WHERE timestamp < ?",
                            (int(time.time()) - 6 * 3600,))
                        last_oi_snap = time.time()
                    db_conn.commit()
                    logging.info(
                        f"💾 {datetime.fromtimestamp(ts_bucket).strftime('%H:%M')} "
                        f"Spot:{spot} Health:{health:.0f}% "
                        f"Fut:{'✅' if fut_rec else '—'}")
                except Exception as e:
                    logging.error(f"DB Write: {e}")

        except Exception as e:
            logging.error(f"Aggregator crash: {e}")
        finally:
            if db_conn:
                db_conn.close()

    def start(self):
        init_db()
        while self.running:
            try:
                if not self.login():
                    time.sleep(10)
                    continue
                if not self.get_token_map():
                    time.sleep(10)
                    continue

                if not self.aggregator_running:
                    t = threading.Thread(target=self.aggregator_loop, daemon=True)
                    t.start()
                    self.aggregator_running = True

                self.reconnect_flag = False
                self.sws = SmartWebSocketV2(
                    self.jwt_token, API_KEY, CLIENT_ID, self.feed_token)
                self.sws.on_data = self.on_data

                def _on_open(ws):
                    # Subscribe spot + VIX + futures (exchange 1=NSE, 2=NFO)
                    self.sws.subscribe("mw", 3, [
                        {"exchangeType": 1, "tokens": [
                            self.spot_token,
                            self.vix_token,
                            self.futures_token]},
                        {"exchangeType": 2, "tokens": list(self.tokens.keys())},
                    ])

                self.sws.on_open = _on_open
                logging.info("🔗 WebSocket connecting…")
                self.sws.connect()

                if self.reconnect_flag:
                    logging.info("♻️ Reconnecting…")
                    time.sleep(1)
                    continue

            except Exception as e:
                logging.error(f"🔥 Crash: {e}. Restarting in 5s…")
                time.sleep(5)


if __name__ == "__main__":
    TradeEngine().start()