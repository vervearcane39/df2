"""
DATA LAYER — db_utils.py
Responsibility: Write helpers (IV log).  Read-only callers must use db_loader.
"""
import sqlite3
import time


_last_iv_write = 0


def write_iv_log(db_path: str, atm_iv: float, straddle: float, throttle_secs: int = 60):
    global _last_iv_write
    if time.time() - _last_iv_write < throttle_secs:
        return
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS iv_log "
                     "(timestamp INTEGER PRIMARY KEY, atm_iv REAL, straddle_price REAL)")
        conn.execute("INSERT OR REPLACE INTO iv_log VALUES (?,?,?)",
                     (int(time.time()), atm_iv, straddle))
        conn.commit()
        conn.close()
        _last_iv_write = time.time()
    except Exception:
        pass
