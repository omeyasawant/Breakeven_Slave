#!/usr/bin/env python
# coding: utf-8

# In[1]:


# slave.py
import socket
import json
import ipaddress
import re
import time
import threading
import subprocess
import psutil
import platform
import pandas as pd
import numpy as np
from breakeven_quant import Fetch_Data_Specs , Feat_Eng
from multiprocessing import Pool, cpu_count
import multiprocessing as mp
import pickle
import base64
import warnings
from pandas.errors import PerformanceWarning
import uuid
import hashlib
import os
import traceback
from tqdm import tqdm
import sys
import datetime 
import ssl
import functools
from typing import Optional, Tuple, List
import zstandard as zstd


# In[2]:


import requests
import hashlib
import io, gzip, pickle, os
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from backtesting import Strategy
from backtesting import Backtest


# In[3]:


from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ## Global Variables
# 

# In[4]:


slave_id = None
slave_name = 'omeya_sudo'
buffer_cores = 6
total_subtasks = 0
completed = None
WORK_STATUS = "idle"
FINAL_RESULT_ACK_EVENTS = {}  # work_id -> threading.Event
APPEND_WAITERS = {}  # req_id -> (Event, holder)


# --- TIMEOUT CONFIG (slave side) ---------------------------------
HEARTBEAT_INTERVAL = 10          # seconds
POOL_TIMEOUT_SEC   = 5400        # 90 min – generous share-timeout
# -----------------------------------------------------------------


# In[ ]:


ACTIVE_WORKS = {}  # work_id -> control dict
ACTIVE_WORKS_LOCK = threading.Lock()


# In[5]:


# Global session for ALL HTTP calls to MinIO (PUT/GET)
_HTTP = requests.Session()

retries = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "PUT", "POST", "HEAD"]
)

adapter = HTTPAdapter(
    pool_connections=200,
    pool_maxsize=200,
    max_retries=retries
)

_HTTP.mount("http://", adapter)
_HTTP.mount("https://", adapter)


# ## Connection DEBUGS

# In[6]:


# -------------------- CONNECT DEBUG + SPEED --------------------
CONNECT_DEBUG = True
FORCE_IPV4 = True            # set False if you want IPv6 too
DNS_CACHE_TTL_SEC = 15       # cache DNS for 60 seconds
TCP_CONNECT_TIMEOUT = 120.0    # seconds
TLS_HANDSHAKE_TIMEOUT = 120.0 # seconds
ASSIGN_ID_TIMEOUT = 120.0     # seconds

_dns_cache = {}  # host -> (expires_at, [(family, sockaddr), ...])

def _now_ms() -> int:
    return int(time.perf_counter() * 1000)

def _dbg(msg: str):
    if CONNECT_DEBUG:
        print(msg)

def resolve_host_cached(host: str, port: int) -> List[Tuple[int, tuple]]:
    """
    Returns a list of (family, sockaddr) tuples, cached for DNS_CACHE_TTL_SEC.
    Forces IPv4 first if FORCE_IPV4=True.
    """
    now = time.time()
    hit = _dns_cache.get(host)
    if hit and hit[0] > now:
        return hit[1]

    # Resolve
    # type=SOCK_STREAM ensures we get connectable results
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    addrs = [(fam, sockaddr) for (fam, _typ, _proto, _cname, sockaddr) in infos]

    # Optional: prefer IPv4 to avoid slow IPv6 path / resolver delays
    if FORCE_IPV4:
        addrs.sort(key=lambda x: 0 if x[0] == socket.AF_INET else 1)

    _dns_cache[host] = (now + DNS_CACHE_TTL_SEC, addrs)
    return addrs

def timed_step(label: str, t0_ms: int) -> int:
    t1 = _now_ms()
    _dbg(f"[CONNECT][{label}] +{t1 - t0_ms} ms")
    return t1

def recv_message_timeout(sock, timeout_sec: float):
    """
    Wait for your length-prefixed recv with timeout.
    Restores socket timeout after call.
    """
    old = sock.gettimeout()
    sock.settimeout(timeout_sec)
    try:
        return recv_message(sock)
    finally:
        sock.settimeout(old)
# ---------------------------------------------------------------


# ## S3 Fetch

# In[7]:


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def fetch_payload_ref(payload_ref: dict) -> dict:
    """
    Download payload bytes from presigned URL, verify hash, decompress zstd, parse JSON dict.
    Returns the decoded work_data dict.
    """
    url = payload_ref["url"]
    expected_sha = payload_ref.get("sha256")
    expected_bytes = int(payload_ref.get("bytes", 0))
    compression = payload_ref.get("compression")

    # Stream download so you don't blow RAM on huge payloads accidentally
    h = hashlib.sha256()
    buf = bytearray()

    with requests.get(url, stream=True, timeout=(30, 600)) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            h.update(chunk)
            buf.extend(chunk)

    got = bytes(buf)

    if expected_bytes and len(got) != expected_bytes:
        raise ValueError(f"Payload size mismatch: got={len(got)} expected={expected_bytes}")

    if expected_sha and h.hexdigest() != expected_sha:
        raise ValueError(f"Payload sha256 mismatch: got={h.hexdigest()} expected={expected_sha}")

    ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
    if compression == "zstd" or got[:4] == ZSTD_MAGIC:
        dctx = zstd.ZstdDecompressor()
        raw = dctx.decompress(got, max_output_size=5_000_000_000)  # adjust if needed
    else:
        raw = got

    # raw is JSON bytes for work_data
    return json.loads(raw.decode("utf-8"))


# In[8]:


def api_append_rows_gz(conn, slave_id, work_id, token, dir_name, abs_path, header, rows, allowed_output_prefix, timeout=600):
    req_id = uuid.uuid4().hex
    rel = to_rel_path_under_run(abs_path, dir_name)
    rel_path = f"{allowed_output_prefix.rstrip('/')}/{rel}"

    msg = {
        "command": "api_append_rows",
        "slave_id": slave_id,
        "work_id": work_id,
        "token": token,
        "req_id": req_id,
        "rel_path": rel_path,
        "header": header,
        "rows": rows,
        "compression": "gzip",
    }

    ev = threading.Event()
    holder = {"resp": None}
    APPEND_WAITERS[req_id] = (ev, holder)

    safe_send(conn, msg)

    if not ev.wait(timeout):
        APPEND_WAITERS.pop(req_id, None)
        raise TimeoutError("api_append_rows_resp timeout")

    APPEND_WAITERS.pop(req_id, None)
    resp = holder["resp"]
    if not resp.get("ok"):
        raise RuntimeError(f"api_append_rows denied: {resp.get('reason')}")
    return True


# ## Logging 

# In[9]:


# --- Logging to file so Electron dashboard can read output ---
#LOG_DIR = os.path.join(os.path.expanduser("~"), "BreakEvenClient", "logs")
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "slave.log")

# --- PID Tracker (parent process only, no tqdm interference) ---
PID_TRACK_DIR = LOG_DIR   # keep it alongside slave.log
PID_TRACK_FILE = os.path.join(PID_TRACK_DIR, "slave_pid_tracker.log")

def pid_track(msg: str):
    ts = datetime.datetime.now().isoformat()
    with open(PID_TRACK_FILE, "a", encoding="utf-8") as f:
        f.write(f"{ts} | {msg}\n")

class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)
            s.flush()

    def flush(self):
        for s in self.streams:
            s.flush()

# Log basic launch info using existing logging
LAUNCH_TIME = datetime.datetime.now().isoformat()
PID = os.getpid()


IS_PARENT_PROCESS = (mp.parent_process() is None)

if IS_PARENT_PROCESS:
    print("")
    print("====================================================")
    print(f"[SLAVE] Breakeven_Slave.py LAUNCH")
    print(f"[SLAVE] PID           : {PID}")
    print(f"[SLAVE] Launch time   : {LAUNCH_TIME}")
    print(f"[SLAVE] Log file      : {LOG_FILE}")
    print(f"[SLAVE] Working dir   : {os.getcwd()}")
    print("====================================================")
    print("")

_log_fp = open(LOG_FILE, "w", buffering=1, encoding="utf-8")
sys.stdout = Tee(sys.stdout, _log_fp)
sys.stderr = Tee(sys.stderr, _log_fp)
# --------------------------------------------------------------



pid_track("=============================================================================")
pid_track("[SLAVE] Breakeven_Slave.py LAUNCH")
pid_track(f"[SLAVE] PID           : {PID}")
pid_track(f"[SLAVE] Launch time   : {LAUNCH_TIME}")
pid_track(f"[SLAVE] Log file      : {LOG_FILE}")
pid_track(f"[SLAVE] Working dir   : {os.getcwd()}")
pid_track("=============================================================================")
'''
print("")
print("====================================================")
print(f"[SLAVE] Breakeven_Slave.py LAUNCH")
print(f"[SLAVE] PID           : {PID}")
print(f"[SLAVE] Launch time   : {LAUNCH_TIME}")
print(f"[SLAVE] Log file      : {LOG_FILE}")
print(f"[SLAVE] Working dir   : {os.getcwd()}")
print("====================================================")
print("")
'''


# ## Messaging Protocols
# 

# In[10]:


def json_default(o):
    # numpy scalars
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    # numpy arrays
    if isinstance(o, (np.ndarray,)):
        return o.tolist()
    # pandas types
    if isinstance(o, (pd.Timestamp,)):
        return o.isoformat()
    if isinstance(o, (pd.Timedelta,)):
        return str(o)
    # python datetime
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    # bytes
    if isinstance(o, (bytes, bytearray)):
        return o.decode("utf-8", errors="replace")
    # fallback
    return str(o)


# In[11]:


def get_machine_uid():
    try:
        # Try actual MAC address
        mac = uuid.getnode()
        if (mac >> 40) % 2:  # If locally administered/random MAC
            raise ValueError("Random MAC address detected")
        mac_str = ':'.join(f'{(mac >> ele) & 0xff:02x}' for ele in reversed(range(0, 8*6, 8)))
        return f"MAC:{mac_str}"
    except:
        # Fallback to host + CPU + platform hash
        data = f"{platform.node()}|{platform.system()}|{platform.processor()}|{os.getpid()}|{socket.gethostname()}"
        hashed = hashlib.sha256(data.encode()).hexdigest()
        return f"UID:{hashed[:16]}"
#get_machine_uid()


# In[12]:


def get_ip_address():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "0.0.0.0"



# In[13]:


'''
def deserialize_dataframes(encoded_list):
    return [pickle.loads(base64.b64decode(s)) for s in encoded_list]
'''
def serialize_dataframes(sub_dfs):
    """
    Pickles and base64-encodes a list of pandas DataFrames.
    Preserves index including DatetimeIndex.
    """
    return [
        base64.b64encode(pickle.dumps(df)).decode('utf-8')
        for df in sub_dfs
    ]

def deserialize_dataframes(encoded_list):
    """
    Decodes base64 and unpickles the list of DataFrames.
    Ensures timestamp index (like DatetimeIndex) is preserved.
    """
    deserialized = []
    for s in encoded_list:
        df = pickle.loads(base64.b64decode(s))
        # Ensure index is DatetimeIndex if it looks like timestamps
        if not isinstance(df.index, pd.DatetimeIndex) and pd.api.types.is_datetime64_any_dtype(df.index):
            df.index = pd.to_datetime(df.index)
        deserialized.append(df)
    return deserialized


# In[14]:


def send_message(conn, message_dict):
    #message_bytes = json.dumps(message_dict).encode()
    message_bytes = json.dumps(message_dict, default=json_default).encode()
    length_prefix = len(message_bytes).to_bytes(4, byteorder='big')  # 4-byte length
    conn.sendall(length_prefix + message_bytes)


# In[15]:


'''
SEND_LOCK = threading.Lock()
def safe_send(conn, payload):
    with SEND_LOCK:
        send_message(conn, payload)
'''

_conn_locks = {}
_conn_locks_lock = threading.Lock()

def _get_conn_lock(conn):
    key = id(conn)
    with _conn_locks_lock:
        if key not in _conn_locks:
            _conn_locks[key] = threading.Lock()
        return _conn_locks[key]

def safe_send(conn, payload):
    with _get_conn_lock(conn):
        send_message(conn, payload)


# In[16]:


def recv_message(sock):
    # First receive 4-byte length prefix
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = int.from_bytes(raw_msglen, byteorder='big')

    # NEW: print when a big message header is seen
    if msglen >= 50 * 1024 * 1024:
        print(f"[SLAVE][HDR] incoming message len={msglen:,} bytes ({msglen/(1024**3):.3f} GB)")

    # Receive full message based on length
    #return recvall(sock, msglen).decode()
    body = recvall(sock, msglen)
    if not body:
        return None
    return body.decode(errors="replace")
'''
def recvall(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data
'''

def recvall(sock, n):
    """
    Receive exactly n bytes.
    For large payloads (>=50MB), print progress every ~2 seconds.
    """
    data = bytearray()
    t0 = time.time()
    last_print = t0
    last_bytes = 0

    # tune these if you want
    CHUNK = 1024 * 1024               # 1 MB chunks
    PROGRESS_MIN_BYTES = 50 * 1024 * 1024  # only show progress if >= 50MB
    PRINT_EVERY_SEC = 2.0

    while len(data) < n:
        to_read = min(CHUNK, n - len(data))
        packet = sock.recv(to_read)
        if not packet:
            return None
        data.extend(packet)

        # Progress logs for big messages
        if n >= PROGRESS_MIN_BYTES:
            now = time.time()
            if now - last_print >= PRINT_EVERY_SEC:
                done = len(data)
                total = n
                done_mb = done / (1024 * 1024)
                total_mb = total / (1024 * 1024)
                pct = (done / total) * 100

                # speed since last print
                delta_bytes = done - last_bytes
                delta_t = now - last_print
                speed_mbps = (delta_bytes / (1024 * 1024)) / max(delta_t, 1e-6)

                elapsed = now - t0
                print(f"[SLAVE][RECV] {done_mb:,.1f}/{total_mb:,.1f} MB ({pct:5.1f}%)  ~{speed_mbps:,.1f} MB/s  elapsed={elapsed:,.1f}s")

                last_print = now
                last_bytes = done

    # Final timing log for big messages
    if n >= PROGRESS_MIN_BYTES:
        elapsed = time.time() - t0
        total_mb = n / (1024 * 1024)
        avg_speed = total_mb / max(elapsed, 1e-6)
        print(f"[SLAVE][RECV] DONE {total_mb:,.1f} MB in {elapsed:,.2f}s  avg={avg_speed:,.1f} MB/s")

    return data


# # FE Imprinting Code

# In[17]:


def perform_core_FE(data):
    
    # --- Fix Warnings Before Pandas Mess ---
    try:
        warnings.resetwarnings()
        warnings.simplefilter(action='ignore', category=UserWarning)
        warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
    except Exception as e:
        print(f"[WARNING] Failed resetting warnings cleanly: {e}")
    
    
    
    inidicators_list = ['VMA','MA','EMA','RSI','TR','ATR']
    trend_signals_list = ['SoloEMA','EMACrossover']
    
    # Variables
    ema_periods = [9,12,21,26,50,100,200]
    rsi_periods = [9,10,14,20,25]
    atr_periods = [7,10,14,20,30]
    backcandles_count = [1,2,3,4,5]
    rsi_sensitivity = [5,10,15]
    
    TA_indicators_parameters = {
        "ema_periods": ema_periods,
        "rsi_periods": rsi_periods,
        "atr_periods": atr_periods,
        "backcandles_count": backcandles_count,
        "rsi_sensitivity": rsi_sensitivity
    }
    '''
    columns_to_watch = ['Open','High','Low','Close']
    for s in signals_to_watch:
        columns_to_watch.append(s)
    print('Columns to watch :',columns_to_watch)
    '''
    # Ignore PerformanceWarning
    #warnings.simplefilter(action='ignore', category=PerformanceWarning)
    
    
    F = Feat_Eng( dir_path = None , df = data , indicators_list = inidicators_list , trend_signals_list = trend_signals_list, verbose = False)
    F.run(Indicators = True, Normalizations =False , Trend_Signals = True , Tot_Signals = True, Rank = False , Save = False)
    #F.print_indicator_details()
    '''
    FE = F.df[columns_to_watch].copy(deep=True)  # Create a deep copy to avoid unintended modifications
    '''
    FE = F.df.copy(deep=True)
    # Clear F variable from memory
    del F
    return FE


# In[18]:


def process_sub_df(sub_df, resample_timeframe):
    try:
        if sub_df.empty:
            print("sub_df is empty.")
            return pd.DataFrame()  # or handle it appropriately
        else:   
            # Save the last row of the sub_df into og_last_row
            og_last_row = sub_df.iloc[-1].copy(deep=True)
    
            # Rename columns in og_last_row
            og_last_row = og_last_row.rename({
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            })
    
            # Ensure the timestamp column is in datetime format
            sub_df.index = pd.to_datetime(sub_df.index)
    
            # Resample to 15-minute intervals and aggregate appropriately
            # You can use the `timeframe` argument here if you need a different resampling rule
            sub_hist_df_15min = sub_df.resample(resample_timeframe).agg({
                'open': 'first',         # First open price in each 15-min interval
                'high': 'max',           # Max high price in each 15-min interval
                'low': 'min',            # Min low price in each 15-min interval
                'close': 'last',         # Last close price in each 15-min interval
                'volume': 'sum',         # Sum volume over each 15-min interval
                'symbol': 'first',       # Assuming symbol is consistent, take the first entry
                'day_of_week': 'first',  # Take the first entry for day_of_week
                'weekday': 'first',      # Take the first entry for weekday
                'month': 'first',        # Take the first entry for month
                'month_name': 'first'    # Take the first entry for month_name
            }).copy(deep=True)
    
            # Perform feature engineering (assuming perform_core_FE is defined elsewhere)
            FE_data = perform_core_FE(sub_hist_df_15min)
    
            if FE_data.empty:
                print("sub_df FE_data is empty after feature engineering.")
                return pd.DataFrame()  # or handle it appropriately
            else:    
                # Extract the last row of FE_data
                last_row = FE_data.iloc[-1].copy(deep=True)
        
                # Combine og_last_row and last_row into a single row
                for col in last_row.index:
                    if col not in og_last_row.index:
                        og_last_row[col] = last_row[col]
        
                # Return the combined row as a DataFrame (single row)
                result_row = pd.DataFrame([og_last_row])
                return result_row
    
    except Exception as e:
        print(f"Error in process_sub_df: {e}")
        traceback.print_exc()
        return pd.DataFrame()  # Return an empty DataFrame on error


# # Backtesting

# In[ ]:





# In[19]:


def align_results(long_headers, short_headers, long_values, short_values):
    """
    Align long and short results using an order-preserving merged header,
    and map original values correctly based on header-value pairs.
    """

    # Step 1: Build a unified header based on earliest appearance index
    header_index_map = {}
    for i, h in enumerate(long_headers):
        if h not in header_index_map:
            header_index_map[h] = i
    for i, h in enumerate(short_headers):
        if h not in header_index_map:
            header_index_map[h] = i

    all_headers = list(dict.fromkeys(long_headers + short_headers))
    final_header = sorted(all_headers, key=lambda h: header_index_map[h])

    # Step 2: Use correct maps from original header-value pairings
    long_map = dict(zip(long_headers, long_values))
    short_map = dict(zip(short_headers, short_values))

    # Step 3: Align values to final header
    aligned_long_values = [long_map.get(h, np.nan) for h in final_header]
    aligned_short_values = [short_map.get(h, np.nan) for h in final_header]

    return final_header, aligned_long_values, aligned_short_values


# In[20]:


def create_long_strategy_class(Strategy, tm_parameters, slippage, signal_col):
    # Only Long Strategy
    class LongStrat(Strategy):
        initial_cash = tm_parameters['initial_cash']
        risk_per_trade = tm_parameters['risk_per_trade']
        max_loss = risk_per_trade * initial_cash
        
        sl_coeff = 0.01*tm_parameters['stop_loss']
        tp_coeff = 0.01*tm_parameters['take_profit']
        
        def init(self):
            super().init()
            #self.signal1 = self.I(SIGNAL)
            self.signal1 = self.I(lambda: self.data.df[signal_col].to_numpy())
            self.max_loss = self.risk_per_trade * self.initial_cash
        
            # Maintiain order book
            self.order_data = {}  # Dictionary to store order and their specific stop losses
        
        
        def finalize(self):
            # This method is called after the last bar
            print("Finalizing the strategy. Closing all positions.")
            self.position.close()
            # Close all active trades
            for trade in self.trades:
                if trade.exit_price == None:
                    trade.close(portion=1.0)
        
        def next(self):
            super().next()
        
            # Entry price with Slippage
            prev_close = self.data.Close[-1]
            slippage_buffer =  slippage * prev_close
            expected_entry_price = prev_close + slippage_buffer
                               
            #Trade Size Calculation with max_loss
            expected_loss_per_unit = self.sl_coeff*expected_entry_price
            self.trade_size = self.max_loss/expected_loss_per_unit

            
        
            # Rounding off Trade_size
            if self.trade_size > 1 :
                self.trade_size = round(self.trade_size)
            elif self.trade_size > 0 and self.trade_size < 1:
                self.trade_size = round(self.trade_size,4)
        
            # Buy Signal
            #if self.signal1==2:
            if self.signal1[-1] == 2:
        
                # Calculate Stop Loss
                sl1 = (1 - self.sl_coeff) * expected_entry_price #- 3000e-4 #45e-4
        
                # Calculate Take Profit
                tp1 = (1 + self.tp_coeff) * expected_entry_price #+ 45e-4 #45
        
                # Place Long Order
                order = self.buy( limit = expected_entry_price , sl=sl1 , tp=tp1 , size=self.trade_size)
                order_id = id(order)  # Using id() to generate a unique identifier
                # Adding to order book 
                self.order_data[order_id] = {'order': order, 'stop': self.data.Close[-1] * (1 - self.sl_coeff), 'type': 'long'}
        
    return LongStrat


# In[21]:


def create_short_strategy_class(Strategy, tm_parameters,slippage, signal_col):
    # Only Short Strategy
    class ShortStrat(Strategy):
        initial_cash = tm_parameters['initial_cash']
        risk_per_trade = tm_parameters['risk_per_trade']
        max_loss = risk_per_trade * initial_cash
        
        sl_coeff = 0.01*tm_parameters['stop_loss']
        tp_coeff = 0.01*tm_parameters['take_profit']
        
        def init(self):
            super().init()
            #self.signal1 = self.I(SIGNAL)
            self.signal1 = self.I(lambda: self.data.df[signal_col].to_numpy())
            self.max_loss = self.risk_per_trade * self.initial_cash
        
            # Maintiain order book
            self.order_data = {}  # Dictionary to store order and their specific stop losses
        
                        
        
        def finalize(self):
            # This method is called after the last bar
            print("Finalizing the strategy. Closing all positions.")
            self.position.close()
            # Close all active trades
            for trade in self.trades:
                if trade.exit_price == None:
                    trade.close(portion=1.0)
        
        def next(self):
            super().next()
                            
            # Entry price with Slippage
            prev_close = self.data.Close[-1]
            slippage_buffer =  slippage * prev_close
            expected_entry_price = prev_close - slippage_buffer
        
            #Trade Size Calculation with max_loss
            expected_loss_per_unit = self.sl_coeff*expected_entry_price
            self.trade_size = self.max_loss/expected_loss_per_unit
                            
             
            # Rounding off Trade_size
            if self.trade_size > 1 :
                self.trade_size = round(self.trade_size)
            elif self.trade_size > 0 and self.trade_size < 1:
                self.trade_size = round(self.trade_size,4)
        
            # Sell Signal
            #if self.signal1==1 :
            if self.signal1[-1]==1 :
        
                # Calculate Stop Loss
                sl1 = (1 + self.sl_coeff) * expected_entry_price #+ 3000e-4 #45e-4
        
                # Calculate Take Profit
                tp1 = (1 - self.tp_coeff) * expected_entry_price #- 45e-4 #45
        
                # Placing Sell Order
                order = self.sell(limit = expected_entry_price , sl=sl1, tp=tp1, size=self.trade_size)
                order_id = id(order)  # Using id() to generate a unique identifier
                    
                # Adding to order book 
                self.order_data[order_id] = {'order': order, 'stop': self.data.Close[-1] * (1 + self.sl_coeff), 'type': 'short'}
        
    return ShortStrat    


# In[22]:


def api_update_gridsignal(conn, slave_id, work_id, token, grid_file, lock_file, row, column, new_flag):
    safe_send(conn, {
        "command": "api_write",
        "slave_id": slave_id,
        "work_id": work_id,
        "token": token,
        "op": "update_gridsignal",
        "data": {
            "grid_file": grid_file,
            "lock_file": lock_file,
            "row": row,
            "column": column,
            "new_flag": new_flag
        }
    })


# In[23]:


# Strategy calls this signal
def SIGNAL():
  return dfpl[signal_name]

def periodic_raw_analysis(period,periodic_frames,root_folder,signal_name,tm_parameters,
                          base_header,base_value,Long_Strategy,Short_Strategy,verbose=True,
                          conn=None, slave_id=None, work_id=None, api_token=None, dir_name=None,
                          allowed_output_prefix= None
                         ):

  #root_folder = Path(root_folder)
    
  # Pre-Req
  periodic_results = []
  periodic_LR=[]
  periodic_SR=[]
  counter = 0
  total_periods = len(periodic_frames)

  initial_cash = tm_parameters['initial_cash']
  comission = tm_parameters['comission']
  leverage = tm_parameters['leverage']
  risk_per_trade = tm_parameters['risk_per_trade']

  if period == 'M':
    periodically_string  = 'Monthly'
    period_string = 'Month'
  elif period == 'W':
    periodically_string  = 'Weekly'
    period_string = 'Week'
  elif period == 'D':
    periodically_string  = 'Daily'
    period_string = 'Day'


  # Create Folder for Periodic Analysis
  periodic_analysis_folder = root_folder+f'{periodically_string}_Analysis/'
  if not os.path.exists(periodic_analysis_folder):
      os.makedirs(periodic_analysis_folder, exist_ok=True)

  # Perform Period Analysis
  for dfpl in periodic_frames:

      # Increment Counter
      counter = counter + 1

      # Create Folder for Current Periodic Analysis
      counter_period = f'{period_string}{counter}'
      counter_period_folder = periodic_analysis_folder+f'{counter_period}/'
      '''
      if not os.path.exists(counter_period_folder):
          os.makedirs(counter_period_folder, exist_ok=True)
      '''
      # Periodic headers and Values for results
      periodic_header = ['Analysis Period','Current Period','Current Period Counter','Total Periods Count','Folder Path']
      periodic_value = [period_string,counter_period,counter,total_periods,counter_period_folder]

      # Count Long, Shorts and Hedge Trades
      count_longs = (dfpl[signal_name] == 2).sum()
      count_shorts = (dfpl[signal_name] == 1).sum()
      count_trades = count_longs + count_shorts

      # Strategy calls this signal
      def new_SIGNAL_function():
        return dfpl[signal_name]
          
      # Redefine the SIGNAL function
      global SIGNAL
      SIGNAL = new_SIGNAL_function
      if verbose:
          print('______________________________________')
          print('Iteration : ',counter,'/',total_periods,f' {period_string}s')
          print('Name:',signal_name)


      # BackTesting Long Strategy
      bt_long = Backtest(dfpl, Long_Strategy, cash = initial_cash, margin=1/leverage, commission=comission,exclusive_orders=False)
      stat_long = bt_long.run()
      
      #print('Backtesting Performed !')
      # save long plot file
      long_plot_file_name = counter_period_folder + 'long_plot'
      '''
      bt_long.plot( results = stat_long, filename=long_plot_file_name, plot_width=None, plot_equity=True, plot_return=True, plot_pl=True, plot_volume=True, plot_drawdown=False, smooth_equity=False, relative_equity=True, superimpose=True, resample=True, reverse_indicators=False, show_legend=True,open_browser=False)
      '''
      #print('Plot Saved')
      
      # count long trades
      if count_longs != 0:
          long_trade_captured = stat_long['# Trades']/count_longs
      elif count_longs==0 and stat_long['# Trades']==0:
          long_trade_captured = 1
      else:
          long_trade_captured = 0
          
      # Save Equity Curve
      long_equity_curve_file_name = counter_period_folder + 'long_equity_curve.csv.gz'
      #stat_long._equity_curve.to_csv(long_equity_curve_file_name)
      #safe_write_dataframe(stat_long._equity_curve, long_equity_curve_file_name, compression="gzip")
      api_write_df_csv_gz(conn, slave_id, work_id, api_token, dir_name, stat_long._equity_curve, long_equity_curve_file_name, index=True, allowed_output_prefix= allowed_output_prefix)
      
      #print('Equity CSV Saved')
      
      # Save Trades
      long_trades_file_name = counter_period_folder + 'long_trades.csv.gz'
      #stat_long._trades.to_csv(long_trades_file_name)
      #safe_write_dataframe(stat_long._trades, long_trades_file_name, compression="gzip")
      api_write_df_csv_gz(conn, slave_id, work_id, api_token, dir_name, stat_long._trades, long_trades_file_name, index=True, allowed_output_prefix= allowed_output_prefix)

      #print('Stat CSV Saved')
      
      # Collect Long Values
      long_headers = ['Trade Direction','Strategy','Plot File','trade_captured','trade_captured [%]','available_trade']
      long_values = ['Long','LongStrat',long_plot_file_name,long_trade_captured,long_trade_captured*100,count_longs]
      for item in (list(stat_long.items())):
          if item[0] not in ['_strategy', '_equity_curve', '_trades']:
              long_headers.append(item[0])
              long_values.append(item[1])
          elif item[0] == '_strategy':
              long_headers.append(item[0])
              long_values.append(item[1])
          elif item[0] == '_equity_curve':
              long_headers.append(item[0])
              long_values.append(long_equity_curve_file_name)
          elif item[0] == '_trades':
              long_headers.append(item[0])
              long_values.append(long_trades_file_name)

      # Build Long Results
      long_results_headers = base_header + periodic_header + long_headers
      long_results = base_value + periodic_value + long_values

      if verbose:
          # Print BackTesting Long Strategy Results
          print('______________________________________')
          print('Trade Parameter ',tm_parameters['name'])
          print('Name:',signal_name)
          print(stat_long['# Trades'],'/',count_longs,' Long Trades Placed')
          print(' Long Trades Captured : ',long_trade_captured)
          print('                      : ',long_trade_captured*100 , '%')
          print('Long Plot saved to file:', long_plot_file_name)
          print('Long Stats:')
          print(stat_long)


      # BackTesting Short Strategy
      bt_short = Backtest(dfpl, Short_Strategy, cash = initial_cash, margin=1/leverage, commission=comission,exclusive_orders=False)
      stat_short = bt_short.run()
      
      
      # save short plot file
      short_plot_file_name = counter_period_folder+'short_plot'
      '''
      bt_short.plot( results=stat_short, filename=short_plot_file_name, plot_width=None, plot_equity=True, plot_return=True, plot_pl=True, plot_volume=True, plot_drawdown=False, smooth_equity=False, relative_equity=True, superimpose=True, resample=True, reverse_indicators=False, show_legend=True, open_browser=False)
      '''
      # count short trades
      if count_shorts != 0:
          short_trade_captured = stat_short['# Trades']/count_shorts
      elif count_shorts == 0 and stat_short['# Trades']==0:
          short_trade_captured = 1
      else:
          short_trade_captured = 0
          
      # Save Equity Curve
      short_equity_curve_file_name = counter_period_folder + 'short_equity_curve.csv.gz'
      #stat_short._equity_curve.to_csv(short_equity_curve_file_name)
      #safe_write_dataframe(stat_short._equity_curve, short_equity_curve_file_name, compression="gzip")
      api_write_df_csv_gz(conn, slave_id, work_id, api_token, dir_name, stat_short._equity_curve, short_equity_curve_file_name, index=True, allowed_output_prefix= allowed_output_prefix)

      # Save Trades
      short_trades_file_name = counter_period_folder + 'short_trades.csv.gz'
      #stat_short._trades.to_csv(short_trades_file_name)
      #safe_write_dataframe(stat_short._trades, short_trades_file_name, compression="gzip")
      api_write_df_csv_gz(conn, slave_id, work_id, api_token, dir_name, stat_short._trades, short_trades_file_name, index=True, allowed_output_prefix= allowed_output_prefix)
      
      
      # Collect Short Values
      short_headers = ['Trade Direction','Strategy','Plot File','trade_captured','trade_captured [%]','available_trade']
      short_values = ['Short','ShortStrat',short_plot_file_name,short_trade_captured,short_trade_captured*100,count_shorts]
      for item in (list(stat_short.items())):
          if item[0] not in ['_strategy', '_equity_curve', '_trades']:
              short_headers.append(item[0])
              short_values.append(item[1])
          elif item[0] == '_strategy':
              short_headers.append(item[0])
              short_values.append(item[1])
          elif item[0] == '_equity_curve':
              short_headers.append(item[0])
              short_values.append(short_equity_curve_file_name)
          elif item[0] == '_trades':
              short_headers.append(item[0])
              short_values.append(short_trades_file_name)

      # Build Short Results
      short_results_headers = base_header + periodic_header + short_headers
      short_results = base_value + periodic_value + short_values

      if verbose:
          # Print BackTesting Short Strategy Results
          print('______________________________________')
          print('Trade Parameter ',tm_parameters['name'])
          print('Name:',signal_name)
          print(stat_short['# Trades'],'/',count_shorts,' Short Trades Placed')
          print(' Short Trades Captured : ',short_trade_captured)
          print('                      : ',short_trade_captured*100 , '%')
          print('Short Plot saved to file:', short_plot_file_name)
          print('Short Stats:')
          print(stat_short)



      # Check for header mismatches
      long_only = [(i, val) for i, val in enumerate(long_results_headers) if val not in short_results_headers]
      short_only = [(i, val) for i, val in enumerate(short_results_headers) if val not in long_results_headers]
    
      if long_only or short_only:
            # Align headers and values
            header, new_long_results, new_short_results = align_results(
                                                                long_results_headers, short_results_headers,
                                                                long_results, short_results
                                                            )
            
            if verbose:
                print("\nHeader mismatch detected!")
                
                if long_only:
                    print("\nLong headers not in short:")
                    for i, val in long_only:
                        print(f"  Index {i}: {val}")
                
                if short_only:
                    print("\nShort headers not in long:")
                    for i, val in short_only:
                        print(f"  Index {i}: {val}")
                
                
                # Print all headers line by line
                print("\nFinal Aligned Headers:")
                for i, h in enumerate(header):
                    print(f"  {i}: {h}")
            
                # Print results before and after alignment
                print("\nOriginal Long Results:")
                for i, (h, v) in enumerate(zip(long_results_headers, long_results)):
                    print(f"  {i}: {h} = {v}")
            
                print("\nOriginal Short Results:")
                for i, (h, v) in enumerate(zip(short_results_headers, short_results)):
                    print(f"  {i}: {h} = {v}")
            
                print("\nNew Aligned Long Results:")
                for i, (h, v) in enumerate(zip(header, new_long_results)):
                    print(f"  {i}: {h} = {v}")
            
                print("\nNew Aligned Short Results:")
                for i, (h, v) in enumerate(zip(header, new_short_results)):
                    print(f"  {i}: {h} = {v}")


                
            if len(new_long_results) != len(new_short_results):
                raise AssertionError('LEN OF NEW : long_results and short_results do not match, this should not occur')
            else:
                long_results, short_results = new_long_results, new_short_results
      else:      
            #raise AssertionError('long_results_header and short_results_header do not match, this should not occur')
            assert long_results_headers == short_results_headers, 'long_results_header and short_results_header do not match, this should not occur'
            header = long_results_headers

      # Save Current Period Results
      results = [long_results,short_results]
      #curr_period_results = pd.DataFrame(results, columns=header)
      curr_period_results_file_name = counter_period_folder + 'results.csv.gz'
      '''
      curr_period_results_df = load_existing_and_merge(results, header, curr_period_results_file_name, verbose=verbose)
      #curr_period_results.to_csv(curr_period_results_file_name, index=False)
      #safe_write_dataframe(curr_period_results_df, curr_period_results_file_name, index=False, compression="gzip")
      api_write_df_csv_gz(conn, slave_id, work_id, api_token, dir_name, curr_period_results_df, curr_period_results_file_name, index=False, allowed_output_prefix= allowed_output_prefix)
      '''
      api_append_rows_gz(
            conn, slave_id, work_id, api_token,
            dir_name=dir_name,
            abs_path=curr_period_results_file_name,
            header=header,
            rows=results,
            allowed_output_prefix=allowed_output_prefix
        )

      if verbose:
          print(f'{period_string} {counter}/{total_periods} Results saved to file:', curr_period_results_file_name)

      # Saving Periodic results
      periodic_results.append(long_results)
      periodic_results.append(short_results)
      #periodic_results_df = pd.DataFrame(periodic_results, columns=header)
      periodic_results_file_name = periodic_analysis_folder + f'{periodically_string}_results.csv.gz'
      '''
      periodic_results_df = load_existing_and_merge([long_results, short_results], header, periodic_results_file_name, verbose=verbose)
      #periodic_results_df.to_csv(periodic_results_file_name, index=False)
      #safe_write_dataframe(periodic_results_df, periodic_results_file_name, index=False, compression="gzip")
      api_write_df_csv_gz(conn, slave_id, work_id, api_token, dir_name, periodic_results_df, periodic_results_file_name, index=False, allowed_output_prefix= allowed_output_prefix)
      '''
      api_append_rows_gz(
            conn, slave_id, work_id, api_token,
            dir_name=dir_name,
            abs_path=periodic_results_file_name,
            header=header,
            rows=[long_results, short_results],
            allowed_output_prefix=allowed_output_prefix
        )

      if verbose:
          print(f'{periodically_string} Results saved to file:', periodic_results_file_name)

      # Saving Periodic_LR
      periodic_LR.append(long_results)
      #periodic_lr_df = pd.DataFrame(periodic_LR, columns=header)
      periodic_lr_file_name = periodic_analysis_folder + f'{periodically_string}_long_results.csv.gz'
      '''
      periodic_lr_df = load_existing_and_merge([long_results], header, periodic_lr_file_name, verbose=verbose)
      #periodic_lr_df.to_csv(periodic_lr_file_name, index=False)
      #safe_write_dataframe(periodic_lr_df, periodic_lr_file_name, index=False, compression="gzip")
      api_write_df_csv_gz(conn, slave_id, work_id, api_token, dir_name, periodic_lr_df, periodic_lr_file_name, index=False, allowed_output_prefix= allowed_output_prefix)
      '''
      api_append_rows_gz(
            conn, slave_id, work_id, api_token,
            dir_name=dir_name,
            abs_path=periodic_lr_file_name,
            header=header,
            rows=[long_results],
            allowed_output_prefix=allowed_output_prefix
        )

      if verbose:
          print(f'{periodically_string} Long Results saved to file:', periodic_lr_file_name)

      # Saving Periodic_SR
      periodic_SR.append(short_results)
      #periodic_sr_df = pd.DataFrame(periodic_SR, columns=header)
      periodic_sr_file_name = periodic_analysis_folder + f'{periodically_string}_short_results.csv.gz'
      '''
      periodic_sr_df = load_existing_and_merge([short_results], header, periodic_sr_file_name, verbose=verbose)
      #periodic_sr_df.to_csv(periodic_sr_file_name, index=False)
      #safe_write_dataframe(periodic_sr_df, periodic_sr_file_name, index=False, compression="gzip")
      api_write_df_csv_gz(conn, slave_id, work_id, api_token, dir_name, periodic_sr_df, periodic_sr_file_name, index=False, allowed_output_prefix= allowed_output_prefix)
      '''
      api_append_rows_gz(
            conn, slave_id, work_id, api_token,
            dir_name=dir_name,
            abs_path=periodic_sr_file_name,
            header=header,
            rows=[short_results],
            allowed_output_prefix=allowed_output_prefix
        )

      if verbose:
          print(f'{periodically_string} Short Results saved to file:', periodic_sr_file_name)


# In[24]:


#def perform_backtesting(INPUTS):
#def perform_backtesting(tm_cache_folder,signal_name, col, flag, tm_parameters,verbose=False):
def perform_backtesting_remote(BIG_DATA,flag, tm_parameters,verbose=False, conn=None, slave_id=None, work_id=None, api_token=None, allowed_output_prefix=None):
    '''
    # 1. Unpack the small pieces from INPUTS
    base_inputs, backtesting_inputs, verbose = INPUTS
    analysis_folder,tm_cache_folder = base_inputs  # example structure
    signal_name, col, flag, tm_parameters = backtesting_inputs
    '''
    # 2. Load big data from the global dictionary
    #global BIG_DATA
    dir_name = BIG_DATA["dir_name"]

    monthly_frames = BIG_DATA["monthly_frames"]
    weekly_frames = BIG_DATA["weekly_frames"]
    daily_frames  = BIG_DATA["daily_frames"]
    explored_signals_info = BIG_DATA["explored_signals_info"]

    symbol      = BIG_DATA["symbol"]
    timeframe   = BIG_DATA["timeframe"]
    analysis_name = BIG_DATA["analysis_name"]
    periodic_flags = BIG_DATA["PERIODIC_FLAGS"]
    strategy_name = BIG_DATA["Strategy_Name"]

    
    '''
    base_inputs,backtesting_inputs,verbose = INPUTS
    # Assemble all Pre requisites from input
    symbol,timeframe,analysis_name,analysis_folder,tm_cache_folder,explored_signals_info,periodic_flags,monthly_frames,weekly_frames,daily_frames = base_inputs
    signal_name,col,flag,tm_parameters = backtesting_inputs
    '''

    
    MONTHLY_ANALYSIS,WEEKLY_ANALYSIS,DAILY_ANALYSIS = periodic_flags


    signal_name = tm_parameters['signal_name']
    #col = tm_parameters['name']
    
    # Create Folder for the signal
    #signal_folder = analysis_folder+signal_name+'/'
    signal_folder = tm_parameters['analysis_cache_folder']+'/'

    # 
    tm_cache_folder = tm_parameters['tm_cache_folder']

    #Define Grid Files
    grid_file = f"{tm_cache_folder}/grid_Signal_TM.csv"
    lock_grid_file = f"{tm_cache_folder}/grid_Signal_TM.lock"

    
    # Currently Selected Signal
    if verbose:
        print(f"Row /  Indicator : {signal_name}")
        
    
    '''
    if not os.path.exists(signal_folder):
        os.makedirs(signal_folder, exist_ok=True)
    '''    
    # Signal Info such as Total Trades, Longs, Shorts
    '''
    signal_info = explored_signals_info[explored_signals_info['Signal Name']==signal_name]
    Total_Trades = signal_info['Trades'].values[0]
    Total_Longs = signal_info['Longs'].values[0]
    Total_Shorts = signal_info['Shorts'].values[0]
    '''
    if verbose:
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
        print(f"Indicator        : {signal_name}")
        print(f"Trade Management : {tm_parameters['name']}")
        print(f"Flag             : {flag}")

    # Retrieve Trading Parameter
    initial_cash = tm_parameters['initial_cash']
    comission = tm_parameters['comission']
    leverage = tm_parameters['leverage']
    risk_per_trade = tm_parameters['risk_per_trade']
    slippage = tm_parameters['slippage']

    if verbose:
        print(f"Name            : {tm_parameters['name']}")
        print(f"Hashed Name     : {tm_parameters['hashed_name']}")
        print(f"Initial Cash    : {tm_parameters['initial_cash']}")
        print(f"Comission       : {tm_parameters['comission']}")
        print(f"Leverage        : {tm_parameters['leverage']}")
        print(f"Risk Per Trade  : {tm_parameters['risk_per_trade']}")
        print(f"Stop Loss       : {tm_parameters['stop_loss']}")
        print(f"Take Profit     : {tm_parameters['take_profit']}")
        print(f"Slippage        : {tm_parameters['slippage']}")
        #print(tm_parameters)
            
    # Create Folder for Trading Parameter
    #tm_folder = Path(signal_folder+tm_parameters['hashed_name']+'/')
    tm_folder = signal_folder+tm_parameters['hashed_name']+'/'
    '''
    if not os.path.exists(tm_folder):
        os.makedirs(tm_folder, exist_ok=True)
        if verbose:
            print('Created Trade Management Directory :',tm_folder)
    else:
        if verbose:
            print('Trade Management Directory already exists at :',tm_folder)
    '''     
        
    # Base Header and Value for Results
    base_header = ['Symbol','Timeframe','Analysis Type','Signal','Trade Management','TM_hashed','risk_per_trade','stop_loss','take_profit','initial_cash','comission','leverage','slippage']
    base_value = [symbol,timeframe,analysis_name,signal_name,tm_parameters['name'],tm_parameters['hashed_name'],tm_parameters['risk_per_trade'],tm_parameters['stop_loss'],tm_parameters['take_profit'],initial_cash,comission,leverage,slippage]

    # Case where backtesting has been performed
    if flag == 3:
        if verbose:
            print('---------------------------------')
            print('Backtesting already done for here')
        
    # Case where backtesting has not been performed
    elif flag < 3:
        # Print Statement
        if verbose:
            print('---------------------------------')
            print('Performing Backtesting')

        # Define Long Strategy
        # Create the dynamic LongStrat class inside the worker
        LongStrat = create_long_strategy_class(Strategy, tm_parameters,slippage, signal_name)

        # Define Short Strategy
        # Create the dynamic ShortStrat class inside the worker
        ShortStrat = create_short_strategy_class(Strategy, tm_parameters,slippage, signal_name)
    
        verbose =False
        # MONTHLY ANALYSIS
        if MONTHLY_ANALYSIS and flag < 1:
            periodic_raw_analysis(period='M',periodic_frames=monthly_frames,root_folder = tm_folder,signal_name=signal_name,tm_parameters=tm_parameters,base_header=base_header,base_value=base_value,Long_Strategy=LongStrat,Short_Strategy=ShortStrat,verbose=verbose,conn=conn, slave_id=slave_id, work_id=work_id, api_token=api_token, dir_name=dir_name, allowed_output_prefix= allowed_output_prefix)
            
            # Update Grid on succesful updation
            '''
            grid_Signal_TM = pd.read_csv(analysis_folder+'grid_Signal_TM.csv',index_col=0)
            grid_Signal_TM.loc[signal_name,tm_parameters['name']] = 1
            grid_Signal_TM.to_csv(analysis_folder+'grid_Signal_TM.csv')
            '''
            new_flag = 1
            #update_feed = [grid_file,signal_name,tm_parameters['name'],new_flag]
            #update_gridsignal(update_feed, lock_grid_file)
            api_update_gridsignal(conn, slave_id, work_id, api_token, grid_file, lock_grid_file, signal_name, tm_parameters['name'], new_flag)
            
            # Update Flag
            flag=1
        else:
            if verbose:
                print('Monthly Analysis already completed or set to OFF')

        # WEEKLY ANALYSIS
        if WEEKLY_ANALYSIS and flag < 2:
            periodic_raw_analysis(period='W',periodic_frames=weekly_frames,root_folder = tm_folder,signal_name=signal_name,tm_parameters=tm_parameters,base_header=base_header,base_value=base_value,Long_Strategy=LongStrat,Short_Strategy=ShortStrat,verbose=verbose,conn=conn, slave_id=slave_id, work_id=work_id, api_token=api_token, dir_name=dir_name, allowed_output_prefix= allowed_output_prefix)
            
            # Update Grid on succesful updation
            '''
            grid_Signal_TM = pd.read_csv(analysis_folder+'grid_Signal_TM.csv',index_col=0)
            grid_Signal_TM.loc[signal_name,tm_parameters['name']] = 2
            grid_Signal_TM.to_csv(analysis_folder+'grid_Signal_TM.csv')
            '''
            new_flag = 2
            #update_feed = [grid_file,signal_name,tm_parameters['name'],new_flag]
            #update_gridsignal(update_feed, lock_grid_file)
            api_update_gridsignal(conn, slave_id, work_id, api_token, grid_file, lock_grid_file, signal_name, tm_parameters['name'], new_flag)
            
            # Update Flag
            flag=2
        else:
            if verbose:
                print('Weekly Analysis already completed or set to OFF')

        # DAILY ANALYSIS
        if DAILY_ANALYSIS and flag < 3:
            periodic_raw_analysis(period='D',periodic_frames=daily_frames,root_folder = tm_folder,signal_name=signal_name,tm_parameters=tm_parameters,base_header=base_header,base_value=base_value,Long_Strategy=LongStrat,Short_Strategy=ShortStrat,verbose=verbose,conn=conn, slave_id=slave_id, work_id=work_id, api_token=api_token, dir_name=dir_name, allowed_output_prefix= allowed_output_prefix)
            
            # Update Grid on succesful updation
            '''
            grid_Signal_TM = pd.read_csv(analysis_folder+'grid_Signal_TM.csv',index_col=0)
            grid_Signal_TM.loc[signal_name,tm_parameters['name']] = 3
            grid_Signal_TM.to_csv(analysis_folder+'grid_Signal_TM.csv')
            '''
            new_flag = 3
            #update_feed = [grid_file,signal_name,tm_parameters['name'],new_flag]
            #update_gridsignal(update_feed, lock_grid_file)
            api_update_gridsignal(conn, slave_id, work_id, api_token, grid_file, lock_grid_file, signal_name, tm_parameters['name'], new_flag)
            
            # Update Flag
            flag=3
        else:
            if verbose:
                print('Daily Analysis already completed or set to OFF')
                        
        

    
    else:
        if verbose:
            print('Error in grid_Signal_TM')
    if verbose:
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')

    return True


# In[25]:


_BIGDATA_CACHE = {}  # sha256 -> big_data dict

def load_big_data_from_ref(big_data_ref: dict) -> dict:
    # big_data_ref is canonical {bucket,key,sha256,bytes,...} from master
    sha = big_data_ref.get("sha256") or big_data_ref.get("sha")
    if sha in _BIGDATA_CACHE:
        return _BIGDATA_CACHE[sha]

    # Need a presigned url: master should pass presigned via presign_existing_payload() like normal assignments
    # easiest: master includes presigned url inside payload, but you can also add a command to refresh it.
    payload = fetch_payload_ref({"url": big_data_ref["url"], "sha256": big_data_ref["sha256"], "bytes": big_data_ref["bytes"], "compression": "zstd"})
    if payload.get("kind") != "big_data_pickle_b64":
        raise ValueError("Unexpected BIG_DATA payload kind")

    blob = base64.b64decode(payload["pickle_b64"])
    big_data = pickle.loads(blob)
    _BIGDATA_CACHE[sha] = big_data
    return big_data


# In[26]:


PRESIGN_WAITERS = {}  # req_id -> (Event, holder)
UPLOAD_MAX_CONCURRENCY = 50
_UPLOAD_SEM = threading.Semaphore(UPLOAD_MAX_CONCURRENCY)

def request_presigned_put(conn, slave_id, work_id, token, rel_path, content_type, size=None, sha256=None, timeout=600):
    req_id = uuid.uuid4().hex
    req = {
        "command": "api_presign_put",
        "slave_id": slave_id,
        "work_id": work_id,
        "token": token,
        "req_id": req_id,
        "rel_path": rel_path,
        "content_type": content_type,
        "size": size,
        "sha256": sha256,
    }

    # Wait for response (simple per-work event)
    ev = threading.Event()
    holder = {"resp": None}
    PRESIGN_WAITERS[req_id] = (ev, holder)

    safe_send(conn, req)

    if not ev.wait(timeout):
        PRESIGN_WAITERS.pop(req_id, None)
        raise TimeoutError("api_presign_put_resp timeout")

    PRESIGN_WAITERS.pop(req_id, None)
    resp = holder["resp"]
    if not resp.get("ok"):
        raise PermissionError(f"presign denied: {resp.get('reason')}")
    return resp

def upload_bytes_to_presigned_put(put_url, headers, data_bytes):
    #r = requests.put(put_url, data=data_bytes, headers=headers, timeout=(30, 900))
    with _UPLOAD_SEM:
        r = _HTTP.put(put_url, data=data_bytes, headers=headers, timeout=(30, 3600))
        r.raise_for_status()


# In[27]:


def to_rel_path_under_run(abs_path: str, dir_name: str) -> str:
    p = abs_path.replace("\\", "/")
    root = dir_name.replace("\\", "/").rstrip("/") + "/"
    if not p.startswith(root):
        # if this happens, you can either deny or fallback to best-effort relative
        raise ValueError(f"path {p} not under run root {root}")
    rel = p[len(root):]
    # rel path as master expects: RUN_ROOT_REL + rel
    # master’s allowed prefix is RUN_ROOT_REL (dir_name relative), so slave must send full rel under that.
    # easiest: send RUN_ROOT_REL + rel, but RUN_ROOT_REL is exactly the dir_name relative.
    # so return "<RUN_ROOT_REL>/<rel>" is done by master; here just return that root-relative.
    return rel.replace("//", "/")

def api_write_df_csv_gz(conn, slave_id, work_id, token, dir_name, df: pd.DataFrame, abs_path: str,
                        index=False, allowed_output_prefix= None):
    # create gzip bytes exactly like df.to_csv(..., compression="gzip") would
    buf = io.StringIO()
    df.to_csv(buf, index=index)
    raw = buf.getvalue().encode("utf-8")
    gz = io.BytesIO()
    with gzip.GzipFile(fileobj=gz, mode="wb") as f:
        f.write(raw)
    data_bytes = gz.getvalue()

    rel = to_rel_path_under_run(abs_path, dir_name)
    # include run-root relative prefix in rel_path:
    # easiest: store RUN_ROOT_REL in work_data or passed kwargs; here assume work_data includes "run_root_rel".
    rel_path = f"{allowed_output_prefix.rstrip('/')}/{rel}"

    resp = request_presigned_put(conn, slave_id, work_id, token, rel_path, "application/gzip", size=len(data_bytes))
    upload_bytes_to_presigned_put(resp["put_url"], resp.get("headers", {}), data_bytes)

def api_write_pickle(conn, slave_id, work_id, token, dir_name, obj, abs_path: str, allowed_output_prefix= None):
    data_bytes = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    rel = to_rel_path_under_run(abs_path, dir_name)
    rel_path = f"{allowed_output_prefix.rstrip('/')}/{rel}"
    resp = request_presigned_put(conn, slave_id, work_id, token, rel_path, "application/octet-stream", size=len(data_bytes))
    upload_bytes_to_presigned_put(resp["put_url"], resp.get("headers", {}), data_bytes)

def api_write_text(conn, slave_id, work_id, token, dir_name, text: str, abs_path: str, content_type="text/plain; charset=utf-8", allowed_output_prefix=None):
    data_bytes = text.encode("utf-8")
    rel = to_rel_path_under_run(abs_path, dir_name)
    rel_path = f"{allowed_output_prefix.rstrip('/')}/{rel}"
    resp = request_presigned_put(conn, slave_id, work_id, token, rel_path, content_type, size=len(data_bytes))
    upload_bytes_to_presigned_put(resp["put_url"], resp.get("headers", {}), data_bytes)


# # Slave Code

# In[ ]:


def load_buffer_cores_from_file(default: int = 6) -> int:
    # looks next to the script (or current working dir if frozen)
    base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    path = os.path.join(base_dir, "buffer_cores.txt")
    try:
        if not os.path.exists(path):
            return default
        raw = open(path, "r", encoding="utf-8").read().strip()
        n = int(raw)
        if n < 0:
            return default
        return n
    except Exception as e:
        print(f"[BUFFER_CORES][WARN] failed reading {path}: {e} -> default={default}")
        return default


# In[ ]:


def _register_active_work(work_id: str, control: dict):
    with ACTIVE_WORKS_LOCK:
        ACTIVE_WORKS[work_id] = control

def _pop_active_work(work_id: str):
    with ACTIVE_WORKS_LOCK:
        return ACTIVE_WORKS.pop(work_id, None)

def _stop_active_work(work_id: str, reason: str = "stop_work"):
    """
    Best-effort immediate stop:
      - set stop_event (cooperative)
      - terminate multiprocessing pool if present
      - kill tracked child pids if present
      - mark WORK_STATUS idle and notify master
    """
    ctrl = None
    with ACTIVE_WORKS_LOCK:
        ctrl = ACTIVE_WORKS.get(work_id)

    if not ctrl:
        return False

    print(f"[STOP_WORK] Received stop for work_id={work_id} reason={reason}")

    # cooperative signal
    try:
        ev = ctrl.get("stop_event")
        if ev:
            ev.set()
    except Exception:
        pass

    # terminate pool
    try:
        pool = ctrl.get("pool")
        if pool:
            pool.terminate()
            pool.join()
    except Exception as e:
        print(f"[STOP_WORK][WARN] pool terminate err: {e}")

    # kill child processes (best effort)
    try:
        pids = list(ctrl.get("child_pids") or [])
        for pid in pids:
            try:
                psutil.Process(pid).kill()
            except Exception:
                pass
    except Exception:
        pass

    # cleanup UI threads/tqdm if present
    try:
        for evt_name in ("heartbeat_stop_event", "progress_stop_event", "watchdog_stop_event"):
            evx = ctrl.get(evt_name)
            if evx:
                evx.set()
    except Exception:
        pass

    # release registry entry
    _pop_active_work(work_id)

    return True


# In[28]:


def is_domain(host: str) -> bool:
    """
    Return True if *host* looks like a DNS name, False if it’s a valid IPv4/IPv6 literal.

    Works by first trying ipaddress.ip_address(); if that fails, does a quick   sanity
    check that host contains at least one dot and an alphabetic character,      which
    filters out things like 'localhost'.
    """
    try:
        # Will succeed for 127.0.0.1, ::1, 192.168.0.10, etc.
        ipaddress.ip_address(host)
        return False
    except ValueError:
        # Rough but practical domain test: must contain a dot + some letters.
        return bool(re.search(r'\.[a-zA-Z]{2,}$', host))


# In[29]:


'''
def heartbeat_sender(conn, work_id, work_name, get_progress):
    try:
        while True:
            time.sleep(10)  # Heartbeat interval (seconds)
            progress = get_progress()
            message = {
                "slave_id": slave_id,
                "work_id": work_id,
                "work_name": work_name,
                "stream_type": "progress_update",
                "progress_percent": progress
            }
            send_message(conn, message)
    except Exception as e:
        print(f"[HEARTBEAT] Heartbeat sender stopped: {e}")
'''


# In[30]:


def heartbeat_sender(conn, work_id, work_name, get_progress, stop_event):
    global HEARTBEAT_INTERVAL
    try:
        while not stop_event.is_set():
            time.sleep(HEARTBEAT_INTERVAL)
            try:
                if conn is None:
                    break
                progress = get_progress()
                message = {
                    "slave_id": slave_id,
                    "work_id": work_id,
                    "work_name": work_name,
                    "stream_type": "progress_update",
                    "progress_percent": progress
                }
                #send_message(conn, message)
                safe_send(conn, message)

            except ssl.SSLError as e:
                print(f"\n[HEARTBEAT ERROR] SSL broken — aborting retries : {e}")
                break
            
            except (socket.error, OSError) as e:
                print(f"\n[HEARTBEAT ERROR] Heartbeat sender stopped: {e}")
                break
    
            except Exception as e:
                print(f"[HEARTBEAT ERROR] Unexpected error: {e}")
                traceback.print_exc()
                break
                
    except Exception as e:
                print(f"[HEARTBEAT ERROR] While Loop error: {e}")
                traceback.print_exc()
                        
def get_progress():
    global completed
    global total_subtasks
    
    with completed.get_lock():
        return (completed.value / total_subtasks) * 100


# In[31]:


def init_child(shared_completed):
                global completed
                completed = shared_completed


def process_wrapper(input_df,input_resample_timeframe):
    global completed
    try:
        # --- TYPE CHECKS ---
        if not isinstance(input_df, pd.DataFrame):
            print(f"[ERROR] process_wrapper: Expected DataFrame for input_df but got {type(input_df)}")
            raise TypeError(f"Invalid input_df type: {type(input_df)}")
    
        if not isinstance(input_resample_timeframe, str):
            print(f"[ERROR] process_wrapper: Expected str for input_resample_timeframe but got {type(input_resample_timeframe)}")
            raise TypeError(f"Invalid input_resample_timeframe type: {type(input_resample_timeframe)}")

        # --- Reset Warnings inside worker ---
        warnings.resetwarnings()
        warnings.simplefilter(action='ignore', category=UserWarning)
        warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

        
        # --- ACTUAL PROCESSING ---
        res = process_sub_df(input_df,input_resample_timeframe)
        
        # --- UPDATE PROGRESS ---
        with completed.get_lock():
            completed.value += 1
        return res

    except Exception as e:
        print(f"[WORKER ERROR] process_wrapper crashed: {e}")
        traceback.print_exc()
        # Return an empty safe dummy result if crash
        return pd.DataFrame()


# In[38]:


def do_work(conn,work_id, work_name, work_type, work_data, work_shares,total_shares,api_token=None, token_expiry_unix=None, allowed_output_prefix=None):
    global buffer_cores
    global completed
    global total_subtasks
    global WORK_STATUS
    global FINAL_RESULT_ACK_EVENTS

    slave_receive_time = int(time.time() * 1000)  # Time when slave received work
    print(f"[WORK] Starting {work_id}: {work_name} ({work_type})")

    # Safe cast to int (and avoid divide by zero)
    work_shares = int(work_shares)
    total_shares = int(total_shares) if int(total_shares) != 0 else 1  # Prevent zero division

    final_results = None
    work_success = False
    stopped_by_master = False
    
    slave_start_time = int(time.time() * 1000)  # Time when starting actual work

    WORK_STATUS = "working"
    #send_message(conn, {"slave_id": slave_id, "work_status": WORK_STATUS})
    safe_send(conn, {"slave_id": slave_id, "work_status": WORK_STATUS})

    
    # STOP control (separate from heartbeat stop_event)
    hard_stop_event = threading.Event()

    control = {
        "stop_event": hard_stop_event,
        "pool": None,
        "child_pids": [],
        "progress_stop_event": None,
        "watchdog_stop_event": None,
    }
    _register_active_work(work_id, control)


    
    try:

        # FE IMPRINTING WORK TYPE
        if work_type == "FE_Imprinting":
            # Assume work_data contains sub_dfs and a function reference to process
            # For real cases, work_data may need to be deserialized safely
            Inputs_encoded = work_data.get("sub_dfs", [])
            Inputs = deserialize_dataframes(Inputs_encoded)

            print(f'[WORK] Number of Shares Recieved: {work_shares}')
            print(f'[WORK] Percent of Shares Recieved: {(work_shares / total_shares) * 100:.2f}%')
            
            # Dynamically estimate available CPU cores
            total_cores = psutil.cpu_count(logical=True)
            cpu_usage = psutil.cpu_percent(interval=1)
            available_cores = int((100 - cpu_usage) / 100 * total_cores)
            pool_size = max(1, min(available_cores - buffer_cores, work_shares))

            print(f"[WORK] Using {pool_size} workers (available: {available_cores}, total: {total_cores})")




            # --- Progress tracking ---
            total_subtasks = len(Inputs)
            completed = mp.Value('i', 0)  # Shared counter
            
            # --- Start Heartbeat ---
            stop_event = threading.Event()
            control["heartbeat_stop_event"] = stop_event
            heartbeat_thread = threading.Thread(
                                                target=heartbeat_sender,
                                                args=(conn, work_id, work_name, get_progress,stop_event),
                                                daemon=True
                                            )
            heartbeat_thread.start()

            # --- Start Progress Bar ---
            progress_bar = tqdm(total=total_subtasks, desc=f"Slave {slave_id} Progress", position=0, leave=True)
            progress_stop_event = threading.Event()
            control["progress_stop_event"] = progress_stop_event
            
            def progress_updater():
                last_completed = 0
                try:
                    while not progress_stop_event.is_set():
                        time.sleep(0.5)  # Update progress every half-second
                        with completed.get_lock():
                            current = completed.value
                        delta = current - last_completed
                        if delta > 0:
                            progress_bar.update(delta)
                            last_completed = current
                        if current >= total_subtasks:
                            break
                except Exception as e:
                    print(f"[TQDM] Progress updater error: {e}")
            
            progress_thread = threading.Thread(target=progress_updater, daemon=True)
            progress_thread.start()


            

            # --- Start Watchdog ---
            watchdog_stop_event = threading.Event()
            control["watchdog_stop_event"] = watchdog_stop_event
            def watchdog():
                timeout_sec = 6000  # 10 minutes
                start_time = time.time()
                try:
                    while not watchdog_stop_event.is_set():
                        if time.time() - start_time > timeout_sec:
                            print("[WATCHDOG] Work timeout reached. Force killing slave.")
                            os._exit(1)
                            break
                        time.sleep(5)  # Check every 5 seconds
                except Exception as e:
                    print(f"[WATCHDOG] Watchdog error: {e}")


            watchdog_thread = threading.Thread(target=watchdog, daemon=True)
            watchdog_thread.start()


            print(f"[WORK] Beginning POOL Processing")
            
            # --- Safe multiprocessing with timeout ---
            try:
                with mp.Pool(processes=pool_size, initializer=init_child, initargs=(completed,)) as pool:

                    control["pool"] = pool

                    # track children (best-effort)
                    try:
                        control["child_pids"] = [p.pid for p in pool._pool if p is not None]
                    except Exception:
                        control["child_pids"] = []



                    
                    #result_async = pool.starmap_async(process_wrapper,[(mini_input[0],mini_input[1],completed) for mini_input in Inputs])
                    result_async = pool.starmap_async(process_wrapper,Inputs)

                    '''
                    try:
                        final_results = result_async.get(timeout=POOL_TIMEOUT_SEC)  # 90 min max
                    except mp.TimeoutError:
                        print("[WORK] Pool timeout! Killing pool...")
                        pool.terminate()
                        pool.join()
                        raise TimeoutError("Pool processing timeout")
                    except Exception as e:
                        print(f"Error while waiting pool processing results {work_id}: {str(e)}")
                        #conn.close()
                    '''

                    # cancellable wait loop (checks stop_work frequently)
                    t0 = time.time()
                    while True:
                        if hard_stop_event.is_set():
                            raise RuntimeError("stopped_by_master")

                        try:
                            final_results = result_async.get(timeout=1.0)
                            break
                        except mp.TimeoutError:
                            if time.time() - t0 > POOL_TIMEOUT_SEC:
                                raise TimeoutError("Pool processing timeout")
                            continue
            
            except Exception as e:
                #print(f"Error while pool processing {work_id}: {str(e)}")
                #conn.close()
                print(f"[WORK][ERROR] {work_id} pool failed: {type(e).__name__}: {e}")
                traceback.print_exc()
                raise
                
            finally:
                control["pool"] = None


            if len(final_results) == len(Inputs):
                work_success = True
            else:
                work_success = False
                print(f"[WORK] Failed Work with ID {work_id}: {work_name} (Result Counts do not match)")
                print(f"[WORK] Expected Count : {len(Inputs)} | Got Count :{len(final_results)}")








        # BACKTESTING WORK TYPE
        elif work_type == "Backtesting":
            big_data_ref = work_data["big_data_ref"]
            tasks = work_data["tasks"]  # list of (flag, tm_parameters, verbose)
            write_token = api_token
        
            # NOTE: big_data_ref coming from master should include a presigned URL, same as normal payload_ref.
            # If you store canonical in master, pass presigned via presign_existing_payload() :contentReference[oaicite:17]{index=17}
            big_data = load_big_data_from_ref(big_data_ref)
        
            total_subtasks = len(tasks)
            completed = mp.Value('i', 0)
        
            stop_event = threading.Event()
            control["heartbeat_stop_event"] = stop_event
            heartbeat_thread = threading.Thread(
                target=heartbeat_sender,
                args=(conn, work_id, work_name, get_progress, stop_event),
                daemon=True
            )
            heartbeat_thread.start()

            # --- Start Progress Bar ---
            progress_bar = tqdm(total=total_subtasks, desc=f"Slave {slave_id} Progress", position=0, leave=True)
            progress_stop_event = threading.Event()
            control["progress_stop_event"] = progress_stop_event
            
            def progress_updater():
                last_completed = 0
                try:
                    while not progress_stop_event.is_set():
                        time.sleep(0.5)  # Update progress every half-second
                        with completed.get_lock():
                            current = completed.value
                        delta = current - last_completed
                        if delta > 0:
                            progress_bar.update(delta)
                            last_completed = current
                        if current >= total_subtasks:
                            break
                except Exception as e:
                    print(f"[TQDM] Progress updater error: {e}")
            
            progress_thread = threading.Thread(target=progress_updater, daemon=True)
            progress_thread.start()


            

            # --- Start Watchdog ---
            watchdog_stop_event = threading.Event()
            control["watchdog_stop_event"] = watchdog_stop_event
            def watchdog():
                timeout_sec = 6000  # 10 minutes
                start_time = time.time()
                try:
                    while not watchdog_stop_event.is_set():
                        if time.time() - start_time > timeout_sec:
                            print("[WATCHDOG] Work timeout reached. Force killing slave.")
                            os._exit(1)
                            break
                        time.sleep(5)  # Check every 5 seconds
                except Exception as e:
                    print(f"[WATCHDOG] Watchdog error: {e}")


            watchdog_thread = threading.Thread(target=watchdog, daemon=True)
            watchdog_thread.start()
        
            # Choose pool size similar to your FE logic
            total_cores = psutil.cpu_count(logical=True)
            cpu_usage = psutil.cpu_percent(interval=1)
            available_cores = int((100 - cpu_usage) / 100 * total_cores)
            pool_size = max(1, min(available_cores - buffer_cores, total_subtasks))

            '''
            def bt_wrapper(flag, tm_params, verbose):
                # CALL YOUR EXISTING perform_backtesting LOGIC,
                # but rewritten as a pure function that uses `big_data` instead of global BIG_DATA.
                ok = perform_backtesting_remote(big_data, flag, tm_params, verbose, conn, slave_id, work_id, write_token, allowed_output_prefix)
                with completed.get_lock():
                    completed.value += 1
                return ok
        
            with mp.Pool(processes=pool_size) as pool:
                final_results = pool.starmap(bt_wrapper, tasks)
        
            work_success = all(final_results)
            '''
            def run_one(task):
                flag, tm_params, verbose = task
                try:
                    return perform_backtesting_remote(
                        big_data, flag, tm_params, verbose,
                        conn=conn, slave_id=slave_id, work_id=work_id,
                        api_token=api_token,
                        allowed_output_prefix=allowed_output_prefix
                    )
                except Exception as e:
                    print(f"[BT][ERROR] work_id={work_id} tm={getattr(tm_params,'get',lambda k, d=None: d)('name',None)} err={e}")
                    traceback.print_exc()
                    return False
                finally:
                    with completed.get_lock():
                        completed.value += 1

            
            with ThreadPoolExecutor(max_workers=pool_size) as ex:
                futures = [ex.submit(run_one, t) for t in tasks]
                final_results = [f.result() for f in futures]
            
            work_success = all(final_results)
        



        # UNKNOWN WORK TYPE
        else:
            # Handle other work types here
            print(f"[WORK] Strange {work_id}: {work_name} ({work_type} not identified)")
            final_results = None
            work_success = False

    except Exception as e:
        if "stopped_by_master" in str(e):
            stopped_by_master = True
            print(f"[WORK] {work_id} stopped_by_master; skipping final_result send.")
        else:
            final_results = f"Error while processing {work_id}: {str(e)}"
            traceback.print_exc()

    if stopped_by_master:
        # best-effort local cleanup (in case stop_work arrived while we were mid-setup)
        try:
            stop_event.set()
        except Exception:
            pass
        try:
            progress_stop_event.set()
        except Exception:
            pass
        try:
            watchdog_stop_event.set()
        except Exception:
            pass
        try:
            progress_bar.close()
        except Exception:
            pass
        # ensure registry entry is cleared even if stop_work wasn't received (rare)
        try:
            _pop_active_work(work_id)
        except Exception:
            pass
        WORK_STATUS = "idle"
        safe_send(conn, {"slave_id": slave_id, "work_status": WORK_STATUS})
        return



    if work_type == "Backtesting":
        result_payload = {"ok": work_success, "count": len(final_results)}
    elif work_type == "FE_Imprinting":
        result_payload = serialize_dataframes(final_results)



    
    slave_end_time = int(time.time() * 1000)  # Finished actual work

    
    slave_send_time = int(time.time() * 1000)  # Time right before sending back
    
    final_packet = {
                    "slave_id": slave_id,
                    "work_id": work_id,
                    "work_name": work_name,
                    "work_type": work_type,
                    "stream_type": "final_result",
                    "work_success": work_success,
                    "result": result_payload, 

                    # Timing Info
                    "slave_receive_time": slave_receive_time,
                    "slave_start_time": slave_start_time,
                    "slave_end_time": slave_end_time,
                    "slave_send_time": slave_send_time
                }

    ack_event = threading.Event()
    FINAL_RESULT_ACK_EVENTS[work_id] = ack_event
    
    MAX_RETRIES = 4
    ACK_TIMEOUT_SEC = 450  # wait per attempt
    ACK_RETRY_DELAY_SEC = 150  # NEW: wait 2 minutes between attempts (tune)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            #send_message(conn, final_packet)
            safe_send(conn, final_packet)
            print(f"\n[FINAL] Sent final result for work_id={work_id} (attempt {attempt}/{MAX_RETRIES})")
        except Exception as e:
            print(f"\n[FINAL][ERROR] send failed for work_id={work_id} (attempt {attempt}/{MAX_RETRIES}): {e}")
    
        # Wait for ACK
        if ack_event.wait(ACK_TIMEOUT_SEC):
            print(f"\n[FINAL] ACK received for work_id={work_id}")
            break
        else:
            print(f"\n[FINAL] No ACK yet for work_id={work_id}; will retry...")
            if attempt < MAX_RETRIES:
                time.sleep(ACK_RETRY_DELAY_SEC)  # NEW delay

    # Clean up event
    FINAL_RESULT_ACK_EVENTS.pop(work_id, None)

    # Only now consider changing local status/heartbeat
    if ack_event.is_set():
        # safe to mark success -> working->idle transition
        # (your existing post-final steps here, e.g., stop work timers, set status idle, send status, etc.)
        
        stop_event.set()  # Tell heartbeat_sender to exit
        progress_stop_event.set()  # Stop progress updater
        watchdog_stop_event.set()  # Stop watch_dog
        
        heartbeat_thread.join(timeout=1)  # Optional: Wait a few seconds for it to clean up
        progress_thread.join(timeout=1)
        watchdog_thread.join(timeout=1)

        progress_bar.close()  # Close tqdm cleanly

        WORK_STATUS = "idle"
        #send_message(conn, {"slave_id": slave_id, "work_status": WORK_STATUS})
        safe_send(conn, {"slave_id": slave_id, "work_status": WORK_STATUS})
        
        pass
    else:
        # optional: you might keep status as "working" or move to "error_retry"
        # and rely on your outer logic/monitor to decide next steps.
        print(f"\n[FINAL][WARN] No ACK after {MAX_RETRIES} attempts for work_id={work_id}; leaving work_status as 'working' for now.")
    '''
    try:
        
        #conn.send(json.dumps(final_packet).encode())
        #send_message(conn, final_packet)
        
        # Implement 30 sec delay
        time.sleep(3)
        
        stop_event.set()  # Tell heartbeat_sender to exit
        progress_stop_event.set()  # Stop progress updater
        watchdog_stop_event.set()  # Stop watch_dog
        
        heartbeat_thread.join(timeout=1)  # Optional: Wait a few seconds for it to clean up
        progress_thread.join(timeout=1)
        watchdog_thread.join(timeout=1)

        progress_bar.close()  # Close tqdm cleanly

        WORK_STATUS = "idle"
        send_message(conn, {"slave_id": slave_id, "status": WORK_STATUS})

    except Exception as e:
        print(f"[ERROR] Failed to send final result to master: {e}")
        traceback.print_exc()
    ''' 
    print(f"[WORK] Completed work {work_id}: {work_name} ({work_type})")


def connect_to_master(master_ip='relay.breakeventx.com', master_port=8888):
    global slave_id
    global buffer_cores
    global slave_name
    global WORK_STATUS
    
    buffer_cores = load_buffer_cores_from_file(default=6)

    # Get CPU frequency (in MHz)
    cpu_freq = psutil.cpu_freq()
    cpu_speed = round(cpu_freq.max, 2) if cpu_freq else None  # max speed in MHz

    # Prepare handshake payload before hand
    handshake = {
        "role": "slave",
        "uid": get_machine_uid()
    }
    
    # Attempt to get memory speed (Windows only)
    try:
        mem_speed = None
        if platform.system() == "Windows":
            output = subprocess.check_output(['wmic', 'memorychip', 'get', 'Speed'], universal_newlines=True)
            speeds = [int(s) for s in output.strip().split() if s.isdigit()]
            mem_speed = max(speeds) if speeds else None
    except Exception as e:
        mem_speed = None

    
    connected = False
    client = None

    # TLS context (reuse across retries)
    if is_domain(master_ip):
        # Public / DNS endpoint (relay etc.) → full verification
        tls_context = ssl.create_default_context()
        tls_context.check_hostname = True
        tls_context.verify_mode = ssl.CERT_REQUIRED
    else:
        # Local IP / localhost → encrypt but don't enforce hostname/CA
        # (master generates its own self-signed cert on the fly)
        tls_context = ssl.create_default_context()
        tls_context.check_hostname = False
        tls_context.verify_mode = ssl.CERT_NONE
    '''
    # Loop until we get assigned ID
    while not connected:
        try:
            
    '''
    '''
            #client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            #client.settimeout(None)  #  block connection forever
            #client.connect((master_ip, master_port))
            #send_message(client, handshake)
            #print(f"[SLAVE] Connected to master at {master_ip}:{master_port}")
    '''
    '''     
            # Step 1: TCP connection
            raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # Windows keepalive tuning
            if sys.platform.startswith("win"):
                SIO_KEEPALIVE_VALS = 0x98000004
                raw_sock.ioctl(SIO_KEEPALIVE_VALS, (1, 30_000, 10_000))  # idle=30s, interval=10s
            raw_sock.settimeout(None)  # block connection forever
            raw_sock.connect((master_ip, master_port))

            # Step 2: TLS wrap
            client = tls_context.wrap_socket(raw_sock, server_hostname=master_ip)

            # Step 3: TLS-secured handshake
            #send_message(client, handshake)
            safe_send(client, handshake)
            print(f"[SLAVE] Connected (TLS) to master at {master_ip}:{master_port}")

            
            # Await `assign_id` command
            msg = recv_message(client)
            if not msg:
                raise Exception("No response from master")

            data = json.loads(msg)
            if data.get("command") != "assign_id":
                raise Exception("Invalid handshake from master")

            slave_id = data["slave_id"]
            started_time = datetime.datetime.now().isoformat()
            print(f"[SLAVE] Assigned ID: {slave_id}")
            print(f"[SLAVE] STARTED: connected to master at {started_time}")
            
            connected = True

        except Exception as e:
            print(f"[SLAVE] Handshake failed: {e}")
            time.sleep(2)
    '''


    while not connected:
        t_start = _now_ms()
        _dbg(f"\n[CONNECT] Attempt -> {master_ip}:{master_port}")
    
        try:
            # 0) DNS resolve (cached)
            t0 = _now_ms()
            addrs = [(socket.AF_INET, (master_ip, master_port))] if not is_domain(master_ip) else resolve_host_cached(master_ip, master_port)
            t1 = timed_step("DNS", t0)
    
            last_err = None
            client = None
    
            # Try each resolved address until one works
            for fam, sockaddr in addrs:
                ip_used = sockaddr[0]
                port_used = sockaddr[1]
                _dbg(f"[CONNECT] Trying {ip_used}:{port_used} fam={'IPv4' if fam==socket.AF_INET else 'IPv6'}")
    
                try:
                    # 1) TCP connect with timeout
                    t_tcp0 = _now_ms()
                    raw_sock = socket.socket(fam, socket.SOCK_STREAM)
                    raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    
                    # Windows keepalive tuning (you already do this)
                    if sys.platform.startswith("win"):
                        SIO_KEEPALIVE_VALS = 0x98000004
                        raw_sock.ioctl(SIO_KEEPALIVE_VALS, (1, 30_000, 10_000))
    
                    raw_sock.settimeout(TCP_CONNECT_TIMEOUT)
                    raw_sock.connect(sockaddr)
                    t_tcp1 = timed_step("TCP_CONNECT", t_tcp0)
    
                    # 2) TLS handshake with timeout
                    t_tls0 = _now_ms()
                    raw_sock.settimeout(TLS_HANDSHAKE_TIMEOUT)
                    client = tls_context.wrap_socket(raw_sock, server_hostname=master_ip)
                    t_tls1 = timed_step("TLS_HANDSHAKE", t_tls0)
                    _dbg(f"[CONNECT] TLS ok: version={client.version()} cipher={client.cipher()}")

    
                    # 3) Send handshake
                    t_hs0 = _now_ms()
                    safe_send(client, handshake)
                    t_hs1 = timed_step("SEND_HANDSHAKE", t_hs0)
    
                    # 4) Wait assign_id (timeout)
                    t_wait0 = _now_ms()
                    msg = recv_message_timeout(client, ASSIGN_ID_TIMEOUT)
                    t_wait1 = timed_step("WAIT_ASSIGN_ID", t_wait0)
    
                    if not msg:
                        raise TimeoutError("No assign_id received (empty/timeout)")
    
                    data = json.loads(msg)
                    if data.get("command") != "assign_id":
                        raise Exception(f"Invalid handshake from master: {data}")
    
                    slave_id = data["slave_id"]
                    total_ms = _now_ms() - t_start
                    total_sec = total_ms / 1000.0
                    print(f"[SLAVE] Assigned ID: {slave_id} (connected via {ip_used})")
                    #print(f"[SLAVE] CONNECTED in {total_ms} ms (DNS+TCP+TLS+assign_id)")
                    print(f"[SLAVE] CONNECTED in {total_sec:.3f}s ({total_ms} ms) (DNS+TCP+TLS+assign_id)")
                    
                    connected = True
                    # Steady-state: never timeout reads, just wait for messages
                    client.settimeout(None)
                    print(f"[SLAVE] Entering message loop (id={slave_id})")
                    break
    
                except Exception as e:
                    last_err = e
                    try:
                        if client:
                            client.close()
                    except:
                        pass
                    try:
                        raw_sock.close()
                    except:
                        pass
                    continue
    
            if not connected:
                raise last_err or Exception("All resolved addresses failed")
    
        except Exception as e:
            total_ms = _now_ms() - t_start
            print(f"[SLAVE] Connect/handshake failed after {total_ms} ms: {type(e).__name__}: {e}")
            time.sleep(1.5)
    
    




    # OG DMZ HERE
    last_msg_time = time.time()
    
    try:
        while True:
            try:
                '''
                # Check for timeout
                if time.time() - last_msg_time > 120:
                    print("[SLAVE] No message from master for 2 minutes. Reconnecting...")
                    client.close()
                    return  # Will trigger a reconnection in the main loop
    
                client.settimeout(10)  # Avoid blocking forever
                '''
                
                '''
                data = client.recv(4096).decode()
                if not data:
                    break
                msg = json.loads(data)
                '''
                data = recv_message(client)
                if not data:
                    raise ConnectionError("Server closed connection")
                    #continue
                
                #print(f"[SLAVE][RECV] bytes={len(data):,}")
                # show first ~120 chars for routing debugging (safe)
                #print(f"[SLAVE][RECV][HEAD] {data[:120]!r}")

                
                last_msg_time = time.time()  # Reset timer on valid message
                
                try:
                    msg = json.loads(data)
                except Exception as e:
                    print(f"[ERROR] JSON decode failed: {e}")
                    continue  # don't crash the whole connection

                '''
                if msg.get('command') !='request_sys_info' and msg.get('command')!='api_append_rows_resp' and msg.get('command')!='api_append_rows_resp':
                    print(f"[SLAVE][DISPATCH] command={msg.get('command')} stream_type={msg.get('stream_type')}")
                '''
                
                
                if msg.get("command") == "assign_id":
                    slave_id = msg["slave_id"]
                    print(f"[SLAVE] Assigned ID: {slave_id}")
                    
                    buffer_cores = load_buffer_cores_from_file(default=6)
                    
                    sys_info = {
                                "slave_id": slave_id,
                                "info": {
                                    "uid": get_machine_uid(),
                                    "work_status": WORK_STATUS,
                                    "slave_name": slave_name,
                                    "host_name":socket.gethostname(),
                                    "ip": get_ip_address(),
                                    "platform": platform.system(),
                                    "cpu_count": psutil.cpu_count(logical=True),
                                    "cpu_available_estimate": max(1, round((100 - psutil.cpu_percent(interval=1)) / 100 * psutil.cpu_count(logical=True))),
                                    "cpu_speed_mhz": cpu_speed,
                                    "memory_available_gb": round(psutil.virtual_memory().available / (1024**3), 2),
                                    "memory_speed_mhz": mem_speed,
                                    "buffer_cores": buffer_cores
                                }
                            }
                    #client.send(json.dumps(sys_info).encode())
                    #send_message(client, sys_info)
                    safe_send(client, sys_info)
    
                elif msg.get("command") == "request_sys_info":

                    buffer_cores = load_buffer_cores_from_file(default=6)
                    
                    sys_info = {
                                "slave_id": slave_id,
                                "info": {
                                    "uid": get_machine_uid(),
                                    "work_status": WORK_STATUS,
                                    "slave_name": slave_name,
                                    "host_name":socket.gethostname(),
                                    "ip": get_ip_address(),
                                    "platform": platform.system(),
                                    "cpu_count": psutil.cpu_count(logical=True),
                                    "cpu_available_estimate": max(1, round((100 - psutil.cpu_percent(interval=1)) / 100 * psutil.cpu_count(logical=True))),
                                    "cpu_speed_mhz": cpu_speed,
                                    "memory_available_gb": round(psutil.virtual_memory().available / (1024**3), 2),
                                    "memory_speed_mhz": mem_speed,
                                    "buffer_cores": buffer_cores
                                }
                            }
                    #send_message(client, sys_info)
                    safe_send(client, sys_info)
    
                elif msg.get("command") == "assign_work":
                    print(f"[SLAVE][ASSIGN_WORK] received work_id={msg.get('work_id')} shares={msg.get('work_shares')} total={msg.get('total_shares')}")
                    
                    work_id = msg["work_id"]
                    work_name = msg["work_name"]
                    work_type = msg["work_type"]
                    #work_data = msg["work_data"]
                    work_shares = msg["work_shares"]
                    total_shares = msg["total_shares"]

                    api_token = msg.get("api_token")
                    token_expiry_unix = msg.get("token_expiry_unix")
                    allowed_output_prefix = msg.get("allowed_output_prefix")
                    

                    '''
                    
                    # If packed (compressed) payload, unpack it back to the original dict
                    if isinstance(work_data, dict) and work_data.get("compression") == "zstd":
                        compressed = base64.b64decode(work_data["data_b64"])
                        dctx = zstd.ZstdDecompressor()
                        raw = dctx.decompress(compressed, max_output_size=work_data.get("orig_bytes", 5_000_000_000))
                        work_data = json.loads(raw.decode("utf-8"))
                    
                    threading.Thread(target=do_work, args=(client,work_id, work_name, work_type, work_data, work_shares,total_shares)).start()
                    '''

                    # NEW: if payload_ref exists, download it
                    payload_ref = msg.get("payload_ref")
                    if payload_ref:
                        print(f"[SLAVE][FETCH] work_id={work_id} downloading payload from url (zstd+json)")
                        try:
                            work_data = fetch_payload_ref(payload_ref)
                            print(f"[SLAVE][FETCH] work_id={work_id} payload OK (keys={list(work_data.keys())[:5]})")
                        except Exception as e:
                            print(f"[SLAVE][FETCH][ERROR] work_id={work_id}: {type(e).__name__}: {e}")
                            traceback.print_exc()
                            # Optionally report failure back to master
                            safe_send(client, {
                                "slave_id": slave_id,
                                "work_id": work_id,
                                "stream_type": "final_result",
                                "work_success": False,
                                "result": serialize_dataframes([pd.DataFrame()]),
                                "error": f"payload_fetch_failed:{type(e).__name__}:{e}",
                            })
                            continue
                    else:
                        # fallback: old inline payload
                        work_data = msg["work_data"]
                        if isinstance(work_data, dict) and work_data.get("compression") == "zstd":
                            compressed = base64.b64decode(work_data["data_b64"])
                            dctx = zstd.ZstdDecompressor()
                            raw = dctx.decompress(compressed, max_output_size=work_data.get("orig_bytes", 5_000_000_000))
                            work_data = json.loads(raw.decode("utf-8"))
                    print(f"[SLAVE][ASSIGN_WORK] trace={msg.get('_trace_id')} work_id={msg.get('work_id')} packed={isinstance(msg.get('work_data'), dict) and msg['work_data'].get('compression')}")
                    threading.Thread(
                        target=do_work,
                        args=(client, work_id, work_name, work_type, work_data, work_shares, total_shares),
                        kwargs={
                            "api_token": api_token,
                            "token_expiry_unix": token_expiry_unix,
                            "allowed_output_prefix": allowed_output_prefix,
                        },
                        daemon=True
                    ).start()



                elif msg.get("command") == "api_presign_put_resp":
                    rid = msg.get("req_id")
                    waiter = PRESIGN_WAITERS.get(rid)
                    if waiter:
                        ev, holder = waiter
                        holder["resp"] = msg
                        ev.set()
                    continue

                
                elif msg.get("command") == "api_append_rows_resp":
                    rid = msg.get("req_id")
                    waiter = APPEND_WAITERS.get(rid)
                    if waiter:
                        ev, holder = waiter
                        holder["resp"] = msg
                        ev.set()
                    continue

                
                elif msg.get("command") == "stop_work":
                    wid = msg.get("work_id")
                    reason = msg.get("reason", "stop_work")
                    if wid:
                        _stop_active_work(wid, reason=reason)

                        # mark idle + notify master immediately
                        WORK_STATUS = "idle"
                        safe_send(client, {"slave_id": slave_id, "work_status": WORK_STATUS})
                        safe_send(client, {
                            "slave_id": slave_id,
                            "command": "stop_work_ack",
                            "work_id": wid,
                            "status": "stopped",
                            "reason": reason,
                        })
                    continue


                    
                elif msg.get("stream_type") == "final_result_ack":
                    wid = msg.get("work_id")
                    ev = FINAL_RESULT_ACK_EVENTS.get(wid)
                    if ev:
                        ev.set()
                        print(f"[ACK] Master acknowledged final result for work_id={wid}")

    
            
            except socket.timeout:
                #continue
                continue
                
            except Exception as e:
                print(f"[SLAVE] Connection lost or error occurred: {e}")
                traceback.print_exc()
                break
      
    finally:
        #client.close()
        print(f"[DISCONNECTED]")


# In[ ]:


if __name__ == "__main__":
    mp.freeze_support()

    try:
        ready_path = os.path.join(os.getcwd(), "slave_ready.txt")
        with open(ready_path, "w", encoding="utf-8") as f:
            f.write(f"Breakeven_Slave READY\n")
            f.write(f"PID={PID}\n")
            f.write(f"launch_time={LAUNCH_TIME}\n")
            f.write(f"ready_time={datetime.datetime.now().isoformat()}\n")
        print(f"[SLAVE] Ready file created at: {ready_path}")
    except Exception as e:
        print(f"[SLAVE] Failed to create ready file: {e}")

    # Optional extra log:
    print("[SLAVE] Entering main connect_to_master loop...")

    
    while True:
        
        #connect_to_master('192.168.1.69',3333)
        #connect_to_master('192.168.1.2',8888)
        #connect_to_master('103.185.236.119',8888)
        connect_to_master('relay.breakeventx.com',8888)


# In[ ]:





# In[ ]:




