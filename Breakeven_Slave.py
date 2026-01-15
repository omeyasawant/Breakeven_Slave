#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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


# In[ ]:


import requests
import hashlib


# ## Global Variables
# 

# In[ ]:


slave_id = None
slave_name = 'omeya_sudo'
buffer_cores = 1
total_subtasks = 0
completed = None
WORK_STATUS = "idle"
FINAL_RESULT_ACK_EVENTS = {}  # work_id -> threading.Event

# --- TIMEOUT CONFIG (slave side) ---------------------------------
HEARTBEAT_INTERVAL = 10          # seconds
POOL_TIMEOUT_SEC   = 5400        # 90 min – generous share-timeout
# -----------------------------------------------------------------


# ## Connection DEBUGS

# In[ ]:


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

# In[ ]:


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


# ## Logging 

# In[ ]:


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

# In[ ]:


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


# In[ ]:


def get_ip_address():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "0.0.0.0"



# In[ ]:


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


# In[ ]:


def send_message(conn, message_dict):
    message_bytes = json.dumps(message_dict).encode()
    length_prefix = len(message_bytes).to_bytes(4, byteorder='big')  # 4-byte length
    conn.sendall(length_prefix + message_bytes)


# In[ ]:


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


# In[ ]:


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

# In[ ]:


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


# In[ ]:


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


# # Slave Code

# In[ ]:


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


# In[ ]:


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


# In[ ]:


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


# In[ ]:


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


# In[ ]:


def do_work(conn,work_id, work_name, work_type, work_data, work_shares,total_shares):
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
    slave_start_time = int(time.time() * 1000)  # Time when starting actual work

    WORK_STATUS = "working"
    #send_message(conn, {"slave_id": slave_id, "work_status": WORK_STATUS})
    safe_send(conn, {"slave_id": slave_id, "work_status": WORK_STATUS})

    
    try:
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
            '''
            def process_wrapper(input_df,input_resample_timeframe):
                
                # --- TYPE CHECKS ---
                if not isinstance(input_df, pd.DataFrame):
                    print(f"[ERROR] process_wrapper: Expected DataFrame for input_df but got {type(input_df)}")
                    raise TypeError(f"Invalid input_df type: {type(input_df)}")
        
                if not isinstance(input_resample_timeframe, str):
                    print(f"[ERROR] process_wrapper: Expected str for input_resample_timeframe but got {type(input_resample_timeframe)}")
                    raise TypeError(f"Invalid input_resample_timeframe type: {type(input_resample_timeframe)}")

                # --- ACTUAL PROCESSING ---
                res = process_sub_df(input_df,input_resample_timeframe)
                
                # --- UPDATE PROGRESS ---
                with completed.get_lock():
                    completed.value += 1
                return res
            '''
            '''
            def init_child(shared_completed):
                global completed
                completed = shared_completed
            ''' 
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
                        
            def get_progress():
                with completed.get_lock():
                    return (completed.value / total_subtasks) * 100
            '''
            # --- Start Heartbeat ---
            stop_event = threading.Event()
            heartbeat_thread = threading.Thread(
                                                target=heartbeat_sender,
                                                args=(conn, work_id, work_name, get_progress,stop_event),
                                                daemon=True
                                            )
            heartbeat_thread.start()

            # --- Start Progress Bar ---
            progress_bar = tqdm(total=total_subtasks, desc=f"Slave {slave_id} Progress", position=0, leave=True)
            progress_stop_event = threading.Event()
            
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
            '''
            def watchdog():
                time.sleep(6000)  # 10 minutes timeout
                print("[WATCHDOG] Work timeout reached. Force killing slave.")
                #os._exit(1)
            
            threading.Thread(target=watchdog, daemon=True).start()
            '''
            watchdog_stop_event = threading.Event()
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
                    #result_async = pool.starmap_async(process_wrapper,[(mini_input[0],mini_input[1],completed) for mini_input in Inputs])
                    result_async = pool.starmap_async(process_wrapper,Inputs)
    
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
            
            except Exception as e:
                        print(f"Error while pool processing {work_id}: {str(e)}")
                        #conn.close()


            '''
            # Create a pool of workers, leaving 3 cores free
            with Pool(processes=pool_size) as pool:
                final_results = pool.starmap(process_sub_df, sub_dfs)
            '''
            if len(final_results) == len(Inputs):
                work_success = True
            else:
                work_success = False
                print(f"[WORK] Failed Work with ID {work_id}: {work_name} (Result Counts do not match)")
                print(f"[WORK] Expected Count : {len(Inputs)} | Got Count :{len(final_results)}")


        else:
            # Handle other work types here
            print(f"[WORK] Strange {work_id}: {work_name} ({work_type} not identified)")
            final_results = None
            work_success = False

    except Exception as e:
        final_results = f"Error while processing {work_id}: {str(e)}"
        #conn.close()

    
    slave_end_time = int(time.time() * 1000)  # Finished actual work
    
    slave_send_time = int(time.time() * 1000)  # Time right before sending back
    
    final_packet = {
                    "slave_id": slave_id,
                    "work_id": work_id,
                    "work_name": work_name,
                    "work_type": work_type,
                    "stream_type": "final_result",
                    "work_success": work_success,
                    "result": serialize_dataframes(final_results), 

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
                if msg.get('command')!='request_sys_info':
                    print(f"[SLAVE][DISPATCH] command={msg.get('command')} stream_type={msg.get('stream_type')}")
                
                if msg.get("command") == "assign_id":
                    slave_id = msg["slave_id"]
                    print(f"[SLAVE] Assigned ID: {slave_id}")
                    
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
                        daemon=True
                    ).start()






                    
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




