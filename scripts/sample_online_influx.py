#!/usr/bin/env python3
"""
Game server online metrics sampling script (InfluxDB Line Protocol).
Writes simulated data every 5 seconds.

Single target:
  # Write to InfluxDB (8086)
  python3 sample_online_influx.py

  # Write to influx2tsdb-proxy (8087)
  INFLUX_PORT=8087 INFLUX_DB=game_monitor python3 sample_online_influx.py
  # Note: The proxy ignores the db parameter and writes to TimescaleDB.
  # The database and TimescaleDB extension must exist beforehand:
  #   CREATE DATABASE tsdb;
  #   \c tsdb
  #   CREATE EXTENSION IF NOT EXISTS timescaledb;

Dual target (both InfluxDB + proxy simultaneously):
  # Option 1: run two background processes
  nohup python3 sample_online_influx.py > /tmp/sample_influx.log 2>&1 &
  INFLUX_PORT=8087 INFLUX_DB=game_monitor nohup python3 sample_online_influx.py > /tmp/sample_proxy.log 2>&1 &

  # Option 2: set DUAL_WRITE=1 to write to both targets in a single process (see code below)
  DUAL_WRITE=1 INFLUX_PORT2=8087 INFLUX_DB2=game_monitor nohup python3 sample_online_influx.py > /tmp/sample_dual.log 2>&1 &

Stop all:
  pkill -f sample_online_influx

Robustness features:
  - Auto-reconnect with exponential backoff (max 60s)
  - Session recreation after persistent failures
  - SIGTERM/SIGINT signal handling for clean shutdown
  - PID file to prevent duplicate instances
  - Per-target independent failure tracking
  - Periodic health check via /ping
"""

import os
import sys
import time
import random
import signal
import logging
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── Configuration ───────────────────────────────────────────────────────────

INFLUX_HOST = os.getenv("INFLUX_HOST", "localhost")
INFLUX_PORT = os.getenv("INFLUX_PORT", "8086")
INFLUX_DB = os.getenv("INFLUX_DB", "game_monitor")
MEASUREMENT = os.getenv("MEASUREMENT", "server_online")
INFLUX_URL = f"http://{INFLUX_HOST}:{INFLUX_PORT}"
WRITE_URL = f"{INFLUX_URL}/write?db={INFLUX_DB}"

DUAL_WRITE = os.getenv("DUAL_WRITE", "0") == "1"
INFLUX_HOST2 = os.getenv("INFLUX_HOST2", "localhost")
INFLUX_PORT2 = os.getenv("INFLUX_PORT2", "8087")
INFLUX_DB2 = os.getenv("INFLUX_DB2", "game_monitor")
INFLUX_URL2 = f"http://{INFLUX_HOST2}:{INFLUX_PORT2}"
WRITE_URL2 = f"{INFLUX_URL2}/write?db={INFLUX_DB2}"

INTERVAL = int(os.getenv("INTERVAL", "5"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
MAX_BACKOFF = int(os.getenv("MAX_BACKOFF", "60"))
PING_INTERVAL = int(os.getenv("PING_INTERVAL", "300"))  # health check every 5 min
PID_FILE = os.getenv("PID_FILE", "/tmp/sample_online_influx.pid")

# ─── Server configuration ────────────────────────────────────────────────────

SERVERS = [
    ("s1", "华东"), ("s2", "华东"),
    ("s3", "华南"), ("s4", "华南"),
    ("s5", "华北"), ("s6", "华北"),
    ("s7", "西南"), ("s8", "西南"),
]

BASE_ONLINE = {
    "s1": 3500, "s2": 2800,
    "s3": 3200, "s4": 2600,
    "s5": 3000, "s6": 2400,
    "s7": 2200, "s8": 1800,
}

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sample")

# ─── Signal handling ─────────────────────────────────────────────────────────

shutdown = False


def handle_signal(signum, frame):
    global shutdown
    log.info(f"收到信号 {signum}，准备退出...")
    shutdown = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# ─── PID file ────────────────────────────────────────────────────────────────


def write_pid_file():
    pid = os.getpid()
    if os.path.exists(PID_FILE):
        try:
            old_pid = int(open(PID_FILE).read().strip())
            os.kill(old_pid, 0)  # check if process exists
            log.error(f"另一个实例正在运行 (PID {old_pid})，退出")
            sys.exit(1)
        except (OSError, ValueError):
            pass  # old process dead or invalid pid
    with open(PID_FILE, "w") as f:
        f.write(str(pid))
    log.info(f"PID: {pid}")


def cleanup_pid_file():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass

# ─── HTTP session ────────────────────────────────────────────────────────────


def create_session():
    """Create a requests session with connection pooling and retry."""
    session = requests.Session()
    session.trust_env = False  # ignore system proxy

    retry = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=4,
        pool_maxsize=4,
        pool_block=False,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# ─── Core functions ──────────────────────────────────────────────────────────


def init_db(session):
    """Create database if not exists (best-effort, ignore failures)."""
    for url in set([INFLUX_URL, INFLUX_URL2] if DUAL_WRITE else [INFLUX_URL]):
        try:
            session.post(f"{url}/query", params={"q": "CREATE DATABASE " + (INFLUX_DB2 if "8087" in url else INFLUX_DB)}, timeout=REQUEST_TIMEOUT)
        except Exception:
            pass


def ping_target(session, url, name):
    """Health check via /ping. Returns True if alive."""
    try:
        resp = session.get(f"{url}/ping", timeout=REQUEST_TIMEOUT)
        return resp.status_code in (200, 204)
    except Exception:
        return False


def generate_line_protocol():
    """Generate Line Protocol data for all servers."""
    lines = []
    for server_id, region in SERVERS:
        base = BASE_ONLINE[server_id]
        online = int(base * random.uniform(0.97, 1.03))
        lines.append(f"{MEASUREMENT},server_id={server_id},region={region} online_count={online}i")
    return lines, sum(int(l.split("online_count=")[1].rstrip("i")) for l in lines)


def write_to_target(session, write_url, body, target_name, state):
    """
    Write data to a single target.
    state: dict with keys: fail_count, total_count, last_ping, session_age
    Returns: True if write succeeded.
    """
    now = time.time()

    # Periodic health check
    if now - state["last_ping"] > PING_INTERVAL:
        alive = ping_target(session, write_url.rsplit("/write", 1)[0], target_name)
        state["last_ping"] = now
        if not alive:
            state["fail_count"] += 1
            if state["fail_count"] <= 3 or state["fail_count"] % 12 == 0:
                log.warning(f"{target_name} ping 失败（第 {state['fail_count']} 次）")
            return False

    try:
        resp = session.post(write_url, data=body.encode("utf-8"), timeout=REQUEST_TIMEOUT)
        if resp.status_code == 204:
            if state["fail_count"] > 0:
                log.info(f"{target_name} 恢复连接，之前连续失败 {state['fail_count']} 次")
            state["fail_count"] = 0
            state["total_count"] += len(body.strip().split("\n"))
            return True
        else:
            state["fail_count"] += 1
            log.warning(f"{target_name} 写入失败: {resp.status_code} {resp.text[:200]}")
            return False
    except requests.ConnectionError:
        state["fail_count"] += 1
        if state["fail_count"] <= 3 or state["fail_count"] % 12 == 0:
            log.warning(f"{target_name} 连接失败（第 {state['fail_count']} 次）")
        return False
    except requests.Timeout:
        state["fail_count"] += 1
        if state["fail_count"] <= 3 or state["fail_count"] % 12 == 0:
            log.warning(f"{target_name} 请求超时（第 {state['fail_count']} 次）")
        return False
    except Exception as e:
        state["fail_count"] += 1
        log.warning(f"{target_name} 写入异常: {e}（第 {state['fail_count']} 次）")
        return False


def calc_backoff(fail_count):
    """Exponential backoff capped at MAX_BACKOFF seconds."""
    if fail_count == 0:
        return 0
    return min(2 ** min(fail_count, 6), MAX_BACKOFF)


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    write_pid_file()

    log.info(f"InfluxDB: {INFLUX_URL}, db: {INFLUX_DB}")
    if DUAL_WRITE:
        log.info(f"Dual write: {INFLUX_URL2}, db: {INFLUX_DB2}")
    log.info(f"采样间隔 {INTERVAL}s, 超时 {REQUEST_TIMEOUT}s, 最大退避 {MAX_BACKOFF}s")

    session = create_session()
    init_db(session)
    now = time.time()

    # Per-target state
    state1 = {"fail_count": 0, "total_count": 0, "last_ping": now}
    state2 = {"fail_count": 0, "total_count": 0, "last_ping": now}

    session_recreate_threshold = 20  # recreate session after N consecutive failures
    log.info(f"采样开始，Ctrl+C 停止")

    try:
        while not shutdown:
            lines, total_online = generate_line_protocol()
            body = "\n".join(lines)

            ok1 = write_to_target(session, WRITE_URL, body, "主目标", state1)
            ok2 = write_to_target(session, WRITE_URL2, body, "副目标", state2) if DUAL_WRITE else True

            if ok1:
                log.info(f"写入 {len(lines)} 条，总在线 {total_online}，累计 {state1['total_count']}")

            # Recreate session if both targets failing persistently
            if state1["fail_count"] > session_recreate_threshold or (DUAL_WRITE and state2["fail_count"] > session_recreate_threshold):
                log.info("持续失败，重建 session...")
                try:
                    session.close()
                except Exception:
                    pass
                session = create_session()
                state1["fail_count"] = max(0, state1["fail_count"] - 5)
                state2["fail_count"] = max(0, state2["fail_count"] - 5)

            # Backoff on failure
            backoff = max(calc_backoff(state1["fail_count"]), calc_backoff(state2["fail_count"]) if DUAL_WRITE else 0)
            if backoff > 0:
                log.info(f"退避 {backoff}s...")

            # Sleep in small increments for responsive shutdown
            sleep_time = backoff if backoff > 0 else INTERVAL
            for _ in range(int(sleep_time)):
                if shutdown:
                    break
                time.sleep(1)
    finally:
        try:
            session.close()
        except Exception:
            pass
        cleanup_pid_file()
        log.info(f"采样停止。主目标累计 {state1['total_count']} 条，副目标累计 {state2['total_count']} 条")


if __name__ == "__main__":
    main()
