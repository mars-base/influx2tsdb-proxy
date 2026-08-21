#!/usr/bin/env python3
"""
Game server online metrics sampling script (InfluxDB Line Protocol).
Writes simulated data every 5 seconds. Designed to run forever.

Single target:
  # Write to InfluxDB (8086)
  python3 sample_online_influx.py

  # Write to influx2tsdb-proxy (8087)
  INFLUX_PORT=8087 INFLUX_DB=game_monitor python3 sample_online_influx.py

Dual target (both InfluxDB + proxy simultaneously):
  DUAL_WRITE=1 INFLUX_PORT2=8087 INFLUX_DB2=game_monitor nohup python3 -u sample_online_influx.py > /tmp/sample_dual.log 2>&1 &

Daemon mode (auto-restart on crash):
  DUAL_WRITE=1 INFLUX_PORT2=8087 INFLUX_DB2=game_monitor nohup python3 -u sample_online_influx.py --daemon > /tmp/sample_dual.log 2>&1 &

Stop:
  pkill -f sample_online_influx

Robustness features:
  - Global exception guard: main loop never exits on unexpected errors
  - Watchdog thread: detects stalls and forces recovery
  - Periodic session recreation: prevents connection pool aging
  - Auto-reconnect with exponential backoff (max 60s)
  - Daemon mode: auto-restart on crash with backoff
  - SIGTERM/SIGINT signal handling for clean shutdown
  - PID file to prevent duplicate instances
  - Per-target independent failure tracking
  - Periodic health check via /ping
  - Unbuffered output for reliable logging
"""

import os
import sys
import time
import random
import signal
import logging
import threading
import traceback
import subprocess
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests

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
SESSION_RECYCLE = int(os.getenv("SESSION_RECYCLE", "600"))  # recycle session every 10 min
WATCHDOG_TIMEOUT = int(os.getenv("WATCHDOG_TIMEOUT", "120"))  # stall detection: 2 min
DAEMON_RESTART_DELAY = int(os.getenv("DAEMON_RESTART_DELAY", "5"))  # seconds before restart
DAEMON_MAX_RESTARTS = int(os.getenv("DAEMON_MAX_RESTARTS", "0"))  # 0 = unlimited
PID_FILE = os.getenv("PID_FILE", "/tmp/sample_online_influx.pid")

DAEMON_MODE = "--daemon" in sys.argv

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
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sample")

# Force flush after every log message
for handler in logging.getLogger().handlers:
    handler.flush()


class FlushHandler(logging.StreamHandler):
    """Handler that flushes after every emit."""
    def emit(self, record):
        super().emit(record)
        self.flush()


# Replace default handler with flush handler
root_logger = logging.getLogger()
for h in root_logger.handlers[:]:
    root_logger.removeHandler(h)
fh = FlushHandler(sys.stderr)
fh.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
root_logger.addHandler(fh)

# ─── Signal handling ─────────────────────────────────────────────────────────

shutdown = False


def handle_signal(signum, frame):
    global shutdown
    log.info(f"收到信号 {signum}，准备退出...")
    shutdown = True
    # Exit with non-zero code so daemon knows to restart on SIGTERM/SIGINT
    # (only the daemon's own shutdown path should result in code 0)
    if DAEMON_MODE:
        # In daemon mode, just set the flag — daemon controls the lifecycle
        return
    else:
        # Direct run: clean exit
        cleanup_pid_file()
        sys.exit(0)


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)
# Ignore SIGHUP so closing terminal doesn't kill us
try:
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
except (AttributeError, OSError):
    pass  # SIGHUP not available on Windows

# ─── PID file ────────────────────────────────────────────────────────────────


def write_pid_file():
    pid = os.getpid()
    if os.path.exists(PID_FILE):
        try:
            old_pid = int(open(PID_FILE).read().strip())
            os.kill(old_pid, 0)  # check if process exists
            log.warning(f"另一个实例正在运行 (PID {old_pid})，尝试终止旧进程...")
            try:
                os.kill(old_pid, signal.SIGTERM)
                time.sleep(2)
                # Check if it's gone
                os.kill(old_pid, 0)
                log.error(f"旧进程 {old_pid} 仍在运行，退出")
                sys.exit(1)
            except OSError:
                pass  # old process dead, good
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


# ─── Watchdog ────────────────────────────────────────────────────────────────

class Watchdog:
    """Background thread that detects stalls and forces recovery."""

    def __init__(self, timeout, callback):
        self.timeout = timeout
        self.callback = callback
        self.last_activity = time.time()
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def heartbeat(self):
        self.last_activity = time.time()

    def _run(self):
        while not self._stop.is_set():
            self._stop.wait(self.timeout // 2)
            if self._stop.is_set():
                break
            elapsed = time.time() - self.last_activity
            if elapsed > self.timeout:
                log.warning(f"看门狗: 检测到卡死 {elapsed:.0f}s > {self.timeout}s，触发恢复")
                try:
                    self.callback()
                except Exception as e:
                    log.error(f"看门狗回调异常: {e}")
                self.last_activity = time.time()  # reset after recovery


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
    state: dict with keys: fail_count, total_count, last_ping
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
            if state["fail_count"] <= 3 or state["fail_count"] % 12 == 0:
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
        if state["fail_count"] <= 3 or state["fail_count"] % 12 == 0:
            log.warning(f"{target_name} 写入异常: {e}（第 {state['fail_count']} 次）")
        return False


def calc_backoff(fail_count):
    """Exponential backoff capped at MAX_BACKOFF seconds."""
    if fail_count == 0:
        return 0
    return min(2 ** min(fail_count, 6), MAX_BACKOFF)


def safe_sleep(seconds, check_shutdown):
    """Sleep in small increments, resilient to interruptions."""
    end = time.time() + seconds
    while time.time() < end:
        if check_shutdown():
            return
        try:
            time.sleep(min(1, end - time.time()))
        except Exception:
            pass


# ─── Main loop ───────────────────────────────────────────────────────────────


def run_main_loop():
    """Main sampling loop with full exception protection. Never exits unless shutdown."""
    write_pid_file()

    log.info(f"InfluxDB: {INFLUX_URL}, db: {INFLUX_DB}")
    if DUAL_WRITE:
        log.info(f"Dual write: {INFLUX_URL2}, db: {INFLUX_DB2}")
    log.info(f"采样间隔 {INTERVAL}s, 超时 {REQUEST_TIMEOUT}s, 最大退避 {MAX_BACKOFF}s")
    log.info(f"Session 回收间隔 {SESSION_RECYCLE}s, 看门狗超时 {WATCHDOG_TIMEOUT}s")
    if DAEMON_MODE:
        log.info("守护模式: 崩溃后自动重启")

    session = create_session()
    init_db(session)
    now = time.time()

    # Per-target state
    state1 = {"fail_count": 0, "total_count": 0, "last_ping": now}
    state2 = {"fail_count": 0, "total_count": 0, "last_ping": now}

    session_recreate_threshold = 20
    last_session_create = time.time()
    loop_error_count = 0  # track consecutive loop errors

    # Watchdog: if no successful write in WATCHDOG_TIMEOUT seconds, force session recreate
    def watchdog_recovery():
        nonlocal session
        log.warning("看门狗触发: 强制重建 session 并重置状态")
        try:
            session.close()
        except Exception:
            pass
        session = create_session()
        state1["fail_count"] = 0
        state2["fail_count"] = 0
        state1["last_ping"] = time.time()
        state2["last_ping"] = time.time()

    watchdog = Watchdog(WATCHDOG_TIMEOUT, watchdog_recovery)
    watchdog.start()

    log.info("采样开始，Ctrl+C 停止")

    try:
        while not shutdown:
            try:
                lines, total_online = generate_line_protocol()
                body = "\n".join(lines)

                ok1 = write_to_target(session, WRITE_URL, body, "主目标", state1)
                ok2 = write_to_target(session, WRITE_URL2, body, "副目标", state2) if DUAL_WRITE else True

                if ok1 or ok2:
                    watchdog.heartbeat()
                    loop_error_count = 0

                if ok1:
                    log.info(f"写入 {len(lines)} 条，总在线 {total_online}，累计 {state1['total_count']}")

                # Periodic session recycling
                if time.time() - last_session_create > SESSION_RECYCLE:
                    log.info(f"定期回收 session（已运行 {SESSION_RECYCLE}s）")
                    try:
                        session.close()
                    except Exception:
                        pass
                    session = create_session()
                    last_session_create = time.time()
                    # Reset ping timers so we check health after session change
                    state1["last_ping"] = time.time()
                    state2["last_ping"] = time.time()

                # Recreate session on persistent failures
                if state1["fail_count"] > session_recreate_threshold or (DUAL_WRITE and state2["fail_count"] > session_recreate_threshold):
                    log.info("持续失败，重建 session...")
                    try:
                        session.close()
                    except Exception:
                        pass
                    session = create_session()
                    last_session_create = time.time()
                    state1["fail_count"] = max(0, state1["fail_count"] - 5)
                    state2["fail_count"] = max(0, state2["fail_count"] - 5)

                # Backoff on failure
                backoff = max(calc_backoff(state1["fail_count"]), calc_backoff(state2["fail_count"]) if DUAL_WRITE else 0)
                if backoff > 0:
                    log.info(f"退避 {backoff}s...")

                # Sleep
                sleep_time = backoff if backoff > 0 else INTERVAL
                safe_sleep(sleep_time, lambda: shutdown)

            except Exception as e:
                # GLOBAL EXCEPTION GUARD: catch ANYTHING, log it, keep going
                loop_error_count += 1
                log.error(f"主循环异常（第 {loop_error_count} 次）: {e}")
                log.debug(traceback.format_exc())

                # If too many consecutive errors, recreate everything
                if loop_error_count >= 10:
                    log.warning("连续异常过多，完全重建 session...")
                    try:
                        session.close()
                    except Exception:
                        pass
                    session = create_session()
                    last_session_create = time.time()
                    state1["fail_count"] = 0
                    state2["fail_count"] = 0
                    loop_error_count = 0

                safe_sleep(min(2 ** min(loop_error_count, 5), 30), lambda: shutdown)

    finally:
        watchdog.stop()
        try:
            session.close()
        except Exception:
            pass
        cleanup_pid_file()
        log.info(f"采样停止。主目标累计 {state1['total_count']} 条，副目标累计 {state2['total_count']} 条")


# ─── Daemon mode ─────────────────────────────────────────────────────────────


def run_daemon():
    """Run the script as a subprocess, auto-restart on ANY exit.
    Only stops when the daemon process itself receives SIGTERM/SIGINT.
    """
    log.info("=== 守护模式启动 ===")

    # Build the command without --daemon, set env to mark as daemon-managed worker
    cmd = [sys.executable, "-u"] + [a for a in sys.argv if a != "--daemon"]
    env = os.environ.copy()
    env["_DAEMON_MANAGED"] = "1"  # worker can check this to know it's managed

    restart_count = 0
    restart_backoff = DAEMON_RESTART_DELAY

    while not shutdown:
        log.info(f"启动工作进程: {' '.join(cmd)}")
        run_start = time.time()
        try:
            # Start worker in its own process group so SIGTERM to daemon
            # doesn't automatically propagate to worker
            proc = subprocess.Popen(cmd, env=env, start_new_session=True)

            # Wait for process, check shutdown periodically
            while proc.poll() is None:
                if shutdown:
                    log.info("收到停止信号，终止工作进程...")
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    except OSError:
                        proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        except OSError:
                            proc.kill()
                        proc.wait()
                    return
                time.sleep(1)

            exit_code = proc.returncode
            run_duration = time.time() - run_start
            restart_count += 1

            if DAEMON_MAX_RESTARTS > 0 and restart_count > DAEMON_MAX_RESTARTS:
                log.error(f"重启次数超限 ({restart_count} > {DAEMON_MAX_RESTARTS})，退出")
                return

            # Reset backoff if the worker ran for a while before dying
            if run_duration > 300:  # 5 minutes
                restart_backoff = DAEMON_RESTART_DELAY

            log.warning(f"工作进程退出 (code={exit_code}, 运行 {run_duration:.0f}s)，第 {restart_count} 次重启，等待 {restart_backoff}s...")
            safe_sleep(restart_backoff, lambda: shutdown)
            restart_backoff = min(restart_backoff * 2, 60)

        except Exception as e:
            restart_count += 1
            log.error(f"守护进程异常: {e}，第 {restart_count} 次重启")
            safe_sleep(restart_backoff, lambda: shutdown)
            restart_backoff = min(restart_backoff * 2, 60)


# ─── Entry point ─────────────────────────────────────────────────────────────


if __name__ == "__main__":
    try:
        if DAEMON_MODE:
            run_daemon()
        else:
            run_main_loop()
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt, 退出")
    except Exception as e:
        log.error(f"致命错误: {e}")
        log.error(traceback.format_exc())
        sys.exit(1)
