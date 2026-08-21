#!/usr/bin/env python3
"""
Game server online metrics sampling script (InfluxDB Line Protocol).
Writes simulated data every 5 seconds.

Single target:
  # Write to InfluxDB (8086)
  python3 sample_online_influx.py

  # Write to influx2tsdb-proxy (8087)
  INFLUX_PORT=8087 INFLUX_DB=tsdb python3 sample_online_influx.py
  # Note: INFLUX_DB must point to a database with PostgreSQL + TimescaleDB
  # extension already set up. The proxy will auto-create hypertable on first
  # write, but the database and extension must exist beforehand:
  #   CREATE DATABASE tsdb;
  #   \c tsdb
  #   CREATE EXTENSION IF NOT EXISTS timescaledb;

Dual target (both InfluxDB + proxy simultaneously):
  # Option 1: run two background processes
  nohup python3 sample_online_influx.py > /tmp/sample_influx.log 2>&1 &
  INFLUX_PORT=8087 INFLUX_DB=tsdb nohup python3 sample_online_influx.py > /tmp/sample_proxy.log 2>&1 &

  # Option 2: use tee to duplicate writes via shell wrapper
  echo 'while true; do' > dual_write.sh
  echo '  data=$(python3 -c "from sample_online_influx import generate_line_protocol; print(chr(10).join(generate_line_protocol()[0]))")' >> dual_write.sh
  echo '  curl -s -X POST http://localhost:8086/write?db=game_monitor --data-binary "$data"' >> dual_write.sh
  echo '  curl -s -X POST http://localhost:8087/write?db=tsdb --data-binary "$data"' >> dual_write.sh
  echo '  sleep 5' >> dual_write.sh
  echo 'done' >> dual_write.sh
  bash dual_write.sh

  # Option 3: set DUAL_WRITE=1 to write to both targets in a single process (see code below)
  DUAL_WRITE=1 INFLUX_PORT2=8087 INFLUX_DB2=tsdb nohup python3 sample_online_influx.py > /tmp/sample_dual.log 2>&1 &

Stop all:
  pkill -f sample_online_influx
"""

import os
import time
import random
import requests
from datetime import datetime

# InfluxDB 连接参数
INFLUX_HOST = os.getenv("INFLUX_HOST", "localhost")
INFLUX_PORT = os.getenv("INFLUX_PORT", "8086")
INFLUX_DB = os.getenv("INFLUX_DB", "game_monitor")
MEASUREMENT = os.getenv("MEASUREMENT", "server_online")
INFLUX_URL = f"http://{INFLUX_HOST}:{INFLUX_PORT}"
WRITE_URL = f"{INFLUX_URL}/write?db={INFLUX_DB}"

# 区服配置
SERVERS = [
    ("s1", "华东"), ("s2", "华东"),
    ("s3", "华南"), ("s4", "华南"),
    ("s5", "华北"), ("s6", "华北"),
    ("s7", "西南"), ("s8", "西南"),
]

# 每个服的基础在线人数
BASE_ONLINE = {
    "s1": 3500, "s2": 2800,
    "s3": 3200, "s4": 2600,
    "s5": 3000, "s6": 2400,
    "s7": 2200, "s8": 1800,
}

INTERVAL = 5  # 采样间隔（秒）


# Dual write target (optional)
DUAL_WRITE = os.getenv("DUAL_WRITE", "0") == "1"
INFLUX_HOST2 = os.getenv("INFLUX_HOST2", "localhost")
INFLUX_PORT2 = os.getenv("INFLUX_PORT2", "8087")
INFLUX_DB2 = os.getenv("INFLUX_DB2", "tsdb")
INFLUX_URL2 = f"http://{INFLUX_HOST2}:{INFLUX_PORT2}"
WRITE_URL2 = f"{INFLUX_URL2}/write?db={INFLUX_DB2}"


def init_db(session):
    """创建数据库（如不存在）"""
    try:
        session.post(f"{INFLUX_URL}/query", params={"q": f"CREATE DATABASE {INFLUX_DB}"}, timeout=5)
    except Exception:
        pass
    if DUAL_WRITE:
        try:
            session.post(f"{INFLUX_URL2}/query", params={"q": f"CREATE DATABASE {INFLUX_DB2}"}, timeout=5)
        except Exception:
            pass


def generate_line_protocol():
    """生成一批 Line Protocol 数据"""
    lines = []
    for server_id, region in SERVERS:
        base = BASE_ONLINE[server_id]
        online = int(base * random.uniform(0.97, 1.03))
        # measurement,tag=value field=value timestamp
        lines.append(f"{MEASUREMENT},server_id={server_id},region={region} online_count={online}i")
    return lines, sum(int(l.split("online_count=")[1].rstrip("i")) for l in lines)


def main():
    print(f"InfluxDB: {INFLUX_URL}, 数据库: {INFLUX_DB}")
    if DUAL_WRITE:
        print(f"Dual write enabled: {INFLUX_URL2}, 数据库: {INFLUX_DB2}")
    session = requests.Session()
    session.trust_env = False  # 忽略系统代理
    init_db(session)
    print(f"采样开始，间隔 {INTERVAL}s，Ctrl+C 停止")

    count = 0
    fail_count = 0
    fail_count2 = 0
    while True:
        lines, total_online = generate_line_protocol()
        body = "\n".join(lines)
        ts = datetime.now().strftime("%H:%M:%S")

        # Write to primary target
        try:
            resp = session.post(WRITE_URL, data=body.encode("utf-8"), timeout=5)
            if resp.status_code != 204:
                print(f"[{ts}] ⚠️ 写入失败: {resp.status_code} {resp.text}")
                fail_count += 1
            else:
                if fail_count > 0:
                    print(f"[{ts}] ✅ 恢复连接，之前连续失败 {fail_count} 次")
                    fail_count = 0
                count += len(lines)
                print(f"[{ts}] 写入 {len(lines)} 条，总在线 {total_online}，累计 {count}")
        except requests.ConnectionError:
            fail_count += 1
            if fail_count <= 3 or fail_count % 12 == 0:
                print(f"[{ts}] ⚠️ 连接失败（第 {fail_count} 次），等待重试...")
        except requests.Timeout:
            fail_count += 1
            if fail_count <= 3 or fail_count % 12 == 0:
                print(f"[{ts}] ⚠️ 请求超时（第 {fail_count} 次），等待重试...")
        except Exception as e:
            fail_count += 1
            print(f"[{ts}] ⚠️ 写入异常: {e}（第 {fail_count} 次）")

        # Write to secondary target if dual write enabled
        if DUAL_WRITE:
            try:
                resp2 = session.post(WRITE_URL2, data=body.encode("utf-8"), timeout=5)
                if resp2.status_code != 204:
                    print(f"[{ts}] ⚠️ 二次写入失败: {resp2.status_code} {resp2.text}")
                    fail_count2 += 1
                else:
                    if fail_count2 > 0:
                        print(f"[{ts}] ✅ 二次写入恢复，之前连续失败 {fail_count2} 次")
                        fail_count2 = 0
            except requests.ConnectionError:
                fail_count2 += 1
                if fail_count2 <= 3 or fail_count2 % 12 == 0:
                    print(f"[{ts}] ⚠️ 二次写入连接失败（第 {fail_count2} 次）...")
            except requests.Timeout:
                fail_count2 += 1
                if fail_count2 <= 3 or fail_count2 % 12 == 0:
                    print(f"[{ts}] ⚠️ 二次写入请求超时（第 {fail_count2} 次）...")
            except Exception as e:
                fail_count2 += 1
                print(f"[{ts}] ⚠️ 二次写入异常: {e}（第 {fail_count2} 次）")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n采样停止")
    except Exception as e:
        print(f"错误: {e}")
