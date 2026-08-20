#!/usr/bin/env python3
"""
游戏服在线人数实时采样脚本（InfluxDB 版）
每 5 秒写入一批模拟数据到 InfluxDB
后台运行: nohup python3 sample_online_influx.py > /dev/null 2>&1 &
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


def init_db(session):
    """创建数据库（如不存在）"""
    session.post(
        f"{INFLUX_URL}/query",
        params={"q": f"CREATE DATABASE {INFLUX_DB}"}
    )


def generate_line_protocol():
    """生成一批 Line Protocol 数据"""
    lines = []
    for server_id, region in SERVERS:
        base = BASE_ONLINE[server_id]
        online = int(base * random.uniform(0.97, 1.03))
        # measurement,tag=value field=value timestamp
        lines.append(f"server_online,server_id={server_id},region={region} online_count={online}i")
    return lines, sum(int(l.split("online_count=")[1].rstrip("i")) for l in lines)


def main():
    print(f"InfluxDB: {INFLUX_URL}, 数据库: {INFLUX_DB}")
    session = requests.Session()
    session.trust_env = False  # 忽略系统代理
    init_db(session)
    print(f"采样开始，间隔 {INTERVAL}s，Ctrl+C 停止")

    count = 0
    fail_count = 0
    while True:
        lines, total_online = generate_line_protocol()
        body = "\n".join(lines)
        ts = datetime.now().strftime("%H:%M:%S")
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
        time.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n采样停止")
    except Exception as e:
        print(f"错误: {e}")
