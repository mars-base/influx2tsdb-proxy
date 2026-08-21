# influx2tsdb-proxy Ansible 部署

使用 Ansible 自动部署 influx2tsdb-proxy 服务（supervisor 进程管理），支持单机多实例部署。

## 目录结构

```
ansible/
├── install.sh              # 一键安装脚本
├── requirements.yml        # ansible-galaxy 依赖
├── playbooks/
│   └── influx2tsdb-proxy.yml
└── roles/
    └── influx2tsdb-proxy/
        ├── defaults/main.yml   # 默认变量（多实例配置）
        ├── handlers/main.yml   # 重启处理器
        ├── meta/main.yml       # Galaxy 元数据
        ├── tasks/main.yml      # 任务编排
        ├── vars/main.yml       # 高优先级变量
        └── templates/
            └── supervisor/
                └── influx2tsdb-proxy.conf.j2
```

## 快速开始

### 方式 1：使用 install.sh（推荐）

```bash
curl -sL https://raw.githubusercontent.com/mars-base/influx2tsdb-proxy/main/ansible/install.sh | bash
```

### 方式 2：手动复制

```bash
git clone https://github.com/mars-base/influx2tsdb-proxy.git
cp -r influx2tsdb-proxy/ansible /srv/influx2tsdb-proxy
cd /srv/influx2tsdb-proxy
```

## 配置

### 1. 创建 inventory 文件

```ini
# hosts
[servers]
192.168.1.100
192.168.1.101
```

### 2. 创建变量文件（多实例）

```yaml
# group_vars/servers.yml
influx2tsdb_proxy_instances:
  - name: game
    pg_dsn: "postgres://user:pass@db1:5432/game_tsdb?sslmode=disable"
    port: 8087
    pool_size: 20
    verbose: true
  
  - name: monitor
    pg_dsn: "postgres://user:pass@db2:5432/monitor_tsdb?sslmode=disable"
    port: 8088
    pool_size: 10
    dir: /opt/influx2tsdb-proxy-monitor  # 可选，自定义目录

# 全局变量（可选）
influx2tsdb_proxy_version: "v1.0.0"  # 默认 latest
```

### 3. 执行 playbook

```bash
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml -e "HOSTS=servers"
```

## 实例变量说明

每个实例支持的配置项：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `name` | *(必需)* | 实例名称（用于进程名、目录、日志） |
| `pg_dsn` | *(必需)* | PostgreSQL/TimescaleDB 连接字符串 |
| `port` | `8087` | HTTP 监听端口 |
| `pool_size` | `10` | 连接池大小 |
| `verbose` | `false` | 启用详细日志 |
| `dir` | `/srv/influx2tsdb-proxy-<name>` | 安装目录（可选） |

**全局变量：**

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `influx2tsdb_proxy_version` | `latest` | 版本（对应 GitHub Release tag） |

## 单机多实例示例

```yaml
# group_vars/servers.yml
influx2tsdb_proxy_instances:
  # 游戏数据实例
  - name: game
    pg_dsn: "postgres://dba:pass@10.241.21.97:5433/game_tsdb?sslmode=disable"
    port: 8087
    pool_size: 20
    verbose: true
  
  # 监控数据实例
  - name: monitor
    pg_dsn: "postgres://dba:pass@10.241.21.97:5433/monitor_tsdb?sslmode=disable"
    port: 8088
    pool_size: 10
  
  # 日志数据实例（自定义目录）
  - name: logs
    pg_dsn: "postgres://dba:pass@10.241.21.97:5433/logs_tsdb?sslmode=disable"
    port: 8089
    dir: /data/influx2tsdb-proxy-logs
```

每个实例会自动创建：
- 独立目录：`/srv/influx2tsdb-proxy-<name>/`
- 独立进程：`influx2tsdb-proxy-<name>`
- 独立日志：`<dir>/logs/out_<name>.log`

## 管理

```bash
# 查看所有实例状态
ansible servers -m shell -a "supervisorctl status"

# 重启所有实例
ansible servers -m shell -a "supervisorctl restart 'influx2tsdb-proxy:*'"

# 重启特定实例
ansible servers -m shell -a "supervisorctl restart influx2tsdb-proxy-game"

# 查看特定实例日志
ansible servers -m shell -a "tail -50 /srv/influx2tsdb-proxy-game/logs/out_game.log"

# 停止特定实例
ansible servers -m shell -a "supervisorctl stop influx2tsdb-proxy-monitor"
```

## Tags

| Tag | 说明 |
|-----|------|
| `supervisor` | 仅检查/安装 supervisor |
| `dir` | 仅创建目录 |
| `sync` / `update` | 仅下载/更新二进制 |
| `start` / `conf` | 仅部署 supervisor 配置 |

```bash
# 只更新二进制（不重装 supervisor）
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml --tags "sync"

# 只部署 supervisor 配置
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml --tags "start"
```

## 注意事项

1. **端口冲突**：确保每个实例使用不同的端口
2. **DSN 必填**：每个实例必须配置 `pg_dsn`，否则部署会失败
3. **共享二进制**：所有实例共享 `/usr/local/bin/influx2tsdb-proxy`，更新时所有实例同步更新
4. **日志轮转**：每个实例的日志独立管理，建议配合 logrotate 使用
