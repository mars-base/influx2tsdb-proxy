# influx2tsdb-proxy Ansible 部署

使用 Ansible 自动部署 influx2tsdb-proxy 服务（supervisor 进程管理）。

## 目录结构

```
ansible/
├── install.sh              # 一键安装脚本
├── requirements.yml        # ansible-galaxy 依赖
├── playbooks/
│   └── influx2tsdb-proxy.yml
└── roles/
    └── influx2tsdb-proxy/
        ├── defaults/main.yml   # 默认变量
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
cd influx2tsdb-proxy/ansible
```

### 方式 3：从 Galaxy 安装

```bash
ansible-galaxy install -r requirements.yml
```

## 配置

### 1. 创建 inventory 文件

```ini
# hosts
[servers]
192.168.1.100
192.168.1.101
```

### 2. 创建变量文件

```yaml
# group_vars/servers.yml
influx2tsdb_proxy_pg_dsn: "postgres://user:pass@db-host:5432/tsdb?sslmode=disable"
influx2tsdb_proxy_port: 8087
influx2tsdb_proxy_pool_size: 10
influx2tsdb_proxy_verbose: false
```

### 3. 执行 playbook

```bash
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml -e "HOSTS=servers"
```

## 变量说明

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `influx2tsdb_proxy_pg_dsn` | *(必需)* | PostgreSQL/TimescaleDB 连接字符串 |
| `influx2tsdb_proxy_port` | `8087` | HTTP 监听端口 |
| `influx2tsdb_proxy_pool_size` | `10` | 连接池大小 |
| `influx2tsdb_proxy_verbose` | `false` | 启用详细日志 |
| `influx2tsdb_proxy_version` | `latest` | 版本（对应 GitHub Release tag） |
| `influx2tsdb_proxy_dir` | `/srv/influx2tsdb-proxy` | 安装目录 |
| `influx2tsdb_proxy_supervisor_user` | `root` | 运行用户 |

## 指定版本

```bash
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml \
  -e "HOSTS=servers" \
  -e "influx2tsdb_proxy_version=v1.0.0"
```

## 管理

```bash
# 查看状态
ansible servers -m shell -a "supervisorctl status influx2tsdb-proxy"

# 重启服务
ansible servers -m shell -a "supervisorctl restart influx2tsdb-proxy"

# 查看日志
ansible servers -m shell -a "tail -50 /srv/influx2tsdb-proxy/logs/out_influx2tsdb-proxy.log"
```

## Tags

| Tag | 说明 |
|-----|------|
| `supervisor` | 仅检查/安装 supervisor |
| `dir` | 仅创建目录 |
| `sync` / `update` | 仅下载/更新二进制 |
| `config` | 仅验证配置 |
| `start` / `conf` | 仅部署 supervisor 配置 |

```bash
# 只更新二进制（不重装 supervisor）
ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml \
  -e "HOSTS=servers" \
  --tags "sync"
```
