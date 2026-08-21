#!/usr/bin/env bash
set -euo pipefail

# influx2tsdb-proxy Ansible 部署安装脚本
# 用法: curl -sL https://raw.githubusercontent.com/mars-base/influx2tsdb-proxy/main/ansible/install.sh | bash

REPO="mars-base/influx2tsdb-proxy"
BRANCH="main"
INSTALL_DIR="${INSTALL_DIR:-/srv/influx2tsdb-proxy}"

echo "==> 安装 influx2tsdb-proxy Ansible 部署配置"
echo "    仓库: ${REPO}"
echo "    分支: ${BRANCH}"
echo "    目标: ${INSTALL_DIR}"
echo ""

# 创建安装目录
mkdir -p "${INSTALL_DIR}"

# 下载 ansible 目录
BASE_URL="https://raw.githubusercontent.com/${REPO}/${BRANCH}/ansible"
FILES=(
  "requirements.yml"
  "playbooks/influx2tsdb-proxy.yml"
  "roles/influx2tsdb-proxy/defaults/main.yml"
  "roles/influx2tsdb-proxy/handlers/main.yml"
  "roles/influx2tsdb-proxy/meta/main.yml"
  "roles/influx2tsdb-proxy/tasks/main.yml"
  "roles/influx2tsdb-proxy/vars/main.yml"
  "roles/influx2tsdb-proxy/templates/supervisor/influx2tsdb-proxy.conf.j2"
)

for file in "${FILES[@]}"; do
  target="${INSTALL_DIR}/${file}"
  mkdir -p "$(dirname "${target}")"
  echo "  下载: ${file}"
  curl -sL "${BASE_URL}/${file}" -o "${target}"
done

echo ""
echo "✓ 安装完成！"
echo ""
echo "使用方法："
echo "  cd ${INSTALL_DIR}"
echo ""
echo "  # 1. 编辑 host 文件（添加目标服务器）"
echo "  cat > hosts << 'EOF'"
echo "[servers]"
echo "192.168.1.100"
echo "EOF"
echo ""
echo "  # 2. 创建变量文件（设置 PostgreSQL DSN）"
echo "  cat > group_vars/servers.yml << 'EOF'"
echo "influx2tsdb_proxy_pg_dsn: \"postgres://user:pass@host:5432/db?sslmode=disable\""
echo "influx2tsdb_proxy_port: 8087"
echo "EOF"
echo ""
echo "  # 3. 执行 playbook"
echo "  ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml -e \"HOSTS=servers\""
echo ""
