#!/usr/bin/env bash
set -euo pipefail

# influx2tsdb-proxy Ansible 部署安装脚本
# 用法: 在已有 ansible 工程目录下执行
#   curl -sL https://raw.githubusercontent.com/mars-base/influx2tsdb-proxy/main/ansible/install.sh | bash -s /path/to/ansible-project
#
# 将 playbook 和 role 原封不动拷贝到目标 ansible 工程中

REPO="mars-base/influx2tsdb-proxy"
BRANCH="main"
TARGET_DIR="${1:-.}"

echo "==> 安装 influx2tsdb-proxy Ansible 部署配置"
echo "    目标目录: ${TARGET_DIR}"
echo ""

# 下载 ansible 目录中的 playbook 和 role 文件
BASE_URL="https://raw.githubusercontent.com/${REPO}/${BRANCH}/ansible"
FILES=(
  "playbooks/influx2tsdb-proxy.yml"
  "roles/influx2tsdb-proxy/defaults/main.yml"
  "roles/influx2tsdb-proxy/handlers/main.yml"
  "roles/influx2tsdb-proxy/meta/main.yml"
  "roles/influx2tsdb-proxy/tasks/main.yml"
  "roles/influx2tsdb-proxy/vars/main.yml"
  "roles/influx2tsdb-proxy/templates/supervisor/influx2tsdb-proxy.conf.j2"
)

for file in "${FILES[@]}"; do
  target="${TARGET_DIR}/${file}"
  mkdir -p "$(dirname "${target}")"
  echo "  安装: ${file}"
  curl -sL "${BASE_URL}/${file}" -o "${target}"
done

echo ""
echo "✓ 安装完成！"
echo ""
echo "使用方法："
echo "  cd ${TARGET_DIR}"
echo ""
echo "  # 执行 playbook（需提供实例配置）"
echo "  ansible-playbook -i hosts playbooks/influx2tsdb-proxy.yml"
echo ""
