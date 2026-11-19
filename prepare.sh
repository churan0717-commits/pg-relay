#!/usr/bin/env bash
# 把生产源码同步进本部署包的 app/（Cloady 上传/构建上下文）。
# 只带运行必需文件：src + vendor + vendor-script + package*.json。
# 配置（config.json / headless.json / .env）一律由 start.sh 按环境变量在启动时生成，
# 不进部署包、不进镜像。
set -eu
cd "$(dirname "$0")"
SRC=".."   # pg-relay 根目录
rm -rf app
mkdir -p app
cp -r "$SRC/src" "$SRC/vendor" "$SRC/vendor-script" app/
cp "$SRC/package.json" "$SRC/package-lock.json" app/
echo "app/ 已同步: $(du -sh app | cut -f1), 文件数 $(find app -type f | wc -l)"
