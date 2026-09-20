#!/usr/bin/env bash
# 把 sub2api 的公开插件协议包 backend/pkg/pluginapi/v1 原样同步到 third_party/sub2api。
#
#   ./tools/sync-pluginapi.sh v0.2.7
#
# 上游把后端模块放在仓库的 backend/ 子目录里，模块路径却是 github.com/Wei-Shaw/sub2api，
# go get 拿不到；所以本仓库只收这一个包，并在 go.mod 里 replace 到 third_party/sub2api。
# 升级宿主协议后记得同步更新 manifest.source.json 的 requires.sub2api / tested_sub2api_versions。
set -euo pipefail

REF="${1:?用法: $0 <sub2api 的 tag、分支或提交，如 v0.2.7>}"
REPO="Wei-Shaw/sub2api"
SRC_DIR="backend/pkg/pluginapi/v1"
FILES=(manifest_schema_test.go manifest.schema.json plugin_grpc.pb.go plugin.pb.go plugin.proto runtime.go)

cd "$(dirname "$0")/.."
DEST="third_party/sub2api"
BASE="https://raw.githubusercontent.com/${REPO}/${REF}"

mkdir -p "$DEST/pkg/pluginapi/v1"
for file in "${FILES[@]}"; do
  echo "==> $SRC_DIR/$file"
  curl -fsSL "$BASE/$SRC_DIR/$file" -o "$DEST/pkg/pluginapi/v1/$file"
done
echo "==> LICENSE"
curl -fsSL "$BASE/LICENSE" -o "$DEST/LICENSE"

# 解析 REF 对应的提交：附注 tag 取 ^{} 剥离后的提交；找不到（REF 本身是提交哈希）就原样记录。
commit="$(git ls-remote "https://github.com/${REPO}" "refs/tags/${REF}" "refs/tags/${REF}^{}" "refs/heads/${REF}" \
  | awk '/\^\{\}$/ { peeled = $1 } !/\^\{\}$/ { plain = $1 } END { print (peeled != "" ? peeled : plain) }')"
[[ -n "$commit" ]] || commit="$REF"
printf 'repo: %s\nref: %s\ncommit: %s\npath: %s\n' "$REPO" "$REF" "$commit" "$SRC_DIR" > "$DEST/UPSTREAM"

echo "==> go mod tidy"
(cd "$DEST" && go mod tidy)
go mod tidy

echo "已同步 ${REPO}@${REF}（${commit}）到 ${DEST}"
