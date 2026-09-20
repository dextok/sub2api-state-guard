#!/usr/bin/env bash
# 构建并打包 Overload Guard 插件。
#
#   ./build.sh -signing-key build/keys/dev-publisher.private -key-id overload-guard-v1
#
# 步骤：go vet → go test → UI 脚本语法检查 → 交叉编译 + 组装 + 签名 + 自校验。
set -euo pipefail

cd "$(dirname "$0")"

SIGNING_KEY=""
KEY_ID=""
OUTPUT="dist/overload-guard.s2plugin"
TARGETS="linux-amd64,linux-arm64,darwin-arm64,darwin-amd64,windows-amd64"
SKIP_TESTS=0

usage() {
  cat <<'EOF'
用法: ./build.sh [选项]

  -signing-key <path>  Ed25519 私钥（tools/keygen 生成的 .private）。
                       不提供则产出未签名包，只能装在 plugins.allow_unsigned=true 的环境。
  -key-id <id>         签名密钥 ID，需与宿主 plugins.trusted_publishers 的键一致。
  -output <path>       产物路径，默认 dist/overload-guard.s2plugin
  -targets <list>      交叉编译目标，逗号分隔，默认五个主流平台。
  -skip-tests          跳过 go vet / go test / UI 语法检查（仅用于反复调试打包）。
  -h, --help           显示本帮助。
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -signing-key) SIGNING_KEY="${2:?-signing-key 需要一个路径}"; shift 2 ;;
    -key-id) KEY_ID="${2:?-key-id 需要一个值}"; shift 2 ;;
    -output) OUTPUT="${2:?-output 需要一个路径}"; shift 2 ;;
    -targets) TARGETS="${2:?-targets 需要一个列表}"; shift 2 ;;
    -skip-tests) SKIP_TESTS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -n "$SIGNING_KEY" && -z "$KEY_ID" ]]; then
  echo "提供 -signing-key 时必须同时提供 -key-id" >&2
  exit 2
fi

if [[ "$SKIP_TESTS" -eq 0 ]]; then
  echo "==> go vet"
  go vet ./...

  echo "==> go test"
  go test ./... -count=1

  if command -v node >/dev/null 2>&1; then
    echo "==> UI 脚本语法检查"
    for script in ui/assets/*.js; do
      node --check "$script"
    done
  else
    echo "跳过 UI 脚本语法检查：未找到 node" >&2
  fi
fi

echo "==> 打包"
ARGS=(-output "$OUTPUT" -targets "$TARGETS")
if [[ -n "$SIGNING_KEY" ]]; then
  ARGS+=(-signing-key "$SIGNING_KEY" -key-id "$KEY_ID")
fi
go run ./tools/packager "${ARGS[@]}"
