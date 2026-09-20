module github.com/dextok/sub2api-plugin-overload-guard

go 1.27.0

require (
	github.com/Wei-Shaw/sub2api v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.58.0
	google.golang.org/grpc v1.83.2
)

require (
	github.com/fatih/color v1.18.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-plugin v1.8.0 // indirect
	github.com/hashicorp/yamux v0.1.2 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/oklog/run v1.1.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

// 插件只依赖宿主的公开协议包 github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1。
// 上游把后端模块放在仓库的 backend/ 子目录里，go get 拿不到，所以把该包原样收在
// third_party/sub2api（来源见其中的 UPSTREAM），用 tools/sync-pluginapi.sh 同步升级。
replace github.com/Wei-Shaw/sub2api => ./third_party/sub2api
