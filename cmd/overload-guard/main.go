// Command overload-guard 是 Overload Guard 插件的子进程入口。
//
// 宿主通过 hashicorp/go-plugin 启动本进程并用 gRPC 通信，所以这里只做两件事：
// 带上打包时注入的身份、把服务实现交给 pluginv1.Serve。
package main

import (
	"log"
	"os"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"

	"github.com/dextok/sub2api-plugin-overload-guard/internal/runtime"
)

// pluginID / pluginVersion 由打包器通过 -ldflags 从 manifest.source.json 注入。
// 它们必须与安装包清单完全一致：宿主会在启动后逐字比对 GetInfo 的返回值，
// 不一致就直接判定「插件运行时信息与已校验清单不一致」并杀掉进程。
var (
	pluginID      = "sub2api.plugin.overload-guard"
	pluginVersion = "0.0.0-dev"
)

func main() {
	// 日志只能写 stderr：stdout 被 go-plugin 用来做握手。
	logger := log.New(os.Stderr, "[overload-guard] ", log.LstdFlags|log.LUTC)
	server := runtime.New(runtime.Identity{
		PluginID:      pluginID,
		PluginVersion: pluginVersion,
	}, logger.Printf)
	defer server.Close()

	pluginv1.Serve(server)
}
