# third_party/sub2api

这里是 sub2api 公开插件协议包 `backend/pkg/pluginapi/v1` 的一份**原样拷贝**。上游把它定义为
插件开发者可以依赖的公开契约，但上游仓库把整个后端放在 `backend/` 子目录下、模块路径却是
`github.com/Wei-Shaw/sub2api`，`go get` 拿不到。所以本仓库只收这一个包，并在根 `go.mod` 里
`replace github.com/Wei-Shaw/sub2api => ./third_party/sub2api`，克隆下来就能直接构建。

- 来源仓库、tag 与提交记录在 `UPSTREAM`。
- `LICENSE` 随上游（LGPL-3.0），只对本目录里的拷贝生效。
- 除 `go.mod` / `go.sum` / `README.md` / `UPSTREAM` 之外，不要手改这里的文件。
- 升级宿主协议：`./tools/sync-pluginapi.sh <tag>`，然后同步更新 `manifest.source.json` 的
  `requires.sub2api` / `tested_sub2api_versions`，并跑一遍 `./build.sh`。
