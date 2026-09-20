// Command packager 把插件交叉编译、组装成 .s2plugin 包并用 Ed25519 签名。
//
// 关键约束来自宿主的安装校验（backend/internal/service/plugin_package.go）：
//   - 包内必须有 manifest.json，且它只能包含清单已定义的字段（宿主用
//     DisallowUnknownFields 解析）；
//   - manifest.json 的**原始字节**就是签名对象，因此它一旦生成就不能再改一个字节；
//   - manifest.files 必须与包内条目（除 manifest.json / signature.json）一一对应，
//     哈希是小写十六进制 SHA-256；
//   - 路径必须是正斜杠相对路径、不能有 ../、不能有符号链接、条目总数 ≤ 512；
//   - 当前运行平台必须在 manifest.runtimes 里，否则宿主判定「不支持当前运行平台」。
//
// 打包完成后会重新打开产物做一次完整自校验，把上面每条都再验一遍。
package main

import (
	"archive/zip"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	manifestFilename  = "manifest.json"
	signatureFilename = "signature.json"
	maxArchiveFiles   = 512
	binaryName        = "sub2api-state-guard"
)

var pluginIDPattern = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)+$`)

// manifestVersionPattern 与宿主 manifest.schema.json 里 version 的 pattern 逐字一致：
// 自校验要能提前拦下宿主上传时会拒绝的版本号（如 0.7.01、1.0.0-）。
var manifestVersionPattern = regexp.MustCompile(
	`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)

// resolvePath 把命令行里的路径解析到模块目录下；绝对路径原样使用。
func resolvePath(moduleDir, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(moduleDir, path)
}

type requirements struct {
	Sub2API                   string   `json:"sub2api"`
	RecommendedSub2APIVersion string   `json:"recommended_sub2api_version,omitempty"`
	TestedSub2APIVersions     []string `json:"tested_sub2api_versions,omitempty"`
	PluginProtocol            int      `json:"plugin_protocol"`
	TransportAPI              int      `json:"transport_api"`
	UIBridge                  int      `json:"ui_bridge"`
}

type capability struct {
	ID          string `json:"id"`
	Platform    string `json:"platform"`
	AccountType string `json:"account_type"`
}

type runtimeEntry struct {
	Path string `json:"path"`
}

type uiManifest struct {
	Entrypoint string `json:"entrypoint"`
}

type manifest struct {
	SchemaVersion int                     `json:"schema_version"`
	ID            string                  `json:"id"`
	Name          string                  `json:"name"`
	Version       string                  `json:"version"`
	Description   string                  `json:"description,omitempty"`
	Author        string                  `json:"author,omitempty"`
	Requires      requirements            `json:"requires"`
	Capabilities  []capability            `json:"capabilities"`
	Runtimes      map[string]runtimeEntry `json:"runtimes"`
	UI            uiManifest              `json:"ui"`
	Files         map[string]string       `json:"files"`
}

type signatureDocument struct {
	Algorithm string `json:"algorithm"`
	KeyID     string `json:"key_id"`
	Signature string `json:"signature"`
}

type target struct {
	GOOS   string
	GOARCH string
}

func (t target) key() string { return t.GOOS + "-" + t.GOARCH }

func (t target) archivePath() string {
	name := binaryName
	if t.GOOS == "windows" {
		name += ".exe"
	}
	return "runtimes/" + t.key() + "/" + name
}

func main() {
	options := struct {
		manifestPath string
		moduleDir    string
		output       string
		targets      string
		signingKey   string
		keyID        string
		buildDir     string
		skipBuild    bool
	}{}

	flag.StringVar(&options.manifestPath, "manifest", "manifest.source.json", "清单源文件")
	flag.StringVar(&options.moduleDir, "module", ".", "插件模块目录（含 cmd/sub2api-state-guard 与 ui/）")
	flag.StringVar(&options.output, "output", "dist/sub2api-state-guard.s2plugin", "产物路径")
	flag.StringVar(&options.targets, "targets",
		"linux-amd64,linux-arm64,darwin-arm64,darwin-amd64,windows-amd64", "交叉编译目标，逗号分隔")
	flag.StringVar(&options.signingKey, "signing-key", "", "Ed25519 私钥文件（keygen 生成的 .private）")
	flag.StringVar(&options.keyID, "key-id", "", "签名密钥 ID，需与 plugins.trusted_publishers 的键一致")
	flag.StringVar(&options.buildDir, "build-dir", "build/runtimes", "交叉编译中间产物目录")
	flag.BoolVar(&options.skipBuild, "skip-build", false, "跳过编译，复用 build-dir 里已有的二进制")
	flag.Parse()

	err := build(options.manifestPath, options.moduleDir, options.output, options.targets,
		options.signingKey, options.keyID, options.buildDir, options.skipBuild)
	if err != nil {
		fmt.Fprintln(os.Stderr, "打包失败:", err)
		os.Exit(1)
	}
}

func build(manifestPath, moduleDir, output, targetList, signingKey, keyID, buildDir string, skipBuild bool) error {
	document, err := loadManifest(resolvePath(moduleDir, manifestPath))
	if err != nil {
		return err
	}
	targets, err := parseTargets(targetList)
	if err != nil {
		return err
	}

	// archive 是「包内路径 → 本地文件」的完整清单，后面的哈希、zip、自校验都以它为准。
	archive := make(map[string]string, 16)

	for _, item := range targets {
		binaryPath := filepath.Join(resolvePath(moduleDir, buildDir), item.key(), binaryFileName(item))
		if !skipBuild {
			if err := compile(moduleDir, item, binaryPath, document); err != nil {
				return err
			}
		}
		if _, err := os.Stat(binaryPath); err != nil {
			return fmt.Errorf("缺少 %s 的二进制 %s: %w", item.key(), binaryPath, err)
		}
		archive[item.archivePath()] = binaryPath
		document.Runtimes[item.key()] = runtimeEntry{Path: item.archivePath()}
	}

	if err := collectUIAssets(moduleDir, archive); err != nil {
		return err
	}

	document.Files = make(map[string]string, len(archive))
	for archivePath, localPath := range archive {
		digest, err := hashFile(localPath)
		if err != nil {
			return err
		}
		document.Files[archivePath] = digest
	}

	if err := validateManifest(document); err != nil {
		return err
	}
	if len(archive)+2 > maxArchiveFiles {
		return fmt.Errorf("包内条目 %d 个，超过宿主上限 %d", len(archive)+2, maxArchiveFiles)
	}

	// 从这里开始 manifestRaw 就是签名对象，不能再改动 document。
	manifestRaw, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化清单: %w", err)
	}
	manifestRaw = append(manifestRaw, '\n')

	var signatureRaw []byte
	var publicKey ed25519.PublicKey
	if signingKey != "" {
		if keyID == "" {
			return errors.New("提供 -signing-key 时必须同时提供 -key-id")
		}
		privateKey, err := loadPrivateKey(signingKey)
		if err != nil {
			return err
		}
		publicKey = privateKey.Public().(ed25519.PublicKey)
		signatureRaw, err = json.MarshalIndent(signatureDocument{
			Algorithm: "ed25519",
			KeyID:     keyID,
			Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, manifestRaw)),
		}, "", "  ")
		if err != nil {
			return fmt.Errorf("序列化签名: %w", err)
		}
		signatureRaw = append(signatureRaw, '\n')
	} else {
		fmt.Fprintln(os.Stderr,
			"警告: 未提供 -signing-key，产物没有 signature.json，只能装在 plugins.allow_unsigned=true 的开发环境")
	}

	outputPath := resolvePath(moduleDir, output)
	if err := writeArchive(outputPath, manifestRaw, signatureRaw, archive); err != nil {
		return err
	}
	if err := verifyArchive(outputPath, manifestRaw, signatureRaw, publicKey); err != nil {
		return fmt.Errorf("产物自校验失败: %w", err)
	}

	info, err := os.Stat(outputPath)
	if err != nil {
		return err
	}
	fmt.Printf("已生成 %s（%.2f MB，%d 个平台，%d 个文件）\n",
		outputPath, float64(info.Size())/(1024*1024), len(document.Runtimes), len(document.Files))
	if publicKey != nil {
		fmt.Printf("签名: ed25519 / key_id=%s\n", keyID)
		fmt.Printf("宿主需配置 plugins.trusted_publishers.%s = %q\n",
			keyID, base64.StdEncoding.EncodeToString(publicKey))
	}
	fmt.Println("自校验通过：清单字节、签名、文件哈希、路径与条目数量全部一致")
	return nil
}

func binaryFileName(item target) string {
	if item.GOOS == "windows" {
		return binaryName + ".exe"
	}
	return binaryName
}

func loadManifest(path string) (*manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取清单源文件: %w", err)
	}
	var document manifest
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("解析清单源文件: %w", err)
	}
	if document.Runtimes == nil {
		document.Runtimes = map[string]runtimeEntry{}
	}
	return &document, nil
}

func parseTargets(list string) ([]target, error) {
	seen := make(map[string]struct{})
	out := make([]target, 0, 5)
	for _, item := range strings.Split(list, ",") {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		parts := strings.Split(trimmed, "-")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("目标 %q 格式应为 goos-goarch", trimmed)
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, target{GOOS: parts[0], GOARCH: parts[1]})
	}
	if len(out) == 0 {
		return nil, errors.New("至少需要一个编译目标")
	}
	return out, nil
}

func compile(moduleDir string, item target, outputPath string, document *manifest) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("创建编译输出目录: %w", err)
	}
	// 身份必须与清单逐字一致：宿主启动后会比对 GetInfo 的返回值。
	ldflags := fmt.Sprintf("-s -w -X main.pluginID=%s -X main.pluginVersion=%s", document.ID, document.Version)
	absolute, err := filepath.Abs(outputPath)
	if err != nil {
		return err
	}
	command := exec.Command("go", "build", "-trimpath", "-ldflags", ldflags, "-o", absolute, "./cmd/"+binaryName)
	command.Dir = moduleDir
	command.Env = append(os.Environ(),
		"GOOS="+item.GOOS,
		"GOARCH="+item.GOARCH,
		"CGO_ENABLED=0",
	)
	command.Stdout = os.Stderr
	command.Stderr = os.Stderr
	fmt.Fprintf(os.Stderr, "编译 %s…\n", item.key())
	if err := command.Run(); err != nil {
		return fmt.Errorf("编译 %s 失败: %w", item.key(), err)
	}
	return nil
}

func collectUIAssets(moduleDir string, archive map[string]string) error {
	root := filepath.Join(moduleDir, "ui")
	return filepath.WalkDir(root, func(localPath string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			// .DS_Store 这类系统垃圾文件不进包：宿主会因「未声明文件」或多余条目而拒绝。
			fmt.Fprintf(os.Stderr, "跳过 %s\n", localPath)
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("ui 目录只允许普通文件: %s", localPath)
		}
		relative, err := filepath.Rel(root, localPath)
		if err != nil {
			return err
		}
		archivePath := "ui/" + filepath.ToSlash(relative)
		if !safeArchivePath(archivePath) {
			return fmt.Errorf("不安全的包内路径: %s", archivePath)
		}
		archive[archivePath] = localPath
		return nil
	})
}

func safeArchivePath(name string) bool {
	if name == "" || strings.HasPrefix(name, "/") || strings.Contains(name, "\x00") || strings.Contains(name, "\\") {
		return false
	}
	cleaned := path.Clean(name)
	return cleaned == name && cleaned != "." && cleaned != ".." && !strings.HasPrefix(cleaned, "../")
}

func validateManifest(document *manifest) error {
	if document.SchemaVersion != 1 {
		return fmt.Errorf("schema_version 必须是 1，当前 %d", document.SchemaVersion)
	}
	if !pluginIDPattern.MatchString(document.ID) || len(document.ID) > 160 {
		return fmt.Errorf("插件 ID %q 不符合宿主要求的小写命名空间格式", document.ID)
	}
	if strings.TrimSpace(document.Name) == "" || len(document.Name) > 160 {
		return errors.New("插件名称不能为空且不超过 160 字符")
	}
	if !manifestVersionPattern.MatchString(document.Version) {
		return fmt.Errorf("版本 %q 不是宿主清单接受的语义化版本", document.Version)
	}
	if strings.TrimSpace(document.Requires.Sub2API) == "" {
		return errors.New("requires.sub2api 不能为空")
	}
	if document.Requires.PluginProtocol != 1 || document.Requires.TransportAPI != 1 || document.Requires.UIBridge != 1 {
		return errors.New("requires 里的协议版本目前必须都是 1")
	}
	if len(document.Capabilities) == 0 {
		return errors.New("必须声明至少一个能力")
	}
	for _, item := range document.Capabilities {
		if item.ID != "openai.oauth.outbound_transport.v1" || item.Platform != "openai" || item.AccountType != "oauth" {
			return fmt.Errorf("宿主目前只接受能力 openai.oauth.outbound_transport.v1，当前 %q", item.ID)
		}
	}
	if len(document.Runtimes) == 0 {
		return errors.New("runtimes 为空")
	}
	for key, entry := range document.Runtimes {
		if !safeArchivePath(entry.Path) {
			return fmt.Errorf("runtimes[%s].path 不安全: %s", key, entry.Path)
		}
		if _, declared := document.Files[entry.Path]; !declared {
			return fmt.Errorf("runtimes[%s].path 未出现在 files 中: %s", key, entry.Path)
		}
	}
	if !strings.HasPrefix(document.UI.Entrypoint, "ui/") || !safeArchivePath(document.UI.Entrypoint) {
		return fmt.Errorf("ui.entrypoint 必须位于 ui/ 目录，当前 %q", document.UI.Entrypoint)
	}
	if _, declared := document.Files[document.UI.Entrypoint]; !declared {
		return fmt.Errorf("ui.entrypoint 未出现在 files 中: %s", document.UI.Entrypoint)
	}
	if len(document.Files) == 0 {
		return errors.New("files 为空")
	}
	hashPattern := regexp.MustCompile(`^[a-f0-9]{64}$`)
	for archivePath, digest := range document.Files {
		if !safeArchivePath(archivePath) {
			return fmt.Errorf("files 中包含不安全路径: %s", archivePath)
		}
		if archivePath == manifestFilename || archivePath == signatureFilename {
			return fmt.Errorf("files 不应声明 %s", archivePath)
		}
		if !hashPattern.MatchString(digest) {
			return fmt.Errorf("files[%s] 的哈希格式无效", archivePath)
		}
	}
	return nil
}

func loadPrivateKey(path string) (ed25519.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取私钥: %w", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("私钥不是 Base64: %w", err)
	}
	if len(decoded) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("私钥长度应为 %d 字节，当前 %d", ed25519.PrivateKeySize, len(decoded))
	}
	return ed25519.PrivateKey(decoded), nil
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("打开 %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("读取 %s: %w", path, err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func writeArchive(outputPath string, manifestRaw, signatureRaw []byte, archive map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("创建产物目录: %w", err)
	}
	temporary := outputPath + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("创建产物: %w", err)
	}
	committed := false
	defer func() {
		_ = file.Close()
		if !committed {
			_ = os.Remove(temporary)
		}
	}()

	writer := zip.NewWriter(file)
	if err := writeArchiveEntry(writer, manifestFilename, manifestRaw); err != nil {
		return err
	}
	if signatureRaw != nil {
		if err := writeArchiveEntry(writer, signatureFilename, signatureRaw); err != nil {
			return err
		}
	}
	paths := make([]string, 0, len(archive))
	for archivePath := range archive {
		paths = append(paths, archivePath)
	}
	sort.Strings(paths)
	for _, archivePath := range paths {
		payload, err := os.ReadFile(archive[archivePath])
		if err != nil {
			return fmt.Errorf("读取 %s: %w", archive[archivePath], err)
		}
		if err := writeArchiveEntry(writer, archivePath, payload); err != nil {
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("收尾 ZIP: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("同步产物: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("关闭产物: %w", err)
	}
	if err := os.Rename(temporary, outputPath); err != nil {
		return fmt.Errorf("提交产物: %w", err)
	}
	committed = true
	return nil
}

func writeArchiveEntry(writer *zip.Writer, name string, payload []byte) error {
	// 只用 Create：不写目录条目、不带外部属性，天然不会出现符号链接位。
	entry, err := writer.Create(name)
	if err != nil {
		return fmt.Errorf("写入条目 %s: %w", name, err)
	}
	if _, err := entry.Write(payload); err != nil {
		return fmt.Errorf("写入条目 %s: %w", name, err)
	}
	return nil
}

// verifyArchive 重开产物，按宿主 inspectArchive/extractArchive 的顺序把校验跑一遍。
func verifyArchive(outputPath string, manifestRaw, signatureRaw []byte, publicKey ed25519.PublicKey) error {
	reader, err := zip.OpenReader(outputPath)
	if err != nil {
		return fmt.Errorf("打开产物: %w", err)
	}
	defer func() { _ = reader.Close() }()

	if len(reader.File) == 0 || len(reader.File) > maxArchiveFiles {
		return fmt.Errorf("条目数量 %d 无效", len(reader.File))
	}
	entries := make(map[string][]byte, len(reader.File))
	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			return fmt.Errorf("不应包含目录条目: %s", file.Name)
		}
		if file.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("不允许符号链接: %s", file.Name)
		}
		if !safeArchivePath(file.Name) {
			return fmt.Errorf("不安全路径: %s", file.Name)
		}
		if _, exists := entries[file.Name]; exists {
			return fmt.Errorf("重复路径: %s", file.Name)
		}
		opened, err := file.Open()
		if err != nil {
			return fmt.Errorf("读取 %s: %w", file.Name, err)
		}
		payload, err := io.ReadAll(opened)
		closeErr := opened.Close()
		if err != nil || closeErr != nil {
			return fmt.Errorf("读取 %s 失败", file.Name)
		}
		entries[file.Name] = payload
	}

	packed, ok := entries[manifestFilename]
	if !ok {
		return errors.New("缺少 manifest.json")
	}
	if string(packed) != string(manifestRaw) {
		return errors.New("包内 manifest.json 与签名对象字节不一致")
	}

	var document manifest
	decoder := json.NewDecoder(strings.NewReader(string(packed)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return fmt.Errorf("宿主将无法解析 manifest.json: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("manifest.json 只能包含一个 JSON 对象")
	}
	if err := validateManifest(&document); err != nil {
		return err
	}

	if signatureRaw != nil {
		packedSignature, ok := entries[signatureFilename]
		if !ok {
			return errors.New("缺少 signature.json")
		}
		var signature signatureDocument
		if err := json.Unmarshal(packedSignature, &signature); err != nil {
			return fmt.Errorf("解析 signature.json: %w", err)
		}
		if signature.Algorithm != "ed25519" || strings.TrimSpace(signature.KeyID) == "" {
			return errors.New("签名算法或 key_id 无效")
		}
		raw, err := base64.StdEncoding.DecodeString(signature.Signature)
		if err != nil {
			return fmt.Errorf("签名不是 Base64: %w", err)
		}
		if publicKey == nil || !ed25519.Verify(publicKey, packed, raw) {
			return errors.New("签名无法用打包时的公钥验证")
		}
	} else if _, ok := entries[signatureFilename]; ok {
		return errors.New("未签名却出现了 signature.json")
	}

	for name := range entries {
		if name == manifestFilename || name == signatureFilename {
			continue
		}
		if _, declared := document.Files[name]; !declared {
			return fmt.Errorf("包含未声明文件: %s", name)
		}
	}
	for name, expected := range document.Files {
		payload, ok := entries[name]
		if !ok {
			return fmt.Errorf("缺少已声明文件: %s", name)
		}
		digest := sha256.Sum256(payload)
		if hex.EncodeToString(digest[:]) != expected {
			return fmt.Errorf("文件哈希不匹配: %s", name)
		}
	}
	return nil
}
