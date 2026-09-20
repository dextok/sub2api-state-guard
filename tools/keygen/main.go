// Command keygen 生成插件发布者的 Ed25519 密钥对。
//
// 私钥只用于本地/CI 的签名步骤，绝不能进包、进仓库、进部署环境；
// 公钥要配到 sub2api 的 plugins.trusted_publishers 里，否则安装时会被
// 「插件发布者密钥不受信任」直接拒绝（第三方插件 ID 不享受内置信任根）。
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	out := flag.String("out", "build/keys/publisher", "密钥输出前缀，会生成 <prefix>.private 与 <prefix>.public")
	keyID := flag.String("key-id", "overload-guard-v1", "密钥 ID：作为宿主 plugins.trusted_publishers 的键；打包时把同一个 ID 传给 packager 的 -key-id，它才会写进 signature.json")
	force := flag.Bool("force", false, "允许覆盖已存在的私钥")
	flag.Parse()

	if err := run(*out, *keyID, *force); err != nil {
		fmt.Fprintln(os.Stderr, "keygen 失败:", err)
		os.Exit(1)
	}
}

func run(prefix, keyID string, force bool) error {
	if keyID == "" {
		return errors.New("-key-id 不能为空")
	}
	privatePath := prefix + ".private"
	publicPath := prefix + ".public"

	if !force {
		if _, err := os.Stat(privatePath); err == nil {
			return fmt.Errorf("%s 已存在，加 -force 才会覆盖", privatePath)
		}
	}
	if err := os.MkdirAll(filepath.Dir(privatePath), 0o700); err != nil {
		return fmt.Errorf("创建密钥目录: %w", err)
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("生成密钥: %w", err)
	}
	encodedPublic := base64.StdEncoding.EncodeToString(publicKey)

	// 0600：私钥只有当前用户可读。
	if err := os.WriteFile(privatePath, []byte(base64.StdEncoding.EncodeToString(privateKey)+"\n"), 0o600); err != nil {
		return fmt.Errorf("写入私钥: %w", err)
	}
	if err := os.WriteFile(publicPath, []byte(encodedPublic+"\n"), 0o600); err != nil {
		return fmt.Errorf("写入公钥: %w", err)
	}

	fmt.Printf("私钥: %s（0600，请勿提交到仓库）\n", privatePath)
	fmt.Printf("公钥: %s\n\n", publicPath)
	fmt.Println("把下面片段合并进 sub2api 的配置文件后重启宿主：")
	fmt.Println("plugins:")
	fmt.Println("  trusted_publishers:")
	fmt.Printf("    %s: \"%s\"\n", keyID, encodedPublic)
	return nil
}
