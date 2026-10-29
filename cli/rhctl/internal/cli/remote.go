package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

// requireKey 是远端命令的第一道闸(§2.2):key 缺失 → 本地错误(exit 2),
// stderr 列出三种来源写法,不发起任何 HTTP。
func requireKey(o Options) error {
	if o.Key != "" {
		return nil
	}
	return fmt.Errorf(`缺少 API Key(未发起请求)。三种来源任选其一:
  --key rhk_…                          一次性覆盖(注意:会进 shell history 与 ps)
  export RH_API_KEY=rhk_…              CI / secret manager 注入
  rhctl profile add <名> --server <url> --key <key>
                                       落盘 profile(~/.config/rhctl/profiles.json),日常默认`)
}

// newAPIClient 用解析后的 Options 构造 API 客户端。
func newAPIClient(o Options) *api.Client {
	return api.NewClient(o.Server, o.Key, o.Timeout, o.Insecure)
}

// withClient 是远端命令的统一骨架:凭据闸 → 客户端 → f。
func withClient(f func(*api.Client) error) error {
	if err := requireKey(opts); err != nil {
		return err
	}
	return f(newAPIClient(opts))
}

// output 按 -o 分派:json 直出信封 data 原样(§2.4),否则走 render
// (table 渲染,由命令闭包解析 raw)。
func output(raw json.RawMessage, render func() error) error {
	if opts.Output == "json" {
		return PrintJSON(os.Stdout, raw)
	}
	return render()
}

// warnf 把诊断提示打到 stderr —— 成功的输出不得污染 stdout(§2.4)。
func warnf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
}

// secretHint 是一次性 secret 的 stderr 警示(§2.6)。
func secretHint() {
	warnf("⚠ secret 仅本次返回(服务端只存哈希,无法找回),请立即保存 —— 关闭即失。")
}
