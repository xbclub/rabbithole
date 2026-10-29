package cli

import (
	"fmt"

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
