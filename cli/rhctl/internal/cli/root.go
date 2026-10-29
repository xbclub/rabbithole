// Package cli 承载 rhctl 命令树:根命令、persistent flags(§2.2)与
// flag > 环境变量 > profile(包 I)> 默认 的取值链。
package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

// Version 默认 dev;正式版本经 -ldflags "-X …=vX.Y.Z" 注入。
// 发版流水线集成不在本期(任务书 §7 第 2 条)。
var Version = "dev"

// 环境变量名(§2.2)。
const (
	envServer  = "RH_SERVER"
	envAPIKey  = "RH_API_KEY"
	envProfile = "RH_PROFILE"
)

// 默认值(§2.2)。server 默认值即服务端 WebConfig::default() 的口。
const (
	defaultServer  = "http://127.0.0.1:8080"
	defaultOutput  = "table"
	defaultTimeout = 30 * time.Second
)

// Options 是一次调用解析后的连接参数,由 PersistentPreRunE 填充,
// 子命令读取包级 opts。server 与 key 各自独立走取值链 ——
// 「prod profile 的 server + --key 临时凭据」是合法组合(§2.2)。
type Options struct {
	Server   string
	Key      string
	Profile  string // 实际生效的 profile 规范名;未用 profile 为空
	Insecure bool
	Timeout  time.Duration
	Output   string
}

// opts 保存本次调用的解析结果。
var opts Options

// flag 绑定变量。默认值一律留空、是否显式给出用 FlagSet.Changed 判断:
// 注册期填默认值会让 cobra 报上来的值无法区分「用户显式给出」与
// 「默认填充」,flag > env > 默认 的取值链就断了。
var (
	flagServer   string
	flagKey      string
	flagProfile  string
	flagOutput   string
	flagTimeout  string
	flagInsecure bool
)

func newRootCmd() *cobra.Command {
	// flag 变量是包级的,每次构造重置,隔离多次构造(测试)间的状态。
	flagServer, flagKey, flagProfile, flagOutput, flagTimeout = "", "", "", "", ""
	flagInsecure = false

	root := &cobra.Command{
		Use:   "rhctl",
		Short: "rabbithole 管理端 CLI(一把 API Key 管全部资源)",
		Long: `rhctl 是 rabbithole 管理 REST API 的命令行控制端:
一把 API Key 完成 clients / services / visitors / stats / events / config / certs
七组管理操作。

凭据取值链:--server/--key > RH_SERVER/RH_API_KEY > profile(见 rhctl profile)> 默认。`,
		Version: Version,
		// 无参调用打印帮助。也让 help 模板带上 usage/flags 段 ——
		// cobra 对「不可运行且无子命令」的命令不输出 usage,包 A 阶段
		// 还没有子命令,--help 会看不到 persistent flags。
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// 到这里 flag 与位置参数解析均已成功;之后的错误属于运行期,
			// 不再附带 usage(flag/参数错误仍由 cobra 附 usage)。
			cmd.SilenceUsage = true
			resolved, err := resolveOptions(cmd)
			if err != nil {
				return err
			}
			opts = resolved
			return nil
		},
	}
	root.SetVersionTemplate("{{.Version}}\n")

	pf := root.PersistentFlags()
	pf.StringVarP(&flagServer, "server", "s", "",
		fmt.Sprintf("管理端地址,URL 或 profile 别名(默认 %s) [%s]", defaultServer, envServer))
	pf.StringVarP(&flagKey, "key", "k", "",
		fmt.Sprintf("API Key(rhk_…) [%s]", envAPIKey))
	pf.StringVar(&flagProfile, "profile", "",
		fmt.Sprintf("profile 名或别名 [%s]", envProfile))
	pf.StringVarP(&flagOutput, "output", "o", "",
		"输出格式 table|json(默认 table)")
	pf.StringVar(&flagTimeout, "timeout", "",
		"请求超时,如 30s、2m(默认 30s)")
	pf.BoolVar(&flagInsecure, "insecure", false,
		"跳过 TLS 证书校验(自签场景)")
	return root
}

// resolveOptions 按取值链解析参数。包 A 实现 flag > env > 默认;
// 包 I 在 env 与默认之间并入 profile 一级(任务书 A-3)。
func resolveOptions(cmd *cobra.Command) (Options, error) {
	o := Options{
		Server:  defaultServer,
		Output:  defaultOutput,
		Timeout: defaultTimeout,
	}

	// Changed 查根命令的 persistent FlagSet:解析期 merge 进子命令的
	// 是同一批 *Flag 指针,Changed 状态在两侧互通;不经 ParseFlags
	// 直接构造(测试)也能查到。
	flags := cmd.Root().PersistentFlags()
	if flags.Changed("server") {
		o.Server = flagServer
	} else if v := os.Getenv(envServer); v != "" {
		o.Server = v
	}

	if flags.Changed("key") {
		o.Key = flagKey
	} else if v := os.Getenv(envAPIKey); v != "" {
		o.Key = v
	}

	if flags.Changed("profile") {
		o.Profile = flagProfile
	} else if v := os.Getenv(envProfile); v != "" {
		o.Profile = v
	}

	if flags.Changed("output") && flagOutput != "" {
		o.Output = flagOutput
	}
	if o.Output != "table" && o.Output != "json" {
		return o, fmt.Errorf("--output 无效:%q(可用 table|json)", o.Output)
	}

	if flags.Changed("timeout") && flagTimeout != "" {
		d, err := time.ParseDuration(flagTimeout)
		if err != nil || d <= 0 {
			return o, fmt.Errorf("--timeout 无效:%q(如 30s、2m)", flagTimeout)
		}
		o.Timeout = d
	}

	o.Insecure = flagInsecure
	return o, nil
}

// Execute 运行 CLI 并返回退出码(§2.4):0 成功;1 远端错误(业务
// code≠0 / 401 / 403 / 网络 / 非信封响应);2 本地错误(用法、文件读不到、
// key 缺失)。错误文本由 cobra 打到 stderr。
func Execute() int {
	root := newRootCmd()
	if err := root.Execute(); err != nil {
		if api.IsRemote(err) {
			return 1
		}
		return 2
	}
	return 0
}
