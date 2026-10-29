// Package cli 承载 rhctl 命令树:根命令、persistent flags(§2.2)与
// flag > 环境变量 > profile(包 I)> 默认 的取值链。
package cli

import (
	"fmt"
	"os"
	"strings"
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

// groupFactories 收集各命令组的构造函数(init 期注册),newRootCmd 时
// 逐个构建挂载。注册工厂而非实例:实例上的 FlagSet.Changed 会跨
// newRootCmd 调用残留,测试(同进程多次建树)会互相污染。
var groupFactories []func() *cobra.Command

func registerGroup(f func() *cobra.Command) {
	groupFactories = append(groupFactories, f)
}

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

	for _, f := range groupFactories {
		root.AddCommand(f())
	}
	return root
}

// resolveOptions 按取值链解析(§2.2):flag > env > profile(包 I)> 默认。
// server 与 key 各自独立取最高优先级;唯一例外:--server <名/别名>
// 生效整个 profile(临时切机,server+key+insecure 一并生效,
// --key/--insecure 仍可再覆盖)。
func resolveOptions(cmd *cobra.Command) (Options, error) {
	o := Options{
		Server:  defaultServer,
		Output:  defaultOutput,
		Timeout: defaultTimeout,
	}

	flags := cmd.Root().PersistentFlags()

	// profile 层:文件加载/校验失败 → 本地错误 exit 2
	// (拼错的键、命名空间冲突、current 非规范名都不静默)。
	cfg, err := loadProfiles()
	if err != nil {
		return o, err
	}

	// profile 选择:--profile > RH_PROFILE > 配置文件 current。
	selectedName := ""
	if flags.Changed("profile") {
		selectedName = flagProfile
	} else if v := os.Getenv(envProfile); v != "" {
		selectedName = v
	} else {
		selectedName = cfg.Current
	}
	var effective *Profile
	if selectedName != "" {
		canonical, p, ok := cfg.resolveName(selectedName)
		if !ok {
			return o, fmt.Errorf("profile %q 不存在。可用:%s", selectedName, strings.Join(cfg.names(), " "))
		}
		selectedName, effective = canonical, p
	}

	// server:--server/RH_SERVER > profile > 默认。值带 http(s):// 前缀
	// → 纯 URL;不带 → 按名/别名解析为整个 profile(别名访问,§2.2)。
	serverGiven := ""
	if flags.Changed("server") {
		serverGiven = flagServer
	} else if v := os.Getenv(envServer); v != "" {
		serverGiven = v
	}
	switch {
	case isURL(serverGiven):
		o.Server = serverGiven
	case serverGiven != "":
		canonical, p, ok := cfg.resolveName(serverGiven)
		if !ok {
			return o, fmt.Errorf("--server %q 解析不到 profile(不带 http(s):// 前缀的值按名/别名解析)。可用:%s",
				serverGiven, strings.Join(cfg.names(), " "))
		}
		selectedName, effective = canonical, p
		o.Server = p.Server
	case effective != nil:
		o.Server = effective.Server
	}

	// key:--key > RH_API_KEY > profile(与 server 各自独立取最高优先级)。
	if flags.Changed("key") {
		o.Key = flagKey
	} else if v := os.Getenv(envAPIKey); v != "" {
		o.Key = v
	} else if effective != nil {
		o.Key = effective.Key
	}

	// insecure:--insecure 显式 > profile。
	if flags.Changed("insecure") {
		o.Insecure = flagInsecure
	} else if effective != nil {
		o.Insecure = effective.Insecure
	}

	if selectedName != "" {
		o.Profile = selectedName
	}

	if flags.Changed("output") && flagOutput != "" {
		o.Output = flagOutput
	}
	if o.Output != "table" && o.Output != "json" {
		return o, fmt.Errorf("--output 无效:%q(可用 table|json)", o.Output)
	}

	if flags.Changed("timeout") && flagTimeout != "" {
		d, terr := time.ParseDuration(flagTimeout)
		if terr != nil || d <= 0 {
			return o, fmt.Errorf("--timeout 无效:%q(如 30s、2m)", flagTimeout)
		}
		o.Timeout = d
	}

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
