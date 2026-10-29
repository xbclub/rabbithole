// profile 子命令(§2.3):本地命令,不访问 REST API,不进 §3 覆盖审计。
package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func init() {
	registerGroup(newProfileCmd)
}

func newProfileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "管理连接 profile(本地命令,不访问 REST API)",
	}
	cmd.AddCommand(
		newProfileListCmd(),
		newProfileAddCmd(),
		newProfileUseCmd(),
		newProfileRemoveCmd(),
	)
	return cmd
}

// profile list:key 列掩码(§2.2),附别名列与当前标记。
func newProfileListCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "列出 profile(key 掩码显示)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadProfiles()
			if err != nil {
				return err
			}
			rows := make([][]string, 0, len(cfg.Profiles))
			for name, p := range cfg.Profiles {
				rows = append(rows, []string{
					boolMark(name == cfg.Current), name,
					strings.Join(p.Aliases, ","), p.Server,
					boolStr(p.Insecure), maskKey(p.Key),
				})
			}
			return WriteTable(os.Stdout,
				[]string{"current", "name", "aliases", "server", "insecure", "key"}, rows)
		},
	}
}

func boolMark(b bool) string {
	if b {
		return "*"
	}
	return ""
}

// profile add <name> --server <url> --key <key> [--alias a]… [--key-stdin] [--force]。
func newProfileAddCmd() *cobra.Command {
	var server, key string
	var aliases []string
	var keyStdin, force bool
	cmd := &cobra.Command{
		Use:   "add <name> --server <url> --key <key> [--alias a]… [--key-stdin] [--force]",
		Short: "添加 profile(--key-stdin 防 key 进 shell history)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if name == "" {
				return fmt.Errorf("profile 名不能为空")
			}
			fl := cmd.Flags()
			hasKey := fl.Changed("key")
			if hasKey && keyStdin {
				return fmt.Errorf("--key 与 --key-stdin 互斥")
			}
			if !hasKey && !keyStdin {
				return fmt.Errorf("缺少 key:--key <key> 或 --key-stdin(从 stdin 读,防进 shell history)")
			}
			if keyStdin {
				b, err := io.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("读 stdin 失败: %w", err)
				}
				key = strings.TrimSpace(string(b))
				if key == "" {
					return fmt.Errorf("stdin 里的 key 为空")
				}
			}
			if !isURL(server) {
				return fmt.Errorf("--server 必须是 http(s):// URL(不带前缀的值会被当成 profile 别名解析)")
			}
			cfg, err := loadProfiles()
			if err != nil {
				return err
			}
			if _, exists := cfg.Profiles[name]; exists && !force {
				return fmt.Errorf("profile %q 已存在(--force 覆盖)", name)
			}
			// 命名空间检查:新名与新别名不得与既有任何名/别名冲突
			// (--force 只豁免与自己同名的覆盖)。
			old := cfg.Profiles[name]
			delete(cfg.Profiles, name)
			cfg.Profiles[name] = &Profile{Server: server, Key: key, Aliases: aliases}
			if verr := cfg.validate(); verr != nil {
				if old != nil {
					cfg.Profiles[name] = old
				} else {
					delete(cfg.Profiles, name)
				}
				return fmt.Errorf("无法添加:%w", verr)
			}
			if err := saveProfiles(cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "profile 已添加:%s(server=%s)\n", name, server)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&server, "server", "", "管理端 URL,http(s)://…(必填)")
	f.StringVar(&key, "key", "", "API Key(必填,除非 --key-stdin;注意会进 shell history)")
	f.StringSliceVar(&aliases, "alias", nil, "别名,可重复")
	f.BoolVar(&keyStdin, "key-stdin", false, "从 stdin 读 key(TrimSpace)")
	f.BoolVar(&force, "force", false, "覆盖同名 profile")
	_ = cmd.MarkFlagRequired("server")
	return cmd
}

// profile use <name>:接受别名,写入规范名。
func newProfileUseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "切换当前 profile(设为 current;名可为别名)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadProfiles()
			if err != nil {
				return err
			}
			canonical, _, ok := cfg.resolveName(args[0])
			if !ok {
				return fmt.Errorf("profile %q 不存在。可用:%s", args[0], strings.Join(cfg.names(), " "))
			}
			cfg.Current = canonical
			if err := saveProfiles(cfg); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "当前 profile:%s\n", canonical)
			return nil
		},
	}
}

// profile remove <name>:接受别名;删的是 current 则同步清空。
func newProfileRemoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "remove <name>",
		Short: "删除 profile(名可为别名;删 current 则一并清空)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadProfiles()
			if err != nil {
				return err
			}
			canonical, _, ok := cfg.resolveName(args[0])
			if !ok {
				return fmt.Errorf("profile %q 不存在。可用:%s", args[0], strings.Join(cfg.names(), " "))
			}
			delete(cfg.Profiles, canonical)
			cleared := false
			if cfg.Current == canonical {
				cfg.Current = ""
				cleared = true
			}
			if err := saveProfiles(cfg); err != nil {
				return err
			}
			if cleared {
				fmt.Fprintf(cmd.OutOrStdout(), "已删除 profile:%s(current 已同步清空)\n", canonical)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "已删除 profile:%s\n", canonical)
			}
			return nil
		},
	}
}
