// clients 命令组(§3.1 #1–#5)。注意 clients 没有独立的 enable/disable
// 路由 —— 启停就是 update {enabled:bool}(§1.3)。
package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/spf13/cobra"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

func init() {
	registerGroup(newClientsCmd)
}

func newClientsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clients",
		Short: "客户端管理",
	}
	cmd.AddCommand(
		newClientsListCmd(),
		newClientsCreateCmd(),
		newClientsUpdateCmd(),
		newClientsRotateSecretCmd(),
		newClientsDeleteCmd(),
	)
	return cmd
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func newClientsListCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "列出全部客户端",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodGet, "/api/clients", nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var rows []struct {
						ID      string `json:"id"`
						Name    string `json:"name"`
						Online  bool   `json:"online"`
						Enabled bool   `json:"enabled"`
						Remark  string `json:"remark"`
					}
					if err := json.Unmarshal(raw, &rows); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					out := make([][]string, 0, len(rows))
					for _, r := range rows {
						out = append(out, []string{r.ID, r.Name, boolStr(r.Online), boolStr(r.Enabled), r.Remark})
					}
					return WriteTable(os.Stdout, []string{"id", "name", "online", "enabled", "remark"}, out)
				})
			})
		},
	}
}

// clientsCreateFlags 承载 create 与 update 共用的可选字段(§2.3:update
// 的可选项「同 create」)。数值配额用 Changed 判断「是否显式给出」:
// 显式 0 = 恢复不限,与不传必须可区分。
type clientsCreateFlags struct {
	remark, rateLimit              string
	maxConns, maxServices, flowLim int64
}

func (v *clientsCreateFlags) bind(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringVar(&v.remark, "remark", "", "备注")
	f.StringVar(&v.rateLimit, "rate-limit", "", "限速(如 1MB;空=不限)")
	f.Int64Var(&v.maxConns, "max-conns", 0, "最大连接数(0=不限)")
	f.Int64Var(&v.maxServices, "max-services", 0, "最大规则数(0=不限)")
	f.Int64Var(&v.flowLim, "flow-limit", 0, "流量限额(字节;0=不限)")
}

// applyCreate 把已显式给出的配额字段填进 create 请求体。
func (v *clientsCreateFlags) applyCreate(cmd *cobra.Command, req *api.CreateClientReq) {
	fl := cmd.Flags()
	req.Remark = v.remark
	if fl.Changed("rate-limit") {
		req.RateLimit = &v.rateLimit
	}
	if fl.Changed("max-conns") {
		req.MaxConns = &v.maxConns
	}
	if fl.Changed("max-services") {
		req.MaxServices = &v.maxServices
	}
	if fl.Changed("flow-limit") {
		req.FlowLimit = &v.flowLim
	}
}

// applyUpdate 把已显式给出的字段填进 update 请求体(含 remark/name)。
func (v *clientsCreateFlags) applyUpdate(cmd *cobra.Command, req *api.UpdateClientReq, name string) {
	fl := cmd.Flags()
	if fl.Changed("name") {
		req.Name = &name
	}
	if fl.Changed("remark") {
		req.Remark = &v.remark
	}
	if fl.Changed("rate-limit") {
		req.RateLimit = &v.rateLimit
	}
	if fl.Changed("max-conns") {
		req.MaxConns = &v.maxConns
	}
	if fl.Changed("max-services") {
		req.MaxServices = &v.maxServices
	}
	if fl.Changed("flow-limit") {
		req.FlowLimit = &v.flowLim
	}
}

func newClientsCreateCmd() *cobra.Command {
	var cf clientsCreateFlags
	var name string
	cmd := &cobra.Command{
		Use:   "create",
		Short: "创建客户端(secret 仅本次返回)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(func(c *api.Client) error {
				req := api.CreateClientReq{Name: name}
				cf.applyCreate(cmd, &req)
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodPost, "/api/clients", req, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						ID     string `json:"id"`
						Name   string `json:"name"`
						Secret string `json:"secret"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					secretHint()
					return WriteTable(os.Stdout,
						[]string{"id", "name", "secret"},
						[][]string{{resp.ID, resp.Name, resp.Secret}})
				})
			})
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "客户端名称(必填)")
	_ = cmd.MarkFlagRequired("name")
	cf.bind(cmd)
	return cmd
}

func newClientsUpdateCmd() *cobra.Command {
	var cf clientsCreateFlags
	var name string
	var enabled, disabled bool
	cmd := &cobra.Command{
		Use:   "update <id>",
		Short: "更新客户端(启停即 --enabled/--disabled,无独立路由)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if enabled && disabled {
				return fmt.Errorf("--enabled 与 --disabled 互斥")
			}
			return withClient(func(c *api.Client) error {
				var req api.UpdateClientReq
				if enabled || disabled {
					b := enabled // --disabled → enabled:false
					req.Enabled = &b
				}
				cf.applyUpdate(cmd, &req, name)
				var raw json.RawMessage
				path := "/api/clients/" + url.PathEscape(args[0])
				if err := c.Do(cmd.Context(), http.MethodPut, path, req, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						Updated bool `json:"updated"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					return WriteTable(os.Stdout,
						[]string{"id", "updated"},
						[][]string{{args[0], boolStr(resp.Updated)}})
				})
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&name, "name", "", "名称")
	f.BoolVar(&enabled, "enabled", false, "启用")
	f.BoolVar(&disabled, "disabled", false, "禁用")
	cf.bind(cmd)
	return cmd
}

func newClientsRotateSecretCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "rotate-secret <id>",
		Aliases: []string{"rotate"},
		Short:   "轮换客户端密钥(旧会话被踢线重认证;secret 仅本次返回)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/clients/" + url.PathEscape(args[0]) + "/rotate-secret"
				if err := c.Do(cmd.Context(), http.MethodPost, path, nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						Secret string `json:"secret"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					secretHint()
					return WriteTable(os.Stdout,
						[]string{"id", "secret"},
						[][]string{{args[0], resp.Secret}})
				})
			})
		},
	}
}

func newClientsDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <id>",
		Aliases: []string{"rm"},
		Short:   "删除客户端(级联删除其全部规则)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/clients/" + url.PathEscape(args[0])
				if err := c.Do(cmd.Context(), http.MethodDelete, path, nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						Deleted bool `json:"deleted"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					return WriteTable(os.Stdout,
						[]string{"id", "deleted"},
						[][]string{{args[0], boolStr(resp.Deleted)}})
				})
			})
		},
	}
}
