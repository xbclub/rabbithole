// services 命令组(§3.1 #6–#14)。create/update 的 body 是整份
// ProxySpec JSON(§2.5):--file 透传,CLI 不复制字段表。
package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

func init() {
	registerGroup(newServicesCmd)
}

func newServicesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "services",
		Short: "转发规则(services)管理",
	}
	cmd.AddCommand(
		newServicesListCmd(),
		newServicesGetCmd(),
		newServicesCreateCmd(),
		newServicesUpdateCmd(),
		newServicesEnableCmd(),
		newServicesDisableCmd(),
		newServicesRotateSecretCmd(),
		newServicesBatchCmd(),
		newServicesDeleteCmd(),
	)
	return cmd
}

// serviceRow 是 list/get 的 table 列投影(§3.1 第 6 行);完整字段看 -o json。
type serviceRow struct {
	ID       int64  `json:"id"`
	ClientID string `json:"client_id"`
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	BindPort int    `json:"bind_port"`
	Target   string `json:"target"`
	Status   string `json:"status"`
}

func writeServiceTable(rows []serviceRow) error {
	out := make([][]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, []string{
			strconv.FormatInt(r.ID, 10), r.ClientID, r.Name, r.Kind,
			strconv.Itoa(r.BindPort), r.Target, r.Status,
		})
	}
	return WriteTable(os.Stdout,
		[]string{"id", "client", "name", "kind", "bind_port", "target", "status"}, out)
}

// §3.1 #6:GET /api/services[?client_id=]。
func newServicesListCmd() *cobra.Command {
	var clientID string
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "列出转发规则(可按客户端过滤)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(func(c *api.Client) error {
				path := "/api/services"
				if cmd.Flags().Changed("client-id") {
					path += "?" + url.Values{"client_id": []string{clientID}}.Encode()
				}
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodGet, path, nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var rows []serviceRow
					if err := json.Unmarshal(raw, &rows); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					return writeServiceTable(rows)
				})
			})
		},
	}
	cmd.Flags().StringVar(&clientID, "client-id", "", "按客户端 ID 过滤")
	return cmd
}

// §3.1 #9:GET /api/services/{id}。单条带 spec(密钥已抹掉);
// -o json 才有 spec,table 只列关键列。
func newServicesGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "查看单条规则(编辑回读用;-o json 含整份 spec)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/services/" + url.PathEscape(args[0])
				if err := c.Do(cmd.Context(), http.MethodGet, path, nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var row serviceRow
					if err := json.Unmarshal(raw, &row); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					return writeServiceTable([]serviceRow{row})
				})
			})
		},
	}
}

// §3.1 #7:POST /api/services,body = 文件原文透传。
func newServicesCreateCmd() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "create --file <spec.json>",
		Short: "创建转发规则(spec JSON 文件透传;响应不含 secret,拿明文需 rotate)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			spec, err := readSpecFile(file)
			if err != nil {
				return err
			}
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodPost, "/api/services", spec, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						ID       int64 `json:"id"`
						BindPort int   `json:"bind_port"`
						PortAuto bool  `json:"port_auto"`
						Revision int64 `json:"revision"`
						// 客户端离线时的延迟生效分支
						Deferred *bool `json:"deferred"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					deferred := "-"
					if resp.Deferred != nil {
						deferred = boolStr(*resp.Deferred)
					}
					return WriteTable(os.Stdout,
						[]string{"id", "bind_port", "port_auto", "revision", "deferred"},
						[][]string{{
							strconv.FormatInt(resp.ID, 10), strconv.Itoa(resp.BindPort),
							boolStr(resp.PortAuto), strconv.FormatInt(resp.Revision, 10), deferred,
						}})
				})
			})
		},
	}
	cmd.Flags().StringVar(&file, "file", "", "ProxySpec JSON 文件(\"-\"=stdin,必填)")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}

// §3.1 #10:PUT /api/services/{id},body = 文件原文透传。
func newServicesUpdateCmd() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "update <id> --file <spec.json>",
		Short: "更新转发规则(spec JSON 文件透传;get 回读的 spec 密钥留空=不改)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, err := readSpecFile(file)
			if err != nil {
				return err
			}
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/services/" + url.PathEscape(args[0])
				if err := c.Do(cmd.Context(), http.MethodPut, path, spec, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						Updated  bool  `json:"updated"`
						Revision int64 `json:"revision"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					return WriteTable(os.Stdout,
						[]string{"id", "updated", "revision"},
						[][]string{{args[0], boolStr(resp.Updated), strconv.FormatInt(resp.Revision, 10)}})
				})
			})
		},
	}
	cmd.Flags().StringVar(&file, "file", "", "ProxySpec JSON 文件(\"-\"=stdin,必填)")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}

// §3.1 #12/#13:enable / disable。响应或带 revision,或带 deferred
// (客户端离线时延迟生效),两者都展示。
func newServicesEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable <id>",
		Short: "启用转发规则(客户端离线时延迟生效)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(func(c *api.Client) error {
				return serviceStateChange(cmd, c, http.MethodPost, "/api/services/"+url.PathEscape(args[0])+"/enable", args[0], "enabled")
			})
		},
	}
}

func newServicesDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable <id>",
		Short: "停用转发规则",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(func(c *api.Client) error {
				return serviceStateChange(cmd, c, http.MethodPost, "/api/services/"+url.PathEscape(args[0])+"/disable", args[0], "disabled")
			})
		},
	}
}

// serviceStateChange 渲染 enable/disable 的 {<field>:true, revision|deferred} 响应。
func serviceStateChange(cmd *cobra.Command, c *api.Client, method, path, id, field string) error {
	var raw json.RawMessage
	if err := c.Do(cmd.Context(), method, path, nil, &raw); err != nil {
		return err
	}
	return output(raw, func() error {
		var resp struct {
			Revision int64 `json:"revision"`
			Deferred *bool `json:"deferred"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			return fmt.Errorf("响应解码失败: %w", err)
		}
		deferred := "-"
		if resp.Deferred != nil {
			deferred = boolStr(*resp.Deferred)
		}
		return WriteTable(os.Stdout,
			[]string{"id", field, "revision", "deferred"},
			[][]string{{id, "true", strconv.FormatInt(resp.Revision, 10), deferred}})
	})
}

// §3.1 #14:POST /api/services/{id}/rotate-secret → {rotated,secret,revision}。
// 仅 STCP/SUDP/XTCP 支持(其余服务端 4004,CLI 不预判)。
func newServicesRotateSecretCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "rotate-secret <id>",
		Aliases: []string{"rotate"},
		Short:   "轮换密钥访问规则凭据(仅 STCP/SUDP/XTCP;secret 仅本次返回)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/services/" + url.PathEscape(args[0]) + "/rotate-secret"
				if err := c.Do(cmd.Context(), http.MethodPost, path, nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						Rotated  bool   `json:"rotated"`
						Secret   string `json:"secret"`
						Revision int64  `json:"revision"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					secretHint()
					return WriteTable(os.Stdout,
						[]string{"id", "rotated", "secret", "revision"},
						[][]string{{args[0], boolStr(resp.Rotated), resp.Secret,
							strconv.FormatInt(resp.Revision, 10)}})
				})
			})
		},
	}
}

// §3.1 #8:POST /api/services/batch,body {ids:[i64], action}。
// action 三选一在本地校验(§D 验收);id 逐项解析,非法即本地错误。
func newServicesBatchCmd() *cobra.Command {
	var ids, action string
	cmd := &cobra.Command{
		Use:   "batch --ids 1,2,3 --action enable|disable|delete",
		Short: "批量 启用/停用/删除 转发规则",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			switch action {
			case "enable", "disable", "delete":
			default:
				return fmt.Errorf("--action 无效:%q(可用 enable|disable|delete)", action)
			}
			parts := strings.Split(ids, ",")
			idList := make([]int64, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}
				n, err := strconv.ParseInt(p, 10, 64)
				if err != nil {
					return fmt.Errorf("--ids 含非数字项:%q", p)
				}
				idList = append(idList, n)
			}
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				body := api.ServicesBatchReq{IDs: idList, Action: action}
				if err := c.Do(cmd.Context(), http.MethodPost, "/api/services/batch", body, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var rows []struct {
						ID      int64  `json:"id"`
						Code    int    `json:"code"`
						Message string `json:"message"`
					}
					if err := json.Unmarshal(raw, &rows); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					out := make([][]string, 0, len(rows))
					for _, r := range rows {
						out = append(out, []string{
							strconv.FormatInt(r.ID, 10), strconv.Itoa(r.Code), r.Message})
					}
					return WriteTable(os.Stdout, []string{"id", "code", "message"}, out)
				})
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&ids, "ids", "", "逗号分隔的规则 ID(必填)")
	f.StringVar(&action, "action", "", "enable|disable|delete(必填)")
	_ = cmd.MarkFlagRequired("ids")
	_ = cmd.MarkFlagRequired("action")
	return cmd
}

// §3.1 #11:DELETE /api/services/{id}。
func newServicesDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <id>",
		Aliases: []string{"rm"},
		Short:   "删除转发规则",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/services/" + url.PathEscape(args[0])
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
