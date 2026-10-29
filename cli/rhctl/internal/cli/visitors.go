// visitors 命令组(§3.1 #15–#20)。create 的 body 是 {client_id,
// …VisitorSpec 扁平化}(服务端 serde(flatten),§1.3 预埋坑:发嵌套
// {client_id, spec:{…}} 会被 422 拒收)。
package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

func init() {
	registerGroup(newVisitorsCmd)
}

func newVisitorsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "visitors",
		Short: "访问器(visitors)管理",
	}
	cmd.AddCommand(
		newVisitorsListCmd(),
		newVisitorsCreateCmd(),
		newVisitorsUpdateCmd(),
		newVisitorsEnableCmd(),
		newVisitorsDisableCmd(),
		newVisitorsDeleteCmd(),
	)
	return cmd
}

type visitorRow struct {
	ID              int64  `json:"id"`
	ClientID        string `json:"client_id"`
	Name            string `json:"name"`
	Kind            string `json:"kind"`
	BindAddr        string `json:"bind_addr"`
	BindPort        int    `json:"bind_port"`
	TargetProxyName string `json:"target_proxy_name"`
}

func writeVisitorTable(rows []visitorRow) error {
	out := make([][]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, []string{
			strconv.FormatInt(r.ID, 10), r.ClientID, r.Name, r.Kind,
			fmt.Sprintf("%s:%d", r.BindAddr, r.BindPort), r.TargetProxyName,
		})
	}
	return WriteTable(os.Stdout,
		[]string{"id", "client", "name", "kind", "bind", "target"}, out)
}

// mergeVisitorCreate 把 --client-id 与文件里的 VisitorSpec 合并成单层
// 请求体。容忍两种输入:纯 spec(扁平)或 {"spec":{…}} 包装(嵌套),
// 一律展平;文件里若带 client_id,--client-id 显式值优先。
func mergeVisitorCreate(clientID string, fileJSON json.RawMessage) (json.RawMessage, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(fileJSON, &top); err != nil {
		return nil, fmt.Errorf("visitor spec 不是 JSON 对象: %w", err)
	}
	spec := top
	if inner, ok := top["spec"]; ok {
		var innerMap map[string]json.RawMessage
		if err := json.Unmarshal(inner, &innerMap); err != nil {
			return nil, fmt.Errorf("spec 字段不是 JSON 对象: %w", err)
		}
		spec = innerMap
	}
	delete(spec, "client_id")
	cid, err := json.Marshal(clientID)
	if err != nil {
		return nil, err
	}
	spec["client_id"] = cid
	return json.Marshal(spec)
}

// §3.1 #15:GET /api/visitors[?client_id=]。
func newVisitorsListCmd() *cobra.Command {
	var clientID string
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "列出访问器(可按客户端过滤)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(func(c *api.Client) error {
				path := "/api/visitors"
				if cmd.Flags().Changed("client-id") {
					path += "?" + url.Values{"client_id": []string{clientID}}.Encode()
				}
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodGet, path, nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var rows []visitorRow
					if err := json.Unmarshal(raw, &rows); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					return writeVisitorTable(rows)
				})
			})
		},
	}
	cmd.Flags().StringVar(&clientID, "client-id", "", "按客户端 ID 过滤")
	return cmd
}

// §3.1 #16:POST /api/visitors,body = {client_id, …spec 扁平}。
func newVisitorsCreateCmd() *cobra.Command {
	var clientID, file string
	cmd := &cobra.Command{
		Use:   "create --client-id <cid> --file <visitor.json>",
		Short: "创建访问器(client_id 与 spec 文件扁平合并成单层 body)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			spec, err := readSpecFile(file)
			if err != nil {
				return err
			}
			body, err := mergeVisitorCreate(clientID, spec)
			if err != nil {
				return err
			}
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodPost, "/api/visitors", body, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						ID       int64 `json:"id"`
						Revision int64 `json:"revision"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					// name/bind 回显自出站 spec(响应只有 id/revision)。
					var sent struct {
						Name     string `json:"name"`
						BindAddr string `json:"bind_addr"`
						BindPort int    `json:"bind_port"`
					}
					_ = json.Unmarshal(body, &sent)
					return WriteTable(os.Stdout,
						[]string{"id", "name", "bind", "revision"},
						[][]string{{strconv.FormatInt(resp.ID, 10), sent.Name,
							fmt.Sprintf("%s:%d", sent.BindAddr, sent.BindPort),
							strconv.FormatInt(resp.Revision, 10)}})
				})
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&clientID, "client-id", "", "归属客户端 ID(必填)")
	f.StringVar(&file, "file", "", "VisitorSpec JSON 文件(\"-\"=stdin,必填)")
	_ = cmd.MarkFlagRequired("client-id")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}

// §3.1 #17:PUT /api/visitors/{id},body = 纯 VisitorSpec 文件原文透传。
func newVisitorsUpdateCmd() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "update <id> --file <visitor.json>",
		Short: "更新访问器(纯 VisitorSpec;secret_key 服务端强制清空,入参忽略)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, err := readSpecFile(file)
			if err != nil {
				return err
			}
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/visitors/" + url.PathEscape(args[0])
				if err := c.Do(cmd.Context(), http.MethodPut, path, spec, &raw); err != nil {
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
	cmd.Flags().StringVar(&file, "file", "", "VisitorSpec JSON 文件(\"-\"=stdin,必填)")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}

// §3.1 #19/#20:enable / disable → {enabled:bool}。
func newVisitorsEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable <id>",
		Short: "启用访问器",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return visitorToggle(cmd, args[0], true)
		},
	}
}

func newVisitorsDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable <id>",
		Short: "停用访问器",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return visitorToggle(cmd, args[0], false)
		},
	}
}

func visitorToggle(cmd *cobra.Command, id string, want bool) error {
	return withClient(func(c *api.Client) error {
		var raw json.RawMessage
		path := "/api/visitors/" + url.PathEscape(id) + "/" + map[bool]string{true: "enable", false: "disable"}[want]
		if err := c.Do(cmd.Context(), http.MethodPost, path, nil, &raw); err != nil {
			return err
		}
		return output(raw, func() error {
			var resp struct {
				Enabled bool `json:"enabled"`
			}
			if err := json.Unmarshal(raw, &resp); err != nil {
				return fmt.Errorf("响应解码失败: %w", err)
			}
			return WriteTable(os.Stdout,
				[]string{"id", "enabled"},
				[][]string{{id, boolStr(resp.Enabled)}})
		})
	})
}

// §3.1 #18:DELETE /api/visitors/{id}。
func newVisitorsDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <id>",
		Aliases: []string{"rm"},
		Short:   "删除访问器",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/visitors/" + url.PathEscape(args[0])
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
