// config 命令组(§3.1 #23–#24)。put 的 body 是 ServerConfigUpdate JSON
// 文件透传 —— 服务端 deny_unknown_fields,未知字段整单 422(§1.3),
// README 提示先 config get 对照(§2.5 工作流)。
package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

func init() {
	registerGroup(newConfigCmd)
}

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "服务器配置",
	}
	cmd.AddCommand(newConfigGetCmd(), newConfigPutCmd())
	return cmd
}

// §3.1 #23:GET /api/server/config。table 只列关键段(config put 的
// 镜像);完整内容(config/hot/authorized_keys/alpn)看 -o json。
func newConfigGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "查看服务器配置(完整内容 -o json;web.jwt_secret 服务端已脱敏)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodGet, "/api/server/config", nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var v struct {
						Hot struct {
							AllowPorts           []string `json:"allow_ports"`
							MaxServicesPerClient int      `json:"max_services_per_client"`
							LogLevel             string   `json:"log_level"`
						} `json:"hot"`
						AuthorizedKeys *string  `json:"authorized_keys"`
						ALPN           []string `json:"alpn"`
					}
					if err := json.Unmarshal(raw, &v); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					keys := "未配置"
					if v.AuthorizedKeys != nil && *v.AuthorizedKeys != "" {
						keys = fmt.Sprintf("已配置(%d 行)", strings.Count(*v.AuthorizedKeys, "\n")+1)
					}
					alpn := "-"
					if len(v.ALPN) > 0 {
						alpn = strings.Join(v.ALPN, ",")
					}
					return WriteTable(os.Stdout,
						[]string{"段", "值"},
						[][]string{
							{"log_level", v.Hot.LogLevel},
							{"allow_ports", strings.Join(v.Hot.AllowPorts, ",")},
							{"max_services_per_client", strconv.Itoa(v.Hot.MaxServicesPerClient)},
							{"alpn", alpn},
							{"authorized_keys", keys},
						})
				})
			})
		},
	}
}

// §3.1 #24:PUT /api/server/config,body = 文件透传(ServerConfigUpdate)。
func newConfigPutCmd() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "put --file <update.json>",
		Short: "更新服务器配置(deny_unknown_fields:先 config get 对照,未知字段整单 422)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			body, err := readSpecFile(file)
			if err != nil {
				return err
			}
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodPut, "/api/server/config", body, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						Applied         []string `json:"applied"`
						RestartRequired []string `json:"restart_required"`
						Persisted       bool     `json:"persisted"`
						Error           *string  `json:"error"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					persisted := boolStr(resp.Persisted)
					if resp.Error != nil {
						persisted += " (" + *resp.Error + ")"
					}
					return WriteTable(os.Stdout,
						[]string{"applied", "restart_required", "persisted"},
						[][]string{{
							strings.Join(resp.Applied, ","),
							strings.Join(resp.RestartRequired, ","),
							persisted,
						}})
				})
			})
		},
	}
	cmd.Flags().StringVar(&file, "file", "", "ServerConfigUpdate JSON 文件(\"-\"=stdin,必填)")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}
