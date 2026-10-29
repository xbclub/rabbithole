// stats 命令组(§3.1 #21)。
package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

func init() {
	registerGroup(newStatsCmd)
}

func newStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "运行统计",
	}
	cmd.AddCommand(newStatsOverviewCmd())
	return cmd
}

// §3.1 #21:GET /api/stats/overview → 七字段一屏。
func newStatsOverviewCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "overview",
		Aliases: []string{"ov"},
		Short:   "总览(客户端/规则/连接/流量)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodGet, "/api/stats/overview", nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var s struct {
						ClientsTotal    int64 `json:"clients_total"`
						ClientsOnline   int64 `json:"clients_online"`
						ServicesTotal   int64 `json:"services_total"`
						ServicesRunning int64 `json:"services_running"`
						ConnsCurrent    int64 `json:"conns_current"`
						BytesIn         int64 `json:"bytes_in"`
						BytesOut        int64 `json:"bytes_out"`
					}
					if err := json.Unmarshal(raw, &s); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					return WriteTable(os.Stdout,
						[]string{"字段", "值"},
						[][]string{
							{"clients_total", strconv.FormatInt(s.ClientsTotal, 10)},
							{"clients_online", strconv.FormatInt(s.ClientsOnline, 10)},
							{"services_total", strconv.FormatInt(s.ServicesTotal, 10)},
							{"services_running", strconv.FormatInt(s.ServicesRunning, 10)},
							{"conns_current", strconv.FormatInt(s.ConnsCurrent, 10)},
							{"bytes_in", FormatBytes(s.BytesIn)},
							{"bytes_out", FormatBytes(s.BytesOut)},
						})
				})
			})
		},
	}
}
