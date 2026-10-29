// events 命令组(§3.1 #22)。limit/level 原样透传:服务端 clamp 1..500
// 是权威(§1.3),CLI 本地「纠正」反而与服务端漂移(预埋坑表)。
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
	registerGroup(newEventsCmd)
}

func newEventsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "events",
		Short: "事件日志",
	}
	cmd.AddCommand(newEventsListCmd())
	return cmd
}

// §3.1 #22:GET /api/events?limit=&level=。
func newEventsListCmd() *cobra.Command {
	var limit int64
	var level string
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "列出事件(limit 服务端 clamp 1..500;level 如 info|warn|error)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(func(c *api.Client) error {
				q := url.Values{}
				fl := cmd.Flags()
				// --limit 0 也原样发出:本地不拦、不纠(服务端 clamp 是权威)。
				if fl.Changed("limit") {
					q.Set("limit", strconv.FormatInt(limit, 10))
				}
				if fl.Changed("level") {
					q.Set("level", level)
				}
				path := "/api/events"
				if enc := q.Encode(); enc != "" {
					path += "?" + enc
				}
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodGet, path, nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var rows []struct {
						ID      int64  `json:"id"`
						TS      int64  `json:"ts"` // 毫秒
						Level   string `json:"level"`
						Source  string `json:"source"`
						Message string `json:"message"`
					}
					if err := json.Unmarshal(raw, &rows); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					out := make([][]string, 0, len(rows))
					for _, r := range rows {
						out = append(out, []string{
							strconv.FormatInt(r.ID, 10), FormatMs(r.TS),
							r.Level, r.Source, r.Message,
						})
					}
					return WriteTable(os.Stdout,
						[]string{"id", "time", "level", "source", "message"}, out)
				})
			})
		},
	}
	f := cmd.Flags()
	f.Int64Var(&limit, "limit", 0, "条数(服务端缺省 100、clamp 1..500)")
	f.StringVar(&level, "level", "", "级别过滤(如 info|warn|error)")
	return cmd
}
