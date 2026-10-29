// certs 命令组(§3.1 #25–#28)。PEM 全文内嵌请求体(cert_pem/key_pem),
// 从 --cert-file/--key-file 读入;domains 缺省由服务端取证书 SAN。
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
	registerGroup(newCertsCmd)
}

func newCertsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "certs",
		Short: "TLS 证书管理",
	}
	cmd.AddCommand(
		newCertsListCmd(),
		newCertsCreateCmd(),
		newCertsUpdateCmd(),
		newCertsDeleteCmd(),
	)
	return cmd
}

// §3.1 #25:GET /api/certs。
func newCertsListCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "列出证书",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodGet, "/api/certs", nil, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var rows []struct {
						ID          int64    `json:"id"`
						Name        string   `json:"name"`
						Domains     []string `json:"domains"`
						NotAfter    *int64   `json:"not_after"` // 毫秒
						Fingerprint string   `json:"fingerprint"`
						CreatedAt   int64    `json:"created_at"`
					}
					if err := json.Unmarshal(raw, &rows); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					out := make([][]string, 0, len(rows))
					for _, r := range rows {
						notAfter := "-"
						if r.NotAfter != nil {
							notAfter = FormatMs(*r.NotAfter)
						}
						out = append(out, []string{
							strconv.FormatInt(r.ID, 10), r.Name,
							strings.Join(r.Domains, ","), notAfter,
						})
					}
					return WriteTable(os.Stdout,
						[]string{"id", "name", "domains", "not_after"}, out)
				})
			})
		},
	}
}

// readPEMFile 读 PEM 文件为字符串(PEM 全文内嵌 CertReq)。
func readPEMFile(name string) (string, error) {
	b, err := os.ReadFile(name)
	if err != nil {
		return "", fmt.Errorf("读取 %s 失败: %w", name, err)
	}
	return string(b), nil
}

// parseDomains 解析逗号分隔的域名列表;全空 = 未给(服务端取证书 SAN)。
func parseDomains(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// certFlags 承载 create/update 共用的 --name/--cert-file/--key-file/--domains。
func newCertsCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create --name <n> --cert-file <pem> --key-file <pem> [--domains a,b]",
		Short: "上传证书(PEM 从文件读入)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			req, err := buildCertReq(cmd)
			if err != nil {
				return err
			}
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				if err := c.Do(cmd.Context(), http.MethodPost, "/api/certs", req, &raw); err != nil {
					return err
				}
				return output(raw, func() error {
					var resp struct {
						ID          int64  `json:"id"`
						Fingerprint string `json:"fingerprint"`
					}
					if err := json.Unmarshal(raw, &resp); err != nil {
						return fmt.Errorf("响应解码失败: %w", err)
					}
					return WriteTable(os.Stdout,
						[]string{"id", "fingerprint"},
						[][]string{{strconv.FormatInt(resp.ID, 10), resp.Fingerprint}})
				})
			})
		},
	}
	bindCertFlags(cmd)
	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("cert-file")
	_ = cmd.MarkFlagRequired("key-file")
	return cmd
}

// §3.1 #27:PUT /api/certs/{id},body 同 create。
func newCertsUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <id> --name <n> --cert-file <pem> --key-file <pem> [--domains a,b]",
		Short: "更新证书(整份替换)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req, err := buildCertReq(cmd)
			if err != nil {
				return err
			}
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/certs/" + url.PathEscape(args[0])
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
	bindCertFlags(cmd)
	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("cert-file")
	_ = cmd.MarkFlagRequired("key-file")
	return cmd
}

func bindCertFlags(cmd *cobra.Command) {
	f := cmd.Flags()
	f.String("name", "", "证书名称(必填)")
	f.String("cert-file", "", "证书 PEM 文件(必填)")
	f.String("key-file", "", "私钥 PEM 文件(必填)")
	f.String("domains", "", "逗号分隔的域名(缺省取证书 SAN)")
}

// buildCertReq 从 flags 组装 CertReq(读 PEM 文件,本地错误即 exit 2)。
func buildCertReq(cmd *cobra.Command) (*api.CertReq, error) {
	f := cmd.Flags()
	name, _ := f.GetString("name")
	certFile, _ := f.GetString("cert-file")
	keyFile, _ := f.GetString("key-file")
	domains, _ := f.GetString("domains")
	certPEM, err := readPEMFile(certFile)
	if err != nil {
		return nil, err
	}
	keyPEM, err := readPEMFile(keyFile)
	if err != nil {
		return nil, err
	}
	return &api.CertReq{
		Name:    name,
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
		Domains: parseDomains(domains),
	}, nil
}

// §3.1 #28:DELETE /api/certs/{id}。
func newCertsDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <id>",
		Aliases: []string{"rm"},
		Short:   "删除证书",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withClient(func(c *api.Client) error {
				var raw json.RawMessage
				path := "/api/certs/" + url.PathEscape(args[0])
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
