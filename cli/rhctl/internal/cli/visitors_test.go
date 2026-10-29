package cli

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

const visitorSpec = `{"name":"v1","kind":"stcp","bind_addr":"127.0.0.1","bind_port":6000,"target_proxy_name":"svc"}`

// 包 E 验收:合并函数单测,嵌套 vs 扁平两种输入都测。
func TestMergeVisitorCreate(t *testing.T) {
	t.Run("扁平输入(纯 VisitorSpec)", func(t *testing.T) {
		body, err := mergeVisitorCreate("u-1", json.RawMessage(visitorSpec))
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			t.Fatal(err)
		}
		if m["client_id"] != "u-1" || m["name"] != "v1" || m["bind_port"] != float64(6000) {
			t.Errorf("merged=%v", m)
		}
		if _, nested := m["spec"]; nested {
			t.Errorf("body 不得有嵌套 spec 键:%s", body)
		}
	})

	t.Run("嵌套输入({spec:{…}} 包装)", func(t *testing.T) {
		nested := `{"client_id":"u-OLD","spec":{"name":"v1","kind":"stcp","bind_port":6000,"target_proxy_name":"svc"}}`
		body, err := mergeVisitorCreate("u-1", json.RawMessage(nested))
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			t.Fatal(err)
		}
		if m["client_id"] != "u-1" {
			t.Errorf("--client-id 应优先于文件内 client_id, got %v", m["client_id"])
		}
		if _, nested := m["spec"]; nested {
			t.Errorf("body 不得有嵌套 spec 键:%s", body)
		}
		if m["name"] != "v1" {
			t.Errorf("spec 字段应被展平:%s", body)
		}
	})

	t.Run("非对象输入报错", func(t *testing.T) {
		if _, err := mergeVisitorCreate("u-1", json.RawMessage(`[1,2]`)); err == nil {
			t.Error("数组输入应报错")
		}
	})
}

// §3.1 #15:GET /api/visitors[?client_id=]。
func TestContract_visitors_list(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`[{"id":5,"client_id":"u-1","name":"v1","kind":"stcp","bind_addr":"127.0.0.1","bind_port":6000,"target_proxy_name":"svc","spec":{}}]`)))
	})
	out, err := runCLI(t, cliArgs(got, "visitors", "list", "--client-id", "u-1")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodGet || got.Path != "/api/visitors" || got.Query != "client_id=u-1" {
		t.Errorf("请求 %s %s?%s", got.Method, got.Path, got.Query)
	}
	for _, want := range []string{"5", "u-1", "v1", "stcp", "127.0.0.1:6000", "svc"} {
		if !strings.Contains(out, want) {
			t.Errorf("table 缺 %q:\n%s", want, out)
		}
	}
}

// §3.1 #16:POST /api/visitors;出站 body 必须是单层扁平 JSON。
func TestContract_visitors_create(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"id":9,"revision":3}`)))
	})
	file := writeSpecFile(t, visitorSpec)
	out, err := runCLI(t, cliArgs(got, "visitors", "create", "--client-id", "u-1", "--file", file)...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPost || got.Path != "/api/visitors" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got.Body), &body); err != nil {
		t.Fatalf("body 不是 JSON 对象: %s", got.Body)
	}
	// 验收核心:单层扁平,client_id 与 spec 字段平铺,无嵌套 spec 键。
	if _, nested := body["spec"]; nested {
		t.Errorf("出站 body 不得含嵌套 spec 键:%s", got.Body)
	}
	if string(body["client_id"]) != `"u-1"` || string(body["name"]) != `"v1"` {
		t.Errorf("body 字段错误:%s", got.Body)
	}
	if !strings.Contains(out, "9") {
		t.Errorf("table 缺 id:\n%s", out)
	}
}

// §3.1 #17:PUT /api/visitors/{id},body = 纯 VisitorSpec 文件原文。
func TestContract_visitors_update(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"updated":true}`)))
	})
	file := writeSpecFile(t, visitorSpec)
	out, err := runCLI(t, cliArgs(got, "visitors", "update", "9", "--file", file)...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPut || got.Path != "/api/visitors/9" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	if got.Body != visitorSpec {
		t.Errorf("body:\n got %s\nwant %s(原文透传)", got.Body, visitorSpec)
	}
	if !strings.Contains(out, "true") {
		t.Errorf("结果行缺 updated:\n%s", out)
	}
}

// §3.1 #19/#20:POST /api/visitors/{id}/enable|disable。
func TestContract_visitors_enable(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"enabled":true}`)))
	})
	_, err := runCLI(t, cliArgs(got, "visitors", "enable", "9")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPost || got.Path != "/api/visitors/9/enable" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
}

func TestContract_visitors_disable(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"enabled":false}`)))
	})
	out, err := runCLI(t, cliArgs(got, "visitors", "disable", "9")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPost || got.Path != "/api/visitors/9/disable" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	if !strings.Contains(out, "false") {
		t.Errorf("结果行缺 enabled:\n%s", out)
	}
}

// §3.1 #18:DELETE /api/visitors/{id}。
func TestContract_visitors_delete(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"deleted":true}`)))
	})
	_, err := runCLI(t, cliArgs(got, "visitors", "delete", "9")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodDelete || got.Path != "/api/visitors/9" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
}

// ---------- certs ----------

// writePEM 写一个假 PEM 文件(服务端在测试桩里不解析内容)。
func writePEM(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "x.pem")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// §3.1 #25:GET /api/certs。
func TestContract_certs_list(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`[{"id":2,"name":"lab","domains":["a.com","b.com"],"not_after":1761740400123,"fingerprint":"AB:CD","created_at":1761700000000}]`)))
	})
	out, err := runCLI(t, cliArgs(got, "certs", "list")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodGet || got.Path != "/api/certs" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	for _, want := range []string{"2", "lab", "a.com,b.com"} {
		if !strings.Contains(out, want) {
			t.Errorf("table 缺 %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "2025-") && !strings.Contains(out, "2026-") {
		t.Errorf("not_after 应转可读时间:\n%s", out)
	}
}

// §3.1 #26:POST /api/certs;PEM 从文件读入,domains 逗号分隔。
func TestContract_certs_create(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"id":3,"fingerprint":"EF:01"}`)))
	})
	cert := writePEM(t, "-----BEGIN CERTIFICATE-----\nZm9v\n-----END CERTIFICATE-----\n")
	key := writePEM(t, "-----BEGIN PRIVATE KEY-----\nYmFy\n-----END PRIVATE KEY-----\n")
	out, err := runCLI(t, cliArgs(got, "certs", "create", "--name", "lab",
		"--cert-file", cert, "--key-file", key, "--domains", "a.com, b.com")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPost || got.Path != "/api/certs" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	var body struct {
		Name    string   `json:"name"`
		CertPEM string   `json:"cert_pem"`
		KeyPEM  string   `json:"key_pem"`
		Domains []string `json:"domains"`
	}
	if err := json.Unmarshal([]byte(got.Body), &body); err != nil {
		t.Fatalf("body=%s", got.Body)
	}
	if body.Name != "lab" || !strings.Contains(body.CertPEM, "BEGIN CERTIFICATE") ||
		!strings.Contains(body.KeyPEM, "BEGIN PRIVATE KEY") {
		t.Errorf("PEM 内嵌错误:%s", got.Body)
	}
	if len(body.Domains) != 2 || body.Domains[0] != "a.com" || body.Domains[1] != "b.com" {
		t.Errorf("domains=%v", body.Domains)
	}
	if !strings.Contains(out, "EF:01") {
		t.Errorf("table 缺 fingerprint:\n%s", out)
	}
}

// §3.1 #27:PUT /api/certs/{id},body 同 create。
func TestContract_certs_update(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"updated":true}`)))
	})
	cert := writePEM(t, "CERT")
	key := writePEM(t, "KEY")
	// 不给 --domains:body 不应带 domains 键(服务端取 SAN)。
	_, err := runCLI(t, cliArgs(got, "certs", "update", "3",
		"--name", "lab", "--cert-file", cert, "--key-file", key)...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPut || got.Path != "/api/certs/3" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	if strings.Contains(got.Body, "domains") {
		t.Errorf("未给 --domains 不应带 domains 键:%s", got.Body)
	}
}

// §3.1 #28:DELETE /api/certs/{id}。
func TestContract_certs_delete(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"deleted":true}`)))
	})
	_, err := runCLI(t, cliArgs(got, "certs", "delete", "3")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodDelete || got.Path != "/api/certs/3" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
}

// certs 的 PEM 文件缺失 → 本地错误零请求。
func TestCerts_MissingPEMIsLocalError(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		t.Error("缺 PEM 不得发起请求")
	})
	_, err := runCLI(t, cliArgs(got, "certs", "create", "--name", "lab",
		"--cert-file", "/nonexistent.pem", "--key-file", "/nonexistent.key")...)
	if err == nil || api.IsRemote(err) {
		t.Errorf("err=%v, want 本地错误", err)
	}
	if got.Method != "" {
		t.Error("不应有请求发出")
	}
}
