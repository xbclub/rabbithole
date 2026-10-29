package cli

import (
	"net/http"
	"strings"
	"testing"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

// cliArgs 拼上连接参数(server/key 指向 captureServer)。
func cliArgs(got *capturedReq, args ...string) []string {
	return append(args, "--server", got.URL, "--key", "rhk_contract")
}

// §3.1 #1:GET /api/clients → clients list。
func TestContract_clients_list(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`[{"id":"u-1","name":"gw-a","online":true,"enabled":false,"remark":"边缘"}]`)))
	})
	out, err := runCLI(t, cliArgs(got, "clients", "list")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodGet || got.Path != "/api/clients" || got.Query != "" {
		t.Errorf("请求 %s %s?q=%s, want GET /api/clients 无 query", got.Method, got.Path, got.Query)
	}
	if got.Auth != "Bearer rhk_contract" || got.CT != "" {
		t.Errorf("Auth=%q CT=%q", got.Auth, got.CT)
	}
	for _, want := range []string{"id", "u-1", "gw-a", "true", "false", "边缘"} {
		if !strings.Contains(out, want) {
			t.Errorf("table 缺 %q:\n%s", want, out)
		}
	}
}

// §3.1 #2:POST /api/clients → clients create;出站 body 与 §1.3
// CreateClientReq 逐字段一致(测试断言,不是人工目检)。
func TestContract_clients_create(t *testing.T) {
	t.Run("全字段", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`{"id":"u-9","name":"gw-1","remark":"网关","secret":"aabbccdd"}`)))
		})
		out, err := runCLI(t, cliArgs(got, "clients", "create", "--name", "gw-1",
			"--remark", "网关", "--rate-limit", "1MB", "--max-conns", "10",
			"--max-services", "5", "--flow-limit", "1000")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Method != http.MethodPost || got.Path != "/api/clients" {
			t.Errorf("请求 %s %s, want POST /api/clients", got.Method, got.Path)
		}
		if got.CT != "application/json" {
			t.Errorf("Content-Type=%q", got.CT)
		}
		want := `{"name":"gw-1","remark":"网关","rate_limit":"1MB","max_conns":10,"max_services":5,"flow_limit":1000}`
		if got.Body != want {
			t.Errorf("body:\n got %s\nwant %s", got.Body, want)
		}
		// §2.6:table 必须完整显示 secret。
		if !strings.Contains(out, "aabbccdd") {
			t.Errorf("table 未显示 secret:\n%s", out)
		}
	})

	t.Run("仅必填", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`{"id":"u-9","name":"x","remark":"","secret":"ff"}`)))
		})
		_, err := runCLI(t, cliArgs(got, "clients", "create", "--name", "x")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if want := `{"name":"x"}`; got.Body != want {
			t.Errorf("body=%s, want %s(未给的可选字段不得出现)", got.Body, want)
		}
	})

	t.Run("缺 --name 本地报错", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			t.Error("缺 --name 不得发起请求")
		})
		_, err := runCLI(t, cliArgs(got, "clients", "create")...)
		if err == nil || api.IsRemote(err) {
			t.Errorf("err=%v, want 本地错误(exit 2)", err)
		}
		if got.Method != "" {
			t.Errorf("不应有请求发出,实际 %s %s", got.Method, got.Path)
		}
	})
}

// §3.1 #3:PUT /api/clients/{id} → clients update;--enabled/--disabled
// 映射 {enabled:bool}。
func TestContract_clients_update(t *testing.T) {
	t.Run("disable + 部分字段", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`{"updated":true}`)))
		})
		out, err := runCLI(t, cliArgs(got, "clients", "update", "u-7",
			"--disabled", "--name", "nn", "--max-conns", "0")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Method != http.MethodPut || got.Path != "/api/clients/u-7" {
			t.Errorf("请求 %s %s, want PUT /api/clients/u-7", got.Method, got.Path)
		}
		// max_conns 显式 0 = 恢复不限,必须出现在 body 里。
		want := `{"enabled":false,"name":"nn","max_conns":0}`
		if got.Body != want {
			t.Errorf("body:\n got %s\nwant %s", got.Body, want)
		}
		if !strings.Contains(out, "u-7") || !strings.Contains(out, "true") {
			t.Errorf("结果行缺 id/updated:\n%s", out)
		}
	})

	t.Run("enable", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`{"updated":true}`)))
		})
		_, err := runCLI(t, cliArgs(got, "clients", "update", "u-7", "--enabled")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if want := `{"enabled":true}`; got.Body != want {
			t.Errorf("body=%s, want %s", got.Body, want)
		}
	})

	t.Run("互斥对报错且零请求", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			t.Error("互斥对不得发起请求")
		})
		_, err := runCLI(t, cliArgs(got, "clients", "update", "u-7", "--enabled", "--disabled")...)
		if err == nil || api.IsRemote(err) {
			t.Errorf("err=%v, want 本地错误", err)
		}
		if got.Method != "" {
			t.Errorf("不应有请求发出")
		}
	})
}

// §3.1 #5:POST /api/clients/{id}/rotate-secret → clients rotate-secret。
func TestContract_clients_rotate_secret(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"secret":"feedface"}`)))
	})
	out, err := runCLI(t, cliArgs(got, "clients", "rotate-secret", "u-7")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPost || got.Path != "/api/clients/u-7/rotate-secret" {
		t.Errorf("请求 %s %s, want POST /api/clients/u-7/rotate-secret", got.Method, got.Path)
	}
	if got.Body != "" || got.CT != "" {
		t.Errorf("无 body 命令不应带 body/CT, got body=%q ct=%q", got.Body, got.CT)
	}
	if !strings.Contains(out, "feedface") {
		t.Errorf("table 未显示 secret:\n%s", out)
	}
}

// §3.1 #4:DELETE /api/clients/{id} → clients delete。
func TestContract_clients_delete(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"deleted":true}`)))
	})
	out, err := runCLI(t, cliArgs(got, "clients", "delete", "u-7")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodDelete || got.Path != "/api/clients/u-7" {
		t.Errorf("请求 %s %s, want DELETE /api/clients/u-7", got.Method, got.Path)
	}
	if !strings.Contains(out, "true") {
		t.Errorf("结果行缺 deleted:\n%s", out)
	}
}
