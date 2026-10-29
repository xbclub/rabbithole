package cli

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xbclub/rabbithole/cli/rhctl/internal/api"
)

// writeSpecFile 把 spec JSON 写进临时文件(带尾换行,模拟真实编辑器产物)。
func writeSpecFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(p, []byte(content+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// §3.1 #6:GET /api/services[?client_id=] → services list。
func TestContract_services_list(t *testing.T) {
	t.Run("无过滤", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`[{"id":3,"client_id":"u-1","name":"web","kind":"tcp","bind_port":8080,"target":"127.0.0.1:80","status":"running"}]`)))
		})
		out, err := runCLI(t, cliArgs(got, "services", "list")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Method != http.MethodGet || got.Path != "/api/services" || got.Query != "" {
			t.Errorf("请求 %s %s?%s, want GET /api/services 无 query", got.Method, got.Path, got.Query)
		}
		for _, want := range []string{"3", "u-1", "web", "tcp", "8080", "running"} {
			if !strings.Contains(out, want) {
				t.Errorf("table 缺 %q:\n%s", want, out)
			}
		}
	})

	t.Run("client-id 过滤", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`[]`)))
		})
		_, err := runCLI(t, cliArgs(got, "services", "list", "--client-id", "u-9")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Path != "/api/services" || got.Query != "client_id=u-9" {
			t.Errorf("请求 %s?%s, want /api/services?client_id=u-9", got.Path, got.Query)
		}
	})
}

// §3.1 #9:GET /api/services/{id} → services get;-o json 输出含 spec。
func TestContract_services_get(t *testing.T) {
	specRow := `{"id":3,"client_id":"u-1","name":"web","kind":"stcp","bind_port":0,"target":"127.0.0.1:80","status":"running","spec":{"name":"web","kind":"stcp","stcp":{"secret_key":""}}}`
	t.Run("table 只列关键列", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(specRow)))
		})
		out, err := runCLI(t, cliArgs(got, "services", "get", "3")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Method != http.MethodGet || got.Path != "/api/services/3" {
			t.Errorf("请求 %s %s, want GET /api/services/3", got.Method, got.Path)
		}
		if !strings.Contains(out, "stcp") || !strings.Contains(out, "web") {
			t.Errorf("table 缺关键列:\n%s", out)
		}
	})

	t.Run("json 含整份 spec", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(specRow)))
		})
		out, err := runCLI(t, cliArgs(got, "services", "get", "3", "-o", "json")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if !strings.Contains(out, `"spec"`) {
			t.Errorf("json 输出应含 spec:\n%s", out)
		}
	})
}

// §3.1 #7:POST /api/services → services create;出站 body 与文件内容
// 逐字节一致(除首尾空白)。
func TestContract_services_create(t *testing.T) {
	spec := `{"name":"web","kind":"tcp","client_id":"u-1","bind_port":0,"target_host":"127.0.0.1","target_port":80}`

	t.Run("文件透传逐字节一致", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`{"id":7,"bind_port":20001,"port_auto":true,"revision":2}`)))
		})
		file := writeSpecFile(t, spec)
		out, err := runCLI(t, cliArgs(got, "services", "create", "--file", file)...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Method != http.MethodPost || got.Path != "/api/services" {
			t.Errorf("请求 %s %s, want POST /api/services", got.Method, got.Path)
		}
		if got.Body != spec {
			t.Errorf("body:\n got %s\nwant %s(除尾换行逐字节一致)", got.Body, spec)
		}
		if !strings.Contains(out, "20001") {
			t.Errorf("table 缺 bind_port:\n%s", out)
		}
	})

	t.Run("stdin 透传", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`{"id":8,"bind_port":20002,"port_auto":false,"revision":1}`)))
		})
		withStdin(t, spec+"\n\n")
		if _, err := runCLI(t, cliArgs(got, "services", "create", "--file", "-")...); err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Body != spec {
			t.Errorf("body=%s, want %s", got.Body, spec)
		}
	})

	t.Run("非 JSON 文件本地报错零请求", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			t.Error("非 JSON 不得发起请求")
		})
		file := writeSpecFile(t, `{not json`)
		_, err := runCLI(t, cliArgs(got, "services", "create", "--file", file)...)
		if err == nil || api.IsRemote(err) {
			t.Errorf("err=%v, want 本地错误", err)
		}
		if got.Method != "" {
			t.Error("不应有请求发出")
		}
	})
}

// §3.1 #10:PUT /api/services/{id} → services update。
func TestContract_services_update(t *testing.T) {
	spec := `{"name":"web2","kind":"tcp","client_id":"u-1","bind_port":20010,"target_host":"127.0.0.1","target_port":8080}`
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"updated":true,"revision":9}`)))
	})
	file := writeSpecFile(t, spec)
	out, err := runCLI(t, cliArgs(got, "services", "update", "7", "--file", file)...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPut || got.Path != "/api/services/7" {
		t.Errorf("请求 %s %s, want PUT /api/services/7", got.Method, got.Path)
	}
	if got.Body != spec {
		t.Errorf("body:\n got %s\nwant %s", got.Body, spec)
	}
	if !strings.Contains(out, "true") {
		t.Errorf("结果行缺 updated:\n%s", out)
	}
}

// §3.1 #12:POST /api/services/{id}/enable。
func TestContract_services_enable(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"enabled":true,"revision":4}`)))
	})
	out, err := runCLI(t, cliArgs(got, "services", "enable", "7")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPost || got.Path != "/api/services/7/enable" || got.Body != "" {
		t.Errorf("请求 %s %s body=%q", got.Method, got.Path, got.Body)
	}
	if !strings.Contains(out, "4") {
		t.Errorf("table 缺 revision:\n%s", out)
	}
}

// §3.1 #13:POST /api/services/{id}/disable。
func TestContract_services_disable(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"disabled":true,"revision":5}`)))
	})
	out, err := runCLI(t, cliArgs(got, "services", "disable", "7")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPost || got.Path != "/api/services/7/disable" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	if !strings.Contains(out, "5") {
		t.Errorf("table 缺 revision:\n%s", out)
	}
}

// §3.1 #14:POST /api/services/{id}/rotate-secret。
func TestContract_services_rotate_secret(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"rotated":true,"secret":"cafebabe","revision":6}`)))
	})
	out, err := runCLI(t, cliArgs(got, "services", "rotate-secret", "7")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPost || got.Path != "/api/services/7/rotate-secret" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	if !strings.Contains(out, "cafebabe") {
		t.Errorf("table 未显示 secret:\n%s", out)
	}
}

// §3.1 #8:POST /api/services/batch;action 非法 → 本地错误(exit 2)。
func TestContract_services_batch(t *testing.T) {
	t.Run("正常批", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`[{"id":1,"code":0,"message":"ok"},{"id":2,"code":3001,"message":"not found"}]`)))
		})
		out, err := runCLI(t, cliArgs(got, "services", "batch", "--ids", "1, 2", "--action", "disable")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Method != http.MethodPost || got.Path != "/api/services/batch" {
			t.Errorf("请求 %s %s", got.Method, got.Path)
		}
		if want := `{"ids":[1,2],"action":"disable"}`; got.Body != want {
			t.Errorf("body=%s, want %s", got.Body, want)
		}
		if !strings.Contains(out, "3001") {
			t.Errorf("逐 id 结果缺失败项:\n%s", out)
		}
	})

	t.Run("非法 action 本地报错零请求", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			t.Error("非法 action 不得发起请求")
		})
		_, err := runCLI(t, cliArgs(got, "services", "batch", "--ids", "1", "--action", "nuke")...)
		if err == nil || api.IsRemote(err) {
			t.Errorf("err=%v, want 本地错误(exit 2)", err)
		}
		if got.Method != "" {
			t.Error("不应有请求发出")
		}
	})

	t.Run("非数字 id 本地报错", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			t.Error("非数字 id 不得发起请求")
		})
		_, err := runCLI(t, cliArgs(got, "services", "batch", "--ids", "1,x", "--action", "delete")...)
		if err == nil || api.IsRemote(err) {
			t.Errorf("err=%v, want 本地错误", err)
		}
	})
}

// §3.1 #11:DELETE /api/services/{id}。
func TestContract_services_delete(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"deleted":true}`)))
	})
	out, err := runCLI(t, cliArgs(got, "services", "delete", "7")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodDelete || got.Path != "/api/services/7" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	if !strings.Contains(out, "true") {
		t.Errorf("结果行缺 deleted:\n%s", out)
	}
}
