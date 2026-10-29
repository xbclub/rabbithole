package cli

import (
	"net/http"
	"strings"
	"testing"
)

// §3.1 #21:GET /api/stats/overview → stats overview。
func TestContract_stats_overview(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"clients_total":3,"clients_online":1,"services_total":7,"services_running":5,"conns_current":42,"bytes_in":3145728,"bytes_out":5368709120}`)))
	})
	out, err := runCLI(t, cliArgs(got, "stats", "overview")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodGet || got.Path != "/api/stats/overview" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	for _, want := range []string{"clients_total", "3", "conns_current", "42"} {
		if !strings.Contains(out, want) {
			t.Errorf("table 缺 %q:\n%s", want, out)
		}
	}
	// bytes 人类可读(§2.4):3145728 → 3.00 MiB,5368709120 → 5.00 GiB。
	if !strings.Contains(out, "3.00 MiB") || !strings.Contains(out, "5.00 GiB") {
		t.Errorf("bytes 应人类可读:\n%s", out)
	}
}

// §3.1 #22:GET /api/events?limit=&level= → events list。
func TestContract_events_list(t *testing.T) {
	t.Run("无参数不带 query", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`[]`)))
		})
		_, err := runCLI(t, cliArgs(got, "events", "list")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Path != "/api/events" || got.Query != "" {
			t.Errorf("请求 %s?%s, want 无 query", got.Path, got.Query)
		}
	})

	t.Run("limit 0 原样发出不本地拦截", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`[]`)))
		})
		_, err := runCLI(t, cliArgs(got, "events", "list", "--limit", "0", "--level", "warn")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		// url.Values.Encode 按键排序:level 在前。
		if want := "level=warn&limit=0"; got.Query != want {
			t.Errorf("query=%q, want %q(服务端 clamp 是权威,CLI 不拦 0)", got.Query, want)
		}
	})

	t.Run("table 时间列可读", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`[{"id":1,"ts":1761740400123,"level":"warn","source":"client","message":"客户端已禁用"}]`)))
		})
		out, err := runCLI(t, cliArgs(got, "events", "list")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if strings.Contains(out, "1761740400123") {
			t.Errorf("table 不应显示原始毫秒(应转可读时间):\n%s", out)
		}
		if !strings.Contains(out, "warn") || !strings.Contains(out, "客户端已禁用") {
			t.Errorf("table 缺 level/message:\n%s", out)
		}
	})
}

// §3.1 #23:GET /api/server/config → config get;json 输出与 data 逐字节一致。
func TestContract_config_get(t *testing.T) {
	t.Run("table 关键段", func(t *testing.T) {
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(`{"config":{},"hot":{"allow_ports":["20000-20100"],"max_services_per_client":16,"log_level":"info"},"authorized_keys":"ssh-ed25519 AAA x\n","alpn":["h2","http/1.1"]}`)))
		})
		out, err := runCLI(t, cliArgs(got, "config", "get")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Method != http.MethodGet || got.Path != "/api/server/config" {
			t.Errorf("请求 %s %s", got.Method, got.Path)
		}
		for _, want := range []string{"log_level", "info", "20000-20100", "h2,http/1.1", "已配置"} {
			if !strings.Contains(out, want) {
				t.Errorf("table 缺 %q:\n%s", want, out)
			}
		}
	})

	t.Run("json 逐字节一致", func(t *testing.T) {
		// 服务端原文带不规则空白:证明 CLI 不重组。
		data := `{"config":  {"bind":  "0.0.0.0"},   "hot":{}, "alpn": []}`
		got := captureServer(t, func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(envelopeOK(data)))
		})
		out, err := runCLI(t, cliArgs(got, "config", "get", "-o", "json")...)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if out != data+"\n" {
			t.Errorf("json 输出:\n got %q\nwant %q", out, data+"\n")
		}
	})
}

// §3.1 #24:PUT /api/server/config → config put,文件透传。
func TestContract_config_put(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`{"applied":["log","ports"],"restart_required":["tls"],"persisted":true}`)))
	})
	update := `{"log":{"level":"warn"},"ports":{"allow_ports":["20000-20100"]}}`
	file := writeSpecFile(t, update)
	out, err := runCLI(t, cliArgs(got, "config", "put", "--file", file)...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Method != http.MethodPut || got.Path != "/api/server/config" {
		t.Errorf("请求 %s %s", got.Method, got.Path)
	}
	if got.Body != update {
		t.Errorf("body:\n got %s\nwant %s", got.Body, update)
	}
	for _, want := range []string{"log,ports", "tls", "true"} {
		if !strings.Contains(out, want) {
			t.Errorf("table 缺 %q:\n%s", want, out)
		}
	}
}

// §2.3 内置别名:ls≡list、rm≡delete、rotate≡rotate-secret、ov≡overview。
func TestAlias_Builtin(t *testing.T) {
	root := newRootCmd()
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"clients", "ls"}, "list"},
		{[]string{"services", "ls"}, "list"},
		{[]string{"visitors", "ls"}, "list"},
		{[]string{"certs", "ls"}, "list"},
		{[]string{"events", "ls"}, "list"},
		{[]string{"clients", "rm"}, "delete"},
		{[]string{"services", "rm"}, "delete"},
		{[]string{"visitors", "rm"}, "delete"},
		{[]string{"certs", "rm"}, "delete"},
		{[]string{"clients", "rotate"}, "rotate-secret"},
		{[]string{"services", "rotate"}, "rotate-secret"},
		{[]string{"stats", "ov"}, "overview"},
	}
	for _, c := range cases {
		cmd, _, err := root.Find(c.args)
		if err != nil {
			t.Fatalf("%v: %v", c.args, err)
		}
		if cmd.Name() != c.want {
			t.Errorf("%v 解析到 %q, want %q", c.args, cmd.Name(), c.want)
		}
	}
}

// 别名端到端:clients ls 走完整命令路径。
func TestAlias_EndToEnd(t *testing.T) {
	got := captureServer(t, func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(envelopeOK(`[{"id":"u-1","name":"gw","online":true,"enabled":true,"remark":""}]`)))
	})
	out, err := runCLI(t, cliArgs(got, "clients", "ls")...)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.Path != "/api/clients" || !strings.Contains(out, "gw") {
		t.Errorf("别名路径异常: path=%s out=%s", got.Path, out)
	}
}
