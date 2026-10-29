package cli

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// runCLI 构建全新命令树执行 args(与生产 Execute 同路径,含
// PersistentPreRunE 取值链),捕获 stdout。返回 stdout 与错误。
func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := newRootCmd()
	root.SetArgs(args)
	return captureStdout(t, func() error { return root.Execute() })
}

func captureStdout(t *testing.T, f func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()
	runErr := f()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatal(err)
	}
	return buf.String(), runErr
}

// capturedReq 记录单次请求的方法/路径/query/body/头,契约断言用。
type capturedReq struct {
	URL    string // 服务端地址(传给 --server)
	Method string
	Path   string
	Query  string
	Body   string
	Auth   string
	CT     string
}

// envelopeOK 拼一条成功信封。
func envelopeOK(data string) string {
	return `{"code":0,"data":` + data + `,"message":"ok"}`
}

// captureServer 起 httptest 服务端:记录(仅允许一次)请求,再由
// respond 写响应。契约测试的通用底座。
func captureServer(t *testing.T, respond func(w http.ResponseWriter)) *capturedReq {
	t.Helper()
	var got capturedReq
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got.Method != "" {
			t.Errorf("不期望第二次请求:%s %s(前一次 %s %s)", r.Method, r.URL.Path, got.Method, got.Path)
		}
		got.Method = r.Method
		got.Path = r.URL.Path
		got.Query = r.URL.RawQuery
		got.Auth = r.Header.Get("Authorization")
		got.CT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		got.Body = string(b)
		respond(w)
	}))
	t.Cleanup(srv.Close)
	got.URL = srv.URL
	return &got
}
