package cli

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// §1.2 第 7 行(§2.2):key 缺失 → 本地错误,列出三种来源,
// 且不发起任何 HTTP(断言服务端零请求)。
func TestEnvelope_MissingKeyNoHTTP(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		_, _ = w.Write([]byte(`{"code":0,"data":null,"message":"ok"}`))
	}))
	defer srv.Close()

	err := requireKey(Options{Server: srv.URL, Key: ""})
	if err == nil {
		t.Fatal("key 缺失应报错")
	}
	for _, want := range []string{"--key", "RH_API_KEY", "profile"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误提示缺来源 %q:\n%s", want, err.Error())
		}
	}
	if n := atomic.LoadInt32(&hits); n != 0 {
		t.Errorf("服务端收到 %d 次请求,应零请求", n)
	}

	if err := requireKey(Options{Key: "rhk_x"}); err != nil {
		t.Errorf("有 key 不应报错: %v", err)
	}
}
