package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// testKey 形如真实 key(rhk_8hex_64hex),专用于断言它不会漏进错误文本。
var testKey = "rhk_0123abcd_" + strings.Repeat("f", 64)

func newTestClient(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return NewClient(srv.URL, testKey, 5*time.Second, false)
}

// §1.2 第 1 行:成功(200 + code=0)→ 解析 data。
func TestEnvelope_OK(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"data":{"id":7,"name":"gw"},"message":"ok"}`))
	})
	var got struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	if err := c.Do(context.Background(), http.MethodGet, "/api/clients", nil, &got); err != nil {
		t.Fatalf("err=%v", err)
	}
	if got.ID != 7 || got.Name != "gw" {
		t.Errorf("decoded=%+v", got)
	}
}

// §1.2 第 1 行附:data 原样字节透传(*json.RawMessage),供 -o json 直出。
func TestEnvelope_RawDataVerbatim(t *testing.T) {
	// 服务端原文带不规则空白:证明 CLI 不重组、逐字节保留。
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"data":{"a":  1,   "b": [2, 3]},"message":"ok"}`))
	})
	var raw json.RawMessage
	if err := c.Do(context.Background(), http.MethodGet, "/api/stats/overview", nil, &raw); err != nil {
		t.Fatalf("err=%v", err)
	}
	if want := `{"a":  1,   "b": [2, 3]}`; string(raw) != want {
		t.Errorf("raw=%q, want %q", raw, want)
	}
}

// §1.2 第 2 行:业务错误 = HTTP 200 + code≠0(最大的坑,不能按状态码分支)。
func TestEnvelope_BusinessErrorIsHTTP200(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		// 状态码 200,body code=1002。
		_, _ = w.Write([]byte(`{"code":1002,"message":"名称已存在","data":null}`))
	})
	err := c.Do(context.Background(), http.MethodPost, "/api/clients", map[string]any{"name": "x"}, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err=%v (%T), want *APIError", err, err)
	}
	if apiErr.Code != 1002 || apiErr.Message != "名称已存在" {
		t.Errorf("apiErr=%+v", apiErr)
	}
	if got := apiErr.Error(); !strings.Contains(got, "api 错误 1002") {
		t.Errorf("Error()=%q 应含 code", got)
	}
}

// §1.2 第 3 行:401(无/坏 Key)→ AuthError,认证失败提示。
func TestEnvelope_Unauthorized401(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":401,"message":"unauthorized","data":null}`))
	})
	err := c.Do(context.Background(), http.MethodGet, "/api/clients", nil, nil)
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("err=%v (%T), want *AuthError", err, err)
	}
	if authErr.Code != 401 {
		t.Errorf("code=%d", authErr.Code)
	}
	if got := authErr.Error(); !strings.Contains(got, "认证失败") || !strings.Contains(got, "unauthorized") {
		t.Errorf("Error()=%q", got)
	}
}

// §1.2 第 4 行:403(权限不足 / session-only)→ AuthError,message 原样透出。
func TestEnvelope_Forbidden403(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":403,"message":"API Key 权限不足","data":null}`))
	})
	err := c.Do(context.Background(), http.MethodGet, "/api/clients", nil, nil)
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("err=%v (%T), want *AuthError", err, err)
	}
	if authErr.Code != 403 || authErr.Message != "API Key 权限不足" {
		t.Errorf("authErr=%+v", authErr)
	}
	if got := authErr.Error(); !strings.Contains(got, "权限不足") {
		t.Errorf("Error()=%q", got)
	}
}

// §1.2 第 5 行:未知 /api/* 路径 = HTTP 200 + body code=404 → 按 code 分支。
func TestEnvelope_UnknownPathIs200WithCode404(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":404,"message":"not found","data":null}`))
	})
	err := c.Do(context.Background(), http.MethodGet, "/api/nope", nil, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err=%v (%T), want *APIError(按 code 分支而非状态码)", err, err)
	}
	if apiErr.Code != 404 {
		t.Errorf("code=%d, want 404", apiErr.Code)
	}
}

// §1.2 第 6 行:网络层失败(连接拒绝)→ NetworkError。
func TestEnvelope_NetworkErrorConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // 立即关闭,Do 必然连接失败
	c := NewClient(url, testKey, 2*time.Second, false)
	err := c.Do(context.Background(), http.MethodGet, "/api/clients", nil, nil)
	var netErr *NetworkError
	if !errors.As(err, &netErr) {
		t.Fatalf("err=%v (%T), want *NetworkError", err, err)
	}
	if got := netErr.Error(); !strings.Contains(got, "连接失败") {
		t.Errorf("Error()=%q", got)
	}
}

// §1.2 第 6 行:超时同样归入网络层失败。
func TestEnvelope_NetworkErrorTimeout(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(`{"code":0,"data":{},"message":"ok"}`))
	})
	c.http.Timeout = 20 * time.Millisecond
	err := c.Do(context.Background(), http.MethodGet, "/api/clients", nil, nil)
	var netErr *NetworkError
	if !errors.As(err, &netErr) {
		t.Fatalf("err=%v (%T), want *NetworkError", err, err)
	}
}

// §1.2 ⚠ 段:body 不是信封(422 Json 拒收返回纯文本)→ HTTPError,不得 panic。
func TestEnvelope_NonJSONBody422(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte("Failed to deserialize the JSON body into the target type"))
	})
	err := c.Do(context.Background(), http.MethodPut, "/api/server/config", map[string]any{"x": 1}, nil)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("err=%v (%T), want *HTTPError", err, err)
	}
	if httpErr.Status != 422 || !strings.Contains(httpErr.Error(), "Failed to deserialize") {
		t.Errorf("httpErr=%+v", httpErr)
	}
	if !IsRemote(err) {
		t.Error("HTTPError 应计为远端错误(exit 1)")
	}
}

// B-2:Authorization 头恰为 "Bearer <入参原样>"。
func TestEnvelope_AuthorizationHeaderExact(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("Authorization"), "Bearer "+testKey; got != want {
			t.Errorf("Authorization=%q, want %q", got, want)
		}
		_, _ = w.Write([]byte(`{"code":0,"data":null,"message":"ok"}`))
	})
	if err := c.Do(context.Background(), http.MethodGet, "/api/clients", nil, nil); err != nil {
		t.Fatal(err)
	}
}

// B-2:POST/PUT 带 Content-Type: application/json;GET 不带。
func TestEnvelope_ContentTypeOnBodyMethods(t *testing.T) {
	for _, tc := range []struct {
		method string
		body   any
		wantCT bool
	}{
		{http.MethodPost, map[string]any{"name": "x"}, true},
		{http.MethodPut, map[string]any{"enabled": true}, true},
		{http.MethodGet, nil, false},
	} {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			ct := r.Header.Get("Content-Type")
			if tc.wantCT && ct != "application/json" {
				t.Errorf("%s: Content-Type=%q, want application/json", tc.method, ct)
			}
			if !tc.wantCT && ct != "" {
				t.Errorf("%s: Content-Type=%q, want 空", tc.method, ct)
			}
			_, _ = w.Write([]byte(`{"code":0,"data":null,"message":"ok"}`))
		})
		if err := c.Do(context.Background(), tc.method, "/api/clients", tc.body, nil); err != nil {
			t.Fatalf("%s: %v", tc.method, err)
		}
	}
}

// B-2(§0 第 6 条):key 值不得出现在任何错误字符串里。
func TestEnvelope_KeyNeverInErrorStrings(t *testing.T) {
	handlers := []http.HandlerFunc{
		// 业务错误
		func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"code":1002,"message":"名称已存在","data":null}`))
		},
		// 401
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":401,"message":"unauthorized","data":null}`))
		},
		// 403
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":403,"message":"API Key 权限不足","data":null}`))
		},
		// 422 非 JSON
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte("deserialize failed"))
		},
	}
	for i, h := range handlers {
		c := newTestClient(t, h)
		err := c.Do(context.Background(), http.MethodGet, "/api/clients", nil, nil)
		if err == nil {
			t.Fatalf("case %d: 应报错", i)
		}
		if !IsRemote(err) {
			t.Errorf("case %d: err=%v 应计为远端错误", i, err)
		}
		if strings.Contains(err.Error(), testKey) || strings.Contains(err.Error(), "Bearer") {
			t.Errorf("case %d: 错误文本泄漏凭据:%q", i, err.Error())
		}
	}
	// 网络错误同样不泄漏(key 在头里,url 不含 key)。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()
	c := NewClient(url, testKey, time.Second, false)
	err := c.Do(context.Background(), http.MethodGet, "/api/clients", nil, nil)
	if err == nil || strings.Contains(err.Error(), testKey) {
		t.Errorf("网络错误泄漏凭据: %v", err)
	}
}

// IsRemote 分类:远端/网络 → true;本地/nil → false。
func TestEnvelope_IsRemoteClassification(t *testing.T) {
	if !IsRemote(&APIError{Code: 1, Message: "x"}) ||
		!IsRemote(&AuthError{Code: 401}) ||
		!IsRemote(&NetworkError{Err: context.DeadlineExceeded}) ||
		!IsRemote(&HTTPError{Status: 422}) {
		t.Error("远端错误应全部判 true")
	}
	if IsRemote(nil) || IsRemote(errors.New("本地")) {
		t.Error("本地错误应判 false")
	}
}
