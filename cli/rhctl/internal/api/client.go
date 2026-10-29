// Package api 是 rhctl 的管理 REST API 客户端:注入 Bearer 凭据、
// 解码 {code,data,message} 信封、按任务书 §1.2 分类错误。
package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// envelope 是服务端统一响应形状(§1.0:成功 code=0、message="ok")。
type envelope struct {
	Code    int             `json:"code"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
}

// APIError 是信封里的业务错误:HTTP 200 + code≠0 —— err() 不带状态码,
// 按 HTTP 状态分支会把失败当成功(§1.2 最大的坑)。Code/Message 原样
// 透出,不做本地翻译(§0 第 8 条,复制必漂移)。
type APIError struct {
	Code    int
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("api 错误 %d: %s", e.Code, e.Message)
}

// AuthError 是真实 HTTP 401/403(无/坏 Key;权限不足或 session-only 组)。
type AuthError struct {
	Code    int // 401 | 403
	Message string
}

func (e *AuthError) Error() string {
	if e.Code == http.StatusUnauthorized {
		return fmt.Sprintf("认证失败(401):%s —— 检查 --key / RH_API_KEY / profile 凭据", e.Message)
	}
	return fmt.Sprintf("权限不足(403):%s", e.Message)
}

// NetworkError 是网络层失败(连接拒绝、超时、DNS 等)。
type NetworkError struct{ Err error }

func (e *NetworkError) Error() string { return "连接失败:" + e.Err.Error() }
func (e *NetworkError) Unwrap() error { return e.Err }

// HTTPError 覆盖 body 不是信封的响应:axum 的 Json 拒收(如 config put
// 未知字段触发 deny_unknown_fields)返回 422 + 非 JSON body,解码必须
// 容忍,回退按状态码 + body 文本报错(§1.2 ⚠ 段)。
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Status, truncate(e.Body, 200))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// IsRemote 报告 err 是否远端/网络错误(退出码 1);本地错误为 false
// (退出码 2)。§2.4 退出码语义的唯一判定入口。
func IsRemote(err error) bool {
	var (
		a *APIError
		u *AuthError
		n *NetworkError
		h *HTTPError
	)
	return errors.As(err, &a) || errors.As(err, &u) || errors.As(err, &n) || errors.As(err, &h)
}

// Client 是管理 API 的 HTTP 客户端。零值不可用,经 NewClient 构造。
type Client struct {
	base string
	key  string
	http *http.Client
}

// NewClient 构造客户端。insecure 为 true 时跳过 TLS 校验(--insecure,
// 自签场景)。
func NewClient(server, key string, timeout time.Duration, insecure bool) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // --insecure 由用户显式给出
	}
	return &Client{
		base: strings.TrimRight(server, "/"),
		key:  key,
		http: &http.Client{Timeout: timeout, Transport: transport},
	}
}

// Do 发一次请求并按信封解码。method 如 "GET";path 如 "/api/clients"
// (query 自带);body 非 nil 时 JSON 序列化并带 Content-Type;out 非 nil
// 时把信封 data 反序列化进去 —— 传 *json.RawMessage 可拿到 data 原样
// 字节,供 --output json 直出不重组(§2.4)。
func (c *Client) Do(ctx context.Context, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("请求体序列化失败: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return fmt.Errorf("构造请求失败: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return &NetworkError{Err: err}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return &NetworkError{Err: err}
	}

	var env envelope
	if jerr := json.Unmarshal(raw, &env); jerr == nil {
		// body 是信封:一律按 code 分支,HTTP 状态只是 401/403 的旁证
		// —— 业务错误是 200,未知 /api/* 也是 200+code404(§1.2)。
		switch {
		case env.Code == 0:
			if out == nil || len(env.Data) == 0 {
				return nil
			}
			if uerr := json.Unmarshal(env.Data, out); uerr != nil {
				return fmt.Errorf("响应 data 解码失败: %w", uerr)
			}
			return nil
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			return &AuthError{Code: resp.StatusCode, Message: env.Message}
		default:
			return &APIError{Code: env.Code, Message: env.Message}
		}
	}

	// body 不是信封(422 Json 拒收等):按状态码 + body 文本报错。
	return &HTTPError{Status: resp.StatusCode, Body: string(raw)}
}
