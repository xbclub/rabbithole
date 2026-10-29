// 请求体结构:仅定义「结构化请求」的那些命令(§3.1「请求形状」列)。
// 字段以服务端 api.rs 的 Deserialize 结构为准(§1.3);spec 透传类
// (services/visitors/config)不在此定义 —— 权威在 rh-protocol,CLI 不复制。
package api

// CreateClientReq 对齐 api.rs CreateClientReq:name 必填;
// 可选项 omitempty —— 缺省与「不传」在服务端语义一致(serde default)。
type CreateClientReq struct {
	Name        string  `json:"name"`
	Remark      string  `json:"remark,omitempty"`
	RateLimit   *string `json:"rate_limit,omitempty"`
	MaxConns    *int64  `json:"max_conns,omitempty"`
	MaxServices *int64  `json:"max_services,omitempty"`
	FlowLimit   *int64  `json:"flow_limit,omitempty"`
}

// UpdateClientReq 对齐 api.rs UpdateClientReq:全部可选,nil = 不修改。
// 指针 + omitempty 使「显式传 0 / 空串」与「不传」可区分
// (如 remark:"" 清空备注、max_conns:0 恢复不限)。
type UpdateClientReq struct {
	Enabled     *bool   `json:"enabled,omitempty"`
	Name        *string `json:"name,omitempty"`
	Remark      *string `json:"remark,omitempty"`
	RateLimit   *string `json:"rate_limit,omitempty"`
	MaxConns    *int64  `json:"max_conns,omitempty"`
	MaxServices *int64  `json:"max_services,omitempty"`
	FlowLimit   *int64  `json:"flow_limit,omitempty"`
}

// ServicesBatchReq 对齐 api.rs batch_services:{ids, action}。
// action 取值 enable|disable|delete(枚举在 CLI 侧本地校验,任务书包 D)。
type ServicesBatchReq struct {
	IDs    []int64 `json:"ids"`
	Action string  `json:"action"`
}

// CertReq 对齐 api.rs CertReq:PEM 全文内嵌;domains 缺省时服务端取证书 SAN。
type CertReq struct {
	Name    string   `json:"name"`
	CertPEM string   `json:"cert_pem"`
	KeyPEM  string   `json:"key_pem"`
	Domains []string `json:"domains,omitempty"`
}
