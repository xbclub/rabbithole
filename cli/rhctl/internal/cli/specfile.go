package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// readSpecFile 读 --file 输入("-" = stdin),本地只校验 JSON 可解析,
// 不校验字段(§2.5:ProxySpec/VisitorSpec 是随协议演进的版本化结构,
// 权威在 rh-protocol,CLI 复制字段表必然漂移)。返回 TrimSpace 后的
// 原文 —— 透传出站 body 与文件内容逐字节一致(除首尾空白)。
func readSpecFile(name string) (json.RawMessage, error) {
	var (
		b   []byte
		err error
	)
	if name == "-" {
		b, err = io.ReadAll(os.Stdin)
	} else {
		b, err = os.ReadFile(name)
	}
	if err != nil {
		return nil, fmt.Errorf("读取 %s 失败: %w", name, err)
	}
	trimmed := bytes.TrimSpace(b)
	var v any
	if jerr := json.Unmarshal(trimmed, &v); jerr != nil {
		return nil, fmt.Errorf("%s 不是合法 JSON: %w", name, jerr)
	}
	return json.RawMessage(trimmed), nil
}
