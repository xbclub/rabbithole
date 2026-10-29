package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// B-3:json 输出与输入 data 逐字节一致(仅追加换行),不重组不丢字段。
func TestOutput_JSONVerbatim(t *testing.T) {
	// 带不规则空白与字段顺序,证明原样直出。
	data := json.RawMessage(`{"b":  2, "a": [1,  3], "name": "网关-01"}`)
	var buf bytes.Buffer
	if err := PrintJSON(&buf, data); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), string(data)+"\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// B-3:data 缺省 → 输出 null。
func TestOutput_JSONEmptyIsNull(t *testing.T) {
	var buf bytes.Buffer
	if err := PrintJSON(&buf, nil); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "null\n" {
		t.Errorf("got %q, want null", got)
	}
}

// B-3:table 渲染包含表头与全部单元格。
func TestOutput_TableRenders(t *testing.T) {
	var buf bytes.Buffer
	err := WriteTable(&buf,
		[]string{"id", "name", "online"},
		[][]string{{"1", "gw-a", "true"}, {"2", "gw-b", "false"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"id", "name", "online", "1", "gw-a", "true", "2", "gw-b", "false"} {
		if !strings.Contains(out, want) {
			t.Errorf("输出缺 %q:\n%s", want, out)
		}
	}
	if !strings.HasPrefix(strings.TrimSpace(out), "id") {
		t.Errorf("首行应是表头:\n%s", out)
	}
}

// B-3:字节人类可读。
func TestOutput_FormatBytes(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{3 << 20, "3.00 MiB"},
		{(5 << 30) + (100 << 20), "5.10 GiB"},
		{2 << 40, "2.00 TiB"},
	}
	for _, c := range cases {
		if got := FormatBytes(c.in); got != c.want {
			t.Errorf("FormatBytes(%d)=%q, want %q", c.in, got, c.want)
		}
	}
}

// B-3:毫秒时间戳转本地时区;非正值显示 -。
func TestOutput_FormatMs(t *testing.T) {
	if got := FormatMs(0); got != "-" {
		t.Errorf("0 → %q, want -", got)
	}
	ms := int64(1761740400123)
	want := time.UnixMilli(ms).Format("2006-01-02 15:04:05")
	if got := FormatMs(ms); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
