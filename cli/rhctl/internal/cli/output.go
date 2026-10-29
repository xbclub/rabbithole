package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

// PrintJSON 把信封 data 原样写 stdout(§2.4:json 输出不重组、不丢字段,
// 便于接 jq),仅追加结尾换行。data 为空(信封 data 缺省)时输出 null。
func PrintJSON(w io.Writer, data json.RawMessage) error {
	if len(data) == 0 {
		data = json.RawMessage("null")
	}
	_, err := fmt.Fprintln(w, string(data))
	return err
}

// WriteTable 用 tabwriter 渲染表(§2.4:表头小写、数据走 stdout)。
// 行不足 header 长度的单元格补空串,多出的截掉。
func WriteTable(w io.Writer, headers []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	all := append([][]string{headers}, rows...)
	for _, row := range all {
		r := make([]string, len(headers))
		copy(r, row)
		if _, err := fmt.Fprintln(tw, strings.Join(r, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// FormatBytes 人类可读字节:KiB/MiB/GiB(§2.4)。
func FormatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// FormatMs 毫秒时间戳 → 本地时区 YYYY-MM-DD HH:MM:SS(§0 第 7 条:
// --output json 原样毫秒,table 转可读)。非正值显示 "-"。
func FormatMs(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	return time.UnixMilli(ms).Format("2006-01-02 15:04:05")
}
