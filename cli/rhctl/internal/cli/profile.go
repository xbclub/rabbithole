// 凭证存储与多管理端 profile(任务书 §2.2 / 包 I)。
// 对齐 kubectl/aws 的通行分层:--key/--server > RH_API_KEY/RH_SERVER >
// profile 文件 > 默认。定案:不做权限检查、不警告、不拒载 —— 凭证侧
// 唯一强制行为是输出脱敏(maskKey);文件创建 0600 只是默认创建模式。
package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Profile 是单个管理端的连接信息:每台 server 独立签发自己的 key
// (服务端只存 sha256,同一把 key 不能跨 server 复用),多管理端 =
// N 对 (server, key) 的 profile。
type Profile struct {
	Server   string   `json:"server"` // 必填,http(s):// URL
	Key      string   `json:"key"`    // 必填,rhk_…
	Insecure bool     `json:"insecure"`
	Aliases  []string `json:"aliases,omitempty"`
}

// ProfileConfig 是 profiles.json 顶层结构。
type ProfileConfig struct {
	Current  string              `json:"current"` // 必须是规范名,不接受别名
	Profiles map[string]*Profile `json:"profiles"`
}

// profileConfigPath:RHCTL_CONFIG 强制覆盖 > XDG_CONFIG_HOME > ~/.config。
func profileConfigPath() (string, error) {
	if p := os.Getenv("RHCTL_CONFIG"); p != "" {
		return p, nil
	}
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("定位配置目录失败: %w", err)
		}
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "rhctl", "profiles.json"), nil
}

// loadProfiles 读取并校验 profile 配置。文件不存在 → 空配置(首次使用)。
// 解码用 DisallowUnknownFields:拼错的键 exit 2,不静默忽略(§2.2)。
func loadProfiles() (*ProfileConfig, error) {
	path, err := profileConfigPath()
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &ProfileConfig{Profiles: map[string]*Profile{}}, nil
		}
		return nil, fmt.Errorf("打开 %s 失败: %w", path, err)
	}
	defer f.Close()

	cfg := &ProfileConfig{}
	dec := json.NewDecoder(io.LimitReader(f, 4<<20))
	dec.DisallowUnknownFields()
	if derr := dec.Decode(cfg); derr != nil {
		return nil, fmt.Errorf("profile 配置解析失败(%s):%v(拼错的键不被接受)", path, derr)
	}
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]*Profile{}
	}
	if verr := cfg.validate(); verr != nil {
		return nil, fmt.Errorf("profile 配置非法(%s):%w", path, verr)
	}
	return cfg, nil
}

// validate:server/key 必填、server 必须 http(s):// URL(不带前缀的
// server 会让 --server 的别名解析产生歧义)、profile 名与全部别名共享
// 全局唯一命名空间、current 必须是规范名。
func (c *ProfileConfig) validate() error {
	seen := map[string]string{} // 名/别名 → 所属规范名
	for name, p := range c.Profiles {
		if name == "" {
			return fmt.Errorf("profile 名不能为空")
		}
		if p == nil {
			return fmt.Errorf("profile %q 内容为空", name)
		}
		if !isURL(p.Server) {
			return fmt.Errorf("profile %q 的 server %q 必须是 http(s):// URL", name, p.Server)
		}
		if p.Key == "" {
			return fmt.Errorf("profile %q 缺少 key", name)
		}
		if owner, dup := seen[name]; dup {
			return fmt.Errorf("命名冲突:%q 已属于 %q", name, owner)
		}
		seen[name] = name
	}
	// 别名按规范名排序后检查:冲突报错稳定指向「既有的占用者」,
	// 不随 map 迭代序漂移。
	names := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		p := c.Profiles[name]
		for _, a := range p.Aliases {
			if a == "" {
				return fmt.Errorf("profile %q 有空别名", name)
			}
			if owner, dup := seen[a]; dup {
				return fmt.Errorf("命名冲突:别名 %q 已被 %q 占用(名与别名共享全局命名空间)", a, owner)
			}
			seen[a] = name
		}
	}
	if c.Current != "" {
		if _, ok := c.Profiles[c.Current]; !ok {
			return fmt.Errorf("current %q 不是已存在的规范名(current 不接受别名)", c.Current)
		}
	}
	return nil
}

// saveProfiles 原子写:临时文件 + rename,防中途崩溃留下半截文件(§2.2)。
// CreateTemp 的创建模式即 0600(仅是默认创建模式,不是权限检查)。
func saveProfiles(cfg *ProfileConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	path, err := profileConfigPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("创建配置目录失败: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".profiles-*.tmp")
	if err != nil {
		return fmt.Errorf("创建临时文件失败: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后是 no-op
	if _, werr := tmp.Write(append(data, '\n')); werr != nil {
		tmp.Close()
		return fmt.Errorf("写临时文件失败: %w", werr)
	}
	if cerr := tmp.Close(); cerr != nil {
		return fmt.Errorf("关闭临时文件失败: %w", cerr)
	}
	return os.Rename(tmpName, path)
}

// resolveName 按规范名或别名精确解析(不做前缀/模糊,可预测性优先)。
func (c *ProfileConfig) resolveName(name string) (string, *Profile, bool) {
	if p, ok := c.Profiles[name]; ok {
		return name, p, true
	}
	for canonical, p := range c.Profiles {
		for _, a := range p.Aliases {
			if a == name {
				return canonical, p, true
			}
		}
	}
	return "", nil, false
}

// names 列出全部可用名(规范名在前、别名随后),错误提示用。
func (c *ProfileConfig) names() []string {
	out := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		out = append(out, name)
	}
	for _, p := range c.Profiles {
		out = append(out, p.Aliases...)
	}
	return out
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// maskKey 掩码显示:保留 rhk_ 前缀与前 8 hex + …。防的不是读文件
// (文件主人 cat 一下全都有),是终端输出被粘贴/截图进工单和群聊;
// 想看完整 key 直接 cat 文件,CLI 不提供 reveal(§2.2)。
func maskKey(k string) string {
	if strings.HasPrefix(k, "rhk_") && len(k) > 12 {
		return k[:12] + "…"
	}
	if len(k) > 8 {
		return k[:8] + "…"
	}
	return k
}
