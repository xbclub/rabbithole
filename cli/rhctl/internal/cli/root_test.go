package cli

import (
	"path/filepath"
	"testing"
	"time"
)

// isolateProfileConfig 把 profile 配置指向不存在的临时路径:取值链
// 测试必须与开发机/CI 机器上是否存在真实 profiles.json 无关。
func isolateProfileConfig(t *testing.T) {
	t.Helper()
	t.Setenv("RHCTL_CONFIG", filepath.Join(t.TempDir(), "profiles.json"))
	t.Setenv(envProfile, "")
}

// A-3 取值链:flag > env > 默认,server 与 key 各自独立(§2.2)。
// flag 显式给出用 FlagSet.Set 模拟(与命令行解析等效地置 Changed)。
func TestResolveOptionsFlagEnvDefault(t *testing.T) {
	t.Run("全部缺省走默认", func(t *testing.T) {
		isolateProfileConfig(t)
		t.Setenv(envServer, "")
		t.Setenv(envAPIKey, "")
		root := newRootCmd()
		o, err := resolveOptions(root)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if o.Server != defaultServer {
			t.Errorf("server=%q, want %q", o.Server, defaultServer)
		}
		if o.Key != "" {
			t.Errorf("key=%q, want 空", o.Key)
		}
		if o.Output != "table" {
			t.Errorf("output=%q, want table", o.Output)
		}
		if o.Timeout != 30*time.Second {
			t.Errorf("timeout=%v, want 30s", o.Timeout)
		}
		if o.Insecure {
			t.Errorf("insecure=true, want false")
		}
	})

	t.Run("env 高于默认", func(t *testing.T) {
		isolateProfileConfig(t)
		t.Setenv(envServer, "http://env:8080")
		t.Setenv(envAPIKey, "rhk_env")
		root := newRootCmd()
		o, err := resolveOptions(root)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if o.Server != "http://env:8080" {
			t.Errorf("server=%q, want env 值", o.Server)
		}
		if o.Key != "rhk_env" {
			t.Errorf("key=%q, want env 值", o.Key)
		}
	})

	t.Run("flag 高于 env", func(t *testing.T) {
		isolateProfileConfig(t)
		t.Setenv(envServer, "http://env:8080")
		t.Setenv(envAPIKey, "rhk_env")
		root := newRootCmd()
		if err := root.PersistentFlags().Set("server", "http://flag:9"); err != nil {
			t.Fatal(err)
		}
		if err := root.PersistentFlags().Set("key", "rhk_flag"); err != nil {
			t.Fatal(err)
		}
		o, err := resolveOptions(root)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if o.Server != "http://flag:9" {
			t.Errorf("server=%q, want flag 值", o.Server)
		}
		if o.Key != "rhk_flag" {
			t.Errorf("key=%q, want flag 值", o.Key)
		}
	})

	t.Run("server 与 key 各自独立取最高优先级", func(t *testing.T) {
		isolateProfileConfig(t)
		// flag 只给 server,env 只给 key:各走各的链(§2.2 定案)。
		t.Setenv(envAPIKey, "rhk_env")
		root := newRootCmd()
		if err := root.PersistentFlags().Set("server", "http://flag:9"); err != nil {
			t.Fatal(err)
		}
		o, err := resolveOptions(root)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if o.Server != "http://flag:9" || o.Key != "rhk_env" {
			t.Errorf("server=%q key=%q, want flag 的 server + env 的 key", o.Server, o.Key)
		}
	})

	t.Run("timeout 解析", func(t *testing.T) {
		isolateProfileConfig(t)
		root := newRootCmd()
		if err := root.PersistentFlags().Set("timeout", "90s"); err != nil {
			t.Fatal(err)
		}
		o, err := resolveOptions(root)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if o.Timeout != 90*time.Second {
			t.Errorf("timeout=%v, want 90s", o.Timeout)
		}
	})

	t.Run("timeout 非法值报错", func(t *testing.T) {
		isolateProfileConfig(t)
		for _, bad := range []string{"abc", "0s", "-5s"} {
			root := newRootCmd()
			if err := root.PersistentFlags().Set("timeout", bad); err != nil {
				t.Fatal(err)
			}
			if _, err := resolveOptions(root); err == nil {
				t.Errorf("timeout=%q 应报错", bad)
			}
		}
	})

	t.Run("output 非法值报错,json 合法", func(t *testing.T) {
		isolateProfileConfig(t)
		root := newRootCmd()
		if err := root.PersistentFlags().Set("output", "yaml"); err != nil {
			t.Fatal(err)
		}
		if _, err := resolveOptions(root); err == nil {
			t.Error("output=yaml 应报错")
		}
		root2 := newRootCmd()
		if err := root2.PersistentFlags().Set("output", "json"); err != nil {
			t.Fatal(err)
		}
		o, err := resolveOptions(root2)
		if err != nil || o.Output != "json" {
			t.Errorf("output=json 不应报错, got err=%v output=%q", err, o.Output)
		}
	})
}
