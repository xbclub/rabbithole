package cli

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// 两台管理端的样例配置:prod(别名 p)+ lab(别名 lb,自签)。
const profileFileTwo = `{
  "current": "prod",
  "profiles": {
    "prod": {"server": "http://10.0.0.1:8080", "key": "rhk_1111aaaa_ProfileSecretA", "insecure": false, "aliases": ["p"]},
    "lab":  {"server": "https://10.0.0.2:8443", "key": "rhk_2222bbbb_ProfileSecretB", "insecure": true,  "aliases": ["lb"]}
  }
}`

// setProfileFile 把 RHCTL_CONFIG 指到临时文件;content 为空则不创建
// (文件不存在路径)。
func setProfileFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "profiles.json")
	if content != "" {
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("RHCTL_CONFIG", p)
	t.Setenv(envProfile, "")
	return p
}

// I-4/I-5:优先级矩阵 —— flag / env / profile / 默认 的交叉。
func TestProfile_PriorityMatrix(t *testing.T) {
	cases := []struct {
		name       string
		file       string
		args       []string // 附加 flag(--profile/--server/--key/--insecure)
		envServer  string
		envKey     string
		wantServer string
		wantKey    string
		wantInsec  bool
		wantProf   string
	}{
		{
			name: "无配置文件走默认", file: "",
			wantServer: defaultServer, wantKey: "",
		},
		{
			name: "current profile 兜底", file: profileFileTwo,
			wantServer: "http://10.0.0.1:8080", wantKey: "rhk_1111aaaa_ProfileSecretA", wantProf: "prod",
		},
		{
			name: "env profile 选择 lab", file: profileFileTwo,
			args:       nil,
			wantServer: "https://10.0.0.2:8443", wantKey: "rhk_2222bbbb_ProfileSecretB",
			wantInsec: true, wantProf: "lab",
		},
		{
			name: "flag --profile 别名 p 解析 prod", file: profileFileTwo,
			args:       []string{"--profile", "p"},
			wantServer: "http://10.0.0.1:8080", wantKey: "rhk_1111aaaa_ProfileSecretA", wantProf: "prod",
		},
		{
			name: "flag server URL + profile key(各自独立)", file: profileFileTwo,
			args:       []string{"--server", "http://flag:9"},
			wantServer: "http://flag:9", wantKey: "rhk_1111aaaa_ProfileSecretA", wantProf: "prod",
		},
		{
			name: "flag key 覆盖 profile key", file: profileFileTwo,
			args:       []string{"--key", "rhk_temp"},
			wantServer: "http://10.0.0.1:8080", wantKey: "rhk_temp", wantProf: "prod",
		},
		{
			name: "env key 高于 profile key", file: profileFileTwo,
			envKey:     "rhk_envk",
			wantServer: "http://10.0.0.1:8080", wantKey: "rhk_envk", wantProf: "prod",
		},
		{
			name: "--server 别名 = 整 profile 生效(lab:server+key+insecure)", file: profileFileTwo,
			args:       []string{"--server", "lb"},
			wantServer: "https://10.0.0.2:8443", wantKey: "rhk_2222bbbb_ProfileSecretB",
			wantInsec: true, wantProf: "lab",
		},
		{
			name: "--server 别名后 --key 仍可覆盖", file: profileFileTwo,
			args:       []string{"--server", "lb", "--key", "rhk_tmp"},
			wantServer: "https://10.0.0.2:8443", wantKey: "rhk_tmp",
			wantInsec: true, wantProf: "lab",
		},
		{
			name: "--server 别名后 --insecure 仍可覆盖", file: profileFileTwo,
			args:       []string{"--server", "lb", "--insecure", "false"},
			wantServer: "https://10.0.0.2:8443", wantKey: "rhk_2222bbbb_ProfileSecretB",
			wantInsec: false, wantProf: "lab",
		},
		{
			name: "env server(URL)高于 profile 的 server", file: profileFileTwo,
			envServer:  "http://envsrv:7",
			wantServer: "http://envsrv:7", wantKey: "rhk_1111aaaa_ProfileSecretA", wantProf: "prod",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setProfileFile(t, c.file)
			t.Setenv(envServer, c.envServer)
			t.Setenv(envAPIKey, c.envKey)
			if c.name == "env profile 选择 lab" {
				t.Setenv(envProfile, "lab")
			}
			root := newRootCmd()
			for i := 0; i+1 < len(c.args); i += 2 {
				name := strings.TrimLeft(c.args[i], "-")
				if err := root.PersistentFlags().Set(name, c.args[i+1]); err != nil {
					t.Fatal(err)
				}
			}
			o, err := resolveOptions(root)
			if err != nil {
				t.Fatalf("err=%v", err)
			}
			if o.Server != c.wantServer || o.Key != c.wantKey || o.Insecure != c.wantInsec || o.Profile != c.wantProf {
				t.Errorf("got server=%q key=%q insecure=%v profile=%q\nwant server=%q key=%q insecure=%v profile=%q",
					o.Server, o.Key, o.Insecure, o.Profile, c.wantServer, c.wantKey, c.wantInsec, c.wantProf)
			}
		})
	}
}

// I-1:拼错的键拒载(DisallowUnknownFields),不静默忽略。
func TestProfile_UnknownFieldRejected(t *testing.T) {
	setProfileFile(t, `{"current":"p","profiles":{"p":{"server":"http://a:1","key":"rhk_x","typo":1}}}`)
	root := newRootCmd()
	if _, err := resolveOptions(root); err == nil || !strings.Contains(err.Error(), "拼错") {
		t.Errorf("err=%v, want 未知字段拒载", err)
	}
}

// I-2:命名空间全局唯一 —— 冲突即拒载。
func TestProfile_NamespaceConflict(t *testing.T) {
	t.Run("两 profile 共用别名", func(t *testing.T) {
		setProfileFile(t, `{"profiles":{
			"a": {"server":"http://a:1","key":"rhk_a","aliases":["x"]},
			"b": {"server":"http://b:1","key":"rhk_b","aliases":["x"]}}}`)
		if _, err := loadProfiles(); err == nil || !strings.Contains(err.Error(), "命名冲突") {
			t.Errorf("err=%v, want 命名冲突拒载", err)
		}
	})
	t.Run("别名撞他者规范名", func(t *testing.T) {
		setProfileFile(t, `{"profiles":{
			"a": {"server":"http://a:1","key":"rhk_a"},
			"b": {"server":"http://b:1","key":"rhk_b","aliases":["a"]}}}`)
		if _, err := loadProfiles(); err == nil || !strings.Contains(err.Error(), "命名冲突") {
			t.Errorf("err=%v, want 命名冲突拒载", err)
		}
	})
	t.Run("current 指向别名", func(t *testing.T) {
		setProfileFile(t, `{"current":"p","profiles":{
			"prod": {"server":"http://a:1","key":"rhk_a","aliases":["p"]}}}`)
		if _, err := loadProfiles(); err == nil || !strings.Contains(err.Error(), "规范名") {
			t.Errorf("err=%v, want current 非规范名拒载", err)
		}
	})
	t.Run("server 缺 URL 前缀拒载", func(t *testing.T) {
		setProfileFile(t, `{"profiles":{"a":{"server":"10.0.0.1:8080","key":"rhk_a"}}}`)
		if _, err := loadProfiles(); err == nil {
			t.Error("server 非 URL 应拒载(会让 --server 别名解析歧义)")
		}
	})
}

// I-2:未知名/别名 → exit 2 且列出可用名。
func TestProfile_UnknownNameListsAvailable(t *testing.T) {
	setProfileFile(t, profileFileTwo)
	t.Run("--profile 未知名", func(t *testing.T) {
		root := newRootCmd()
		if err := root.PersistentFlags().Set("profile", "ghost"); err != nil {
			t.Fatal(err)
		}
		_, err := resolveOptions(root)
		if err == nil {
			t.Fatal("应报错")
		}
		for _, want := range []string{"prod", "p", "lab", "lb"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("错误应列出可用名 %q:%v", want, err)
			}
		}
	})
	t.Run("--server 未知别名", func(t *testing.T) {
		root := newRootCmd()
		if err := root.PersistentFlags().Set("server", "ghost"); err != nil {
			t.Fatal(err)
		}
		if _, err := resolveOptions(root); err == nil || !strings.Contains(err.Error(), "可用") {
			t.Errorf("err=%v, want 列可用名", err)
		}
	})
}

// I-3:add → load JSON 往返。
func TestProfile_AddLoadRoundtrip(t *testing.T) {
	path := setProfileFile(t, "")
	_, err := runCLI(t, "profile", "add", "edge",
		"--server", "http://10.9.9.9:8080", "--key", "rhk_9999cccc_RoundTrip",
		"--alias", "e", "--alias", "edge2")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	cfg, err := loadProfiles()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	p, ok := cfg.Profiles["edge"]
	if !ok || p.Server != "http://10.9.9.9:8080" || p.Key != "rhk_9999cccc_RoundTrip" ||
		len(p.Aliases) != 2 || p.Aliases[0] != "e" || p.Aliases[1] != "edge2" {
		t.Errorf("roundtrip 后 profile=%+v", p)
	}
	// 文件内容是合法 JSON 且带缩进(人可编辑)。
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), "\"edge\"") || !strings.Contains(string(raw), "\n  ") {
		t.Errorf("落盘文件应是人类可读的 JSON:\n%s", raw)
	}
}

// I-3:add 重复名与别名冲突。
func TestProfile_AddConflicts(t *testing.T) {
	t.Run("同名无 --force 报错", func(t *testing.T) {
		setProfileFile(t, profileFileTwo)
		_, err := runCLI(t, "profile", "add", "prod", "--server", "http://x:1", "--key", "rhk_k")
		if err == nil || !strings.Contains(err.Error(), "--force") {
			t.Errorf("err=%v, want 已存在提示", err)
		}
	})
	t.Run("同名 --force 覆盖", func(t *testing.T) {
		setProfileFile(t, profileFileTwo)
		_, err := runCLI(t, "profile", "add", "prod", "--server", "http://new:1",
			"--key", "rhk_new", "--force")
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		cfg, _ := loadProfiles()
		if cfg.Profiles["prod"].Server != "http://new:1" {
			t.Errorf("覆盖失败:%+v", cfg.Profiles["prod"])
		}
	})
	t.Run("别名撞既有名报错", func(t *testing.T) {
		setProfileFile(t, profileFileTwo)
		_, err := runCLI(t, "profile", "add", "newp", "--server", "http://x:1",
			"--key", "rhk_k", "--alias", "lb")
		if err == nil || !strings.Contains(err.Error(), "命名冲突") {
			t.Errorf("err=%v, want 命名冲突", err)
		}
	})
	t.Run("server 无 URL 前缀报错", func(t *testing.T) {
		setProfileFile(t, "")
		_, err := runCLI(t, "profile", "add", "x", "--server", "10.0.0.1:8080", "--key", "rhk_k")
		if err == nil {
			t.Error("server 无前缀应报错")
		}
	})
}

// I-3:--key-stdin。
func TestProfile_AddKeyStdin(t *testing.T) {
	setProfileFile(t, "")
	withStdin(t, "rhk_7777dddd_FromStdin\n")
	if _, err := runCLI(t, "profile", "add", "s1", "--server", "http://x:1", "--key-stdin"); err != nil {
		t.Fatalf("err=%v", err)
	}
	cfg, err := loadProfiles()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Profiles["s1"].Key != "rhk_7777dddd_FromStdin" {
		t.Errorf("key=%q", cfg.Profiles["s1"].Key)
	}
	// --key 与 --key-stdin 互斥。
	withStdin(t, "x")
	if _, err := runCLI(t, "profile", "add", "s2", "--server", "http://x:1",
		"--key", "rhk_a", "--key-stdin"); err == nil {
		t.Error("互斥应报错")
	}
}

// I-3:use 接受别名写入规范名;remove 删 current 同步清空。
func TestProfile_UseRemove(t *testing.T) {
	t.Run("use 别名写规范名", func(t *testing.T) {
		setProfileFile(t, profileFileTwo)
		if _, err := runCLI(t, "profile", "use", "lb"); err != nil {
			t.Fatalf("err=%v", err)
		}
		cfg, _ := loadProfiles()
		if cfg.Current != "lab" {
			t.Errorf("current=%q, want lab(规范名)", cfg.Current)
		}
	})
	t.Run("use 未知名列出可用", func(t *testing.T) {
		setProfileFile(t, profileFileTwo)
		_, err := runCLI(t, "profile", "use", "ghost")
		if err == nil || !strings.Contains(err.Error(), "prod") {
			t.Errorf("err=%v", err)
		}
	})
	t.Run("remove 别名删规范名", func(t *testing.T) {
		setProfileFile(t, profileFileTwo)
		if _, err := runCLI(t, "profile", "remove", "lb"); err != nil {
			t.Fatalf("err=%v", err)
		}
		cfg, _ := loadProfiles()
		if _, ok := cfg.Profiles["lab"]; ok {
			t.Error("lab 应被删除")
		}
		if cfg.Current != "prod" {
			t.Errorf("current=%q, want prod 不受影响", cfg.Current)
		}
	})
	t.Run("remove current 同步清空", func(t *testing.T) {
		setProfileFile(t, profileFileTwo)
		if _, err := runCLI(t, "profile", "remove", "p"); err != nil {
			t.Fatalf("err=%v", err)
		}
		cfg, _ := loadProfiles()
		if _, ok := cfg.Profiles["prod"]; ok {
			t.Error("prod 应被删除")
		}
		if cfg.Current != "" {
			t.Errorf("current=%q, want 清空", cfg.Current)
		}
	})
}

// I-3/I-5:list 输出不得含任何完整 key(脱敏)。
func TestProfile_ListMasksKey(t *testing.T) {
	setProfileFile(t, profileFileTwo)
	out, err := runCLI(t, "profile", "list")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if strings.Contains(out, "ProfileSecretA") || strings.Contains(out, "ProfileSecretB") {
		t.Errorf("list 泄漏完整 key:\n%s", out)
	}
	for _, want := range []string{"prod", "p", "lb", "rhk_1111aaaa…", "rhk_2222bbbb…", "*", "true"} {
		if !strings.Contains(out, want) {
			t.Errorf("list 缺 %q:\n%s", want, out)
		}
	}
}

// I-1:原子写 —— 落盘内容完整、无 .tmp 残留。
func TestProfile_AtomicWrite(t *testing.T) {
	path := setProfileFile(t, `{"profiles":{"old":{"server":"http://old:1","key":"rhk_old"}}}`)
	cfg, err := loadProfiles()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Profiles["new"] = &Profile{Server: "http://new:1", Key: "rhk_new", Aliases: []string{"n"}}
	if err := saveProfiles(cfg); err != nil {
		t.Fatal(err)
	}
	// 目录里只有 profiles.json,没有半截临时文件。
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".profiles-") || strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("残留临时文件:%s", e.Name())
		}
	}
	// 内容完整可回读。
	cfg2, err := loadProfiles()
	if err != nil {
		t.Fatalf("回读失败:%v", err)
	}
	if _, ok := cfg2.Profiles["old"]; !ok {
		t.Error("旧 profile 丢失")
	}
	if cfg2.Profiles["new"].Key != "rhk_new" {
		t.Errorf("新 profile 不完整:%+v", cfg2.Profiles["new"])
	}
	// 文件权限 0600(创建模式,非检查)。
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o600 {
		t.Errorf("权限=%v, want 0600", info.Mode().Perm())
	}
}

// maskKey 单元:rhk_ 前缀保留 8 hex;短值截断;不展示全文。
func TestProfile_MaskKey(t *testing.T) {
	full := "rhk_1111aaaa_" + strings.Repeat("z", 64)
	if got := maskKey(full); got != "rhk_1111aaaa…" {
		t.Errorf("maskKey=%q", got)
	}
	if got := maskKey("short"); got != "short" {
		t.Errorf("短值应原样:%q", got)
	}
	if got := maskKey("123456789"); got != "12345678…" {
		t.Errorf("非 rhk_ 前缀:%q", got)
	}
}

// 包 I 验收:两个 profile 指向两台本地服务,--profile <别名> 与
// --server <别名> 两条路径分别打通 stats overview。
func TestProfile_TwoServersOverview(t *testing.T) {
	mkSrv := func(total string) (*httptest.Server, *int32) {
		var hits int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits, 1)
			if r.URL.Path != "/api/stats/overview" {
				t.Errorf("path=%s", r.URL.Path)
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"clients_total":` + total + `,"clients_online":0,"services_total":0,"services_running":0,"conns_current":0,"bytes_in":0,"bytes_out":0},"message":"ok"}`))
		}))
		t.Cleanup(srv.Close)
		return srv, &hits
	}
	srvA, hitsA := mkSrv("11")
	srvB, hitsB := mkSrv("22")
	setProfileFile(t, `{"current":"a","profiles":{
		"a": {"server":"`+srvA.URL+`", "key":"rhk_aaaa_KeyOfA", "aliases":["al"]},
		"b": {"server":"`+srvB.URL+`", "key":"rhk_bbbb_KeyOfB", "aliases":["bl"]}}}`)

	// --profile <别名 al>:走 a 服务端。
	out, err := runCLI(t, "stats", "overview", "--profile", "al")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if !strings.Contains(out, "11") {
		t.Errorf("--profile al 应打 a 服务端(11):\n%s", out)
	}

	// --server <别名 bl>:整 profile 生效,走 b 服务端。
	out, err = runCLI(t, "stats", "overview", "--server", "bl")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if !strings.Contains(out, "22") {
		t.Errorf("--server bl 应打 b 服务端(22):\n%s", out)
	}
	if atomic.LoadInt32(hitsA) != 1 || atomic.LoadInt32(hitsB) != 1 {
		t.Errorf("hits a=%d b=%d, want 各 1 次", *hitsA, *hitsB)
	}
}
