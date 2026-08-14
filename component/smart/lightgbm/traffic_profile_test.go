package lightgbm

import "testing"

func TestLookupBuiltinTrafficLevel(t *testing.T) {
	cases := []struct {
		host  string
		want  int
	}{
		// 纯视频 CDN 子域 → heavy（内置表核心价值：域名挡在正确子域上）
		{"r1---sn-xxx.googlevideo.com", TrafficLevelHeavy},
		{"xxx.gvt1.com", TrafficLevelHeavy},
		{"upos-sz-mirror08.bilivideo.com", TrafficLevelHeavy},
		{"ipv4-c101-xxx.nflxvideo.net", TrafficLevelHeavy},
		{"manifest.googlevideo.com", TrafficLevelHeavy},
		// 混合 / 音频 CDN → medium
		{"a248.e.akamai.net.akamaized.net", TrafficLevelMedium},
		{"cf-img.bmcdn.net", TrafficLevelMedium},
		// 应用壳域名 → 不判 heavy（youtube.com 只是 API 壳，视频在 googlevideo.com）
		{"www.youtube.com", TrafficLevelUnknown},
		{"www.netflix.com", TrafficLevelUnknown},
		{"www.bilibili.com", TrafficLevelUnknown},
		// 普通网站 → unknown
		{"news.ycombinator.com", TrafficLevelUnknown},
		{"www.wikipedia.org", TrafficLevelUnknown},
		// 大小写不敏感
		{"MEDIA.GOOGLEVIDEO.COM", TrafficLevelHeavy},
	}
	for _, c := range cases {
		if got := lookupBuiltinTrafficLevel(c.host); got != c.want {
			t.Errorf("lookupBuiltinTrafficLevel(%q)=%d, want %d", c.host, got, c.want)
		}
	}
}

func TestUpdateTargetTrafficLevelThresholds(t *testing.T) {
	// downloadTotalMB 参数单位为 MB；avg = totalMB / sampleCount（MiB/次）。
	// avg 100 MiB/sample → heavy
	UpdateTargetTrafficLevel("vid.example.com", 1000, 0, 10)
	if v, ok := getTrafficProfileCache().Get("vid.example.com"); !ok || v != TrafficLevelHeavy {
		t.Fatal("100MiB/sample should be heavy")
	}
	// avg 1 MiB/sample → medium
	UpdateTargetTrafficLevel("mix.example.com", 10, 0, 10)
	if v, ok := getTrafficProfileCache().Get("mix.example.com"); !ok || v != TrafficLevelMedium {
		t.Fatal("1MiB/sample should be medium")
	}
	// avg 100 KiB/sample → small
	UpdateTargetTrafficLevel("web.example.com", 1, 0, 10) // 0.1 MiB/sample
	if v, ok := getTrafficProfileCache().Get("web.example.com"); !ok || v != TrafficLevelSmall {
		t.Fatal("0.1MiB/sample should be small")
	}
	// boundary: exactly 512 KiB → medium
	UpdateTargetTrafficLevel("edge.example.com", 5, 0, 10) // 0.5 MiB/sample
	if v, ok := getTrafficProfileCache().Get("edge.example.com"); !ok || v != TrafficLevelMedium {
		t.Fatal("512KiB boundary should be medium")
	}
	// boundary: exactly 4 MiB → heavy
	UpdateTargetTrafficLevel("e2.example.com", 40, 0, 10)
	if v, ok := getTrafficProfileCache().Get("e2.example.com"); !ok || v != TrafficLevelHeavy {
		t.Fatal("4MiB boundary should be heavy")
	}
	// peak rate short-circuit: small avg but huge peak → heavy
	UpdateTargetTrafficLevel("spike.example.com", 1, 10000, 10)
	if v, ok := getTrafficProfileCache().Get("spike.example.com"); !ok || v != TrafficLevelHeavy {
		t.Fatal("peak 10MB/s should force heavy")
	}
	// zero/invalid sample count is ignored
	UpdateTargetTrafficLevel("bad.example.com", 100, 0, 0)
	if _, ok := getTrafficProfileCache().Get("bad.example.com"); ok {
		t.Fatal("sampleCount<=0 must not write cache")
	}
}

func TestLookupDomainTrafficLevelPriority(t *testing.T) {
	// builtin table wins over cached profile
	UpdateTargetTrafficLevel("googlevideo.com", 1.0, 0, 10) // would be small via profile
	if got := LookupDomainTrafficLevel("www.googlevideo.com", "googlevideo.com"); got != TrafficLevelHeavy {
		t.Fatalf("builtin should override profile, got %d", got)
	}
	// profile fills in for unknown hosts
	UpdateTargetTrafficLevel("unknown-art.example.com", 50, 0, 50) // 1MiB/sample
	if got := LookupDomainTrafficLevel("api.unknown-art.example.com", "unknown-art.example.com"); got != TrafficLevelMedium {
		t.Fatalf("profile should provide level for unknown host, got %d", got)
	}
	// no signal anywhere → unknown
	if got := LookupDomainTrafficLevel("brand-new-site.com", "brand-new-site.com"); got != TrafficLevelUnknown {
		t.Fatalf("cold target should be unknown, got %d", got)
	}
}