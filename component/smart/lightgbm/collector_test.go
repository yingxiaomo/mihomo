//go:build (linux && (amd64 || arm64 || 386 || arm || riscv64 || loong64 || ppc64le || s390x)) || (darwin && (amd64 || arm64)) || (windows && (amd64 || arm64 || 386)) || (freebsd && (amd64 || arm64))

package lightgbm

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCollectorSQLiteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "smart_samples.db")

	c := &PLDDataCollector{
		dataPath:           dbPath,
		smartCollectorSize: defaultPLDCollectorSize,
	}

	if err := c.initializeDB(); err != nil {
		t.Fatalf("initializeDB: %v", err)
	}
	defer c.Close()

	// 写一条完整 44 列样本
	features := make([]interface{}, 30)
	for i := range features {
		features[i] = float64(i + 1)
	}
	args := append(features,
		"group-a", "node-1", "example.com",
		int64(120), float64(10),
		"as13335", "www.example.com", "1.2.3.4", uint64(443), "US|CA",
		0.5, "Traditional", 0.3, time.Now().Unix(),
	)
	if _, err := c.db.Exec(insertSampleSQL, args...); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// 读回验证
	var (
		group, node, target, host string
		download, reward          float64
		ts                        int64
	)
	err := c.db.QueryRow(`SELECT group_name, node_name, target, host_raw, download_mb, reward, ts FROM samples`).
		Scan(&group, &node, &target, &host, &download, &reward, &ts)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if group != "group-a" || node != "node-1" || target != "example.com" || host != "www.example.com" {
		t.Fatalf("metadata mismatch: %q %q %q %q", group, node, target, host)
	}
	if download != 9 { // features[8] = 9.0 → download_mb（第 9 个特征列）
		t.Fatalf("download_mb=%v want 9", download)
	}
	if reward != 0.3 {
		t.Fatalf("reward=%v want 0.3", reward)
	}
	if ts <= 0 {
		t.Fatalf("ts=%v want unix seconds", ts)
	}

	// 文件确实存在且非空
	if st, err := os.Stat(dbPath); err != nil || st.Size() == 0 {
		t.Fatalf("db file missing or empty: %v", err)
	}
}

func TestCollectorSQLiteRetention(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "smart_samples.db")

	c := &PLDDataCollector{dataPath: dbPath, smartCollectorSize: defaultPLDCollectorSize}
	if err := c.initializeDB(); err != nil {
		t.Fatalf("initializeDB: %v", err)
	}
	defer c.Close()

	old := time.Now().Add(-(sampleRetentionDays + 2) * 24 * time.Hour).Unix()
	fresh := time.Now().Unix()
	if _, err := c.db.Exec(`INSERT INTO samples (ts, group_name) VALUES (?, 'old'), (?, 'new')`, old, fresh); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// 重新初始化（模拟重启）应触发清理
	c.closeDB()
	if err := c.initializeDB(); err != nil {
		t.Fatalf("re-initializeDB: %v", err)
	}

	var n int
	if err := c.db.QueryRow(`SELECT COUNT(*) FROM samples`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("after retention prune count=%d, want 1 (fresh row kept, old row pruned)", n)
	}
}

func TestCollectorInitSetsDataPath(t *testing.T) {
	// InitCollector 使用 HomeDir 路径；这里只验证路径文件名正确性
	old := pldSmartCollector
	defer func() { pldSmartCollector = old }()
	InitPLDCollector(0)
	if pldSmartCollector == nil {
		t.Fatal("InitPLDCollector(0) should create collector")
	}
	if filepath.Base(pldSmartCollector.dataPath) != "smart_samples.db" {
		t.Fatalf("dataPath=%q want smart_samples.db", pldSmartCollector.dataPath)
	}
	if pldSmartCollector.smartCollectorSize != defaultPLDCollectorSize {
		t.Fatalf("default size=%d", pldSmartCollector.smartCollectorSize)
	}
}