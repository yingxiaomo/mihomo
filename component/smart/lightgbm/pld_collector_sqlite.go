//go:build (linux && (amd64 || arm64 || 386 || arm || riscv64 || loong64 || ppc64le || s390x)) || (darwin && (amd64 || arm64)) || (windows && (amd64 || arm64 || 386)) || (freebsd && (amd64 || arm64))

// Package lightgbm SQLite collector. modernc.org/libc (the pure-Go SQLite
// backend) does not support every GOOS/GOARCH the release matrix ships
// (linux mips/mipsle/mips64/mips64le and freebsd/386 fail to compile), so this
// file is restricted to the supported set and collector_csv.go is the fallback
// on the remaining platforms.
package lightgbm

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	_ "modernc.org/sqlite" // pure-Go SQLite driver, works with CGO_ENABLED=0
)

var (
	pldCollectMutex   sync.Mutex
	pldSmartCollector *PLDDataCollector
)

// PLDDataCollector appends one training sample per connection into a single
// SQLite database (smart_samples.db). Schema mirrors the previous 44-column
// CSV (30 model features + 14 metadata columns) so the training pipeline can
// keep reading the same columns, now with SQL slicing/aggregation instead of
// pandas full-file parsing.
type PLDDataCollector struct {
	mutex              sync.Mutex
	sampleCount        int
	dataPath           string
	db                 *sql.DB
	configured         bool
	smartCollectorSize int64
	lastFileCheck      time.Time
}

const (
	defaultPLDCollectorSize = 100 * 1024 * 1024
	// sampleRetentionDays keeps two training cycles (7-day collection × 2) of
	// samples; older rows are pruned on startup.
	sampleRetentionDays = 14
)

// createTableSQL mirrors the previous CSV header row (44 columns). ts is
// stored as unix seconds so retention pruning is a simple numeric comparison
// and the trainer can parse it with pd.to_datetime(ts, unit='s').
const createTableSQL = `
CREATE TABLE IF NOT EXISTS samples (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	success REAL, failure REAL, connect_time REAL, latency REAL,
	upload_mb REAL, history_upload_mb REAL, maxuploadrate_kb REAL, history_maxuploadrate_kb REAL,
	download_mb REAL, history_download_mb REAL, maxdownloadrate_kb REAL, history_maxdownloadrate_kb REAL,
	duration_minutes REAL, history_duration_minutes REAL, last_used_seconds REAL,
	is_udp REAL, is_tcp REAL, loss_rate REAL, cumul_loss_rate REAL,
	asn_feature REAL, country_feature REAL, address_feature REAL, port_feature REAL,
	traffic_ratio REAL, traffic_density REAL, connection_type_feature REAL,
	asn_hash REAL, host_hash REAL, ip_hash REAL, geoip_hash REAL,
	group_name TEXT, node_name TEXT, target TEXT,
	node_delay INTEGER, node_type REAL,
	asn_raw TEXT, host_raw TEXT, ip_raw TEXT, port_raw TEXT, geoip_raw TEXT,
	weight REAL, weight_source TEXT, reward REAL,
	sample_weight REAL NOT NULL DEFAULT 1.0,
	ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples(ts);
CREATE INDEX IF NOT EXISTS idx_samples_target ON samples(target);`

const insertSampleSQL = `
INSERT INTO samples (
	success, failure, connect_time, latency,
	upload_mb, history_upload_mb, maxuploadrate_kb, history_maxuploadrate_kb,
	download_mb, history_download_mb, maxdownloadrate_kb, history_maxdownloadrate_kb,
	duration_minutes, history_duration_minutes, last_used_seconds,
	is_udp, is_tcp, loss_rate, cumul_loss_rate,
	asn_feature, country_feature, address_feature, port_feature,
	traffic_ratio, traffic_density, connection_type_feature,
	asn_hash, host_hash, ip_hash, geoip_hash,
	group_name, node_name, target, node_delay, node_type,
	asn_raw, host_raw, ip_raw, port_raw, geoip_raw,
	weight, weight_source, reward, sample_weight, ts
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

func InitPLDCollector(collectSize float64) {
	var pldSmartCollectorSize int64
	if collectSize > 0 {
		pldSmartCollectorSize = int64(collectSize * 1024 * 1024)
	} else {
		pldSmartCollectorSize = defaultPLDCollectorSize
	}

	pldSmartCollector = &PLDDataCollector{
		dataPath:           filepath.Join(C.Path.HomeDir(), "smart_samples.db"),
		smartCollectorSize: pldSmartCollectorSize,
	}
}

func GetPLDCollector() *PLDDataCollector {
	return pldSmartCollector
}

func (c *PLDDataCollector) AddSample(input *smart.ModelInput, metadata *C.Metadata, actualWeight, reward float64, weightSource string, nodeDelayMs int64, nodeType float64, sampleWeight float64) {
	if c == nil {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.configured && time.Since(c.lastFileCheck) > 5*time.Second {
		c.lastFileCheck = time.Now()
		if _, err := os.Stat(c.dataPath); os.IsNotExist(err) {
			log.Infoln("[Smart] Data file was deleted, reinitializing collector")
			c.closeDB()
		}
	}

	// 检查文件大小限制
	if c.db != nil {
		if stat, err := os.Stat(c.dataPath); err == nil && stat.Size() > c.smartCollectorSize {
			log.Infoln("[Smart] Maximum file size limit reached (%d MB), stopping data collection", c.smartCollectorSize/(1024*1024))
			return
		}
	}

	if !c.configured {
		if err := c.initializeDB(); err != nil {
			log.Warnln("[Smart] Failed to initialize training data collector: %v", err)
			return
		}
	}

	features := prepareFeatures(input)
	if len(features) == 0 {
		log.Debugln("[Smart] Feature extraction failed, skipping sample collection")
		return
	}

	var geoIPStr string
	if metadata.DstGeoIP != nil {
		geoIPStr = strings.Join(metadata.DstGeoIP, "|")
	} else {
		geoIPStr = "unknown"
	}

	var dstASN string
	if metadata.DstIPASN != "" {
		dstASN = metadata.DstIPASN
	} else {
		dstASN = "unknown"
	}

	dstIP := "unknown"
	if metadata.DstIP.IsValid() {
		dstIP = metadata.DstIP.String()
	}

	host := "unknown"
	if metadata.Host != "" {
		host = metadata.Host
	}

	// 归一化目标（wildcard 域名或 IP）。优先 SmartTarget（selectProxies 写入），
	// 兜底 WildcardTarget/host，供训练脚本按 (target, node) 分组计算 reward EMA，
	// 与内核 stats 的 (group, config, target, node) 键语义对齐。
	target := metadata.SmartTarget
	if target == "" {
		target = metadata.WildcardTarget
	}
	if target == "" {
		target = host
	}

	standardizedSource := weightSource
	if standardizedSource == "" {
		standardizedSource = "unknown"
	}

	args := make([]interface{}, 0, 44)
	for _, f := range features {
		args = append(args, f)
	}
	args = append(args,
		input.GroupName,
		input.NodeName,
		target,
		nodeDelayMs,
		nodeType,
		dstASN,
		host,
		dstIP,
		uint64(metadata.DstPort),
		geoIPStr,
		actualWeight,
		standardizedSource,
		reward,
		sampleWeight,
		time.Now().Unix(),
	)

	if _, err := c.db.Exec(insertSampleSQL, args...); err != nil {
		log.Warnln("[Smart] Failed to write training sample: %v", err)
		c.closeDB()
		return
	}

	c.sampleCount++
}

func (c *PLDDataCollector) initializeDB() error {
	log.Infoln("[Smart] Initializing data collector for %s", c.dataPath)

	// journal_mode stays the default DELETE (not WAL): collection volume is low
	// (one INSERT per connection) so WAL buys nothing, while DELETE + FULL means
	// the main db file is always a complete, consistent snapshot — the weekly
	// upcsv job can rclone-copy it directly without checkpointing, and crashes
	// stay safe via the rollback journal.
	dsn := "file:" + c.dataPath +
		"?_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	// Single writer; avoid SQLITE_BUSY contention from pooled connections.
	db.SetMaxOpenConns(1)

	// Force DELETE journal mode explicitly: journal_mode is persisted in the
	// db file header, so a database previously created under WAL stays in WAL
	// even when the DSN omits it — the weekly upload would then copy only the
	// 4KB main file and lose everything. This also checkpoints any leftover
	// WAL into the main file on first open.
	if _, err := db.Exec(`PRAGMA journal_mode=DELETE`); err != nil {
		db.Close()
		return err
	}

	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return err
	}

	// 老库兼容：早期 smart_samples.db 没有 sample_weight 列（新 INSERT 语句会
	// 失败），这里在线补列。历史行取默认 1.0（等权），新样本写入时覆盖为按
	// 流量计算的对数权重。duplicate-column 错误说明列已存在，忽略即可。
	if _, err := db.Exec(`ALTER TABLE samples ADD COLUMN sample_weight REAL NOT NULL DEFAULT 1.0`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") &&
			!strings.Contains(err.Error(), "already exists") {
			log.Debugln("[Smart] ALTER samples add sample_weight skipped: %v", err)
		}
	}

	// 保留两周期（14 天），更早的样本按时间戳清理
	cutoff := time.Now().Add(-sampleRetentionDays * 24 * time.Hour).Unix()
	if res, err := db.Exec(`DELETE FROM samples WHERE ts < ?`, cutoff); err == nil {
		if n, _ := res.RowsAffected(); n > 0 {
			log.Debugln("[Smart] Pruned %d samples older than %d days", n, sampleRetentionDays)
		}
	}

	c.db = db
	c.configured = true
	return nil
}

func (c *PLDDataCollector) closeDB() {
	if c.db != nil {
		c.db.Close()
		c.db = nil
	}
	c.configured = false
}

func (c *PLDDataCollector) Flush() error {
	if c == nil || c.db == nil {
		return nil
	}
	return nil
}

func (c *PLDDataCollector) Close() error {
	if c == nil {
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.closeDB()
	return nil
}

func CloseAllPLDCollectors() {
	pldCollectMutex.Lock()
	defer pldCollectMutex.Unlock()

	if pldSmartCollector != nil {
		pldSmartCollector.Close()
	}
}
