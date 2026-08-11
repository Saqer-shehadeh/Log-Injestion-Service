-- ضبط إعدادات الأداء لسرعة الإدخال
SET synchronous_commit = off;

-- إنشاء جدول اللوجات كـ Partitioned Table حسب الشهر/اليوم
CREATE TABLE IF NOT EXISTS logs (
    id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    level VARCHAR(10) NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (timestamp);

-- إنشاء Partition لشهور السنة الحالية (مثال)
CREATE TABLE IF NOT EXISTS logs_y2026m08 PARTITION OF logs
    FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00');

-- الفهارس تُطبق على الـ Partitions وتكون أصغر وأسرع بكثير
CREATE INDEX IF NOT EXISTS idx_logs_2026_08_ts_level ON logs_y2026m08 (timestamp DESC, level);
CREATE INDEX IF NOT EXISTS idx_logs_2026_08_svc_ts ON logs_y2026m08 (service_name, timestamp DESC);