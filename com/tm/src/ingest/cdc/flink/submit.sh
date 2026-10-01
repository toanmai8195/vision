#!/usr/bin/env bash
# Nộp job Flink SQL ingest bronze lên session cluster (service compose `flink-sql-submit`, one-shot).
# Idempotent: nếu job `bronze_ingest` đang RUNNING thì bỏ qua (tránh chạy trùng mỗi lần `docker compose up`).
# Biến: JM (mặc định flink-jobmanager:8081), SQL_FILE.
set -euo pipefail

JM=${JM:-flink-jobmanager:8081}
SQL_FILE=${SQL_FILE:-/cdc/flink/bronze_ingest.sql}

# Kiểm tra job `bronze_ingest` đã RUNNING chưa (đọc REST của jobmanager).
job_running() {
  curl -sf "http://$JM/jobs/overview" | tr -d ' \n' \
    | grep -q '"name":"bronze_ingest","start-time":[0-9]*,"end-time":-1,"duration":[0-9]*,"state":"RUNNING"'
}

if job_running; then
  echo "flink-sql-submit: job bronze_ingest đang RUNNING, bỏ qua"
  exit 0
fi

# Entrypoint của service là script này nên docker-entrypoint của Flink không chạy -> tự trỏ sql-client tới jobmanager.
printf 'rest.address: %s\nrest.port: %s\n' "${JM%%:*}" "${JM##*:}" >> /opt/flink/conf/config.yaml

# sql-client trả exit 0 cả khi câu lệnh lỗi -> bắt chữ [ERROR] trong output.
out=$(/opt/flink/bin/sql-client.sh -f "$SQL_FILE" 2>&1) || true
echo "$out" | grep -vE '^WARNING|^SLF4J|^\s+at |^>|^$' | tail -40
if echo "$out" | grep -q '\[ERROR\]'; then
  echo "flink-sql-submit: LỖI khi chạy $SQL_FILE" >&2
  exit 1
fi

# Chờ job lên RUNNING.
for _ in $(seq 1 30); do
  if job_running; then echo "flink-sql-submit: bronze_ingest RUNNING"; exit 0; fi
  sleep 2
done
echo "flink-sql-submit: bronze_ingest không lên RUNNING" >&2
exit 1
