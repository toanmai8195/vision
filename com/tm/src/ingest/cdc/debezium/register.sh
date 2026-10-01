#!/usr/bin/env bash
# Đăng ký (idempotent) connector Debezium: PUT /connectors/<name>/config tạo mới hoặc cập nhật.
# Chạy trong service compose `debezium-register`. Biến: CONNECT_URL (mặc định http://debezium:8083), CONFIG_FILE.
set -euo pipefail

CONNECT_URL=${CONNECT_URL:-http://debezium:8083}
CONFIG_FILE=${CONFIG_FILE:-/cdc/debezium/oltp-connector.json}
NAME=vision-oltp

curl -sf -X PUT -H 'Content-Type: application/json' --data @"$CONFIG_FILE" "$CONNECT_URL/connectors/$NAME/config" >/dev/null
echo "debezium-register: đã đăng ký connector $NAME"

# Chờ connector + task RUNNING (snapshot chạy trong task).
for _ in $(seq 1 60); do
  state=$(curl -sf "$CONNECT_URL/connectors/$NAME/status" || true)
  if echo "$state" | grep -q '"state":"FAILED"'; then echo "$state"; exit 1; fi
  if [ "$(echo "$state" | grep -o '"state":"RUNNING"' | wc -l)" -ge 2 ]; then
    echo "debezium-register: connector RUNNING"; exit 0
  fi
  sleep 2
done
echo "debezium-register: quá thời gian chờ RUNNING"; echo "$state"; exit 1
