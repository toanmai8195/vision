#!/usr/bin/env bash
# Chạy trong service compose `oltp-seed` (one-shot): áp schema, rồi nạp data ví dụ NẾU CHƯA CÓ.
#
# Vì sao có kiểm tra: seed_examples.sql xoá + chèn lại dòng của bảng trạng thái (profile, product). Từ bước 3
# Debezium sẽ thấy đó là một loạt delete/create giả mỗi lần `docker compose up`. Nên chỉ seed khi chưa có
# dòng đánh dấu `e-9001`. Nạp lại có chủ đích: FORCE_SEED=1 docker compose up oltp-seed
#
# Biến môi trường: PGHOST/PGUSER/PGPASSWORD/PGDATABASE (psql đọc trực tiếp), SEED_DIR (mặc định /seed).
set -euo pipefail

SEED_DIR=${SEED_DIR:-/seed}
PSQL="psql -v ON_ERROR_STOP=1 -q"
export PGOPTIONS=--client-min-messages=warning

$PSQL < "$SEED_DIR/schema.sql"

already=$($PSQL -Atc "SELECT count(*) FROM src.payment_event WHERE event_id = 'e-9001'")
if [ "$already" != "0" ] && [ "${FORCE_SEED:-0}" != "1" ]; then
  echo "oltp-seed: data ví dụ đã có, bỏ qua (FORCE_SEED=1 để nạp lại)"
  exit 0
fi

$PSQL < "$SEED_DIR/seed_examples.sql"
echo "oltp-seed: đã nạp data ví dụ"
