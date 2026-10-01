#!/usr/bin/env bash
# Seed Postgres OLTP, chạy lại bao nhiêu lần cũng được (xem header từng file SQL).
#
#   seed.sh                         schema + data ví dụ golden (seed_examples.sql)
#   seed.sh --users 10000 --days 30 thêm data sinh ra (generate.py, mọi tham số của nó)
#
# Mỗi file SQL chạy NGUYÊN KHỐI trong 1 phiên psql với ON_ERROR_STOP=1: lỗi giữa chừng -> rollback, không dở dang.
# Biến môi trường: PSQL (mặc định: psql trong container vision-postgres-oltp).
set -euo pipefail
cd "$(dirname "$0")"

PSQL=${PSQL:-docker exec -i -e PGOPTIONS=--client-min-messages=warning vision-postgres-oltp psql -U vision -d oltp -v ON_ERROR_STOP=1 -q}

$PSQL < schema.sql
$PSQL < seed_examples.sql
if [ "$#" -gt 0 ]; then
  python3 generate.py "$@" | $PSQL
fi
echo "seed OK"
