#!/usr/bin/env bash
# Kiểm tra CDC end-to-end: insert -> update -> delete một user thử ở OLTP, rồi chờ bronze có đủ 3 dòng op c, u, d
# đúng thứ tự, với before/after đúng. Cần stack đang chạy kèm profile `query` (StarRocks để đọc Iceberg):
#   docker compose -f com/tm/docker/vision/docker-compose.yml --profile query up -d
#   com/tm/src/ingest/cdc/verify_bronze.sh
# Chỉ đụng user mã `T<epoch>` (không đụng user seed U…, user live L…). Biến: TIMEOUT (giây, mặc định 180).
set -euo pipefail

TIMEOUT=${TIMEOUT:-180}
USER_ID="T$(date +%s)"
PG="docker exec -i vision-postgres-oltp psql -U vision -d oltp -v ON_ERROR_STOP=1 -q -tA"
SR="docker exec -i vision-starrocks mysql -h127.0.0.1 -P9030 -uroot -N -B"

echo "verify_bronze: user thử $USER_ID"
$PG -c "INSERT INTO src.user_profile (user_id, city_code, birth_date, gender) VALUES ('$USER_ID', 'HCM', '1990-01-01', 'F')"
sleep 1
$PG -c "UPDATE src.user_profile SET city_code = 'HN', updated_at = now() WHERE user_id = '$USER_ID'"
sleep 1
$PG -c "DELETE FROM src.user_profile WHERE user_id = '$USER_ID'"

# Mỗi dòng: op|city trước|city sau. Insert: trước rỗng; delete: sau rỗng.
query="SET query_timeout=120;
REFRESH EXTERNAL TABLE ice.bronze.user_profile_cdc_raw;
SELECT op, IFNULL(get_json_string(payload,'\$.before.city_code'),''), IFNULL(get_json_string(payload,'\$.after.city_code'),'')
FROM ice.bronze.user_profile_cdc_raw
WHERE msg_key = '{\"user_id\":\"$USER_ID\"}' ORDER BY kafka_offset"
expected=$'c\t\tHCM\nu\tHCM\tHN\nd\tHN\t'

deadline=$((SECONDS + TIMEOUT))
while :; do
  got=$($SR -e "$query" 2>/dev/null || true)
  if [ "$got" = "$expected" ]; then
    echo "verify_bronze: OK — bronze có đủ c (HCM), u (HCM→HN), d (HN) cho $USER_ID"
    exit 0
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "verify_bronze: FAIL sau ${TIMEOUT}s. Mong đợi:" >&2
    printf '%s\n' "$expected" >&2
    echo "Nhận được:" >&2
    printf '%s\n' "$got" >&2
    exit 1
  fi
  sleep 5
done
