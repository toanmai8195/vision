#!/usr/bin/env bash
# Nghiệm thu bước 3: bronze khớp OLTP sau khi sửa/xoá.
#   1. Tạo một loạt thay đổi ở OLTP (insert nhiều user thử, đổi city, đổi giới tính, xoá city, xoá user).
#   2. Chờ bronze bắt kịp rồi kiểm tra:
#      (a) SỐ BẢN GHI: số cặp (partition, offset) khác nhau ở bronze == số message trong topic Kafka (không mất,
#          không thừa; bản ghi trùng do at-least-once nếu có sẽ không làm lệch phép đếm này).
#      (b) NỘI DUNG: trạng thái cuối mỗi user suy từ bronze (event cuối theo (partition, offset), bỏ user có op=d)
#          == bảng src.user_profile ở OLTP (city, birth_date, gender).
# Cần stack + profile `query`. Nên tắt generator live trước (docker stop vision-oltp-generator) để OLTP đứng yên.
# Biến: TIMEOUT (giây, mặc định 180).
set -euo pipefail

TIMEOUT=${TIMEOUT:-180}
TOPIC=vision.src.user_profile.v1
PG="docker exec -i vision-postgres-oltp psql -U vision -d oltp -v ON_ERROR_STOP=1 -q -tA"
SR="docker exec -i vision-starrocks mysql -h127.0.0.1 -P9030 -uroot -N -B"
TAG="T$(date +%s)"

if [ "$(docker inspect -f '{{.State.Running}}' vision-oltp-generator 2>/dev/null || echo false)" = "true" ]; then
  echo "verify: generator live đang chạy, dừng nó trước (docker stop vision-oltp-generator)" >&2
  exit 2
fi

echo "verify: tạo thay đổi ở OLTP (nhãn $TAG)"
$PG <<SQL
INSERT INTO src.user_profile (user_id, city_code, birth_date, gender) VALUES
  ('${TAG}a', 'HCM', '1990-01-01', 'F'),
  ('${TAG}b', 'HN',  '1985-06-15', NULL),
  ('${TAG}c', 'DN',  '2000-12-31', 'M'),
  ('${TAG}d', 'CT',  '1975-03-03', 'O');
UPDATE src.user_profile SET city_code = 'HN', updated_at = now() WHERE user_id = '${TAG}a';
UPDATE src.user_profile SET gender = 'F', updated_at = now() WHERE user_id = '${TAG}b';
UPDATE src.user_profile SET city_code = NULL, updated_at = now() WHERE user_id = '${TAG}c';
DELETE FROM src.user_profile WHERE user_id = '${TAG}d';
SQL

# Trạng thái cuối ở OLTP: user_id|city|birth_date(số ngày từ 1970-01-01, như Debezium)|gender, NULL -> rỗng.
oltp_state() {
  $PG -c "SELECT user_id || '|' || coalesce(city_code,'') || '|' || coalesce((birth_date - date '1970-01-01')::text,'') || '|' || coalesce(gender,'') FROM src.user_profile" | LC_ALL=C sort
}

# Số message trong topic = tổng (end offset - begin offset) của các partition.
kafka_total() {
  local end begin
  end=$(docker exec vision-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "$TOPIC" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  begin=$(docker exec vision-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "$TOPIC" --time -2 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  echo $((end - begin))
}

PRE="SET query_timeout=120; REFRESH EXTERNAL TABLE ice.bronze.user_profile_cdc_raw;"
bronze_distinct() {
  $SR -e "$PRE SELECT count(DISTINCT concat(kafka_partition, ':', kafka_offset)) FROM ice.bronze.user_profile_cdc_raw" 2>/dev/null
}
bronze_state() {
  $SR -e "$PRE
    SELECT concat(uid, '|', ifnull(city,''), '|', ifnull(bd,''), '|', ifnull(g,'')) FROM (
      SELECT get_json_string(msg_key,'\$.user_id') AS uid, op,
             get_json_string(payload,'\$.after.city_code') AS city,
             get_json_string(payload,'\$.after.birth_date') AS bd,
             get_json_string(payload,'\$.after.gender') AS g,
             row_number() OVER (PARTITION BY get_json_string(msg_key,'\$.user_id') ORDER BY kafka_partition DESC, kafka_offset DESC) AS rn
      FROM ice.bronze.user_profile_cdc_raw) t
    WHERE rn = 1 AND op <> 'd'" 2>/dev/null | LC_ALL=C sort
}

deadline=$((SECONDS + TIMEOUT))
while :; do
  want_count=$(kafka_total)
  got_count=$(bronze_distinct || true)
  want_state=$(oltp_state)
  got_state=$(bronze_state || true)
  if [ "$want_count" = "$got_count" ] && [ "$want_state" = "$got_state" ]; then
    echo "verify: OK — số bản ghi bronze == Kafka == $want_count; trạng thái cuối $(printf '%s\n' "$want_state" | wc -l | tr -d ' ') user khớp OLTP"
    exit 0
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "verify: FAIL sau ${TIMEOUT}s" >&2
    echo "số bản ghi: Kafka=$want_count bronze=$got_count" >&2
    diff <(printf '%s\n' "$want_state") <(printf '%s\n' "$got_state") >&2 || true
    exit 1
  fi
  sleep 5
done
