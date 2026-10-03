#!/usr/bin/env bash
# Nghiệm thu bước 4 trên dữ liệu thật: chạy chuỗi job silver (events -> dictionary -> SCD2) HAI lần cùng `ds`,
# rồi so mã băm nội dung của cả 4 bảng silver sau mỗi lần; phải giống hệt nhau (idempotent).
# Bảng so: user_profile_cdc_events (ds), user_profile_cdc_dlq (ds), user_dict, user_profile_scd2.
# Cần: stack + profile `query` (đọc Iceberg bằng StarRocks), image Spark đã build; generator live đã tắt và bronze đã bắt kịp Kafka
# (script kiểm tra và chờ). Chạy:
#   DS=2026-10-03 com/tm/src/batch/silver/verify_silver_idempotent.sh      # mặc định DS = hôm nay ICT
# Mỗi lần chạy chuỗi mất ~2-3 phút. Biến: TIMEOUT (giây chờ bronze bắt kịp, mặc định 120).
set -euo pipefail

cd "$(dirname "$0")/../../../../.."
COMPOSE="docker compose -f com/tm/docker/vision/docker-compose.yml"
DS=${DS:-$(TZ=Asia/Ho_Chi_Minh date +%F)}
TIMEOUT=${TIMEOUT:-120}
SR() { docker exec -i vision-starrocks mysql -h127.0.0.1 -P9030 -uroot -N -B -e "SET query_timeout=120; $1" 2>/dev/null; }

if [ "$(docker inspect -f '{{.State.Running}}' vision-oltp-generator 2>/dev/null || echo false)" = "true" ]; then
  echo "verify_silver: generator live đang chạy, dừng nó trước (docker stop vision-oltp-generator)" >&2
  exit 2
fi

# Bronze phải bắt kịp Kafka, nếu không hai lần chạy có thể thấy dữ liệu khác nhau (đó không phải lỗi idempotent).
deadline=$((SECONDS + TIMEOUT))
while :; do
  kafka=$(docker exec vision-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic vision.src.user_profile.v1 --time -1 | awk -F: '{s+=$3} END{print s+0}')
  bronze=$(SR "REFRESH EXTERNAL TABLE ice.bronze.user_profile_cdc_raw; SELECT count(DISTINCT concat(kafka_partition, ':', kafka_offset)) FROM ice.bronze.user_profile_cdc_raw" || true)
  [ "$kafka" = "$bronze" ] && break
  [ "$SECONDS" -ge "$deadline" ] && { echo "verify_silver: bronze ($bronze) chưa bắt kịp Kafka ($kafka)" >&2; exit 2; }
  sleep 5
done
echo "verify_silver: bronze == Kafka == $kafka message, DS=$DS"

# Mã băm nội dung 4 bảng (sắp theo khoá để không phụ thuộc thứ tự đọc).
snapshot() {
  SR "REFRESH EXTERNAL TABLE ice.silver.user_profile_cdc_events; REFRESH EXTERNAL TABLE ice.silver.user_profile_cdc_dlq;
      REFRESH EXTERNAL TABLE ice.silver.user_dict; REFRESH EXTERNAL TABLE ice.silver.user_profile_scd2;
      SELECT 'events', count(*), md5(ifnull(group_concat(concat(kafka_partition,':',kafka_offset,'|',user_id,'|',op,'|',source_ts_ms,'|',ifnull(city_code,''),'|',ifnull(cast(birth_date as string),''),'|',ifnull(gender,''),'|',cast(ds as string)) ORDER BY kafka_partition, kafka_offset SEPARATOR ';'),'')) FROM ice.silver.user_profile_cdc_events WHERE ds = '$DS';
      SELECT 'dlq', count(*), md5(ifnull(group_concat(concat(kafka_partition,':',kafka_offset,'|',reason,'|',ifnull(payload,'')) ORDER BY kafka_partition, kafka_offset SEPARATOR ';'),'')) FROM ice.silver.user_profile_cdc_dlq WHERE ds = '$DS';
      SELECT 'dict', count(*), md5(ifnull(group_concat(concat(user_id,':',uidx,':',cast(first_seen_ds as string)) ORDER BY uidx SEPARATOR ';'),'')) FROM ice.silver.user_dict;
      SELECT 'scd2', count(*), md5(ifnull(group_concat(concat(user_id,'|',uidx,'|',ifnull(city_code,''),'|',ifnull(cast(birth_date as string),''),'|',ifnull(gender,''),'|',cast(valid_from as string),'|',cast(valid_to as string),'|',cast(is_current as string)) ORDER BY uidx, valid_from SEPARATOR ';'),'')) FROM ice.silver.user_profile_scd2"
}

run_chain() { DS="$DS" $COMPOSE --profile silver run --rm spark-silver-user-profile 2>&1 | grep -E '^silver user|Traceback|Exception' || true; }

echo "== lần chạy 1"; run_chain; first=$(snapshot); printf '%s\n' "$first"
echo "== lần chạy 2"; run_chain; second=$(snapshot); printf '%s\n' "$second"

if [ -n "$first" ] && [ "$first" = "$second" ]; then
  echo "verify_silver: OK — chạy lại cùng ds=$DS cho kết quả y hệt ở cả 4 bảng"
  exit 0
fi
echo "verify_silver: FAIL — kết quả hai lần chạy khác nhau" >&2
diff <(printf '%s\n' "$first") <(printf '%s\n' "$second") >&2 || true
exit 1
