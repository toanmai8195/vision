#!/usr/bin/env bash
# Kiểm tra tiêu chí "Done khi" của P0 trên stack local (com/tm/docs/phases.md).
#   com/tm/docker/vision/scripts/verify.sh
set -uo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f docker-compose.yml)
fail=0

ok()   { printf '  \033[32m✔\033[0m %s\n' "$1"; }
bad()  { printf '  \033[31m✘\033[0m %s\n' "$1"; fail=1; }

psql_meta() {
  "${COMPOSE[@]}" exec -T postgres psql -U vision -d vision_meta -v ON_ERROR_STOP=1 -tAq "$@"
}

echo "1. Service healthy / init hoàn tất"
services=$("${COMPOSE[@]}" ps --all --format '{{.Service}}|{{.State}}|{{.Health}}|{{.ExitCode}}')
while IFS='|' read -r svc state health code; do
  [ -z "$svc" ] && continue
  case "$svc" in
    *-init|flyway)
      if [ "$state" = "exited" ] && [ "$code" = "0" ]; then ok "$svc (exit 0)"; else bad "$svc state=$state exit=$code"; fi ;;
    *)
      if [ "$state" != "running" ]; then bad "$svc state=$state"
      elif [ -n "$health" ] && [ "$health" != "healthy" ]; then bad "$svc health=$health"
      else ok "$svc ${health:-running}"; fi ;;
  esac
done <<< "$services"

echo "2. Catalog có attribute cho cả 4 dataType"
types=$(psql_meta -c "select string_agg(distinct data_type, ',' order by data_type) from meta.attribute")
if [ "$types" = "MUTEX,NOT_MUTEX,PARTIAL_VALUE,PARTIAL_VALUE_BY_TAG" ]; then ok "data_type = $types"; else bad "data_type = '$types'"; fi
feeds=$(psql_meta -c "select string_agg(distinct data_type || ':' || feed_mode, ',' order by data_type || ':' || feed_mode) from meta.attribute where data_type in ('MUTEX','NOT_MUTEX')")
if [ "$feeds" = "MUTEX:EVENT,MUTEX:STATE,NOT_MUTEX:EVENT,NOT_MUTEX:STATE" ]; then ok "MUTEX/NOT_MUTEX có EVENT + STATE"; else bad "feed_mode = '$feeds'"; fi

echo "3. DB chặn dữ liệu sai loại"
expect_reject() {
  local name="$1" sql="$2"
  if psql_meta -c "begin; $sql; rollback;" >/dev/null 2>&1; then bad "không chặn: $name"; else ok "chặn: $name"; fi
}
expect_reject "PARTIAL_VALUE với feed STATE" \
  "insert into meta.attribute(id,name,data_type,feed_mode,supported_date_ranges,attr_group_id) values (901,'x','PARTIAL_VALUE','STATE',array['A7'],1)"
expect_reject "data_type ngoài 4 loại" \
  "insert into meta.attribute(id,name,data_type,feed_mode,supported_date_ranges,attr_group_id) values (902,'y','PARTIAL_MUTEX','EVENT',array['A7'],1)"
expect_reject "tag PARTIAL_VALUE thiếu value range" \
  "insert into meta.tag(attr_id,id,name) values (102,9,'no_range')"
expect_reject "tag MUTEX có value range" \
  "insert into meta.tag(attr_id,id,name,value_from,value_from_inclusive) values (201,9,'bad',0,true)"
expect_reject "tag PARTIAL_VALUE_BY_TAG có value range" \
  "insert into meta.tag(attr_id,id,name,value_from,value_from_inclusive) values (103,9,'bad',0,true)"
expect_reject "tag id 0 (__any__)" \
  "insert into meta.tag(attr_id,id,name) values (101,0,'__any__')"
expect_reject "đổi data_type khi đã có tag" \
  "update meta.attribute set data_type='NOT_MUTEX' where id=201"

echo "4. StarRocks + Iceberg catalog"
dbs=$("${COMPOSE[@]}" exec -T starrocks mysql -h127.0.0.1 -P9030 -uroot -N -e "show databases" 2>/dev/null | tr '\n' ' ')
for db in gold seg meta dq; do
  if [[ " $dbs " == *" $db "* ]]; then ok "database $db"; else bad "thiếu database $db"; fi
done
if "${COMPOSE[@]}" exec -T starrocks mysql -h127.0.0.1 -P9030 -uroot -N -e "show catalogs" 2>/dev/null | grep -q iceberg_vision; then
  ok "external catalog iceberg_vision"
else
  bad "thiếu external catalog iceberg_vision"
fi

echo "5. Kafka topics"
topics=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null)
for t in vision.src.payment_event.v1 vision.segment.published.v1; do
  if grep -qx "$t" <<< "$topics"; then ok "topic $t"; else bad "thiếu topic $t"; fi
done

echo "6. Airflow nạp DAG vision_healthcheck"
if "${COMPOSE[@]}" exec -T airflow-scheduler airflow dags list 2>/dev/null | grep -q vision_healthcheck; then
  ok "DAG vision_healthcheck"
else
  bad "Airflow chưa thấy DAG vision_healthcheck"
fi

echo
if [ "$fail" = 0 ]; then echo "P0 stack: OK"; else echo "P0 stack: CÓ LỖI"; fi
exit "$fail"
