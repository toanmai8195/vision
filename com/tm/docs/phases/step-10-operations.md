# Bước 10 — Vận hành

> Làm dần từ bước 5, không đợi đến cuối.

| Mảng | Nội dung |
|---|---|
| **DQ** | Invariant ở `CLAUDE.md` §9 → `dq.result`; fail BLOCK thì không publish |
| **Airflow** | DAG theo layer, dynamic mapping theo `attrGroupId`, late data, backfill, maintenance |
| **Observability** | Prometheus + Grafana; cấm label cardinality cao (số liệu theo tag/segment → StarRocks) |
| **Scale** | Synthetic 100M user / 500 attr / 5K segment; benchmark từng layer; tune StarRocks; runbook |

- SLA (ICT): silver 02:00 · daily 03:30 · range 05:00 · DQ 05:15 · 5K segment published 07:00.

## Checklist
- [ ] DQ: các invariant ở `CLAUDE.md` §9, fail BLOCK thì không publish
- [ ] Airflow: DAG theo từng layer + late data + backfill
- [ ] Metrics, dashboard, alert (không dùng label cardinality cao)
- [ ] Scale 100M user / 500 attribute / 5K segment; runbook

**Done khi**: 5K segment publish trước 07:00 ICT trên dữ liệu synthetic.
