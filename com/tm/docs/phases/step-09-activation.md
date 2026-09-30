# Bước 9 — Activation (L7)

| | Nội dung |
|---|---|
| **Input** | `.roar` đã publish; event published |
| **Output** | API `count`, `contains`, `users by segment`, `segments by user`, export async |

```
Kafka published ──▶ tải .roar ──mmap──▶ hot-swap (AtomicReference)    [ONLINE]
OFFLINE ──▶ fallback StarRocks bitmap_contains/bitmap_count
user_id ──Caffeine ▶ Redis ▶ StarRocks──▶ uidx
```

- Thêm **Redis** vào compose.
- Mọi response có `version` + `asOfDs`.
- SLO: `contains`/`count` p99 < 10ms; `segments by user` p99 < 20ms.

## Checklist
**Mục tiêu**: tra cứu segment nhanh.
- [ ] `count`, `contains`, `users by segment`, `segments by user`
- [ ] Segment ONLINE: mmap `.roar`, hot-swap khi có version mới; OFFLINE: fallback StarRocks

**Done khi**: `contains`/`count` p99 < 10ms trên dữ liệu local.
