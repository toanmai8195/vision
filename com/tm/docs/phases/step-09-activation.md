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

Checklist: xem mục "Bước 9" trong `../checklist.md`.
