"""Sinh thêm data OLTP để test lớn hơn (seed generator, bước 2).

In ra stdout một file SQL chạy được bằng psql trong MỘT transaction:

    python3 generate.py --users 10000 --days 30 | docker exec -i vision-postgres-oltp psql -U vision -d oltp -v ON_ERROR_STOP=1 -q

- Tham số theo CLAUDE.md §6: `--users --days --attrs` (attrs = nhóm nguồn: payment, profile, product, voucher, oa, app).
- Deterministic: cùng tham số (kể cả `--seed`) ra cùng data; mỗi nguồn có RNG riêng nên chọn tập `--attrs`
  khác nhau không làm đổi data của nguồn còn lại.
- Chạy lại được: user sinh ra có mã `G0000001…` (không đụng U1001..U1004 của seed_examples.sql).
  Bảng event: `ON CONFLICT DO NOTHING` theo event_id xác định; bảng trạng thái: xoá dòng của user `G%` rồi phát
  lại chuỗi INSERT -> UPDATE -> DELETE. `--reset` xoá luôn event `G%` của các nguồn được chọn (dùng khi đổi
  tham số, vì ON CONFLICT DO NOTHING không sửa dòng cũ).
- Ca biên có trong data sinh ra: giao dịch FAILED, đến muộn (created_at > event_ts), user đổi city, user mất
  city (xoá dòng), đóng sản phẩm, unfollow OA, tag EXTENDED mới xuất hiện dần theo ngày.
"""

from __future__ import annotations

import argparse
import datetime as dt
import random
import sys
from dataclasses import dataclass
from typing import Iterator, Sequence

ATTRS = ("payment", "profile", "product", "voucher", "oa", "app")
STATE_ATTRS = ("profile", "product")  # bảng trạng thái: UPDATE/DELETE
BATCH = 1000

MCCS = ("5812", "5814", "4722", "4900", "5411", "5311")
CITIES = ("HCM", "HN", "DN", "CT")
PRODUCTS = ("paylater", "insurance", "wallet_plus")
APP_EVENTS = ("view_promo", "open_screen_x", "open_screen_y", "tap_banner")

UTC = dt.timezone.utc


@dataclass(frozen=True)
class Config:
    users: int = 1000
    days: int = 30
    end_date: dt.date = dt.date(2026, 9, 15)
    attrs: tuple[str, ...] = ATTRS
    seed: int = 42
    reset: bool = False
    txn_per_user_day: float = 0.3
    voucher_per_user_day: float = 0.02
    app_per_user_day: float = 0.5
    tag_pool: int = 200  # số mã quà/OA ban đầu; tăng dần theo ngày (EXTENDED)

    @property
    def start_date(self) -> dt.date:
        return self.end_date - dt.timedelta(days=self.days - 1)


# Dòng dữ liệu: tuple đúng thứ tự cột của câu INSERT tương ứng (xem COLUMNS).
Row = tuple


def user_ids(n: int) -> list[str]:
    return [f"G{i:07d}" for i in range(1, n + 1)]


def _rng(cfg: Config, name: str) -> random.Random:
    return random.Random(f"{cfg.seed}:{name}")


def _ts(day: dt.date, rng: random.Random) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(), tzinfo=UTC) + dt.timedelta(seconds=rng.randrange(86400))


def _days(cfg: Config) -> Iterator[tuple[int, dt.date]]:
    for i in range(cfg.days):
        yield i, cfg.start_date + dt.timedelta(days=i)


def _count(rate: float, rng: random.Random) -> int:
    """Số sự kiện của 1 user trong 1 ngày: phần nguyên + Bernoulli(phần lẻ)."""
    whole = int(rate)
    return whole + (1 if rng.random() < rate - whole else 0)


def gen_payment(cfg: Config) -> list[Row]:
    """(event_id, user_id, mcc, amount, status, event_ts, created_at). ~3% FAILED, ~2% đến muộn 1–3 ngày."""
    rng = _rng(cfg, "payment")
    rows: list[Row] = []
    n = 0
    for _, day in _days(cfg):
        for uid in user_ids(cfg.users):
            for _ in range(_count(cfg.txn_per_user_day, rng)):
                n += 1
                ts = _ts(day, rng)
                amount = max(1000, round(rng.lognormvariate(11, 1.0) / 1000) * 1000)
                status = "FAILED" if rng.random() < 0.03 else "SUCCESS"
                late = dt.timedelta(days=rng.randint(1, 3)) if rng.random() < 0.02 else dt.timedelta(seconds=1)
                rows.append((f"gp-{n:09d}", uid, rng.choice(MCCS), amount, status, ts, ts + late))
    return rows


def gen_profile(cfg: Config) -> tuple[list[Row], list[Row], list[str]]:
    """(inserts, updates, deletes). 10% user đổi city, 2% mất city (xoá dòng).

    inserts: (user_id, city_code, created_at, updated_at); updates: (user_id, city_code, updated_at).
    """
    rng = _rng(cfg, "profile")
    inserts: list[Row] = []
    updates: list[Row] = []
    deletes: list[str] = []
    start = dt.datetime.combine(cfg.start_date, dt.time(), tzinfo=UTC)
    for uid in user_ids(cfg.users):
        created = start - dt.timedelta(days=rng.randint(1, 700), seconds=rng.randrange(86400))
        city = rng.choice(CITIES)
        inserts.append((uid, city, created, created))
        roll = rng.random()
        when = _ts(cfg.start_date + dt.timedelta(days=rng.randrange(cfg.days)), rng)
        if roll < 0.10:
            updates.append((uid, rng.choice([c for c in CITIES if c != city]), when))
        elif roll < 0.12:
            deletes.append(uid)
    return inserts, updates, deletes


def gen_product(cfg: Config) -> tuple[list[Row], list[tuple[str, str]]]:
    """(inserts, deletes). Mỗi user 0–2 sản phẩm; 20% người có sản phẩm đóng một cái.

    inserts: (user_id, product, opened_at, updated_at); deletes: (user_id, product).
    """
    rng = _rng(cfg, "product")
    inserts: list[Row] = []
    deletes: list[tuple[str, str]] = []
    start = dt.datetime.combine(cfg.start_date, dt.time(), tzinfo=UTC)
    for uid in user_ids(cfg.users):
        held = rng.sample(PRODUCTS, rng.choice((0, 0, 1, 1, 2)))
        for product in held:
            opened = start - dt.timedelta(days=rng.randint(1, 365), seconds=rng.randrange(86400))
            inserts.append((uid, product, opened, opened))
        if held and rng.random() < 0.20:
            deletes.append((uid, rng.choice(held)))
    return inserts, deletes


def _tag_index(cfg: Config, day_i: int, rng: random.Random) -> int:
    """Chỉ số mã quà/OA; vùng mã mở rộng dần mỗi ngày -> luôn có tag EXTENDED mới."""
    return rng.randrange(cfg.tag_pool + day_i * 5)


def gen_voucher(cfg: Config) -> list[Row]:
    """(event_id, user_id, voucher_code, value, event_ts, created_at)."""
    rng = _rng(cfg, "voucher")
    rows: list[Row] = []
    n = 0
    for i, day in _days(cfg):
        for uid in user_ids(cfg.users):
            for _ in range(_count(cfg.voucher_per_user_day, rng)):
                n += 1
                ts = _ts(day, rng)
                rows.append((f"gv-{n:09d}", uid, f"gift_g{_tag_index(cfg, i, rng)}",
                             rng.choice((10000, 20000, 50000, 100000, 200000)), ts, ts + dt.timedelta(seconds=1)))
    return rows


def gen_oa(cfg: Config) -> list[Row]:
    """(event_id, user_id, oa_code, action, event_ts, created_at). 30% FOLLOW có UNFOLLOW sau đó."""
    rng = _rng(cfg, "oa")
    rows: list[Row] = []
    n = 0
    end = dt.datetime.combine(cfg.end_date + dt.timedelta(days=1), dt.time(), tzinfo=UTC)
    for i, day in _days(cfg):
        for uid in user_ids(cfg.users):
            for _ in range(_count(cfg.voucher_per_user_day, rng)):
                code = f"oa_g{_tag_index(cfg, i, rng)}"
                ts = _ts(day, rng)
                n += 1
                rows.append((f"go-{n:09d}", uid, code, "FOLLOW", ts, ts + dt.timedelta(seconds=1)))
                if rng.random() < 0.30 and ts < end - dt.timedelta(hours=1):
                    later = ts + dt.timedelta(seconds=rng.randrange(1, int((end - ts).total_seconds())))
                    n += 1
                    rows.append((f"go-{n:09d}", uid, code, "UNFOLLOW", later, later + dt.timedelta(seconds=1)))
    return rows


def gen_app(cfg: Config) -> list[Row]:
    """(event_id, user_id, event_name, event_ts, created_at)."""
    rng = _rng(cfg, "app")
    rows: list[Row] = []
    n = 0
    for _, day in _days(cfg):
        for uid in user_ids(cfg.users):
            for _ in range(_count(cfg.app_per_user_day, rng)):
                n += 1
                ts = _ts(day, rng)
                rows.append((f"ga-{n:09d}", uid, rng.choice(APP_EVENTS), ts, ts + dt.timedelta(seconds=1)))
    return rows


# --------------------------------------------------------------------------------------------
# Render SQL
# --------------------------------------------------------------------------------------------

def lit(v: object) -> str:
    if isinstance(v, dt.datetime):
        return "'" + v.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ") + "'"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    if "'" in s:
        raise ValueError(f"giá trị không an toàn để nhúng vào SQL: {s!r}")
    return "'" + s + "'"


def _batches(rows: Sequence[Row]) -> Iterator[Sequence[Row]]:
    for i in range(0, len(rows), BATCH):
        yield rows[i:i + BATCH]


def _insert(table: str, cols: str, rows: Sequence[Row], conflict: str = "") -> Iterator[str]:
    for chunk in _batches(rows):
        values = ",\n  ".join("(" + ", ".join(lit(v) for v in r) + ")" for r in chunk)
        yield f"INSERT INTO src.{table} ({cols}) VALUES\n  {values}{conflict};"


_NOTHING = "\nON CONFLICT (event_id) DO NOTHING"

EVENT_TABLES = {
    "payment": ("payment_event", "event_id, user_id, mcc, amount, status, event_ts, created_at", gen_payment),
    "voucher": ("voucher_grant", "event_id, user_id, voucher_code, value, event_ts, created_at", gen_voucher),
    "oa": ("oa_follow", "event_id, user_id, oa_code, action, event_ts, created_at", gen_oa),
    "app": ("app_event", "event_id, user_id, event_name, event_ts, created_at", gen_app),
}
STATE_TABLES = {"profile": "user_profile", "product": "user_product"}


def render(cfg: Config) -> Iterator[str]:
    unknown = set(cfg.attrs) - set(ATTRS)
    if unknown:
        raise ValueError(f"attrs không hỗ trợ: {sorted(unknown)} (hợp lệ: {list(ATTRS)})")
    yield (f"-- Sinh bởi generate.py: users={cfg.users} days={cfg.days} end_date={cfg.end_date} "
           f"attrs={','.join(cfg.attrs)} seed={cfg.seed} reset={cfg.reset}")
    yield "BEGIN;"
    for attr in ATTRS:
        if attr not in cfg.attrs:
            continue
        if attr in EVENT_TABLES:
            table, cols, gen = EVENT_TABLES[attr]
            if cfg.reset:
                yield f"DELETE FROM src.{table} WHERE user_id LIKE 'G%';"
            yield from _insert(table, cols, gen(cfg), _NOTHING)
        elif attr == "profile":
            yield "DELETE FROM src.user_profile WHERE user_id LIKE 'G%';"
            inserts, updates, deletes = gen_profile(cfg)
            yield from _insert("user_profile", "user_id, city_code, created_at, updated_at", inserts)
            for uid, city, when in updates:
                yield (f"UPDATE src.user_profile SET city_code = {lit(city)}, updated_at = {lit(when)} "
                       f"WHERE user_id = {lit(uid)};")
            for chunk in _batches([(u,) for u in deletes]):
                yield "DELETE FROM src.user_profile WHERE user_id IN (" + ", ".join(lit(u) for (u,) in chunk) + ");"
        elif attr == "product":
            yield "DELETE FROM src.user_product WHERE user_id LIKE 'G%';"
            inserts, deletes_p = gen_product(cfg)
            yield from _insert("user_product", "user_id, product, opened_at, updated_at", inserts)
            for uid, product in deletes_p:
                yield f"DELETE FROM src.user_product WHERE user_id = {lit(uid)} AND product = {lit(product)};"
        else:  # exhaustive: thêm attr mới mà quên nhánh -> lỗi rõ ràng
            raise NotImplementedError(attr)
    yield "COMMIT;"


def parse_args(argv: Sequence[str] | None = None) -> Config:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--users", type=int, default=1000)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--end-date", type=dt.date.fromisoformat, default=Config.end_date, help="YYYY-MM-DD (ngày cuối)")
    p.add_argument("--attrs", default=",".join(ATTRS), help=f"nhóm nguồn, phân tách dấu phẩy: {','.join(ATTRS)}")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--reset", action="store_true", help="xoá event G%% của các nguồn được chọn trước khi sinh")
    p.add_argument("--txn-per-user-day", type=float, default=Config.txn_per_user_day)
    p.add_argument("--voucher-per-user-day", type=float, default=Config.voucher_per_user_day)
    p.add_argument("--app-per-user-day", type=float, default=Config.app_per_user_day)
    p.add_argument("--tag-pool", type=int, default=Config.tag_pool)
    a = p.parse_args(argv)
    if a.users < 1 or a.days < 1:
        p.error("--users và --days phải ≥ 1")
    return Config(users=a.users, days=a.days, end_date=a.end_date,
                  attrs=tuple(x for x in a.attrs.split(",") if x), seed=a.seed, reset=a.reset,
                  txn_per_user_day=a.txn_per_user_day, voucher_per_user_day=a.voucher_per_user_day,
                  app_per_user_day=a.app_per_user_day, tag_pool=a.tag_pool)


def main(argv: Sequence[str] | None = None) -> int:
    cfg = parse_args(argv)
    try:
        for stmt in render(cfg):
            sys.stdout.write(stmt + "\n")
    except ValueError as e:
        print(f"lỗi: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
