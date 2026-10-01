"""Sinh thêm data OLTP để test lớn hơn (seed generator, bước 2).

Mặc định GHI THẲNG vào Postgres OLTP (driver psycopg, 1 transaction: lỗi -> rollback hết):

    bazel run //com/tm/src/ingest/oltp:generate -- --users 10000 --days 30

- Kết nối: `--dsn` hoặc biến môi trường VISION_OLTP_DSN (mặc định = Postgres OLTP trong compose, cổng 5433).
- `--print-sql`: không kết nối DB, chỉ in SQL ra stdout (để đọc/kiểm tra, hoặc pipe vào psql).
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
import os
import random
import sys
from dataclasses import dataclass
from typing import Iterator, Sequence

ATTRS = ("payment", "profile", "product", "voucher", "oa", "app")
STATE_ATTRS = ("profile", "product")  # bảng trạng thái: UPDATE/DELETE

MCCS = ("5812", "5814", "4722", "4900", "5411", "5311")
CITIES = ("HCM", "HN", "DN", "CT")
PRODUCTS = ("paylater", "insurance", "wallet_plus")
APP_EVENTS = ("view_promo", "open_screen_x", "open_screen_y", "tap_banner")

UTC = dt.timezone.utc


# Gói toàn bộ tham số chạy vào một object bất biến. @dataclass tự sinh constructor/equals/hashCode/toString
# (như Lombok @Value hoặc Java record); frozen=True = không sửa được sau khi tạo. Field có `= giá trị` là mặc định.
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

    # Ngày đầu của khoảng dữ liệu = end_date lùi (days-1) ngày. @property = getter, gọi như field: cfg.start_date.
    @property
    def start_date(self) -> dt.date:
        return self.end_date - dt.timedelta(days=self.days - 1)


# Dòng dữ liệu: tuple đúng thứ tự cột của câu INSERT tương ứng (cột khai báo trong EVENT_TABLES hoặc docstring từng hàm gen_*).
Row = tuple


# Sinh danh sách mã user: G0000001, G0000002, ...
# Python: f"G{i:07d}" là string template (như String.format("G%07d", i)); [... for i in range(...)] là
# "list comprehension" = vòng for gộp thành 1 biểu thức, tương đương stream().map().toList() của Java.
def user_ids(n: int) -> list[str]:
    return [f"G{i:07d}" for i in range(1, n + 1)]


# Tạo bộ sinh số ngẫu nhiên RIÊNG cho từng nguồn, seed = "<seed>:<tên nguồn>".
# Cùng seed -> cùng chuỗi số (như new java.util.Random(seed)). Tách RNG theo nguồn để bật/tắt một nguồn
# không làm đổi data của nguồn khác. Dấu `_` đầu tên = quy ước "private" (Python không có từ khoá private).
def _rng(cfg: Config, name: str) -> random.Random:
    return random.Random(f"{cfg.seed}:{name}")


# Chọn ngẫu nhiên một thời điểm (UTC) trong ngày `day`: 00:00:00Z cộng thêm 0..86399 giây.
def _ts(day: dt.date, rng: random.Random) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(), tzinfo=UTC) + dt.timedelta(seconds=rng.randrange(86400))


# Duyệt từng ngày của khoảng thời gian, trả về cặp (chỉ số ngày, ngày).
# `yield` biến hàm thành generator: mỗi lần yield là một phần tử, tính lười như Iterator/Stream của Java
# (không dựng cả list trong bộ nhớ). Gọi: `for i, day in _days(cfg): ...` (tự tách tuple thành i, day).
def _days(cfg: Config) -> Iterator[tuple[int, dt.date]]:
    for i in range(cfg.days):
        yield i, cfg.start_date + dt.timedelta(days=i)


# Số sự kiện của 1 user trong 1 ngày, với tần suất trung bình `rate`.
# Ví dụ rate=0.3 -> 30% xác suất có 1 sự kiện; rate=1.5 -> luôn có 1, thêm 1 với xác suất 50%.
def _count(rate: float, rng: random.Random) -> int:
    """Số sự kiện của 1 user trong 1 ngày: phần nguyên + Bernoulli(phần lẻ)."""
    whole = int(rate)
    return whole + (1 if rng.random() < rate - whole else 0)


# Sinh dữ liệu cho src.payment_event (S1). Trả về list các tuple (hàng), mỗi tuple đúng thứ tự cột:
# (event_id, user_id, mcc, amount, status, event_ts, created_at).
# Ca biên: ~3% giao dịch FAILED; ~2% đến muộn (created_at = event_ts + 1..3 ngày).
# Tuple = bộ giá trị bất biến, giống một record/Object[] nhỏ; ở đây dùng thay cho class Row.
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


# Sinh dữ liệu cho src.user_profile (S2a, bảng trạng thái). Trả về 3 nhóm thao tác để phát lại theo thứ tự:
#   inserts: (user_id, city_code, created_at, updated_at)  - mỗi user 1 dòng
#   updates: (user_id, city_moi, updated_at)               - ~10% user đổi sang city KHÁC
#   deletes: [user_id]                                     - ~2% user mất city (xoá dòng)
# Python cho hàm trả nhiều giá trị cùng lúc (thực chất là 1 tuple); nơi gọi tách: a, b, c = gen_profile(cfg).
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


# Sinh dữ liệu cho src.user_product (S2b, bảng trạng thái). Trả về (inserts, deletes):
#   inserts: (user_id, product, opened_at, updated_at) - mỗi user có 0..2 sản phẩm
#   deletes: (user_id, product)                        - ~20% người có sản phẩm đóng một cái
# rng.sample(PRODUCTS, k) = chọn k phần tử khác nhau, không lặp.
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


# Chọn chỉ số mã quà/OA (gift_g<k>, oa_g<k>). Vùng chọn mở rộng thêm 5 mã mỗi ngày
# (tag_pool + day_i*5) để mỗi ngày đều xuất hiện mã MỚI -> mô phỏng tag EXTENDED cardinality cao.
def _tag_index(cfg: Config, day_i: int, rng: random.Random) -> int:
    """Chỉ số mã quà/OA; vùng mã mở rộng dần mỗi ngày -> luôn có tag EXTENDED mới."""
    return rng.randrange(cfg.tag_pool + day_i * 5)


# Sinh dữ liệu cho src.voucher_grant (S4a): (event_id, user_id, voucher_code, value, event_ts, created_at).
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


# Sinh dữ liệu cho src.oa_follow (S4b): (event_id, user_id, oa_code, action, event_ts, created_at).
# Mỗi lần FOLLOW có 30% xác suất kèm một UNFOLLOW sau đó (luôn sau thời điểm FOLLOW, trong khoảng dữ liệu).
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


# Sinh dữ liệu cho src.app_event (S5): (event_id, user_id, event_name, event_ts, created_at).
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
# Ghi vào DB / in SQL
# --------------------------------------------------------------------------------------------

DEFAULT_DSN = "postgresql://vision:vision@localhost:5433/oltp"  # Postgres OLTP trong docker compose


# Một bước ghi: câu SQL có placeholder %s + danh sách các bộ tham số (mỗi tuple = 1 lần chạy câu SQL).
# Giống PreparedStatement + addBatch() của JDBC.
#   rows = None  -> câu SQL chạy 1 lần, không tham số (vd DELETE ... LIKE 'G%')
#   rows = ()    -> có placeholder nhưng không có dòng nào -> bỏ qua, không chạy gì
@dataclass(frozen=True)
class Op:
    sql: str
    rows: tuple[Row, ...] | None = None


# Chuyển một giá trị Python thành literal SQL, CHỈ dùng cho chế độ --print-sql (khi ghi DB thật, driver tự
# truyền tham số): datetime -> '2026-09-15T03:00:00Z', số -> nguyên văn, chuỗi -> 'abc'.
# Chuỗi chứa dấu nháy đơn bị từ chối (ném ValueError) vì ta ghép chuỗi SQL thủ công.
def lit(v: object) -> str:
    if isinstance(v, dt.datetime):
        return "'" + v.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ") + "'"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    if "'" in s:
        raise ValueError(f"giá trị không an toàn để nhúng vào SQL: {s!r}")
    return "'" + s + "'"


EVENT_TABLES = {
    "payment": ("payment_event", "event_id, user_id, mcc, amount, status, event_ts, created_at", gen_payment),
    "voucher": ("voucher_grant", "event_id, user_id, voucher_code, value, event_ts, created_at", gen_voucher),
    "oa": ("oa_follow", "event_id, user_id, oa_code, action, event_ts, created_at", gen_oa),
    "app": ("app_event", "event_id, user_id, event_name, event_ts, created_at", gen_app),
}


# Tạo Op INSERT cho `rows` vào bảng `table` (cols = danh sách cột, đúng thứ tự phần tử của mỗi tuple).
# `conflict` là đoạn thêm cuối câu, vd ON CONFLICT DO NOTHING (bỏ qua nếu trùng khoá -> chạy lại được).
def _insert(table: str, cols: str, rows: Sequence[Row], conflict: str = "") -> Op:
    placeholders = ", ".join(["%s"] * len(cols.split(",")))
    return Op(f"INSERT INTO src.{table} ({cols}) VALUES ({placeholders}){conflict}", tuple(rows))


# Hàm chính: trả về TOÀN BỘ các bước ghi theo thứ tự, theo từng nguồn trong cfg.attrs:
#   - Bảng event: (tuỳ --reset) DELETE data G% cũ, rồi INSERT ... ON CONFLICT DO NOTHING.
#   - Bảng trạng thái: luôn DELETE dòng G% cũ rồi phát lại INSERT -> UPDATE -> DELETE.
# Hàm KHÔNG đụng DB: chỉ mô tả "sẽ ghi gì", để dùng chung cho cả ghi thật (apply) lẫn in SQL (render) và
# dễ test. `yield` trả từng Op một (generator, như Iterator của Java). Nhánh `else` cố ý ném lỗi (không rơi
# ngầm), theo luật "switch exhaustive" ở CLAUDE.md §11.
def build_ops(cfg: Config) -> Iterator[Op]:
    unknown = set(cfg.attrs) - set(ATTRS)
    if unknown:
        raise ValueError(f"attrs không hỗ trợ: {sorted(unknown)} (hợp lệ: {list(ATTRS)})")
    for attr in ATTRS:
        if attr not in cfg.attrs:
            continue
        if attr in EVENT_TABLES:
            table, cols, gen = EVENT_TABLES[attr]
            if cfg.reset:
                yield Op(f"DELETE FROM src.{table} WHERE user_id LIKE 'G%'")
            yield _insert(table, cols, gen(cfg), "\nON CONFLICT (event_id) DO NOTHING")
        elif attr == "profile":
            yield Op("DELETE FROM src.user_profile WHERE user_id LIKE 'G%'")
            inserts, updates, deletes = gen_profile(cfg)
            yield _insert("user_profile", "user_id, city_code, created_at, updated_at", inserts)
            yield Op("UPDATE src.user_profile SET city_code = %s, updated_at = %s WHERE user_id = %s",
                     tuple((city, when, uid) for uid, city, when in updates))
            yield Op("DELETE FROM src.user_profile WHERE user_id = %s", tuple((u,) for u in deletes))
        elif attr == "product":
            yield Op("DELETE FROM src.user_product WHERE user_id LIKE 'G%'")
            inserts_p, deletes_p = gen_product(cfg)
            yield _insert("user_product", "user_id, product, opened_at, updated_at", inserts_p)
            yield Op("DELETE FROM src.user_product WHERE user_id = %s AND product = %s", tuple(deletes_p))
        else:
            raise NotImplementedError(attr)


# GHI THẲNG vào Postgres: chạy mọi Op trong MỘT transaction.
# `connect` là hàm tạo kết nối (mặc định psycopg.connect); tách thành tham số để test truyền kết nối giả.
# `with conn:` giống try-with-resources: thoát bình thường -> COMMIT, có exception -> ROLLBACK rồi đóng kết nối.
# executemany = chạy 1 câu với nhiều bộ tham số (như addBatch/executeBatch của JDBC). Trả về số dòng đã gửi.
def apply(cfg: Config, dsn: str, connect=None) -> int:
    if connect is None:
        import psycopg  # import muộn: chế độ --print-sql không cần cài driver

        connect = psycopg.connect
    sent = 0
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            for op in build_ops(cfg):
                if op.rows is None:
                    cur.execute(op.sql)
                elif op.rows:
                    cur.executemany(op.sql, op.rows)
                    sent += len(op.rows)
    return sent


# Chế độ --print-sql: dựng file SQL thuần (BEGIN; ... COMMIT;) từ các Op, mỗi dòng dữ liệu là 1 câu riêng
# (thay %s bằng literal). Chỉ để đọc/kiểm tra hoặc pipe vào psql; ghi thật nên dùng apply().
def render(cfg: Config) -> Iterator[str]:
    yield (f"-- Sinh bởi generate.py: users={cfg.users} days={cfg.days} end_date={cfg.end_date} "
           f"attrs={','.join(cfg.attrs)} seed={cfg.seed} reset={cfg.reset}")
    yield "BEGIN;"
    for op in build_ops(cfg):
        if op.rows is None:
            yield op.sql + ";"
        else:
            for row in op.rows:
                yield (op.sql % tuple(lit(v) for v in row)) + ";"
    yield "COMMIT;"


# Đọc tham số dòng lệnh (--users, --days, ...) bằng argparse (như picocli/commons-cli của Java).
# Trả về (Config, dsn, print_sql). Giá trị mặc định lấy từ chính class Config để không lặp số ở hai nơi.
def parse_args(argv: Sequence[str] | None = None) -> tuple[Config, str, bool]:
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
    p.add_argument("--dsn", default=os.environ.get("VISION_OLTP_DSN", DEFAULT_DSN),
                   help="chuỗi kết nối Postgres (mặc định: $VISION_OLTP_DSN hoặc OLTP trong compose)")
    p.add_argument("--print-sql", action="store_true", help="không ghi DB, chỉ in SQL ra stdout")
    a = p.parse_args(argv)
    if a.users < 1 or a.days < 1:
        p.error("--users và --days phải ≥ 1")
    cfg = Config(users=a.users, days=a.days, end_date=a.end_date,
                 attrs=tuple(x for x in a.attrs.split(",") if x), seed=a.seed, reset=a.reset,
                 txn_per_user_day=a.txn_per_user_day, voucher_per_user_day=a.voucher_per_user_day,
                 app_per_user_day=a.app_per_user_day, tag_pool=a.tag_pool)
    return cfg, a.dsn, a.print_sql


# Điểm vào của script. Trả mã thoát: 0 = OK, 2 = tham số sai. Lỗi in ra stderr.
def main(argv: Sequence[str] | None = None) -> int:
    cfg, dsn, print_sql = parse_args(argv)
    try:
        if print_sql:
            for stmt in render(cfg):
                sys.stdout.write(stmt + "\n")
        else:
            sent = apply(cfg, dsn)
            print(f"đã ghi {sent} dòng (users={cfg.users} days={cfg.days} attrs={','.join(cfg.attrs)})")
    except ValueError as e:
        print(f"lỗi: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
