"""Sinh thêm data OLTP để test lớn hơn (seed generator, bước 2).

Mặc định GHI THẲNG vào Postgres OLTP (driver psycopg, 1 transaction: lỗi -> rollback hết):

    bazel run //com/tm/src/ingest/oltp:generate -- --users 10000 --days 30

- Kết nối: `--dsn` hoặc biến môi trường VISION_OLTP_DSN (mặc định = Postgres OLTP trong compose, cổng 5433).
- `--print-sql`: không kết nối DB, chỉ in SQL ra stdout (để đọc/kiểm tra, hoặc pipe vào psql).
- Tham số theo CLAUDE.md §6: `--users --days --attrs` (attrs = nhóm nguồn; hiện chỉ `profile`, thêm nguồn ở bước 11–15).
- Deterministic: cùng tham số (kể cả `--seed`) ra cùng data; mỗi nguồn có RNG riêng nên chọn tập `--attrs`
  khác nhau không làm đổi data của nguồn còn lại.
- Chạy lại được: user sinh ra có mã `G0000001…` (không đụng U1001..U1004 của seed_examples.sql).
  Bảng trạng thái: xoá dòng của user `G%` rồi phát lại chuỗi INSERT -> UPDATE -> DELETE. `--reset` chỉ có nghĩa
  với bảng event (chưa có nguồn event nào; thêm cùng nguồn event ở bước 11).
- Ca biên có trong data sinh ra: user đổi city, user mất city (xoá dòng).
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import random
import sys
from dataclasses import dataclass
from typing import Iterator, Sequence

ATTRS = ("profile",)  # nhóm nguồn hiện có; thêm nguồn (payment, product, ...) ở bước 11–15
STATE_ATTRS = ("profile",)  # bảng trạng thái: UPDATE/DELETE

CITIES = ("HCM", "HN", "DN", "CT")

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


# Tạo Op INSERT cho `rows` vào bảng `table` (cols = danh sách cột, đúng thứ tự phần tử của mỗi tuple).
# `conflict` là đoạn thêm cuối câu, vd ON CONFLICT DO NOTHING (bỏ qua nếu trùng khoá -> chạy lại được).
def _insert(table: str, cols: str, rows: Sequence[Row], conflict: str = "") -> Op:
    placeholders = ", ".join(["%s"] * len(cols.split(",")))
    return Op(f"INSERT INTO src.{table} ({cols}) VALUES ({placeholders}){conflict}", tuple(rows))


# Hàm chính: trả về TOÀN BỘ các bước ghi theo thứ tự, theo từng nguồn trong cfg.attrs:
#   - Bảng trạng thái (profile): luôn DELETE dòng G% cũ rồi phát lại INSERT -> UPDATE -> DELETE.
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
        if attr == "profile":
            yield Op("DELETE FROM src.user_profile WHERE user_id LIKE 'G%'")
            inserts, updates, deletes = gen_profile(cfg)
            yield _insert("user_profile", "user_id, city_code, created_at, updated_at", inserts)
            yield Op("UPDATE src.user_profile SET city_code = %s, updated_at = %s WHERE user_id = %s",
                     tuple((city, when, uid) for uid, city, when in updates))
            yield Op("DELETE FROM src.user_profile WHERE user_id = %s", tuple((u,) for u in deletes))
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
    p.add_argument("--dsn", default=os.environ.get("VISION_OLTP_DSN", DEFAULT_DSN),
                   help="chuỗi kết nối Postgres (mặc định: $VISION_OLTP_DSN hoặc OLTP trong compose)")
    p.add_argument("--print-sql", action="store_true", help="không ghi DB, chỉ in SQL ra stdout")
    a = p.parse_args(argv)
    if a.users < 1 or a.days < 1:
        p.error("--users và --days phải ≥ 1")
    cfg = Config(users=a.users, days=a.days, end_date=a.end_date,
                 attrs=tuple(x for x in a.attrs.split(",") if x), seed=a.seed, reset=a.reset)
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
