import datetime as dt
import re
import sys

import pytest

from com.tm.src.ingest.oltp import generate as g

SMALL = g.Config(users=50, days=10, end_date=dt.date(2026, 9, 15))


# Cùng tham số -> SQL giống hệt; đổi seed -> SQL khác. (Test là hàm bắt đầu bằng test_, pytest tự tìm và chạy;
# dùng `assert` thay cho assertEquals của JUnit.)
def test_deterministic() -> None:
    assert list(g.render(SMALL)) == list(g.render(SMALL))
    assert list(g.render(SMALL)) != list(g.render(g.Config(users=50, days=10, seed=7)))


# Chỉ chọn nguồn payment vẫn ra đúng data payment như khi chạy đủ 6 nguồn (RNG mỗi nguồn độc lập).
def test_attrs_independent_rng() -> None:
    only_pay = g.Config(users=50, days=10, attrs=("payment",))
    assert g.gen_payment(only_pay) == g.gen_payment(SMALL)


# Câu đầu (sau dòng comment) phải là BEGIN; và câu cuối là COMMIT; để cả file là 1 transaction.
def test_transaction_wrapper() -> None:
    stmts = list(g.render(SMALL))
    assert stmts[1] == "BEGIN;" and stmts[-1] == "COMMIT;"


# User sinh ra (G...) không được trùng tiền tố U của data ví dụ U1001..U1004.
def test_users_do_not_clash_with_examples() -> None:
    assert all(not u.startswith("U") for u in g.user_ids(5))


# Với mỗi bảng event: có data, event_id không trùng, created_at >= event_ts.
# @pytest.mark.parametrize chạy cùng một test cho nhiều giá trị `name` (như @ParameterizedTest của JUnit 5).
@pytest.mark.parametrize("name", ["payment", "voucher", "oa", "app"])
def test_event_ids_unique_and_in_window(name: str) -> None:
    rows = {"payment": g.gen_payment, "voucher": g.gen_voucher, "oa": g.gen_oa, "app": g.gen_app}[name](SMALL)
    assert rows
    ids = [r[0] for r in rows]
    assert len(ids) == len(set(ids))
    assert all(r[-1] >= r[-2] for r in rows)  # created_at >= event_ts


# Data payment có đủ ca biên: cả SUCCESS lẫn FAILED, có giao dịch đến muộn >= 1 ngày, amount >= 1000.
def test_payment_edge_cases_present() -> None:
    rows = g.gen_payment(g.Config(users=300, days=20))
    assert {r[4] for r in rows} == {"SUCCESS", "FAILED"}
    assert any(r[6] - r[5] >= dt.timedelta(days=1) for r in rows)  # đến muộn
    assert all(r[3] >= 1000 for r in rows)


# Có cả FOLLOW và UNFOLLOW, và mỗi UNFOLLOW luôn xảy ra SAU một FOLLOW cùng (user, oa_code).
def test_oa_unfollow_after_follow() -> None:
    rows = g.gen_oa(g.Config(users=300, days=20, voucher_per_user_day=0.3))
    assert {r[3] for r in rows} == {"FOLLOW", "UNFOLLOW"}
    first: dict[tuple, dt.datetime] = {}
    for r in sorted(rows, key=lambda r: r[4]):
        key = (r[1], r[2])
        if r[3] == "FOLLOW":
            first.setdefault(key, r[4])
        else:
            assert key in first and r[4] > first[key]


# Bảng trạng thái: user đổi city phải sang city KHÁC, user mất city không đồng thời bị update,
# sản phẩm bị đóng phải là sản phẩm user đang có.
def test_state_edge_cases() -> None:
    cfg = g.Config(users=500, days=10)
    inserts, updates, deletes = g.gen_profile(cfg)
    assert len(inserts) == 500 and updates and deletes
    assert not {u for u, *_ in updates} & set(deletes)
    old = {r[0]: r[1] for r in inserts}
    assert all(old[u] != city for u, city, _ in updates)  # đổi sang city khác
    p_inserts, p_deletes = g.gen_product(cfg)
    held = {(r[0], r[1]) for r in p_inserts}
    assert p_deletes and set(p_deletes) <= held


# Với profile: thứ tự trong SQL phải là DELETE cũ -> INSERT -> UPDATE (để CDC thấy đúng lịch sử).
def test_state_replay_order() -> None:
    sql = "\n".join(g.render(g.Config(users=200, days=5, attrs=("profile",))))
    i_del = sql.index("DELETE FROM src.user_profile WHERE user_id LIKE")
    i_ins = sql.index("INSERT INTO src.user_profile")
    i_upd = sql.index("UPDATE src.user_profile")
    assert i_del < i_ins < i_upd


# Mọi câu INSERT vào bảng event đều kèm ON CONFLICT (event_id) DO NOTHING -> chạy lại không lỗi trùng khoá.
def test_events_are_idempotent_sql() -> None:
    sql = "\n".join(g.render(SMALL))
    inserts = re.findall(r"INSERT INTO src\.(payment_event|voucher_grant|oa_follow|app_event)", sql)
    assert inserts
    assert sql.count("ON CONFLICT (event_id) DO NOTHING") == len(inserts)


# --reset chỉ xoá bảng của nguồn được chọn (payment), không đụng bảng khác.
def test_reset_only_selected_attrs() -> None:
    sql = "\n".join(g.render(g.Config(users=10, days=3, attrs=("payment",), reset=True)))
    assert "DELETE FROM src.payment_event WHERE user_id LIKE 'G%'" in sql
    assert "app_event" not in sql and "user_profile" not in sql


# Kết nối giả để test apply() mà không cần Postgres: ghi lại các câu SQL được gửi và đánh dấu commit/rollback.
# `__enter__`/`__exit__` là cách Python cài "try-with-resources" (như AutoCloseable.close() của Java).
class FakeConn:
    def __init__(self, fail_on: str | None = None) -> None:
        self.calls: list[tuple[str, int]] = []  # (sql, số bộ tham số; -1 = execute không tham số)
        self.fail_on = fail_on
        self.outcome = ""

    def __enter__(self) -> "FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.outcome = "rollback" if exc_type else "commit"
        return False  # không nuốt exception

    def cursor(self) -> "FakeConn":
        return self

    def execute(self, sql: str) -> None:
        self._check(sql)
        self.calls.append((sql, -1))

    def executemany(self, sql: str, rows) -> None:
        self._check(sql)
        self.calls.append((sql, len(rows)))

    def _check(self, sql: str) -> None:
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("giả lập lỗi DB")


# apply() gửi đúng số dòng, dùng đúng 1 kết nối/transaction và commit khi mọi thứ OK.
def test_apply_sends_all_rows_and_commits() -> None:
    conn = FakeConn()
    cfg = g.Config(users=100, days=5)
    sent = g.apply(cfg, "dsn", connect=lambda dsn: conn)
    assert conn.outcome == "commit"
    assert sent == sum(c[1] for c in conn.calls if c[1] > 0)
    inserts = {c[0].split("(")[0] for c in conn.calls if c[0].startswith("INSERT")}
    assert inserts == {"INSERT INTO src.payment_event ", "INSERT INTO src.user_profile ", "INSERT INTO src.user_product ",
                       "INSERT INTO src.voucher_grant ", "INSERT INTO src.oa_follow ", "INSERT INTO src.app_event "}


# Lỗi giữa chừng -> exception thoát khỏi `with` -> rollback (không commit dở dang).
def test_apply_rolls_back_on_error() -> None:
    conn = FakeConn(fail_on="user_product")
    with pytest.raises(RuntimeError):
        g.apply(g.Config(users=50, days=3), "dsn", connect=lambda dsn: conn)
    assert conn.outcome == "rollback"


# Op có placeholder nhưng không có dòng nào (vd không user nào đổi city) thì bị bỏ qua, không gửi SQL chứa %s trần.
def test_empty_row_ops_are_skipped() -> None:
    conn = FakeConn()
    g.apply(g.Config(users=1, days=1, attrs=("payment",), txn_per_user_day=0.0), "dsn", connect=lambda dsn: conn)
    assert all("%s" not in sql or n > 0 for sql, n in conn.calls)
    assert not any(sql.startswith("INSERT") for sql, _ in conn.calls)


# Chế độ in SQL: không còn placeholder %s trong output.
def test_render_has_no_placeholders() -> None:
    assert not any("%s" in s for s in g.render(SMALL))


# Tên nguồn không hợp lệ phải ném ValueError (pytest.raises như assertThrows của JUnit).
def test_unknown_attr_rejected() -> None:
    with pytest.raises(ValueError):
        list(g.render(g.Config(attrs=("payment", "bogus"))))


# Chuỗi chứa dấu nháy đơn bị từ chối, tránh SQL bị vỡ/injection.
def test_unsafe_value_rejected() -> None:
    with pytest.raises(ValueError):
        g.lit("a'b")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, *sys.argv[1:]]))
