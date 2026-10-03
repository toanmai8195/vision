import datetime as dt
import sys

import pytest

from com.tm.src.ingest.oltp import generate as g

SMALL = g.Config(users=50, days=10, end_date=dt.date(2026, 9, 15))


# Cùng tham số -> SQL giống hệt; đổi seed -> SQL khác. (Test là hàm bắt đầu bằng test_, pytest tự tìm và chạy;
# dùng `assert` thay cho assertEquals của JUnit.)
def test_deterministic() -> None:
    assert list(g.render(SMALL)) == list(g.render(SMALL))
    assert list(g.render(SMALL)) != list(g.render(g.Config(users=50, days=10, seed=7)))


# Câu đầu (sau dòng comment) phải là BEGIN; và câu cuối là COMMIT; để cả file là 1 transaction.
def test_transaction_wrapper() -> None:
    stmts = list(g.render(SMALL))
    assert stmts[1] == "BEGIN;" and stmts[-1] == "COMMIT;"


# User sinh ra (G...) không được trùng tiền tố U của data ví dụ U1001..U1004.
def test_users_do_not_clash_with_examples() -> None:
    assert all(not u.startswith("U") for u in g.user_ids(5))


# birth_date nằm trong 1960..2008, gender thuộc M/F/O hoặc NULL (chưa khai báo); SQL in ra dùng NULL, không phải 'None'.
def test_profile_birth_date_and_gender() -> None:
    inserts, _, _ = g.gen_profile(g.Config(users=400, days=5))
    assert all(dt.date(1960, 1, 1) <= r[2] <= dt.date(2008, 12, 31) for r in inserts)
    assert {r[3] for r in inserts} == {"M", "F", "O", None}
    sql = "\n".join(g.render(g.Config(users=400, days=5)))
    assert "'None'" not in sql and "NULL" in sql


# Bảng trạng thái: user đổi city phải sang city KHÁC, user mất city không đồng thời bị update.
def test_state_edge_cases() -> None:
    cfg = g.Config(users=500, days=10)
    inserts, updates, deletes = g.gen_profile(cfg)
    assert len(inserts) == 500 and updates and deletes
    assert not {u for u, *_ in updates} & set(deletes)
    old = {r[0]: r[1] for r in inserts}
    assert all(old[u] != city for u, city, _ in updates)  # đổi sang city khác


# Với profile: thứ tự trong SQL phải là DELETE cũ -> INSERT -> UPDATE (để CDC thấy đúng lịch sử).
def test_state_replay_order() -> None:
    sql = "\n".join(g.render(g.Config(users=200, days=5, attrs=("profile",))))
    i_del = sql.index("DELETE FROM src.user_profile WHERE user_id LIKE")
    i_ins = sql.index("INSERT INTO src.user_profile")
    i_upd = sql.index("UPDATE src.user_profile")
    assert i_del < i_ins < i_upd


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
    assert inserts == {"INSERT INTO src.user_profile "}


# Lỗi giữa chừng -> exception thoát khỏi `with` -> rollback (không commit dở dang).
def test_apply_rolls_back_on_error() -> None:
    conn = FakeConn(fail_on="UPDATE")
    with pytest.raises(RuntimeError):
        g.apply(g.Config(users=500, days=3), "dsn", connect=lambda dsn: conn)
    assert conn.outcome == "rollback"


# Chế độ in SQL: không còn placeholder %s trong output.
def test_render_has_no_placeholders() -> None:
    assert not any("%s" in s for s in g.render(SMALL))


# Tên nguồn không hợp lệ phải ném ValueError (pytest.raises như assertThrows của JUnit).
def test_unknown_attr_rejected() -> None:
    with pytest.raises(ValueError):
        list(g.render(g.Config(attrs=("profile", "bogus"))))


# Chuỗi chứa dấu nháy đơn bị từ chối, tránh SQL bị vỡ/injection.
def test_unsafe_value_rejected() -> None:
    with pytest.raises(ValueError):
        g.lit("a'b")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, *sys.argv[1:]]))
