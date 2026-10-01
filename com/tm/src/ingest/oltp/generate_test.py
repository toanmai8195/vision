import datetime as dt
import re
import sys

import pytest

from com.tm.src.ingest.oltp import generate as g

SMALL = g.Config(users=50, days=10, end_date=dt.date(2026, 9, 15))


def test_deterministic() -> None:
    assert list(g.render(SMALL)) == list(g.render(SMALL))
    assert list(g.render(SMALL)) != list(g.render(g.Config(users=50, days=10, seed=7)))


def test_attrs_independent_rng() -> None:
    only_pay = g.Config(users=50, days=10, attrs=("payment",))
    assert g.gen_payment(only_pay) == g.gen_payment(SMALL)


def test_transaction_wrapper() -> None:
    stmts = list(g.render(SMALL))
    assert stmts[1] == "BEGIN;" and stmts[-1] == "COMMIT;"


def test_users_do_not_clash_with_examples() -> None:
    assert all(not u.startswith("U") for u in g.user_ids(5))


@pytest.mark.parametrize("name", ["payment", "voucher", "oa", "app"])
def test_event_ids_unique_and_in_window(name: str) -> None:
    rows = {"payment": g.gen_payment, "voucher": g.gen_voucher, "oa": g.gen_oa, "app": g.gen_app}[name](SMALL)
    assert rows
    ids = [r[0] for r in rows]
    assert len(ids) == len(set(ids))
    assert all(r[-1] >= r[-2] for r in rows)  # created_at >= event_ts


def test_payment_edge_cases_present() -> None:
    rows = g.gen_payment(g.Config(users=300, days=20))
    assert {r[4] for r in rows} == {"SUCCESS", "FAILED"}
    assert any(r[6] - r[5] >= dt.timedelta(days=1) for r in rows)  # đến muộn
    assert all(r[3] >= 1000 for r in rows)


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


def test_state_replay_order() -> None:
    sql = "\n".join(g.render(g.Config(users=200, days=5, attrs=("profile",))))
    i_del = sql.index("DELETE FROM src.user_profile WHERE user_id LIKE")
    i_ins = sql.index("INSERT INTO src.user_profile")
    i_upd = sql.index("UPDATE src.user_profile")
    assert i_del < i_ins < i_upd


def test_events_are_idempotent_sql() -> None:
    sql = "\n".join(g.render(SMALL))
    inserts = re.findall(r"INSERT INTO src\.(payment_event|voucher_grant|oa_follow|app_event)", sql)
    assert inserts
    assert sql.count("ON CONFLICT (event_id) DO NOTHING") == len(inserts)


def test_reset_only_selected_attrs() -> None:
    sql = "\n".join(g.render(g.Config(users=10, days=3, attrs=("payment",), reset=True)))
    assert "DELETE FROM src.payment_event WHERE user_id LIKE 'G%'" in sql
    assert "app_event" not in sql and "user_profile" not in sql


def test_batching() -> None:
    cfg = g.Config(users=100, days=30, txn_per_user_day=2.0, attrs=("payment",))
    n = len(g.gen_payment(cfg))
    inserts = [s for s in g.render(cfg) if s.startswith("INSERT")]
    assert n > g.BATCH and len(inserts) == -(-n // g.BATCH)


def test_unknown_attr_rejected() -> None:
    with pytest.raises(ValueError):
        list(g.render(g.Config(attrs=("payment", "bogus"))))


def test_unsafe_value_rejected() -> None:
    with pytest.raises(ValueError):
        g.lit("a'b")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, *sys.argv[1:]]))
