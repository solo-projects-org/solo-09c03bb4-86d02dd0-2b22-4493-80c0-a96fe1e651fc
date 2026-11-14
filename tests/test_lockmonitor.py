"""锁等待观察逻辑的单元测试。

只测纯函数（build_snapshot / summarize_state），
不需要真实数据库；真实阻塞链的集成验证见 README「演示锁等待」一节。
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from lockmonitor import build_snapshot, summarize_state  # noqa: E402


def act(pid, start, state="active", query="SELECT 1", blocking=(), wait_event="transactionid"):
    return {
        "pid": pid,
        "backend_start": start,
        "state": state,
        "query": query,
        "query_start": "2026-11-14T18:00:00+00:00",
        "xact_start": "2026-11-14T18:00:00+00:00",
        "wait_event_type": "Lock" if wait_event else None,
        "wait_event": wait_event,
        "usename": "postgres",
        "datname": "settlement",
        "application_name": "",
        "client_addr": None,
        "blocking_pids": list(blocking),
    }


def lock(pid, relation="settlement_orders", mode="ShareLock"):
    return {
        "pid": pid,
        "locktype": "relation",
        "mode": mode,
        "granted": False,
        "database": 1,
        "relation": 1,
        "page": None,
        "tuple": None,
        "transactionid": None,
        "virtualxid": None,
        "classid": None,
        "objid": None,
        "objsubid": None,
        "virtualtransaction": "1/5",
        "relname": relation,
        "relkind": "r",
        "nspname": "public",
        "datname": "settlement",
    }


def test_three_level_chain_is_confirmed():
    acts = [
        act(101, "t101", blocking=[102]),
        act(102, "t102", blocking=[103]),
        act(103, "t103", state="idle in transaction", wait_event=None),
    ]
    snap = build_snapshot(acts, [lock(101, mode="RowExclusiveLock")], batch_pid=101)
    assert snap["batch_waiting"] is True
    chain = snap["chains"][0]
    assert [n["role"] for n in chain["nodes"]] == ["waiter", "blocker", "holder"]
    assert chain["confirmed"] is True
    assert chain["holder_pid"] == 103
    assert all(n["verified"] for n in chain["nodes"])
    assert chain["wait_objects"][0]["summary"].startswith("relation · public.settlement_orders")


def test_missing_blocker_session_is_unconfirmed():
    snap = build_snapshot([act(101, "t101", blocking=[999])], [lock(101)], batch_pid=101)
    chain = snap["chains"][0]
    assert chain["confirmed"] is False
    assert chain["nodes"][1]["issue"] == "session_missing"
    assert any("会话已退出" in i for i in chain["issues"])


def test_pid_reuse_detected_via_backend_start():
    first = build_snapshot([act(101, "start-old")], [], batch_pid=None)
    snap = build_snapshot(
        [act(101, "start-new", blocking=[102]), act(102, "t102", state="idle in transaction", wait_event=None)],
        [lock(101)],
        batch_pid=None,
        previous_identities=first["identities"],
    )
    chain = snap["chains"][0]
    assert chain["confirmed"] is False
    assert chain["nodes"][0]["issue"] == "pid_reused"


def test_reuse_baseline_keeps_vanished_pids():
    first = build_snapshot([act(101, "start-old")], [], batch_pid=None)
    # 101 在本次采样中消失，之后又以新会话出现，仍应被识别为复用。
    second = build_snapshot([], [], batch_pid=None, previous_identities=first["identities"])
    snap = build_snapshot(
        [act(101, "start-new", blocking=[102]), act(102, "t102", state="idle in transaction", wait_event=None)],
        [lock(101)],
        batch_pid=None,
        previous_identities=second["identities"],
    )
    assert snap["chains"][0]["nodes"][0]["issue"] == "pid_reused"


def test_batch_pid_start_mismatch_is_reuse():
    snap = build_snapshot(
        [act(101, "new-start", blocking=[])],
        [],
        batch_pid=101,
        batch_backend_start="old-start",
    )
    assert snap["batch_pid_reused"] is True


def test_blocker_still_waiting_without_upstream_is_broken():
    snap = build_snapshot(
        [act(101, "t101", blocking=[102]), act(102, "t102", blocking=[], wait_event=None)],
        [lock(101), lock(102)],
        batch_pid=None,
    )
    chain = next(c for c in snap["chains"] if c["root_pid"] == 102)
    assert chain["confirmed"] is False
    assert chain["nodes"][0]["issue"] == "chain_broken"


def test_cycle_is_unconfirmed():
    acts = [act(101, "t101", blocking=[102]), act(102, "t102", blocking=[101])]
    snap = build_snapshot(acts, [lock(101), lock(102)], batch_pid=None)
    assert snap["chains"][0]["confirmed"] is False
    assert any("循环" in i for i in snap["chains"][0]["issues"])


def test_no_waiting_locks_means_empty_chains():
    snap = build_snapshot([act(101, "t101")], [], batch_pid=101)
    assert snap["chains"] == []
    assert snap["batch_waiting"] is False
    assert snap["waiting_pid_count"] == 0


def test_null_pid_lock_is_reported_as_unconfirmed():
    snap = build_snapshot([], [dict(lock(None))], batch_pid=None)
    assert len(snap["chains"]) == 1
    assert snap["chains"][0]["confirmed"] is False
    assert "预备事务" in snap["chains"][0]["issues"][0]


def test_summarize_state_never_reports_failure_as_empty():
    state = {
        "active": False,
        "sample_count": 2,
        "error_count": 1,
        "last_sample": {"ok": False, "error": "connection refused", "chains": []},
        "last_confirmed": None,
    }
    summary = summarize_state(state)
    assert summary["last_sample_ok"] is False
    assert summary["last_sample_error"] == "connection refused"
    assert summary["chain_count"] == 0
