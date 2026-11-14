"""锁等待观察器。

批次运行时，用一条**独立于对照事务**的连接周期性采样 PostgreSQL 的
``pg_stat_activity`` / ``pg_locks``，并把 ``pg_blocking_pids()`` 给出的阻塞关系
展开成一条或多条「等待会话 → 阻塞方 → … → 持锁者」的链。

几个刻意保留的语义：

- 每个节点都带 ``backend_start``。采样时若 PID 已不在活动会话里、PID 的
  会话启动时间与上一次观察不一致（PID 复用），或阻塞方自身在等待却解析不出
  更上层的阻塞方，则该链标为「无法确认」，不把旧会话当成当前阻塞方；
- 只有整条链都核对通过（终端是真正的持锁者）才记为「已确认」；
  「最后一次确认的链」一直保留，批次结束、超时或取消后仍可查看，但会带上
  采样时间和「历史观察」标记；
- 采样失败只记录失败，绝不当成「无阻塞」；
- 采样连接、语句限时和采样任务都与批次执行器隔离，采样慢或失败不会拖住
  对照 SQL、取消操作或运行名额的释放。
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import date, datetime
from typing import Any

import asyncpg

# 采样周期与单次采样总限时（连接 + 两条查询）。环境变量便于演示时调快。
SAMPLE_INTERVAL_S = max(
    0.2, float(os.environ.get("LOCK_SAMPLE_INTERVAL_MS", "1000")) / 1000
)
SAMPLE_TIMEOUT_S = max(
    0.5, float(os.environ.get("LOCK_SAMPLE_TIMEOUT_MS", "3000")) / 1000
)

MAX_QUERY_SUMMARY = 320
MAX_CHAINS = 30
MAX_DEPTH = 12

_ACTIVITY_SQL = """
SELECT a.pid,
       a.backend_start,
       a.state,
       a.query,
       a.query_start,
       a.xact_start,
       a.wait_event_type,
       a.wait_event,
       a.usename,
       a.datname,
       a.application_name,
       host(a.client_addr) AS client_addr,
       pg_blocking_pids(a.pid) AS blocking_pids
FROM pg_stat_activity a
WHERE a.backend_type = 'client backend'
  AND a.pid <> pg_backend_pid()
ORDER BY a.pid
"""

_WAITING_LOCKS_SQL = """
SELECT l.pid,
       l.locktype,
       l.mode,
       l.granted,
       l.database,
       l.relation,
       l.page,
       l.tuple,
       l.transactionid::text AS transactionid,
       l.virtualxid,
       l.classid,
       l.objid,
       l.objsubid,
       l.virtualtransaction,
       c.relname,
       c.relkind,
       n.nspname,
       d.datname
FROM pg_locks l
LEFT JOIN pg_class c ON c.oid = l.relation
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_database d ON d.oid = l.database
WHERE NOT l.granted
ORDER BY l.pid NULLS LAST, l.locktype
"""

# 内部问题码 -> 给值班员看的说明
_ISSUE_TEXT = {
    "session_missing": "会话已退出或当前查无此 PID，无法确认",
    "pid_reused": "PID 的会话启动时间与上次观察不一致（PID 可能已被复用），无法确认",
    "chain_broken": "该会话仍在等锁，但解析不出更上层的阻塞方，链条断开，无法确认",
    "cycle": "阻塞关系出现循环，无法确认",
    "no_pid": "等待锁没有对应的会话 PID（可能是预备事务），无法确认",
    "depth_limit": "阻塞链超过追踪深度上限，无法确认",
}


def _iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _summary(query: str | None, limit: int = MAX_QUERY_SUMMARY) -> str:
    if not query:
        return ""
    text = " ".join(query.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def describe_lock(lock: dict) -> dict:
    """把一条 pg_locks 等待记录归一化成网页可读的等待对象。"""
    obj = {
        "locktype": lock.get("locktype"),
        "mode": lock.get("mode"),
        "granted": bool(lock.get("granted")),
        "relation": lock.get("relname"),
        "schema": lock.get("nspname"),
        "database": lock.get("datname"),
        "page": lock.get("page"),
        "tuple": lock.get("tuple"),
        "transactionid": lock.get("transactionid"),
        "virtualxid": lock.get("virtualxid"),
        "classid": lock.get("classid"),
        "objid": lock.get("objid"),
        "objsubid": lock.get("objsubid"),
    }

    locktype = obj["locktype"] or "lock"
    if obj["relation"]:
        target = (
            f"{obj['schema']}.{obj['relation']}"
            if obj["schema"]
            else str(obj["relation"])
        )
        if obj["locktype"] == "tuple" and obj["page"] is not None:
            target += f"（页 {obj['page']}，元组 {obj['tuple']}）"
    elif obj["transactionid"]:
        target = f"事务 {obj['transactionid']}"
    elif obj["virtualxid"]:
        target = f"虚拟事务 {obj['virtualxid']}"
    elif obj["classid"] is not None:
        target = f"classid={obj['classid']} objid={obj['objid']} objsubid={obj['objsubid']}"
    else:
        target = locktype

    obj["target"] = target
    obj["summary"] = f"{locktype} · {target} · {obj['mode']}"
    return obj


def _node(
    pid: int | None,
    role: str,
    activities: dict[int, dict],
    wait_by_pid: dict[int, list[dict]],
    batch_pid: int | None,
    reused_pids: set[int],
    issue: str | None = None,
) -> dict:
    act = activities.get(pid) if pid is not None else None
    node: dict[str, Any] = {
        "pid": pid,
        "role": role,
        "backend_start": None,
        "verified": False,
        "session_present": act is not None,
        "state": None,
        "wait_event_type": None,
        "wait_event": None,
        "query_start": None,
        "xact_start": None,
        "sql_summary": "",
        "wait_objects": list(wait_by_pid.get(pid, [])) if pid is not None else [],
        "waiting": bool(pid is not None and pid in wait_by_pid),
        "blocking_pids": [],
        "is_batch_session": pid is not None and pid == batch_pid,
        "issue": issue,
        "issue_text": _ISSUE_TEXT.get(issue, "") if issue else "",
    }

    if act is None:
        if node["issue"] is None:
            node["issue"] = "session_missing"
            node["issue_text"] = _ISSUE_TEXT["session_missing"]
        return node

    node["backend_start"] = _iso(act.get("backend_start"))
    node["state"] = act.get("state")
    node["wait_event_type"] = act.get("wait_event_type")
    node["wait_event"] = act.get("wait_event")
    node["query_start"] = _iso(act.get("query_start"))
    node["xact_start"] = _iso(act.get("xact_start"))
    node["sql_summary"] = _summary(act.get("query"))
    node["blocking_pids"] = [
        int(p) for p in (act.get("blocking_pids") or []) if p is not None
    ]

    if pid in reused_pids and node["issue"] is None:
        node["issue"] = "pid_reused"
        node["issue_text"] = _ISSUE_TEXT["pid_reused"]

    # 关键核对：PID 必须能在本次采样的 pg_stat_activity 里找到，且 backend_start
    # 存在；否则宁可标「无法确认」，也不拿旧会话的 PID 冒充当前阻塞方。
    node["verified"] = bool(
        node["session_present"]
        and node["backend_start"]
        and node["issue"] is None
    )
    return node


def _is_waiter_node(node: dict) -> bool:
    """会话处于等待状态：pg_locks 有未授予锁，或 wait_event_type 为 Lock。"""
    return bool(node.get("waiting") or (
        node.get("wait_event_type") == "Lock" and node.get("wait_event")
    ))


def _trace_paths(
    pid: int | None,
    role: str,
    activities: dict[int, dict],
    wait_by_pid: dict[int, list[dict]],
    batch_pid: int | None,
    reused_pids: set[int],
    stack: tuple[int, ...] = (),
) -> list[list[dict]]:
    """从 pid 向上追踪所有可能的阻塞路径；每个路径的最后一个是持锁者。"""
    if pid is not None and pid in stack:
        return [[_node(pid, role, activities, wait_by_pid, batch_pid, reused_pids, "cycle")]]
    if len(stack) >= MAX_DEPTH:
        return [[_node(pid, role, activities, wait_by_pid, batch_pid, reused_pids, "depth_limit")]]

    node = _node(pid, role, activities, wait_by_pid, batch_pid, reused_pids)
    if node["issue"] in ("session_missing", "pid_reused"):
        return [[node]]

    blockers = []
    for b in node["blocking_pids"]:
        if b not in blockers:
            blockers.append(b)

    # 没有阻塞方：若会话仍在等锁，链就断在这里；否则它就是终端持锁者。
    if not blockers:
        if _is_waiter_node(node):
            if node["issue"] is None:
                node["issue"] = "chain_broken"
                node["issue_text"] = _ISSUE_TEXT["chain_broken"]
                node["verified"] = False
        else:
            node["role"] = "holder"
        return [[node]]

    next_stack = stack + ((pid,) if pid is not None else ())
    paths: list[list[dict]] = []
    for b in blockers:
        if b == 0:  # 预备事务占位：没有会话 PID
            paths.append(
                [
                    node,
                    _node(None, "blocker", activities, wait_by_pid, batch_pid, reused_pids, "no_pid"),
                ]
            )
            continue
        if b in next_stack:
            paths.append(
                [
                    node,
                    _node(b, "blocker", activities, wait_by_pid, batch_pid, reused_pids, "cycle"),
                ]
            )
            continue
        for sub_path in _trace_paths(
            b, "blocker", activities, wait_by_pid, batch_pid, reused_pids, next_stack
        ):
            paths.append([node] + sub_path)
    return paths


def _chain_confirmed(nodes: list[dict]) -> bool:
    if not nodes:
        return False
    if any(node["issue"] for node in nodes):
        return False
    if not all(node["verified"] for node in nodes):
        return False
    return nodes[-1]["role"] == "holder" and not _is_waiter_node(nodes[-1])


def summarize_state(state: dict | None) -> dict | None:
    """给批次列表用的轻量摘要，避免每 1.5 秒把所有链的正文都传一遍。"""
    if state is None:
        return None
    last = state.get("last_sample") or {}
    chains = last.get("chains") or []
    confirmed = [c for c in chains if c.get("confirmed")]
    last_confirmed = state.get("last_confirmed") or {}
    return {
        "active": state.get("active", False),
        "started_at": state.get("started_at"),
        "stopped_at": state.get("stopped_at"),
        "stop_reason": state.get("stop_reason"),
        "batch_pid": state.get("batch_pid"),
        "batch_pid_backend_start": state.get("batch_pid_backend_start"),
        "sample_count": state.get("sample_count", 0),
        "error_count": state.get("error_count", 0),
        "last_sample_ok": last.get("ok"),
        "last_sample_at": last.get("sampled_at"),
        "last_sample_error": last.get("error"),
        "waiting_pid_count": last.get("waiting_pid_count"),
        "batch_waiting": last.get("batch_waiting"),
        "chain_count": len(chains),
        "confirmed_chain_count": len(confirmed),
        "unconfirmed_chain_count": len(chains) - len(confirmed),
        "last_confirmed_at": last_confirmed.get("sampled_at"),
        "last_confirmed_chain_count": len(last_confirmed.get("chains") or []),
    }


def build_snapshot(
    activities: list[dict],
    waiting_locks: list[dict],
    batch_pid: int | None,
    previous_identities: dict[int, str] | None = None,
    batch_backend_start: str | None = None,
) -> dict:
    """由一次采样结果构造等待链快照（纯函数，便于测试）。

    返回的 ``identities`` 用作下一次采样的会话身份基线；``chains`` 中
    每条链都标明是否 confirmed。只有成功完成两条查询才会调用本函数，
    因此它不会把采样失败误报成无阻塞。
    """
    previous_identities = previous_identities or {}
    activities_by_pid: dict[int, dict] = {
        int(a["pid"]): a for a in activities if a.get("pid") is not None
    }

    identities: dict[int, str] = {}
    reused_pids: set[int] = set()
    for pid, act in activities_by_pid.items():
        start = _iso(act.get("backend_start"))
        if start:
            identities[pid] = start
            old = previous_identities.get(pid)
            if old is not None and old != start:
                reused_pids.add(pid)
    # 保留已消失 PID 的旧身份：若之后同一 PID 以新会话出现，仍能识别为复用。
    known_identities = {**previous_identities, **identities}

    # 对照会话的 PID 是执行器在建连时记录的，这里再用它自己的 backend_start
    # 复核一次：对不上说明 PID 已被别的会话复用，本批会话其实已不在。
    if batch_pid is not None:
        act = activities_by_pid.get(batch_pid)
        observed_start = _iso(act.get("backend_start")) if act else None
        if batch_backend_start and observed_start:
            if observed_start != batch_backend_start:
                reused_pids.add(batch_pid)

    wait_by_pid: dict[int, list[dict]] = {}
    null_pid_waits = 0
    for lock in waiting_locks:
        pid = lock.get("pid")
        if pid is None:
            # pg_locks.pid 为空：通常是预备事务或进程刚退出，没有可核对的会话。
            null_pid_waits += 1
            continue
        pid = int(pid)
        obj = describe_lock(dict(lock))
        wait_by_pid.setdefault(pid, []).append(obj)

    chains: list[dict] = []
    root_pids = sorted(wait_by_pid.keys(), key=lambda p: (p != batch_pid, p))

    for root_pid in root_pids:
        if len(chains) >= MAX_CHAINS:
            break
        paths = _trace_paths(
            root_pid, "waiter", activities_by_pid, wait_by_pid, batch_pid, reused_pids
        )
        for path in paths:
            if len(chains) >= MAX_CHAINS:
                break
            confirmed = _chain_confirmed(path)
            chains.append(
                {
                    "root_pid": root_pid,
                    "batch_waiting": root_pid == batch_pid,
                    "wait_objects": wait_by_pid[root_pid],
                    "nodes": path,
                    "confirmed": confirmed,
                    "holder_pid": path[-1]["pid"] if confirmed else None,
                    "issues": [
                        node["issue_text"] for node in path if node.get("issue")
                    ]
                    or (["阻塞链未能完整确认"] if not confirmed else []),
                    "observed_at": None,  # 由调用方填采样时间
                }
            )

    if null_pid_waits and len(chains) < MAX_CHAINS:
        chains.append(
            {
                "root_pid": None,
                "batch_waiting": False,
                "wait_objects": [],
                "nodes": [
                    _node(
                        None,
                        "waiter",
                        activities_by_pid,
                        wait_by_pid,
                        batch_pid,
                        reused_pids,
                        "no_pid",
                    )
                ],
                "confirmed": False,
                "holder_pid": None,
                "issues": [
                    f"有 {null_pid_waits} 个等待锁没有会话 PID（可能是预备事务），无法确认"
                ],
                "observed_at": None,
            }
        )

    batch_act = activities_by_pid.get(batch_pid) if batch_pid is not None else None
    batch_session = (
        _node(batch_pid, "batch", activities_by_pid, wait_by_pid, batch_pid, reused_pids)
        if batch_pid is not None
        else None
    )
    return {
        "chains": chains,
        "identities": known_identities,
        "waiting_pid_count": len(wait_by_pid),
        "batch_waiting": any(c["batch_waiting"] for c in chains),
        "unconfirmed_count": sum(1 for c in chains if not c["confirmed"]),
        "truncated": len(chains) >= MAX_CHAINS,
        "batch_pid_present": batch_act is not None,
        "batch_session": batch_session,
        "batch_pid_backend_start": _iso(batch_act.get("backend_start"))
        if batch_act
        else None,
        "batch_pid_reused": batch_pid in reused_pids,
    }


MAX_STATES = 200  # 只保留最近若干个批次的观察状态，避免长期运行内存增长


class LockMonitor:
    """按批次运行锁等待采样任务；状态挂在批次对象上供网页读取。"""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._states: dict[str, dict] = {}
        self._order: list[str] = []
        self._identity_baselines: dict[str, dict[int, str]] = {}
        self._stop_events: dict[str, asyncio.Event] = {}
        self._tasks: dict[str, asyncio.Task] = {}

    def _prune(self) -> None:
        while len(self._order) > MAX_STATES:
            oldest = self._order[0]
            state = self._states.get(oldest)
            if state is not None and state.get("active"):
                break  # 正在采样的批次不淘汰
            self._order.pop(0)
            self._states.pop(oldest, None)
            self._identity_baselines.pop(oldest, None)
            self._stop_events.pop(oldest, None)
            self._tasks.pop(oldest, None)

    def begin(self, batch) -> dict:
        state = {
            "active": True,
            "batch_pid": None,
            "batch_pid_backend_start": None,
            "started_at": time.time(),
            "stopped_at": None,
            "stop_reason": None,
            "interval_ms": int(SAMPLE_INTERVAL_S * 1000),
            "sample_timeout_ms": int(SAMPLE_TIMEOUT_S * 1000),
            "last_sample": None,
            "last_confirmed": None,
            "sample_count": 0,
            "error_count": 0,
        }
        self._states[batch.id] = state
        self._order.append(batch.id)
        self._prune()
        self._identity_baselines[batch.id] = {}
        stop_event = asyncio.Event()
        self._stop_events[batch.id] = stop_event
        batch.lock_monitor = state
        self._tasks[batch.id] = asyncio.create_task(
            self._loop(batch, state, stop_event)
        )
        return state

    def stop(self, batch_id: str, reason: str) -> None:
        state = self._states.get(batch_id)
        if state is None:
            return
        if not state["active"]:
            return
        state["active"] = False
        state["stopped_at"] = time.time()
        state["stop_reason"] = reason
        event = self._stop_events.get(batch_id)
        if event is not None:
            event.set()

    def state_for(self, batch_id: str) -> dict | None:
        return self._states.get(batch_id)

    async def shutdown(self) -> None:
        for batch_id in list(self._stop_events):
            self.stop(batch_id, "shutdown")
        tasks = list(self._tasks.values())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _loop(self, batch, state: dict, stop_event: asyncio.Event) -> None:
        conn: asyncpg.Connection | None = None
        identity_baseline: dict[int, str] = self._identity_baselines.get(batch.id, {})
        try:
            while not stop_event.is_set():
                if batch.backend_pid is None:
                    await self._sleep(stop_event)
                    continue

                state["batch_pid"] = batch.backend_pid
                sampled_at = time.time()
                try:
                    if conn is None or conn.is_closed():
                        conn = await asyncio.wait_for(
                            asyncpg.connect(self._dsn), timeout=SAMPLE_TIMEOUT_S
                        )
                        await conn.execute(
                            f"SET statement_timeout = {int(SAMPLE_TIMEOUT_S * 1000)}"
                        )
                    activities, waiting_locks = await asyncio.wait_for(
                        self._fetch(conn), timeout=SAMPLE_TIMEOUT_S
                    )
                except Exception as exc:
                    if conn is not None:
                        try:
                            conn.terminate()
                        except Exception:
                            pass
                        conn = None
                    if stop_event.is_set():
                        break
                    # 采样失败是明确的「无法确认」，绝不能落成「无阻塞」。
                    state["error_count"] += 1
                    state["last_sample"] = {
                        "ok": False,
                        "attempted_at": sampled_at,
                        "sampled_at": time.time(),
                        "error": f"{type(exc).__name__}: {exc}",
                        "chains": [],
                        "batch_session": None,
                    }
                    await self._sleep(stop_event)
                    continue

                if stop_event.is_set():
                    # 批次已结束：丢弃停止后才回来的样本，保留更早确认的链。
                    break

                try:
                    snapshot = build_snapshot(
                        activities,
                        waiting_locks,
                        batch.backend_pid,
                        identity_baseline,
                        batch.backend_start,
                    )
                except Exception as exc:
                    state["error_count"] += 1
                    state["last_sample"] = {
                        "ok": False,
                        "attempted_at": sampled_at,
                        "sampled_at": time.time(),
                        "error": f"解析阻塞链失败: {type(exc).__name__}: {exc}",
                        "chains": [],
                        "batch_session": None,
                    }
                    await self._sleep(stop_event)
                    continue

                identity_baseline = snapshot.pop("identities")
                self._identity_baselines[batch.id] = identity_baseline
                observed_at = time.time()
                for chain in snapshot["chains"]:
                    chain["observed_at"] = observed_at
                state["sample_count"] = state.get("sample_count", 0) + 1

                # 记录本批会话的 backend_start，网页可与 PID 一起展示。
                batch_session = snapshot.get("batch_session")
                if batch_session and batch_session.get("verified"):
                    state["batch_pid_backend_start"] = batch_session["backend_start"]

                state["last_sample"] = {
                    "ok": True,
                    "attempted_at": sampled_at,
                    "sampled_at": observed_at,
                    **snapshot,
                }

                confirmed_chains = [
                    c for c in snapshot["chains"] if c["confirmed"]
                ]
                if confirmed_chains:
                    # 只保留最近一次「整条链都核对通过」的结果；结束/超时/取消
                    # 后网页会把它标为历史观察。
                    state["last_confirmed"] = {
                        "sampled_at": observed_at,
                        "batch_pid": batch.backend_pid,
                        "batch_pid_backend_start": batch.backend_start,
                        "chains": confirmed_chains,
                        "waiting_pid_count": snapshot["waiting_pid_count"],
                    }

                await self._sleep(stop_event)
        except Exception as exc:  # 采样器自身的意外错误：记录，不拖累批次
            state["error_count"] += 1
            state["last_sample"] = {
                "ok": False,
                "attempted_at": time.time(),
                "sampled_at": time.time(),
                "error": f"采样任务异常: {type(exc).__name__}: {exc}",
                "chains": [],
            }
        finally:
            if conn is not None:
                try:
                    await asyncio.wait_for(conn.close(), timeout=2)
                except Exception:
                    try:
                        conn.terminate()
                    except Exception:
                        pass
            state["active"] = False
            if state["stopped_at"] is None:
                state["stopped_at"] = time.time()

    @staticmethod
    async def _fetch(conn: asyncpg.Connection):
        activities = [dict(r) for r in await conn.fetch(_ACTIVITY_SQL)]
        waiting_locks = [dict(r) for r in await conn.fetch(_WAITING_LOCKS_SQL)]
        return activities, waiting_locks

    @staticmethod
    async def _sleep(stop_event: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=SAMPLE_INTERVAL_S)
        except asyncio.TimeoutError:
            pass
