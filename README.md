# 结算 SQL 计划对照台

迁库后夜间结算 SQL 变慢，用这个小工具在同一份数据上对比 **默认计划** 与
**抑制嵌套循环（`SET LOCAL enable_nestloop=off`）** 的实际执行效果。

## 快速开始

```bash
docker compose up --build
```

打开 <http://localhost:8000> 。首次启动时数据库会自动建结算样表并灌入
100 万行订单（约需半分钟），`web` 会等 `db` 健康检查通过后再起。

重新初始化样表数据：

```bash
docker compose down -v && docker compose up --build
```

## 它怎么对照

提交一条只读 SELECT 后，后台会开一条**专用连接**，在**同一个
`REPEATABLE READ, READ ONLY` 事务**里执行 4 次 `EXPLAIN (ANALYZE, FORMAT JSON)`：

| 顺序 | 轮次 | 设置 |
|----|----|------------------------|
| #1 | 1  | 默认（enable_nestloop=on） |
| #2 | 1  | 关闭嵌套循环（=off）       |
| #3 | 2  | 关闭嵌套循环（=off）       |
| #4 | 2  | 默认（=on）               |

第二轮颠倒顺序，用来抵消缓存预热造成的偏差。四次执行共享**同一事务快照**，
对照期间其他连接照常写入（页面上的「补记样表数据」按钮可随时插一行），
新数据不会进入本批结果。

## 结论判定规则

- 两轮快慢结论**相反** → 标为「无法判断改善」；- 两种设置得到**相同计划结构**（节点类型/扫描关系/索引/连接策略一致，
  忽略代价与行数等数值）→ 标为「无法判断改善」；
- 两轮一致 → 给出「关闭嵌套循环更快 / 默认计划更快」的结论；
- **超时、取消或报错** → 本批不给比较结论。

## 运行约束

- 同时只运行一批；运行中再提交会收到 **409 繁忙** 提示，网页也会显示名额占用；
- 每次执行由服务端 `statement_timeout = 10s` 限时，另有 15 秒客户端兜底
  （到点直接断开连接，确保 SQL 实际停止）；
- 「取消」通过 `pg_cancel_backend` 让正在执行的 SQL 真正停下来；
- 运行名额在 **本批 SQL 实际停止 → 事务结束 → 连接关闭** 之后才释放，
  期间其他请求（查询批次、补记数据）照常响应；
- 输入校验：仅单条只读 SELECT（sqlparse 静态检查 + READ ONLY 事务双保险）。

## 锁等待观察（分清「计划慢」还是「被锁住」）

夜间对照迟迟不返回时，网页顶部的「锁等待观察」面板会显示本批 SQL 是否在等表锁。
采样器用**独立连接**（与对照事务分开）每 1 秒读取 `pg_locks` 与
`pg_stat_activity`，并沿 `pg_blocking_pids()` 向上追踪：

- **等待对象**：`relation · public.settlement_orders · RowExclusiveLock`，
  或事务 ID / 虚拟事务 / 元组锁等；
- **阻塞方 PID 与 SQL 摘要**：每一层都给出 PID、`backend_start`、状态、
  等待事件和 SQL 摘要；若阻塞方自己也在等待，会继续向上追，直到持有锁的会话；
- **两条链都展示**：本批 SQL 的等待链（标记「本批次 SQL 正在等锁」）和其他
  会话之间的等待链都会展示，最多 30 条、单链最多 12 层。

### PID 与会话启动时间核对

每个节点在采样时都带 `backend_start`，并按下述规则判定：

| 情况 | 显示 |
|----|----|
| PID 在本次 `pg_stat_activity` 中能找到，且启动时间与上次观察一致 | 已核对 |
| 会话已退出 / 查无此 PID | **无法确认：会话已退出** |
| 同一 PID 的 `backend_start` 与上次观察不同（PID 被复用） | **无法确认：PID 可能已被复用** |
| 阻塞方仍在等锁，但解析不出更上层的阻塞方 | **无法确认：链条断开** |
| 阻塞关系成环、预备事务没有会话 PID、超过追踪深度 | **无法确认**（附原因） |

只有**整条链的每个节点都核对通过、终点确实是持锁者**时才标为「已确认阻塞链」。
任何一处不满足都标「无法确认」，绝不把旧会话的 PID 当成当前阻塞方。

### 结束、超时、取消与采样失败

- 批次正常结束、报错、超时或被取消后，**最后一次确认的阻塞链会保留**，
  并标为「历史观察」——写明确认时间，说明它不代表当前锁状态；
- **采样失败**（连接不上、查询超时等）只显示「状态无法确认」，
  并附错误信息；**不会显示「无阻塞」**，也不会用空链覆盖已有记录；
- 采样任务使用自己的连接、自己的 `statement_timeout`（默认 3 秒）与独立任务，
  **不占用运行名额、不参与对照事务**；采样慢、失败或卡住都不影响
  对照 SQL、`取消` 按钮和名额释放。采样停止后才返回的迟到样本会被丢弃。

采样周期与单次采样限时可用环境变量调整（毫秒）：

```bash
LOCK_SAMPLE_INTERVAL_MS=1000   # 采样间隔，默认 1000
LOCK_SAMPLE_TIMEOUT_MS=3000    # 单次采样（连接 + 两条查询）限时，默认 3000
```

### 用本地数据库会话演示锁等待

在 `docker compose up` 之后，打开两个终端，用普通的 `psql` 会话模拟
「另一个夜间任务锁住结算表」：

```bash
# 终端 A：持锁者（下面称为 A）
docker compose exec db psql -U postgres -d settlement
BEGIN;
LOCK TABLE settlement_orders IN ACCESS EXCLUSIVE MODE;
-- 保持这个事务开着，不要 COMMIT/ROLLBACK
```

```bash
# 终端 B：中间阻塞方。先拿到 merchants 的锁，再去锁 settlement_orders——
# 后者会被 A 挡住，于是 B 自己也处于等待中（这正是要向上追踪的一层）。
docker compose exec db psql -U postgres -d settlement
BEGIN;
LOCK TABLE merchants IN ACCESS EXCLUSIVE MODE;   -- 先拿到 merchants
LOCK TABLE settlement_orders IN ACCESS EXCLUSIVE MODE;  -- 这里会卡住，等待 A
```

然后用一条会触发表锁的 SQL 提交对照，例如 `SELECT count(*) FROM settlement_orders;`
（对 `settlement_orders` 的 `ACCESS EXCLUSIVE` 会让本批的 `AccessShareLock` 排队）。
点「开始对照」后约 1 秒内面板会出现：

```
链 1  已确认阻塞链  本批次 SQL 正在等锁  持锁者 PID <A 的 PID>
  [等待会话] PID <本批 PID>  启动于 …  已核对
  ↑ 被其阻塞
  [阻塞方]   PID <B 的 PID>  启动于 …  已核对  正在等待 Lock relation
  ↑ 被其阻塞
  [持锁者]   PID <A 的 PID>  启动于 …  已核对  LOCK TABLE settlement_orders …
```

- 想只看单层等待：A 持锁、B 不执行第二条 `LOCK`，只把本批 SQL 卡在 A 后面；
- 想验证「历史观察」：先点「取消本批」（此时 A 仍持锁），面板会保留刚才确认的链
  并标注历史观察；
- 想验证「无法确认」：在面板显示链条后，在 B 里按 `Ctrl-C` 取消当前 `LOCK` 再
  `ROLLBACK`，或直接 `\q` 退出 A/B，下一次采样对应节点会变成「无法确认」；
- 本批 SQL 会在 10 秒 `statement_timeout` 后超时结束，或随时点「取消本批」。

也可以用 `psql` 直接查证网页给出的 PID：

```sql
SELECT pid, backend_start, wait_event_type, wait_event, state, query
FROM pg_stat_activity WHERE pid IN (<面板上的 PID>);
SELECT pg_blocking_pids(<本批 PID>);
```

## 样表与示例 SQL

- `merchants`：5000 个商户，类别 A 只有 100 个；
- `settlement_orders`：100 万行结算订单，**类别 A 的 100 个商户沉淀了 70 万单**
  （数据倾斜）。数据库关闭了并行查询（`max_parallel_workers_per_gather=0`，
  模拟迁库后实例未开并行的场景），此时优化器估算「每商户约 200 单」，
  按类别 A 关联时容易误选嵌套循环——实际要捞 70 万行。

页面提供三个预设样例，分别对应三种结论分支：

1. **类别 A 结算汇总**（默认）：`left(m.category, 1) = 'A'` 的表达式过滤让优化器
   只能用默认选择率（把 100 个商户估成 25 个），默认计划误选嵌套循环，
   通常可得到「关闭嵌套循环更快」的结论；
2. **单商户结算明细**：高选择性过滤，索引嵌套循环占绝对优势，
   通常得到「默认计划更快」；
3. **每日结算汇总**：无关联查询，两种设置计划结构相同 → 「无法判断改善」。

默认预置的示例 SQL：

```sql
SELECT m.merchant_id,
       m.merchant_name,
       COUNT(*)      AS order_cnt,
       SUM(o.amount) AS total_amount,
       SUM(o.fee)    AS total_fee
FROM settlement_orders o
JOIN merchants m ON m.merchant_id = o.merchant_id
WHERE left(m.category, 1) = 'A'
GROUP BY m.merchant_id, m.merchant_name
ORDER BY total_amount DESC;
```

## API 一览

| 方法 | 路径 | 说明 |
|-----|------|------|
| POST | `/api/batches` | 提交对照批次（409=繁忙，400=SQL 校验失败） |
| GET  | `/api/batches` | 批次列表（不含完整计划 JSON，含锁观察摘要） |
| GET  | `/api/batches/{id}` | 批次详情（含 4 次计划 JSON 与结构摘要） |
| GET  | `/api/batches/{id}/locks` | 锁等待观察：最近一次采样 + 最后一次确认的链 |
| POST | `/api/batches/{id}/cancel` | 取消运行中的批次 |
| POST | `/api/sample-row` | 向 `settlement_orders` 补记一行 |

`/api/batches/{id}/locks` 的 `monitor.last_sample.ok` 为 `false` 时表示
**采样失败、状态未知**（不是「无阻塞」）；`monitor.last_confirmed` 保存
最后一次整条链核对通过的结果，`monitor.stop_reason` 和 `active` 用于判断
它是否已成历史观察。

## 目录结构

```
├── docker-compose.yml
├── db/init.sql            # 结算样表 + 倾斜数据
├── tests/                 # 锁等待链纯函数单元测试（pytest）
└── app/
    ├── Dockerfile
    ├── main.py            # FastAPI 入口
    ├── runner.py          # 批次执行器（单事务 4 次执行、限时、取消、名额管理）
    ├── lockmonitor.py     # 锁等待采样、阻塞链追踪、PID 核对
    ├── plans.py           # 计划结构归一化 + 结论判定
    ├── sqlcheck.py        # 只读 SELECT 校验
    ├── store.py           # 内存批次仓库
    └── static/            # 网页
```

运行单元测试（需要 `pytest`，不需要数据库）：

```bash
python3 -m pytest tests -q
```

> 说明：该工具面向受信任的内网工程师，输入的 SQL 会被直接执行（只读事务保护），
> 请勿暴露到公网。批次数据与锁观察结果保存在内存中，重启即清空。
