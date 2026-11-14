const PRESETS = {
  skew: `SELECT m.merchant_id,
       m.merchant_name,
       COUNT(*)      AS order_cnt,
       SUM(o.amount) AS total_amount,
       SUM(o.fee)    AS total_fee
FROM settlement_orders o
JOIN merchants m ON m.merchant_id = o.merchant_id
WHERE left(m.category, 1) = 'A'
GROUP BY m.merchant_id, m.merchant_name
ORDER BY total_amount DESC`,
  single: `SELECT o.order_no, o.amount, o.fee, o.settle_date, m.merchant_name
FROM settlement_orders o
JOIN merchants m ON m.merchant_id = o.merchant_id
WHERE m.merchant_id = 42
  AND o.settle_date >= DATE '2026-11-01'
  AND o.settle_date <  DATE '2026-11-14'
ORDER BY o.settle_date DESC, o.order_no`,
  daily: `SELECT settle_date, COUNT(*) AS order_cnt, SUM(amount) AS total_amount
FROM settlement_orders
WHERE settle_date >= DATE '2026-11-01'
  AND settle_date <  DATE '2026-11-14'
GROUP BY settle_date
ORDER BY settle_date`,
};

const sqlInput = document.getElementById('sql-input');
sqlInput.value = PRESETS.skew;
document.getElementById('preset').onchange = (e) => {
  sqlInput.value = PRESETS[e.target.value] || '';
};

const expanded = new Set();   // 展开了计划详情的批次 id

let lockFocusId = null;      // 锁等待面板当前查看的批次
let lockManual = false;      // 用户是否手动选过批次
let lastRunningId = null;    // 上一轮看到的运行中批次，用于新批次开始时自动切换
let lastLockRender = '';     // 避免每轮重绘导致展开状态丢失
let lockIntervalMs = 1000;

const STATUS_TEXT = {
  pending: '排队中', running: '运行中', completed: '已完成',
  failed: '失败', timeout: '超时', cancelled: '已取消',
};
const SETTING_TEXT = {
  default: '默认 (enable_nestloop=on)',
  nestloop_off: '抑制嵌套循环 (enable_nestloop=off)',
};
const EX_STATUS_TEXT = {
  pending: '待执行', running: '执行中', ok: '成功',
  timeout: '超时', cancelled: '已取消', error: '报错',
};
const VERDICT_TEXT = {
  improved: '有改善', not_improved: '无改善', inconclusive: '无法判断改善',
};
const STOP_REASON_TEXT = {
  completed: '批次已完成', failed: '批次报错结束', timeout: '批次超时结束',
  cancelled: '批次已取消', shutdown: '服务停止',
};

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}
function fmtMs(v) { return v == null ? '—' : Number(v).toFixed(1) + ' ms'; }
function fmtTime(ts) { return ts ? new Date(ts * 1000).toLocaleString() : '—'; }

async function refresh() {
  try {
    const res = await fetch('/api/batches');
    const data = await res.json();
    renderSlot(data);
    await renderBatches(data.batches);
    await refreshLocks(data.batches);
  } catch (e) { /* 忽略瞬时网络错误，下一轮再试 */ }
}

function renderSlot(data) {
  const pill = document.getElementById('slot-pill');
  if (data.busy) {
    pill.textContent = '运行中（名额已占用）';
    pill.className = 'pill busy';
  } else {
    pill.textContent = '空闲，可提交对照';
    pill.className = 'pill idle';
  }
}

function conclusionHtml(b) {
  if (b.conclusion) {
    const v = b.conclusion.verdict;
    return `<div class="conclusion ${v}">
      <b>${VERDICT_TEXT[v] || v}</b> · ${escapeHtml(b.conclusion.message)}
    </div>`;
  }
  if (['failed', 'timeout', 'cancelled'].includes(b.status)) {
    const reason = { failed: '报错', timeout: '超时', cancelled: '已取消' }[b.status];
    return `<div class="conclusion none">无比较结论（批次${reason}）</div>`;
  }
  return '';
}

function executionsHtml(b) {
  if (!b.executions.length) {
    return b.status === 'running' ? '<p class="hint">正在建立事务快照…</p>' : '';
  }
  const rows = b.executions.map(ex => `<tr>
    <td>第 ${ex.round} 轮</td>
    <td>#${ex.seq}</td>
    <td>${SETTING_TEXT[ex.setting] || ex.setting}</td>
    <td><span class="ex-status ${ex.status}">${EX_STATUS_TEXT[ex.status] || ex.status}</span></td>
    <td>${fmtMs(ex.execution_time_ms)}</td>
    <td>${fmtMs(ex.planning_time_ms)}</td>
    <td>${ex.error ? escapeHtml(ex.error) : ''}</td>
  </tr>`).join('');
  return `<table class="ex-table">
    <thead><tr><th>轮次</th><th>顺序</th><th>设置</th><th>状态</th>
    <th>执行耗时</th><th>规划耗时</th><th>备注</th></tr></thead>
    <tbody>${rows}</tbody></table>`;
}

function batchCardHtml(b) {
  const cancelBtn = (b.status === 'running' || b.status === 'pending')
    ? (b.cancel_requested
        ? '<span class="msg warn">取消中…</span>'
        : `<button class="btn-cancel" data-cancel="${b.id}">取消本批</button>`)
    : '';
  const toggleBtn = b.executions.length
    ? `<button class="btn-toggle" data-toggle="${b.id}">${expanded.has(b.id) ? '收起计划' : '查看计划与结构'}</button>`
    : '';
  const locksBtn = b.lock_monitor
    ? `<button class="btn-toggle" data-locks="${b.id}">在锁等待观察中查看</button>`
    : '';
  return `<div class="card batch">
    <div class="batch-head">
      <span class="badge ${b.status}">${STATUS_TEXT[b.status] || b.status}</span>
      <code>#${b.id}</code>
      <span class="hint">提交于 ${fmtTime(b.created_at)}</span>
      <span class="spacer"></span>
      ${cancelBtn}${locksBtn}${toggleBtn}
    </div>
    ${lockSummaryLine(b)}
    <pre class="sql">${escapeHtml(b.sql)}</pre>
    ${b.error ? `<div class="conclusion none">错误：${escapeHtml(b.error)}</div>` : ''}
    ${conclusionHtml(b)}
    ${executionsHtml(b)}
    <div class="detail" id="detail-${b.id}"></div>
  </div>`;
}

function lockSummaryLine(b) {
  const m = b.lock_monitor;
  if (!m) return '';
  const parts = [];
  if (m.last_sample_ok === false) {
    parts.push(`<span class="lock-bad">锁等待采样失败，状态无法确认</span>`);
  } else if (m.last_sample_ok && m.chain_count > 0) {
    const bad = m.unconfirmed_chain_count
      ? `，其中 ${m.unconfirmed_chain_count} 条无法确认` : '';
    const past = !m.active ? '（批次结束前的最后采样，历史观察）' : '';
    parts.push(`<span class="${m.unconfirmed_chain_count ? 'lock-bad' : 'lock-ok'}">采样到 ${m.chain_count} 条等待链${bad}${past}</span>`);
  } else if (m.last_sample_ok) {
    const hist = !m.active && m.last_confirmed_chain_count
      ? `（最后一次确认于 ${fmtTime(m.last_confirmed_at)}，历史观察）` : '';
    parts.push(`<span class="lock-ok">采样时无等待锁${hist}</span>`);
  } else if (!m.active && m.last_confirmed_chain_count) {
    parts.push(`<span class="lock-bad">观察期间曾确认到等待链；结束前最后一次采样失败，当前状态未知</span>`);
  } else if (!m.active && m.sample_count === 0 && m.error_count > 0) {
    parts.push(`<span class="lock-bad">锁等待采样失败，未能观察</span>`);
  } else if (!m.active && m.sample_count === 0) {
    parts.push(`<span class="hint">批次结束太快，未采集到锁等待样本（不代表无阻塞）</span>`);
  } else {
    parts.push(`<span class="hint">等待采样…</span>`);
  }
  return `<div class="lock-summary">${parts.join('')}</div>`;
}

async function renderBatches(batches) {
  const container = document.getElementById('batches');
  container.innerHTML = batches.length
    ? batches.map(batchCardHtml).join('')
    : '<p class="hint">暂无批次</p>';
  container.querySelectorAll('[data-cancel]').forEach(btn => {
    btn.onclick = () => cancelBatch(btn.dataset.cancel);
  });
  container.querySelectorAll('[data-toggle]').forEach(btn => {
    btn.onclick = () => toggleDetail(btn.dataset.toggle);
  });
  container.querySelectorAll('[data-locks]').forEach(btn => {
    btn.onclick = () => {
      lockManual = true;
      lockFocusId = btn.dataset.locks;
      lastLockRender = '';
      refresh();
    };
  });
  for (const id of expanded) {
    if (batches.some(b => b.id === id)) {
      const box = document.getElementById(`detail-${id}`);
      if (box && !box.innerHTML) await loadDetail(id);
    }
  }
}

async function loadDetail(id) {
  const box = document.getElementById(`detail-${id}`);
  if (!box) return;
  try {
    const res = await fetch(`/api/batches/${id}`);
    if (!res.ok) { box.innerHTML = ''; return; }
    const d = await res.json();
    box.innerHTML = d.executions.map(ex => {
      const struct = (ex.structure_lines || []).join('\n');
      const plan = ex.plan ? JSON.stringify(ex.plan, null, 2) : '（无计划）';
      return `<div class="ex-detail">
        <h4>第 ${ex.round} 轮 · #${ex.seq} · ${SETTING_TEXT[ex.setting]} ·
            ${EX_STATUS_TEXT[ex.status]}${ex.execution_time_ms != null ? ' · ' + fmtMs(ex.execution_time_ms) : ''}</h4>
        ${struct ? `<pre class="struct">${escapeHtml(struct)}</pre>` : ''}
        <details><summary>完整计划 JSON</summary><pre class="plan">${escapeHtml(plan)}</pre></details>
      </div>`;
    }).join('');
  } catch (e) { /* 下一轮再试 */ }
}

async function toggleDetail(id) {
  if (expanded.has(id)) {
    expanded.delete(id);
    const box = document.getElementById(`detail-${id}`);
    if (box) box.innerHTML = '';
  } else {
    expanded.add(id);
  }
  await loadDetail(id);
  refresh();
}

async function cancelBatch(id) {
  await fetch(`/api/batches/${id}/cancel`, { method: 'POST' });
  refresh();
}

// ---- 锁等待观察面板 ----

function fmtTimeMs(ts) {
  return ts ? new Date(ts * 1000).toLocaleTimeString() : '—';
}

function issueHtml(issues) {
  if (!issues || !issues.length) return '';
  return `<div class="lock-issues">无法确认原因：${escapeHtml(issues.join('；'))}</div>`;
}

function waitObjectsHtml(objs) {
  if (!objs || !objs.length) return '';
  return `<div class="lock-waits">等待对象：${objs.map(o =>
    `<code class="lock-mode">${escapeHtml(o.mode || '')}</code> ${escapeHtml(o.summary)}`
  ).join('；')}</div>`;
}

function nodeHtml(node, isLast) {
  const verified = node.verified
    ? '<span class="lock-ok">已核对</span>'
    : '<span class="lock-bad">无法确认</span>';
  const role = { waiter: '等待会话', blocker: '阻塞方', holder: '持锁者', batch: '本批会话' }[node.role] || node.role;
  const pid = node.pid == null ? 'PID 未知' : `PID ${node.pid}`;
  const started = node.backend_start
    ? `启动于 ${escapeHtml(node.backend_start)}`
    : '会话未找到';
  const waitInfo = node.waiting
    ? `<div class="lock-waits">正在等待：${escapeHtml(node.wait_event_type || '')} ${escapeHtml(node.wait_event || '')}</div>`
    : '';
  return `<div class="lock-node ${isLast ? 'holder' : ''} ${node.verified ? '' : 'unverified'}">
    <div class="lock-node-head">
      <span class="lock-role">${role}</span>
      <b>${pid}</b>
      <span class="hint">${started}</span>
      ${verified}
      ${node.is_batch_session ? '<span class="lock-tag">本批次 SQL 会话</span>' : ''}
    </div>
    ${waitInfo}
    ${node.sql_summary ? `<pre class="lock-sql">${escapeHtml(node.sql_summary)}</pre>` : '<div class="hint">（无 SQL 摘要）</div>'}
    ${node.issue_text ? `<div class="lock-issues">${escapeHtml(node.issue_text)}</div>` : ''}
    ${isLast && node.role === 'holder' ? '<div class="lock-holder-tag">↑ 链条终点：该会话持有锁，未在等待</div>' : ''}
  </div>`;
}

function chainHtml(chain, index, live = true) {
  const nodes = chain.nodes || [];
  const st = chain.confirmed
    ? '<span class="lock-status ok">已确认阻塞链</span>'
    : '<span class="lock-status bad">无法确认</span>';
  const batchTag = chain.batch_waiting
    ? `<span class="lock-tag">${live ? '本批次 SQL 正在等锁' : '采样当时本批次 SQL 在等锁'}</span>`
    : '';
  return `<div class="lock-chain ${chain.confirmed ? 'confirmed' : 'unconfirmed'}">
    <div class="lock-chain-head">
      <b>链 ${index + 1}</b>
      ${st}${batchTag}
      <span class="hint">采样于 ${fmtTimeMs(chain.observed_at)}</span>
      ${chain.holder_pid != null ? `<span class="hint">持锁者 PID ${chain.holder_pid}</span>` : ''}
    </div>
    ${waitObjectsHtml(chain.wait_objects)}
    <div class="lock-chain-body">
      ${nodes.map((n, i) => `${i > 0 ? '<div class="lock-arrow">↑ 被其阻塞</div>' : ''}${nodeHtml(n, i === nodes.length - 1)}`).join('')}
    </div>
    ${issueHtml(chain.issues)}
  </div>`;
}

function monitorHeaderHtml(m) {
  const live = m.active;
  const statePill = live
    ? '<span class="pill busy">采样中</span>'
    : `<span class="pill idle">已停止${m.stop_reason ? '（' + escapeHtml(STOP_REASON_TEXT[m.stop_reason] || m.stop_reason) + '）' : ''}</span>`;
  const pidInfo = m.batch_pid
    ? `<span class="hint">本批会话 PID <b>${m.batch_pid}</b>${m.batch_pid_backend_start ? ' · 启动于 ' + escapeHtml(m.batch_pid_backend_start) : ''}</span>`
    : '<span class="hint">本批会话尚未建立</span>';
  return `<div class="lock-head">
    ${statePill}${pidInfo}
    <span class="spacer"></span>
    <span class="hint">已采样 ${m.sample_count} 次${m.error_count ? ' · 失败 ' + m.error_count + ' 次' : ''} · 上次 ${fmtTimeMs((m.last_sample || {}).sampled_at)} · 间隔 ${m.interval_ms}ms</span>
  </div>`;
}

function lastSampleErrorHtml(m) {
  if (!m.last_sample || m.last_sample.ok !== false) return '';
  return `<div class="lock-failure">
    <b>采样失败：状态无法确认</b>（不能据此判断为「无阻塞」）
    <div class="hint">${escapeHtml(m.last_sample.error || '')}</div>
  </div>`;
}

function historicalHtml(m) {
  const lc = m.last_confirmed;
  if (!lc) return '';
  const hist = !m.active;
  return `<div class="lock-historical ${hist ? 'past' : ''}">
    <div class="lock-historical-head">
      <b>最后一次确认的阻塞链</b>
      <span class="hint">确认于 ${fmtTimeMs(lc.sampled_at)}</span>
      ${hist
        ? '<span class="lock-tag past">历史观察：批次已结束，这是采样当时的记录，不代表当前锁状态</span>'
        : '<span class="hint">（本次运行中已确认；之后变化以最新采样为准）</span>'}
    </div>
    ${(lc.chains || []).map((c, i) => chainHtml(c, i, !hist)).join('')}
  </div>`;
}

function currentChainsHtml(m) {
  const s = m.last_sample;
  if (!s) {
    return m.error_count > 0
      ? '<p class="hint lock-bad">采样失败过，且尚未取得有效样本；当前锁状态未知。</p>'
      : '<p class="hint">等待首次采样…</p>';
  }
  if (s.ok === false) return '<p class="hint">最近一次采样失败，当前锁状态未知。</p>';

  const chains = s.chains || [];
  if (!chains.length) {
    if (m.active) {
      return '<p class="hint lock-ok">最近一次采样时没有等待锁；本批 SQL 若慢，更可能是计划本身或数据量，而非锁阻塞。</p>';
    }
    const hist = m.last_confirmed_chain_count
      ? '（此前的等待链见下方最后一次确认记录）' : '';
    return `<p class="hint">最近一次采样时没有等待锁。批次已结束，该结论仅代表采样当时${hist}。</p>`;
  }
  let html = chains.map(chainHtml).join('');
  if (s.truncated) html += '<p class="hint">等待链超过展示上限，仅显示前若干条。</p>';
  return html;
}

function batchSessionHtml(m) {
  const bs = (m.last_sample || {}).batch_session;
  if (!bs) return '';
  return `<div class="lock-batch-session">${nodeHtml({ ...bs, role: 'batch' }, false)}</div>`;
}

async function refreshLocks(batches) {
  const running = batches.find(b => b.status === 'running' || b.status === 'pending');
  const runningId = running ? running.id : null;
  // 有新批次开始运行时，自动把观察面板切到它（除非用户正手动看别的批次）。
  if (runningId && runningId !== lastRunningId) {
    lockFocusId = runningId;
    lockManual = false;
  }
  lastRunningId = runningId;
  if (!lockFocusId || !batches.some(b => b.id === lockFocusId)) {
    lockFocusId = runningId || (batches[0] ? batches[0].id : null);
    lockManual = false;
  }

  const focus = document.getElementById('lock-focus');
  const panel = document.getElementById('lock-panel');
  if (!lockFocusId) {
    focus.textContent = '';
    panel.innerHTML = '<p class="hint">提交一个对照批次后，这里会显示运行期间的锁等待情况。</p>';
    lastLockRender = '';
    return;
  }
  focus.textContent = '· 批次 #' + lockFocusId;
  try {
    const res = await fetch(`/api/batches/${lockFocusId}/locks`);
    if (!res.ok) { panel.innerHTML = '<p class="hint">锁等待状态读取失败</p>'; return; }
    const d = await res.json();
    const m = d.monitor;
    let html;
    if (!m) {
      html = '<p class="hint">该批次没有锁等待观察数据（批次未实际运行）。</p>';
    } else {
      lockIntervalMs = m.interval_ms || lockIntervalMs;
      html = monitorHeaderHtml(m)
        + lastSampleErrorHtml(m)
        + batchSessionHtml(m)
        + `<div class="lock-current">${currentChainsHtml(m)}</div>`
        + historicalHtml(m);
    }
    if (html !== lastLockRender) {
      lastLockRender = html;
      panel.innerHTML = html;
    }
    document.getElementById('lock-interval').textContent = String(Math.round(lockIntervalMs / 100) / 10);
  } catch (e) {
    panel.innerHTML = '<p class="hint">锁等待状态读取失败（网络错误），当前锁状态未知。</p>';
    lastLockRender = '';
  }
}

document.getElementById('run-btn').onclick = async () => {
  const msg = document.getElementById('submit-msg');
  msg.className = 'msg';
  msg.textContent = '提交中…';
  try {
    const res = await fetch('/api/batches', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: sqlInput.value }),
    });
    if (res.status === 409) {
      msg.textContent = '繁忙：已有对照批次在运行，请稍后再试';
      msg.classList.add('warn');
    } else if (res.status === 400) {
      const d = await res.json();
      msg.textContent = '校验失败：' + (d.detail || '');
      msg.classList.add('err');
    } else if (res.ok) {
      msg.textContent = '批次已创建，开始运行';
      msg.classList.add('ok');
    } else {
      msg.textContent = '提交失败（HTTP ' + res.status + '）';
      msg.classList.add('err');
    }
  } catch (e) {
    msg.textContent = '网络错误：' + e;
    msg.classList.add('err');
  }
  refresh();
};

document.getElementById('insert-btn').onclick = async () => {
  const msg = document.getElementById('insert-msg');
  msg.className = 'msg';
  try {
    const res = await fetch('/api/sample-row', { method: 'POST' });
    const d = await res.json();
    if (d.ok) {
      msg.textContent = `已补记 ${d.row.order_no}（商户 ${d.row.merchant_id}，金额 ${d.row.amount}，结算日 ${d.row.settle_date}）`;
      msg.classList.add('ok');
    } else {
      msg.textContent = '写入失败';
      msg.classList.add('err');
    }
  } catch (e) {
    msg.textContent = '网络错误：' + e;
    msg.classList.add('err');
  }
};

refresh();
setInterval(refresh, 1500);
