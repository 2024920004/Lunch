// 小吃点单后端：托管前端页面 + 用 Upstash Redis 保存共享数据
// 环境变量：
//   UPSTASH_REDIS_REST_URL     Upstash 控制台里的 REST URL
//   UPSTASH_REDIS_REST_TOKEN   Upstash 控制台里的 REST Token
//   PORT                       端口（Render 会自动注入）
import express from 'express';
import { Redis } from '@upstash/redis';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(express.json({ limit: '4mb' }));

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const redis = (REDIS_URL && REDIS_TOKEN) ? new Redis({ url: REDIS_URL, token: REDIS_TOKEN }) : null;

// 每种数据一个 key
const K = {
  menu:     'snack:menu',
  cats:     'snack:cats',
  users:    'snack:users',
  orders:   'snack:orders',
  orderDel: 'snack:orderdel',  // 已删除订单的「墓碑」：{ 订单唯一标识: 删除时该单的提交时间 }
  prepDone:    'snack:prep_done',     // 备料单「已完成」标记：{ 订单uid: 标记时间 }（多设备共享）
  pendingDone: 'snack:pending_done',  // 待处理看板「已完成」标记：{ 订单uid: 标记时间 }（多设备共享）
  rev:      'snack:rev'
};

// 订单唯一标识：单号会重复（自动编号与手填撞号、两台设备各自编号），
// 合并/删除一律按 uid 判定，单号只作显示。旧订单没有 uid 时按「单号+提交时间」推算，
// 与前端 orderUid() 的算法保持一致。
function orderUid(o) {
  if (!o) return null;
  if (o.uid) return String(o.uid);
  if (o.no == null) return null;
  return 'u' + String(o.no) + '_' + (Number(o.at) || 0);
}

// 把「已删除的订单」记入墓碑（保留更新的时间），并限制数量避免无限增长
function putTombstone(tomb, list) {
  if (!Array.isArray(list)) return tomb;
  for (const d of list) {
    if (d == null) continue;
    const uid = orderUid(typeof d === 'object' ? d : { no: d });
    if (!uid) continue;
    const at = Number(typeof d === 'object' ? d.at : 0) || 0;
    if (!tomb[uid] || tomb[uid] < at) tomb[uid] = at;
  }
  const ks = Object.keys(tomb);
  if (ks.length > 1000) {
    ks.sort((a, b) => (tomb[a] || 0) - (tomb[b] || 0));
    ks.slice(0, ks.length - 1000).forEach(k => delete tomb[k]);
  }
  return tomb;
}

// 合并订单：按唯一标识取并集（incoming 覆盖同一张单的旧数据），并剔除已删除的
function mergeOrders(cur, incoming, tomb) {
  const gone = o => { const k = orderUid(o); return k != null && tomb[k] !== undefined && (tomb[k] || 0) >= (Number(o.at) || 0); };
  const map = new Map();
  (Array.isArray(cur) ? cur : []).forEach(o => { const k = orderUid(o); if (k && !gone(o)) map.set(k, o); });
  (Array.isArray(incoming) ? incoming : []).forEach(o => { const k = orderUid(o); if (k && !gone(o)) map.set(k, o); });
  return [...map.values()];
}

// 「已完成」标记（备料单 / 待处理看板）：各设备各点各的单，所以按 uid 取并集而不是整段覆盖，
// 避免后点的设备把先点的那批标记冲掉。标记超过 2 天自动清掉，避免无限增长（订单都是当天用完）。
// 取值带符号：正数 = 标记完成的时间；负数 = 撤回完成的时间（撤回后要能在所有设备上真正取消，
// 若单纯删除会被别的设备的并集推送复活，所以按「绝对值最大的那次操作」取胜，即最新操作生效）。
const DONE_MAX_AGE = 2 * 24 * 3600 * 1000;
function mergeMarks(cur, incoming) {
  const now = Date.now();
  const out = {};
  const put = v => {
    if (!v || typeof v !== 'object' || Array.isArray(v)) return;
    for (const k of Object.keys(v)) {
      const at = Number(v[k]) || 0;
      if (!at || now - Math.abs(at) > DONE_MAX_AGE) continue;
      if (!out[k] || Math.abs(out[k]) < Math.abs(at)) out[k] = at;
    }
  };
  put(cur); put(incoming);
  return out;
}

const noRedis = res => res.status(503).json({
  error: 'Upstash 未配置：请设置 UPSTASH_REDIS_REST_URL 和 UPSTASH_REDIS_REST_TOKEN'
});

app.get('/api/health', (req, res) => res.json({ ok: true, redis: !!redis }));

// 实时推送（SSE）：任何一次写入后把新版本号推给所有在线页面，
// 让别的设备立刻看到变化，而不用等下一次轮询（把 3 秒延迟降到几乎实时）。
const sseClients = new Set();
app.get('/api/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });
  res.write('retry: 3000\n\n');
  res.write(`data: ${JSON.stringify({ rev: 0 })}\n\n`);   // 先发一帧，让前端立刻握手
  sseClients.add(res);
  const hb = setInterval(() => { try { res.write(': hb\n\n'); } catch (e) { /* 连接已断，close 事件会清理 */ } }, 20000);
  req.on('close', () => { clearInterval(hb); sseClients.delete(res); });
});
function broadcast(rev) {
  const msg = `data: ${JSON.stringify({ rev })}\n\n`;
  for (const c of sseClients) { try { c.write(msg); } catch (e) { sseClients.delete(c); } }
}

// 读取全部共享数据（rev 为版本号，用于多设备判断是否有更新）
app.get('/api/state', async (req, res) => {
  if (!redis) return noRedis(res);
  try {
    const [menu, cats, users, orders, prepDone, pendingDone, rev] = await Promise.all([
      redis.get(K.menu), redis.get(K.cats), redis.get(K.users), redis.get(K.orders),
      redis.get(K.prepDone), redis.get(K.pendingDone), redis.get(K.rev)
    ]);
    res.json({ menu, cats, users, orders, prepDone, pendingDone, rev: Number(rev) || 0 });
  } catch (e) {
    res.status(500).json({ error: String(e && e.message || e) });
  }
});

// 保存共享数据：body 里给哪个集合就只写哪个集合
// 订单特殊处理：按唯一标识「合并」而不是整段覆盖，避免多台设备同时下单时互相把对方的新单冲掉
app.put('/api/state', async (req, res) => {
  if (!redis) return noRedis(res);
  try {
    const body = req.body || {};
    const out = { ok: true };

    // ---- 订单：合并 + 删除墓碑 ----
    if (body.orders !== undefined || body.orderDel !== undefined) {
      const [cur, tombRaw] = await Promise.all([redis.get(K.orders), redis.get(K.orderDel)]);
      const tomb = (tombRaw && typeof tombRaw === 'object' && !Array.isArray(tombRaw)) ? { ...tombRaw } : {};
      putTombstone(tomb, body.orderDel);
      const merged = mergeOrders(cur, body.orders, tomb);
      await Promise.all([redis.set(K.orders, merged), redis.set(K.orderDel, tomb)]);
      // 把合并后的权威结果回给客户端，保证各设备立刻收敛
      out.orders = merged;
    }

    // ---- 「已完成」标记：按 uid 取并集，把合并后的权威结果回给客户端，保证多人立刻收敛 ----
    if (body.prepDone !== undefined || body.pendingDone !== undefined) {
      const [curPrep, curPend] = await Promise.all([redis.get(K.prepDone), redis.get(K.pendingDone)]);
      const prep = body.prepDone !== undefined ? mergeMarks(curPrep, body.prepDone) : curPrep;
      const pend = body.pendingDone !== undefined ? mergeMarks(curPend, body.pendingDone) : curPend;
      await Promise.all([redis.set(K.prepDone, prep || {}), redis.set(K.pendingDone, pend || {})]);
      out.prepDone = prep || {};
      out.pendingDone = pend || {};
    }

    // ---- 其余集合：整段覆盖即可 ----
    const jobs = [];
    for (const c of ['menu', 'cats', 'users']) if (body[c] !== undefined) jobs.push(redis.set(K[c], body[c]));
    if (jobs.length) await Promise.all(jobs);

    out.rev = await redis.incr(K.rev);
    broadcast(out.rev);          // 立刻通知所有在线页面有更新
    res.json(out);
  } catch (e) {
    res.status(500).json({ error: String(e && e.message || e) });
  }
});

// 前端静态文件（index.html 与 server.js 同目录）
app.use(express.static(__dirname, { index: 'index.html' }));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`🍢 小吃点单已启动：http://localhost:${port}  (Redis: ${redis ? '已连接' : '未配置'})`);
});
