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
  orderDel: 'snack:orderdel',  // 已删除订单的「墓碑」：{ 单号: 删除时该单的提交时间 }
  rev:      'snack:rev'
};

// 把「已删除的单号」记入墓碑（保留更新的时间），并限制数量避免无限增长
function putTombstone(tomb, list) {
  if (!Array.isArray(list)) return tomb;
  for (const d of list) {
    if (d == null) continue;
    const no = String(typeof d === 'object' ? d.no : d);
    const at = Number(typeof d === 'object' ? d.at : 0) || 0;
    if (no === 'undefined' || no === 'null') continue;
    if (!tomb[no] || tomb[no] < at) tomb[no] = at;
  }
  const ks = Object.keys(tomb);
  if (ks.length > 1000) {
    ks.sort((a, b) => (tomb[a] || 0) - (tomb[b] || 0));
    ks.slice(0, ks.length - 1000).forEach(k => delete tomb[k]);
  }
  return tomb;
}

// 合并订单：按单号取并集（incoming 覆盖同号的旧数据），并剔除已删除的
function mergeOrders(cur, incoming, tomb) {
  // 墓碑只对「同一时间的那张单」有效；若同号订单的提交时间更晚（单号被重新启用），则不再算已删除
  const gone = o => o && o.no != null && tomb[String(o.no)] !== undefined
                 && (tomb[String(o.no)] || 0) >= (Number(o.at) || 0);
  const map = new Map();
  (Array.isArray(cur) ? cur : []).forEach(o => { if (o && o.no != null && !gone(o)) map.set(String(o.no), o); });
  (Array.isArray(incoming) ? incoming : []).forEach(o => { if (o && o.no != null && !gone(o)) map.set(String(o.no), o); });
  return [...map.values()];
}

const noRedis = res => res.status(503).json({
  error: 'Upstash 未配置：请设置 UPSTASH_REDIS_REST_URL 和 UPSTASH_REDIS_REST_TOKEN'
});

app.get('/api/health', (req, res) => res.json({ ok: true, redis: !!redis }));

// 读取全部共享数据（rev 为版本号，用于多设备判断是否有更新）
app.get('/api/state', async (req, res) => {
  if (!redis) return noRedis(res);
  try {
    const [menu, cats, users, orders, rev] = await Promise.all([
      redis.get(K.menu), redis.get(K.cats), redis.get(K.users), redis.get(K.orders), redis.get(K.rev)
    ]);
    res.json({ menu, cats, users, orders, rev: Number(rev) || 0 });
  } catch (e) {
    res.status(500).json({ error: String(e && e.message || e) });
  }
});

// 保存共享数据：body 里给哪个集合就只写哪个集合
// 订单特殊处理：按单号「合并」而不是整段覆盖，避免多台设备同时下单时互相把对方的新单冲掉
app.put('/api/state', async (req, res) => {
  if (!redis) return noRedis(res);
  try {
    const body = req.body || {};

    // ---- 订单：合并 + 删除墓碑 ----
    if (body.orders !== undefined || body.orderDel !== undefined) {
      const [cur, tombRaw] = await Promise.all([redis.get(K.orders), redis.get(K.orderDel)]);
      const tomb = (tombRaw && typeof tombRaw === 'object' && !Array.isArray(tombRaw)) ? { ...tombRaw } : {};
      putTombstone(tomb, body.orderDel);
      const merged = mergeOrders(cur, body.orders, tomb);
      await Promise.all([redis.set(K.orders, merged), redis.set(K.orderDel, tomb)]);
      const rev = await redis.incr(K.rev);
      // 把合并后的权威结果回给客户端，保证各设备立刻收敛
      return res.json({ ok: true, rev, orders: merged });
    }

    // ---- 其余集合：整段覆盖即可 ----
    const jobs = [];
    for (const c of ['menu', 'cats', 'users']) if (body[c] !== undefined) jobs.push(redis.set(K[c], body[c]));
    if (jobs.length) await Promise.all(jobs);
    const rev = await redis.incr(K.rev);
    res.json({ ok: true, rev });
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
