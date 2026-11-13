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
  menu:   'snack:menu',
  cats:   'snack:cats',
  users:  'snack:users',
  orders: 'snack:orders',
  rev:    'snack:rev'
};
const COLLS = ['menu', 'cats', 'users', 'orders'];

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

// 保存共享数据：body 里给哪个集合就只写哪个集合（局部更新，避免互相覆盖）
app.put('/api/state', async (req, res) => {
  if (!redis) return noRedis(res);
  try {
    const body = req.body || {};
    const jobs = [];
    for (const c of COLLS) if (body[c] !== undefined) jobs.push(redis.set(K[c], body[c]));
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
