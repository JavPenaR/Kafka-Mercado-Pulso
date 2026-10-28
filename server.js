import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import crypto from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';

const root = path.dirname(fileURLToPath(import.meta.url));
const port = Number(process.env.PORT || 3000);
const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const intervalMs = Math.max(700, Number(process.env.SIMULATOR_INTERVAL_MS || 2400));
const topics = ['commerce.orders', 'commerce.payments', 'commerce.inventory', 'commerce.fulfillment'];
const kafka = new Kafka({ clientId: 'mercado-pulse', brokers, logLevel: logLevel.ERROR, retry: { retries: 12, initialRetryTime: 500 } });
const admin = kafka.admin();
const producer = kafka.producer();
const consumers = [];
const clients = new Set();
const state = { connected: false, simulator: process.env.SIMULATOR_ENABLED !== 'false', orders: new Map(), events: [], topicCounts: Object.fromEntries(topics.map(t => [t, 0])), bucketCounts: [], startedAt: Date.now() };

const catalog = [
  { name: 'Audífonos Studio Pro', category: 'Tecnología', price: 89990, icon: 'headphones' },
  { name: 'Cafetera Barista Mini', category: 'Hogar', price: 129990, icon: 'coffee' },
  { name: 'Zapatillas Urban Run', category: 'Moda', price: 64990, icon: 'shoe' },
  { name: 'Lámpara Nórdica', category: 'Hogar', price: 45990, icon: 'lamp' },
  { name: 'Mochila Transit 22L', category: 'Accesorios', price: 56990, icon: 'bag' },
  { name: 'Teclado Mecánico 75%', category: 'Tecnología', price: 109990, icon: 'keyboard' },
  { name: 'Reloj Aurora', category: 'Moda', price: 74990, icon: 'watch' },
  { name: 'Parlante Wave Mini', category: 'Tecnología', price: 39990, icon: 'speaker' }
];
const firstNames = ['Camila', 'Martín', 'Isidora', 'Diego', 'Valentina', 'Tomás', 'Antonia', 'Joaquín', 'Florencia', 'Benjamín', 'Catalina', 'Matías'];
const lastNames = ['González', 'Muñoz', 'Rojas', 'Díaz', 'Contreras', 'Silva', 'Martínez', 'Soto', 'Pérez', 'Fernández'];
const cities = ['Santiago', 'Valparaíso', 'Concepción', 'La Serena', 'Temuco', 'Rancagua'];
const pick = a => a[Math.floor(Math.random() * a.length)];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const money = n => Math.round(n);
const event = (type, orderId, data = {}) => ({ eventId: crypto.randomUUID(), type, orderId, occurredAt: new Date().toISOString(), ...data });

async function publish(topic, payload) {
  await producer.send({ topic, messages: [{ key: payload.orderId, value: JSON.stringify(payload) }] });
}

function broadcast() {
  const msg = `data: ${JSON.stringify(snapshot())}\n\n`;
  for (const client of clients) {
    try { client.write(msg); } catch { clients.delete(client); }
  }
}

function project(topic, payload, partition, offset) {
  const entry = { ...payload, topic, partition, offset, receivedAt: new Date().toISOString() };
  state.events.unshift(entry);
  state.events.length = Math.min(state.events.length, 160);
  state.topicCounts[topic]++;
  const bucket = Math.floor(new Date(payload.occurredAt).getTime() / 5000) * 5000;
  let current = state.bucketCounts.find(item => item.time === bucket);
  if (!current) {
    current = { time: bucket, total: 0, topics: Object.fromEntries(topics.map(t => [t, 0])) };
    state.bucketCounts.push(current);
    state.bucketCounts.sort((a, b) => a.time - b.time);
    state.bucketCounts = state.bucketCounts.slice(-24);
  }
  current.total++;
  current.topics[topic]++;
  let order = state.orders.get(payload.orderId) || { orderId: payload.orderId, status: 'processing', payment: 'pending', inventory: 'pending', fulfillment: 'pending' };
  if (topic === 'commerce.orders') {
    order = { ...order, ...payload };
  } else {
    if (topic === 'commerce.payments') order.payment = payload.result;
    if (topic === 'commerce.inventory') order.inventory = payload.result;
    if (topic === 'commerce.fulfillment') order.fulfillment = payload.result;
  }
  if (order.payment === 'declined') order.status = 'failed';
  else if (order.inventory === 'out_of_stock') order.status = 'attention';
  else if (order.fulfillment === 'dispatched' && order.payment === 'approved') order.status = 'dispatched';
  else if (order.payment === 'approved' && order.inventory === 'reserved') order.status = 'ready';
  state.orders.set(payload.orderId, order);
  broadcast();
}

function snapshot() {
  const orders = [...state.orders.values()].filter(o => o.occurredAt).sort((a, b) => b.occurredAt.localeCompare(a.occurredAt));
  const revenue = orders.filter(o => o.payment === 'approved').reduce((sum, o) => sum + o.total, 0);
  return {
    connected: state.connected, simulator: state.simulator, startedAt: state.startedAt,
    orders: orders.slice(0, 80), events: state.events.slice(0, 100), topicCounts: state.topicCounts,
    buckets: state.bucketCounts, topics,
    metrics: { orders: orders.length, revenue, dispatched: orders.filter(o => o.status === 'dispatched').length,
      attention: orders.filter(o => ['attention', 'failed'].includes(o.status)).length,
      approvalRate: orders.length ? Math.round(100 * orders.filter(o => o.payment === 'approved').length / orders.filter(o => o.payment !== 'pending').length || 0) : 0 }
  };
}

async function createOrder(scenario = 'normal') {
  const product = pick(catalog);
  const quantity = Math.random() < .15 ? 2 : 1;
  const total = money(product.price * quantity);
  const orderId = `MP-${String(Date.now()).slice(-7)}-${Math.floor(100 + Math.random() * 900)}`;
  const customer = `${pick(firstNames)} ${pick(lastNames)}`;
  const payload = event('order.created', orderId, {
    customer, city: pick(cities), product: product.name, category: product.category, icon: product.icon,
    quantity, total: scenario === 'high_value' ? total * 4 : total,
    channel: pick(['Web', 'App móvil', 'Marketplace']), scenario
  });
  await publish('commerce.orders', payload);
  return payload;
}

async function startConsumer(groupId, topicList, handler, fromBeginning = false) {
  const consumer = kafka.consumer({ groupId });
  consumers.push(consumer);
  await consumer.connect();
  for (const topic of topicList) await consumer.subscribe({ topic, fromBeginning });
  await consumer.run({ eachMessage: async ({ topic, partition, message }) => {
    try { await handler(topic, JSON.parse(message.value.toString()), partition, message.offset); }
    catch (err) { console.error(`Error procesando ${topic}:`, err); throw err; }
  } });
}

async function startKafka() {
  await admin.connect();
  const existing = new Set(await admin.listTopics());
  const missing = topics.filter(topic => !existing.has(topic));
  if (missing.length) await admin.createTopics({ topics: missing.map(topic => ({ topic, numPartitions: 3, replicationFactor: 1 })), waitForLeaders: true });
  await producer.connect();
  await startConsumer('mercado-pulse-order-worker', ['commerce.orders'], async (_topic, order) => {
    await sleep(300 + Math.random() * 850);
    const declined = order.scenario === 'stockout' ? false : order.scenario === 'high_value' ? Math.random() < .27 : Math.random() < .045;
    await publish('commerce.payments', event('payment.evaluated', order.orderId, { result: declined ? 'declined' : 'approved', amount: order.total, method: pick(['Visa •••• 4242', 'Mastercard •••• 1088', 'Débito •••• 9921']), riskScore: order.scenario === 'high_value' ? 72 + Math.floor(Math.random() * 22) : 4 + Math.floor(Math.random() * 24) }));
    if (declined) return;
    await sleep(300 + Math.random() * 600);
    const outOfStock = order.scenario === 'stockout' || Math.random() < .06;
    await publish('commerce.inventory', event('inventory.checked', order.orderId, { result: outOfStock ? 'out_of_stock' : 'reserved', sku: order.product.toLowerCase().replace(/[^a-z0-9]+/g, '-'), quantity: order.quantity, warehouse: pick(['CD Santiago Norte', 'CD Quilicura', 'CD San Bernardo']) }));
  });
  await startConsumer('mercado-pulse-fulfillment-worker', ['commerce.inventory'], async (_topic, inventory) => {
    if (inventory.result !== 'reserved') return;
    await sleep(600 + Math.random() * 1400);
    await publish('commerce.fulfillment', event('fulfillment.dispatched', inventory.orderId, { result: 'dispatched', carrier: pick(['Blue Express', 'Chilexpress', 'Starken']), tracking: `CL${Math.floor(100000000 + Math.random() * 900000000)}` }));
  });
  await startConsumer(`mercado-pulse-view-${crypto.randomUUID()}`, topics,
    (topic, payload, partition, offset) => project(topic, payload, partition, offset), true);
  state.connected = true;
  broadcast();
  console.log(`Kafka conectado: ${brokers.join(', ')}`);
}

const types = { '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.svg': 'image/svg+xml' };
function json(res, code, data) { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(data)); }
async function body(req) {
  let raw = '';
  for await (const chunk of req) { raw += chunk; if (raw.length > 10000) throw new Error('Solicitud demasiado grande'); }
  return raw ? JSON.parse(raw) : {};
}

const server = http.createServer(async (req, res) => {
  const pathname = new URL(req.url, `http://localhost:${port}`).pathname;
  try {
    if (pathname === '/api/health') return json(res, state.connected ? 200 : 503, { status: state.connected ? 'ok' : 'connecting', kafka: state.connected, brokers });
    if (pathname === '/api/state' && req.method === 'GET') return json(res, 200, snapshot());
    if (pathname === '/api/stream' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive' });
      res.write(`data: ${JSON.stringify(snapshot())}\n\n`);
      clients.add(res);
      req.on('close', () => clients.delete(res));
      return;
    }
    if (pathname === '/api/orders' && req.method === 'POST') {
      if (!state.connected) return json(res, 503, { error: 'Kafka aún no está conectado' });
      const input = await body(req);
      const scenario = ['normal', 'high_value', 'stockout'].includes(input.scenario) ? input.scenario : 'normal';
      return json(res, 202, { order: await createOrder(scenario) });
    }
    if (pathname === '/api/simulator' && req.method === 'POST') {
      const input = await body(req);
      if (typeof input.enabled !== 'boolean') return json(res, 400, { error: 'enabled debe ser booleano' });
      state.simulator = input.enabled;
      broadcast();
      return json(res, 200, { enabled: state.simulator });
    }
    const files = { '/': 'index.html', '/__kafka': 'kafka.html', '/app.css': 'app.css', '/app.js': 'app.js', '/kafka.js': 'kafka.js' };
    const file = files[pathname];
    if (!file) return json(res, 404, { error: 'No encontrado' });
    const content = await readFile(path.join(root, 'public', file));
    res.writeHead(200, { 'Content-Type': types[path.extname(file)], 'Cache-Control': 'no-store' });
    res.end(content);
  } catch (err) { console.error(err); json(res, err instanceof SyntaxError ? 400 : 500, { error: err.message }); }
});

server.listen(port, () => console.log(`Mercado Pulse: http://localhost:${port}`));
let closing = false;
async function shutdown() {
  if (closing) return; closing = true;
  server.close();
  for (const client of clients) client.end();
  await Promise.allSettled(consumers.map(c => c.disconnect()));
  await Promise.allSettled([producer.disconnect(), admin.disconnect()]);
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

for (let attempt = 1; !closing; attempt++) {
  try { await startKafka(); break; }
  catch (err) { console.error(`Kafka no disponible (intento ${attempt}): ${err.message}`); await sleep(Math.min(1000 * attempt, 8000)); }
}
setInterval(async () => {
  if (!state.connected || !state.simulator) return;
  try { await createOrder(Math.random() < .06 ? 'stockout' : Math.random() < .08 ? 'high_value' : 'normal'); }
  catch (err) { console.error('Error en simulador:', err); }
}, intervalMs).unref();
