const $ = selector => document.querySelector(selector);
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
const topicColors = { 'commerce.orders': '#897bfb', 'commerce.payments': '#4ed4ae', 'commerce.inventory': '#f3b365', 'commerce.fulfillment': '#65a7eb' };
let latest = null;
let filter = 'all';
let renderedEvents = [];
function formatTime(date) { return new Date(date).toLocaleTimeString('es-CL', { hour: '2-digit', minute: '2-digit', second: '2-digit' }); }
function drawChart(data) {
  const canvas = $('#throughput-chart'); const dpr = devicePixelRatio || 1; const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr; canvas.height = rect.height * dpr;
  const ctx = canvas.getContext('2d'); ctx.scale(dpr, dpr); const w = rect.width, h = rect.height;
  const left = 26, right = 8, top = 13, bottom = 24;
  const now = Math.floor(Date.now() / 5000) * 5000;
  const buckets = Array.from({ length: 24 }, (_, i) => { const time = now - (23 - i) * 5000; return data.buckets.find(b => b.time === time) || { time, total: 0, topics: {} }; });
  const max = Math.max(4, ...buckets.map(b => b.total));
  ctx.font = '9px DM Sans, Arial'; ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) { const y = top + (h - top - bottom) * i / 4; ctx.strokeStyle = '#2b354c'; ctx.setLineDash([3, 4]); ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(w - right, y); ctx.stroke(); ctx.setLineDash([]); ctx.fillStyle = '#64718a'; ctx.fillText(String(Math.round(max * (1 - i / 4))), left - 7, y + 3); }
  const gap = 4; const bar = Math.max(3, (w - left - right) / 24 - gap);
  for (let i = 0; i < 24; i++) { const x = left + i * (w - left - right) / 24 + gap / 2; let y = h - bottom; for (const topic of data.topics) { const count = buckets[i].topics[topic] || 0; const height = count / max * (h - top - bottom); if (height) { y -= height; ctx.fillStyle = topicColors[topic]; ctx.fillRect(x, y, bar, height); } } }
  ctx.fillStyle = '#64718a'; ctx.textAlign = 'center'; ctx.fillText('-2m', left + 8, h - 5); ctx.fillText('-1m', (w + left) / 2, h - 5); ctx.fillText('ahora', w - right - 15, h - 5);
}
function render(data) {
  latest = data;
  $('#obs-connection').innerHTML = `<i></i> ${data.connected ? 'BROKER ONLINE' : 'CONECTANDO'}`;
  $('#obs-total').textContent = Object.values(data.topicCounts).reduce((a, b) => a + b, 0).toLocaleString('es-CL');
  const recent = data.events.filter(e => Date.now() - new Date(e.occurredAt).getTime() < 60000);
  $('#obs-rate').innerHTML = `${(recent.length / 60).toFixed(1)} <em>evt/s</em>`;
  $('#obs-topics').innerHTML = `${Object.values(data.topicCounts).filter(Boolean).length} <em>/ 4</em>`;
  $('#obs-status').textContent = data.connected ? '● Operativo' : '● Conectando';
  ['orders', 'payments', 'inventory', 'fulfillment'].forEach(name => { $(`#count-${name}`).textContent = (data.topicCounts[`commerce.${name}`] || 0).toLocaleString('es-CL'); });
  drawChart(data);
  renderedEvents = data.events.filter(e => filter === 'all' || e.topic === filter).slice(0, 60);
  $('#stream-body').innerHTML = renderedEvents.length ? renderedEvents.map((event, i) => { const group = event.topic.split('.')[1]; return `<tr><td>${formatTime(event.occurredAt)}</td><td><span class="topic-badge ${group}">${esc(event.topic)}</span></td><td><span class="event-name">${esc(event.type)}</span></td><td>${esc(event.orderId)}</td><td>${esc(event.partition)}</td><td>${esc(event.offset)}</td><td><button data-index="${i}">Ver JSON →</button></td></tr>`; }).join('') : '<tr><td colspan="7" class="empty">Esperando mensajes de Kafka…</td></tr>';
}
$('#topic-filter').addEventListener('change', e => { filter = e.target.value; if (latest) render(latest); });
$('#stream-body').addEventListener('click', e => { const button = e.target.closest('button[data-index]'); if (!button) return; const event = renderedEvents[Number(button.dataset.index)]; if (!event) return; const { topic, partition, offset, receivedAt, ...payload } = event; $('#payload-meta').innerHTML = `Tópico: <strong>${esc(topic)}</strong><br>Partición: <strong>${esc(partition)}</strong> &nbsp; Offset: <strong>${esc(offset)}</strong><br>Clave Kafka: <strong>${esc(event.orderId)}</strong>`; $('#payload-json').textContent = JSON.stringify(payload, null, 2); $('#drawer-backdrop').classList.add('open'); $('#payload-drawer').classList.add('open'); });
function closeDrawer() { $('#drawer-backdrop').classList.remove('open'); $('#payload-drawer').classList.remove('open'); }
$('#drawer-close').addEventListener('click', closeDrawer); $('#drawer-backdrop').addEventListener('click', closeDrawer);
const stream = new EventSource('/api/stream');
stream.onmessage = message => render(JSON.parse(message.data));
stream.onerror = () => { $('#obs-connection').innerHTML = '<i></i> RECONECTANDO'; };
addEventListener('resize', () => { if (latest) drawChart(latest); });
setInterval(() => { if (latest) drawChart(latest); }, 5000);
