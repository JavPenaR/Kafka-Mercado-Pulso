const $ = selector => document.querySelector(selector);
const formatMoney = value => new Intl.NumberFormat('es-CL', { style: 'currency', currency: 'CLP', maximumFractionDigits: 0 }).format(value || 0);
const formatTime = value => new Date(value).toLocaleTimeString('es-CL', { hour: '2-digit', minute: '2-digit' });
const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character]);
let current = null;
let filter = 'all';
let toastTimer;
const statusText = { processing: 'Procesando', ready: 'Preparando', dispatched: 'Enviado', attention: 'Sin stock', failed: 'Pago rechazado' };
const eventText = {
  'order.created': ['Nuevo pedido recibido', 'orders', '▣'],
  'payment.evaluated': ['Pago evaluado', 'payments', '✓'],
  'inventory.checked': ['Inventario verificado', 'inventory', '◫'],
  'fulfillment.dispatched': ['Pedido despachado', 'fulfillment', '↗']
};
function showToast(message) { const el = $('#toast'); el.textContent = message; el.classList.add('show'); clearTimeout(toastTimer); toastTimer = setTimeout(() => el.classList.remove('show'), 3200); }
function render(data) {
  current = data;
  $('#connection').innerHTML = `<i></i> ${data.connected ? 'Sistema en vivo' : 'Conectando Kafka'}`;
  $('#connection').classList.toggle('offline', !data.connected);
  $('#simulator-btn').innerHTML = data.simulator ? 'Ⅱ&nbsp; Pausar simulación' : '▶&nbsp; Reanudar simulación';
  $('#metric-revenue').textContent = formatMoney(data.metrics.revenue);
  $('#metric-orders').textContent = data.metrics.orders.toLocaleString('es-CL');
  $('#metric-dispatched').textContent = data.metrics.dispatched.toLocaleString('es-CL');
  $('#metric-attention').textContent = data.metrics.attention.toLocaleString('es-CL');
  $('#order-count').textContent = data.orders.length;
  let orders = data.orders;
  if (filter === 'dispatched') orders = orders.filter(order => order.status === 'dispatched');
  if (filter === 'attention') orders = orders.filter(order => ['attention', 'failed'].includes(order.status));
  $('#orders-body').innerHTML = orders.length ? orders.slice(0, 20).map(order => `<tr><td><span class="order-id">${escapeHtml(order.orderId)}</span></td><td><div class="customer-cell"><span class="customer-avatar">${escapeHtml(order.customer.split(' ').map(p => p[0]).join('').slice(0, 2))}</span>${escapeHtml(order.customer)}</div></td><td><div class="product-cell"><span class="product-symbol">${({ headphones: '♫', coffee: '☕', shoe: '◈', lamp: '✦', bag: '▣', keyboard: '⌘', watch: '◷', speaker: '♪' })[order.icon] || '◈'}</span>${escapeHtml(order.product)}</div></td><td class="amount">${formatMoney(order.total)}</td><td><span class="status ${escapeHtml(order.status)}">${statusText[order.status] || 'Procesando'}</span></td><td>${formatTime(order.occurredAt)}</td></tr>`).join('') : `<tr><td colspan="6" class="empty">${filter === 'all' ? 'Esperando pedidos…' : 'No hay pedidos en esta categoría'}</td></tr>`;
  $('#activity-list').innerHTML = data.events.length ? data.events.slice(0, 7).map(event => {
    const [label, group, icon] = eventText[event.type] || [event.type, 'orders', '◈'];
    const detail = event.type === 'payment.evaluated' ? (event.result === 'approved' ? 'Aprobado' : 'Rechazado') : event.type === 'inventory.checked' ? (event.result === 'reserved' ? 'Stock reservado' : 'Sin stock') : event.type === 'fulfillment.dispatched' ? event.carrier : event.customer;
    return `<div class="activity-item"><div class="activity-icon ${group}">${icon}</div><div class="activity-copy"><strong>${escapeHtml(label)}</strong><span>${escapeHtml(event.orderId)} · ${escapeHtml(detail)}</span></div><div class="activity-time">${formatTime(event.occurredAt)}</div></div>`;
  }).join('') : '<div class="empty">Esperando eventos…</div>';
}
for (const button of document.querySelectorAll('.filter')) button.addEventListener('click', () => { filter = button.dataset.filter; document.querySelectorAll('.filter').forEach(b => b.classList.toggle('active', b === button)); if (current) render(current); });
$('#new-order-btn').addEventListener('click', () => $('#modal').classList.add('open'));
$('#modal-close').addEventListener('click', () => $('#modal').classList.remove('open'));
$('#modal').addEventListener('click', e => { if (e.target.id === 'modal') $('#modal').classList.remove('open'); });
$('#submit-order').addEventListener('click', async () => {
  const button = $('#submit-order'); button.disabled = true; button.textContent = 'Enviando…';
  try { const response = await fetch('/api/orders', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ scenario: $('#scenario').value }) }); const result = await response.json(); if (!response.ok) throw new Error(result.error); $('#modal').classList.remove('open'); showToast(`Pedido ${result.order.orderId} enviado a Kafka`); }
  catch (error) { showToast(error.message || 'No se pudo crear el pedido'); }
  finally { button.disabled = false; button.innerHTML = 'Enviar pedido <span>→</span>'; }
});
$('#simulator-btn').addEventListener('click', async () => {
  try { const response = await fetch('/api/simulator', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled: !current.simulator }) }); if (!response.ok) throw new Error(); showToast(current.simulator ? 'Simulación pausada' : 'Simulación reanudada'); }
  catch { showToast('No se pudo cambiar la simulación'); }
});
const stream = new EventSource('/api/stream');
stream.onmessage = message => render(JSON.parse(message.data));
stream.onerror = () => { $('#connection').innerHTML = '<i></i> Reconectando'; $('#connection').classList.add('offline'); };
