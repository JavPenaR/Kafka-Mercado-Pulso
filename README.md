# Mercado Pulse

Plataforma local de operaciones de comercio con un flujo real de eventos por Apache Kafka. La página principal muestra los pedidos, pagos, inventario y despachos. El visor de Kafka está en **http://localhost:3000/__kafka** y no aparece en la navegación principal.

Manual para estudiantes: [Kafka conectado a una plataforma real](output/pdf/manual_kafka_mercado_pulse_estudiantes.pdf).

## Requisitos

- Node.js 20 o superior
- Docker Desktop con el motor de contenedores iniciado

## Iniciar

```powershell
docker compose up -d
npm install
npm start
```

Abrir **http://localhost:3000**. La simulación empieza automáticamente y envía pedidos a Kafka cada pocos segundos. También se puede crear un pedido desde la interfaz. Para detener: `Ctrl+C` y `docker compose down`.

## Arquitectura

```text
Simulador / API → commerce.orders → procesador de pedidos
                                    ├→ commerce.payments
                                    └→ commerce.inventory → procesador de inventario → commerce.fulfillment
                                                               ↓
                                         proyección en memoria + SSE → interfaces web
```

Los cuatro tópicos tienen tres particiones. Cada evento lleva `eventId`, `orderId`, `occurredAt`, `type` y datos del negocio. La clave Kafka es `orderId`, de modo que los eventos del mismo pedido mantienen su orden dentro de cada tópico. Los consumidores usan grupos independientes para procesar y proyectar eventos. El estado de la interfaz se reconstruye leyendo los tópicos al iniciar. La demo conserva los mensajes en el volumen Docker; `docker compose down -v` los borra.

Variables opcionales: `PORT` (3000), `KAFKA_BROKERS` (`localhost:9092`), `SIMULATOR_INTERVAL_MS` (2400), `SIMULATOR_ENABLED` (`true`).

## Endpoints

- `GET /api/state`: proyección, métricas y eventos recientes.
- `GET /api/stream`: actualizaciones SSE en tiempo real.
- `POST /api/orders`: crea un pedido. Cuerpo opcional: `{ "scenario": "normal|high_value|stockout" }`.
- `POST /api/simulator`: `{ "enabled": true|false }`.
- `GET /api/health`: estado de Kafka y el servidor.
- `GET /__kafka`: observabilidad de tópicos, flujo y payloads.

Esta es una demo de un solo nodo: pagos e inventario son simulados; no procesa dinero ni existencias reales. El visor técnico es una ruta poco visible, no una medida de seguridad ni autenticación.
