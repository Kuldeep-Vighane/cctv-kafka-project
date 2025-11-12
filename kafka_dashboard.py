# kafka_dashboard.py
"""
Single-file FastAPI dashboard for CCTV Kafka project.

Features:
- AIOKafka consumer reading camera_events, camera_telemetry, video_metadata
- WebSocket broadcasting of every message + periodic KPI snapshots
- In-memory analytics: events per 60s per camera (sliding window)
- Reads video_metadata_store.csv (if present) to show stored-video count
- Single-page UI with Chart.js and live logs

Usage:
  export KAFKA_BOOTSTRAP=localhost:9092
  uvicorn kafka_dashboard:app --host 0.0.0.0 --port 8000 --reload
"""
import os
import asyncio
import csv
import json
import logging
import time
from collections import defaultdict, deque
from typing import Any, Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from aiokafka import AIOKafkaConsumer

# ---------- config ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPICS = os.getenv("KAFKA_TOPICS", "camera_events,camera_telemetry,video_metadata").split(",")
VIDEO_METADATA_CSV = os.getenv("VIDEO_METADATA_CSV", "video_metadata_store.csv")

# analytics window seconds
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))
# how many telemetry points to keep/display
MAX_TELEMETRY_POINTS = int(os.getenv("MAX_TELEMETRY_POINTS", "120"))
# metrics broadcast interval seconds
METRICS_BROADCAST_INTERVAL = float(os.getenv("METRICS_BROADCAST_INTERVAL", "2.0"))

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("kafka_dashboard")

# ---------- fastapi app ----------
app = FastAPI(title="CCTV Kafka Dashboard")

# connected WS clients
clients: Set[WebSocket] = set()

# in-memory state
state = {
    "events_total": 0,
    "last_events": deque(maxlen=200),  # keep recent events for UI
    "telemetry": deque(maxlen=MAX_TELEMETRY_POINTS),  # (ts_iso, temp)
    "events_window": defaultdict(lambda: deque()),  # per-camera timestamps
    "events_per_min": {},  # computed view of events per WINDOW_SECONDS
    "stored_videos_count": 0,
}

# ---------- helper functions ----------
def read_stored_videos_count() -> int:
    """Count lines in the storage CSV (excluding header) if exists."""
    if not os.path.exists(VIDEO_METADATA_CSV):
        return 0
    try:
        with open(VIDEO_METADATA_CSV, "r", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
            if not rows:
                return 0
            # assume first row header
            return max(0, len(rows) - 1)
    except Exception as e:
        LOG.exception("reading CSV failed: %s", e)
        return 0

async def broadcast_message(payload: Dict[str, Any]):
    """Send JSON payload to all connected clients; remove dead connections."""
    if not clients:
        return
    data = json.dumps(payload)
    to_remove = []
    for ws in list(clients):
        try:
            await ws.send_text(data)
        except Exception:
            to_remove.append(ws)
    for ws in to_remove:
        clients.discard(ws)

def update_event_window(camera_id: str, ts: int):
    dq = state["events_window"][camera_id]
    dq.append(ts)
    # evict old
    while dq and (ts - dq[0] > WINDOW_SECONDS):
        dq.popleft()
    state["events_per_min"][camera_id] = len(dq)

# ---------- routes / websocket ----------
@app.get("/", response_class=HTMLResponse)
async def index():
    # serve embedded UI (Chart.js from CDN)
    html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>CCTV Kafka Dashboard</title>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <style>
    body {{ font-family: Inter, Roboto, Arial; margin:12px; background:#071028; color:#e6eef8; }}
    .container {{ max-width:1200px; margin:0 auto; }}
    header {{ display:flex; align-items:center; justify-content:space-between; gap:12px; }}
    h1 {{ margin:0; font-size:20px; }}
    .kpis {{ display:flex; gap:12px; margin-top:12px; flex-wrap:wrap; }}
    .card {{ background:#081425; padding:12px; border-radius:10px; box-shadow:0 6px 18px rgba(2,6,23,0.6); }}
    .kpi {{ min-width:180px; text-align:center; }}
    #logs {{ height:340px; overflow:auto; background:#000814; padding:8px; border-radius:8px; margin-top:12px; font-family:monospace; font-size:13px; color:#cfe9ff; }}
    #charts {{ display:flex; gap:12px; margin-top:12px; align-items:flex-start; }}
    canvas {{ background:#000814; border-radius:8px; padding:8px; }}
    table {{ width:100%; border-collapse:collapse; color:#e6eef8; }}
    td,th {{ padding:6px 8px; border-bottom:1px solid rgba(255,255,255,0.04); text-align:left; font-size:13px; }}
    .muted {{ color:#9fb6d8; font-size:12px; }}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>📡 CCTV Kafka Dashboard</h1>
      <div class="muted">Broker: <strong id="broker">{KAFKA_BOOTSTRAP}</strong> • Topics: <strong id="topics">{",".join(TOPICS)}</strong></div>
    </header>

    <div class="kpis">
      <div class="card kpi">
        <div class="muted">Events (total)</div>
        <div id="events_total" style="font-size:26px;">0</div>
      </div>
      <div class="card kpi">
        <div class="muted">Avg Temp (°C)</div>
        <div id="avg_temp" style="font-size:26px;">-</div>
      </div>
      <div class="card kpi">
        <div class="muted">Stored videos</div>
        <div id="stored_videos" style="font-size:26px;">0</div>
      </div>
      <div class="card kpi">
        <div class="muted">Connected clients</div>
        <div id="clients_count" style="font-size:26px;">0</div>
      </div>
    </div>

    <div id="charts">
      <div style="flex:1">
        <div class="card">
          <canvas id="telemetryChart" width="700" height="260"></canvas>
        </div>
      </div>

      <div style="width:420px">
        <div class="card" style="margin-bottom:12px;">
          <div class="muted">Events per camera (last {WINDOW_SECONDS}s)</div>
          <div id="events_table" style="margin-top:8px; max-height:220px; overflow:auto;">
            <table id="ev_table"><thead><tr><th>Camera</th><th>Events</th></tr></thead><tbody></tbody></table>
          </div>
        </div>

        <div class="card">
          <div class="muted">Live logs</div>
          <div id="logs"></div>
        </div>
      </div>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <script>
    const eventsTotalEl = document.getElementById('events_total');
    const avgTempEl = document.getElementById('avg_temp');
    const clientsCountEl = document.getElementById('clients_count');
    const storedVideosEl = document.getElementById('stored_videos');
    const logsEl = document.getElementById('logs');
    const evTableBody = document.querySelector('#ev_table tbody');

    // chart setup
    let labels = [];
    let data = [];
    const ctx = document.getElementById('telemetryChart');
    const chart = new Chart(ctx, {{
      type: 'line',
      data: {{ labels, datasets: [{{ label: 'Temperature (°C)', data, tension:0.3, borderWidth:2, pointRadius:2, borderColor:'rgba(125,211,252,1)', backgroundColor:'rgba(125,211,252,0.08)', fill:true }}]}},
      options: {{ responsive:true, maintainAspectRatio:false, scales:{{ x:{{display:false}}, y:{{beginAtZero:false}} }}, plugins:{{ legend:{{display:false}} }} }}
    }});

    // websocket
    const proto = (location.protocol === "https:") ? "wss" : "ws";
    const ws = new WebSocket(`${{proto}}://${{location.host}}/ws`);
    ws.addEventListener('open', () => appendLog('[ws] connected'));
    ws.addEventListener('close', () => appendLog('[ws] disconnected'));
    ws.addEventListener('message', ev => {
      try {{
        const msg = JSON.parse(ev.data);
        // message types: 'kafka_msg' (raw), 'metrics' (kpi snapshot)
        if (msg.type === 'kafka_msg') {{
          const t = msg.topic;
          appendLog(`[${{t}}] ${JSON.stringify(msg.value)}`);
          if (t === 'camera_telemetry' && typeof msg.value.temperature_c !== 'undefined') {{
            const ts = new Date().toLocaleTimeString();
            labels.push(ts);
            data.push(msg.value.temperature_c);
            if (labels.length > 120) {{ labels.shift(); data.shift(); }}
            chart.update();
            const avg = (data.reduce((a,b)=>a+b,0)/data.length) || 0;
            avgTempEl.textContent = data.length ? avg.toFixed(1) : '-';
          }}
        }} else if (msg.type === 'metrics') {{
          // metrics snapshot
          eventsTotalEl.textContent = msg.payload.events_total;
          storedVideosEl.textContent = msg.payload.stored_videos_count;
          clientsCountEl.textContent = msg.payload.clients_count;
          // events per camera
          updateEventsTable(msg.payload.events_per_min);
        }}
      }} catch (e) {{
        console.error('ws parse failed', e);
      }}
    });

    function appendLog(text) {{
      const div = document.createElement('div');
      div.textContent = `[${{new Date().toLocaleTimeString()}}] ${text}`;
      logsEl.prepend(div);
      while (logsEl.children.length > 300) logsEl.removeChild(logsEl.lastChild);
    }}

    function updateEventsTable(map) {{
      evTableBody.innerHTML = '';
      // sort by events desc
      const items = Object.entries(map).sort((a,b)=>b[1]-a[1]);
      for (const [cam, cnt] of items) {{
        const tr = document.createElement('tr');
        const c1 = document.createElement('td');
        c1.textContent = cam;
        const c2 = document.createElement('td');
        c2.textContent = cnt;
        tr.appendChild(c1);
        tr.appendChild(c2);
        evTableBody.appendChild(tr);
      }}
    }}

    // poll clients_count for fallback
    setInterval(async () => {{
      try {{
        const r = await fetch('/clients_count');
        const j = await r.json();
        clientsCountEl.textContent = j.count;
      }} catch(_){{}}
    }}, 3000);
  </script>
</body>
</html>
"""
    # convert doubled braces used to escape f-string into single braces for JS/html
    html = html.replace("{{", "{").replace("}}", "}")
    # substitute Python placeholders left in the literal string
    html = html.replace("{KAFKA_BOOTSTRAP}", KAFKA_BOOTSTRAP).replace('{",".join(TOPICS)}', ",".join(TOPICS)).replace("{WINDOW_SECONDS}", str(WINDOW_SECONDS))
    return HTMLResponse(content=html)

@app.get("/clients_count")
async def clients_count():
    return JSONResponse({"count": len(clients)})

# ---------- websocket endpoint ----------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    LOG.info("client connected (total=%d)", len(clients))
    try:
        # keep websocket open and respond to pings — we don't expect messages from client
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
        LOG.info("client disconnected (total=%d)", len(clients))
    except Exception as e:
        clients.discard(ws)
        LOG.exception("ws error: %s", e)

# ---------- Kafka consumer background task ----------
async def kafka_consumer_loop():
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="dashboard-ui-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
    )

    # connect with retries
    while True:
        try:
            LOG.info("Starting AIOKafkaConsumer bootstrap=%s topics=%s", KAFKA_BOOTSTRAP, TOPICS)
            await consumer.start()
            break
        except Exception as e:
            LOG.warning("Kafka connect failed: %s — retrying in 3s", e)
            await asyncio.sleep(3)

    try:
        async for msg in consumer:
            # runtime payload
            payload = {
                "type": "kafka_msg",
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "ts": int(time.time()),
                "value": msg.value,
            }

            # update server-side state
            try:
                if msg.topic == "camera_events":
                    state["events_total"] += 1
                    state["last_events"].appendleft(msg.value)
                    # update sliding window per camera
                    cam = msg.value.get("camera_id")
                    if cam:
                        update_event_window(cam, int(time.time()))
                elif msg.topic == "camera_telemetry":
                    temp = msg.value.get("temperature_c")
                    if temp is not None:
                        state["telemetry"].append((time.time(), float(temp)))
                elif msg.topic == "video_metadata":
                    # stored count may be incremented by separate storage consumer writing CSV.
                    # we'll refresh stored count on metrics broadcast.
                    state["last_events"].appendleft({"video_metadata": msg.value})
            except Exception:
                LOG.exception("state update failed for topic %s", msg.topic)

            # broadcast raw kafka message
            await broadcast_message(payload)
    finally:
        await consumer.stop()

# ---------- periodic metrics broadcaster ----------
async def metrics_broadcaster():
    while True:
        try:
            # build metrics snapshot
            stored_count = read_stored_videos_count()
            state["stored_videos_count"] = stored_count

            # copy events_per_min to a plain dict (defaultdict -> normal dict)
            events_map = {k: len(v) if hasattr(v, "__len__") else int(v) for k,v in state["events_window"].items()}

            metrics = {
                "type": "metrics",
                "payload": {
                    "events_total": state["events_total"],
                    "events_per_min": {k: len(v) for k,v in state["events_window"].items()},
                    "telemetry_last": list(state["telemetry"])[-50:],  # small sample
                    "stored_videos_count": stored_count,
                    "clients_count": len(clients),
                }
            }
            await broadcast_message(metrics)
        except Exception:
            LOG.exception("metrics broadcast failed")
        await asyncio.sleep(METRICS_BROADCAST_INTERVAL)

# ---------- startup / shutdown ----------
@app.on_event("startup")
async def startup_event():
    LOG.info("Starting dashboard background tasks")
    app.state.kafka_task = asyncio.create_task(kafka_consumer_loop())
    app.state.metrics_task = asyncio.create_task(metrics_broadcaster())

@app.on_event("shutdown")
async def shutdown_event():
    LOG.info("Shutting down dashboard background tasks")
    task = getattr(app.state, "kafka_task", None)
    if task: task.cancel()
    task2 = getattr(app.state, "metrics_task", None)
    if task2: task2.cancel()
    await asyncio.sleep(0.1)

# ---------- run when executed directly ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("kafka_dashboard:app", host="0.0.0.0", port=8000, reload=False)
