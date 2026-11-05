import os
import json
import sqlite3
import time
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import paho.mqtt.client as mqtt
import uvicorn

# =====================================================================
# 1. CONFIGURATION & TOPICS (Matches Wokwi Firmware)
# =====================================================================
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
TOPIC_TELEMETRY = "finn_smart_office/v1/devices/me/telemetry"
TOPIC_RPC_PUB = "finn_smart_office/v1/devices/me/rpc/request/1"
DB_FILE = "smart_office.db"

app = FastAPI(title="Custom Mini-ThingsBoard IoT Server")

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Custom Mini-ThingsBoard IoT Server")

# Allow teammates, Flutter Mobile App, and Cloudflare Tunnel to connect freely:
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory live state cache for ultra-fast dashboard polling
live_state = {
    "zone1": {
        "door": "LOCKED",
        "occupied": False,
        "last_badge": "None",
        "last_access": "None",
        "updated_at": "Waiting for Wokwi..."
    },
    "zone2": {
        "temperature": 0.0,
        "humidity": 0.0,
        "light_level": 0,
        "fan": False,
        "light": False,
        "auto_mode": True,
        "updated_at": "Waiting for Wokwi..."
    }
}

# =====================================================================
# 2. DATABASE LAYER (SQLite Time-Series & Access Logs)
# =====================================================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS climate_telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    temperature REAL,
                    humidity REAL,
                    light_level INTEGER,
                    fan INTEGER,
                    light INTEGER,
                    auto_mode INTEGER
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS access_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    badge_id TEXT,
                    access_status TEXT,
                    door_state TEXT
                )''')
    conn.commit()
    conn.close()

init_db()

# =====================================================================
# 3. MQTT TRANSPORT LAYER (Connects to Wokwi via HiveMQ)
# =====================================================================
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[MQTT] Connected to {MQTT_BROKER} (rc={rc})")
    client.subscribe(TOPIC_TELEMETRY)
    print(f"[MQTT] Subscribed to: {TOPIC_TELEMETRY}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        now_str = datetime.now().strftime("%H:%M:%S")
        node = payload.get("node", "")

        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        # Handle Zone 2: Workspace Climate & Actuators
        if node == "zone2_workspace":
            live_state["zone2"]["temperature"] = float(payload.get("temperature", 0.0))
            live_state["zone2"]["humidity"] = float(payload.get("humidity", 0.0))
            live_state["zone2"]["light_level"] = int(payload.get("light_level", 0))
            live_state["zone2"]["fan"] = bool(payload.get("fan", False))
            live_state["zone2"]["light"] = bool(payload.get("light", False))
            live_state["zone2"]["auto_mode"] = bool(payload.get("auto_mode", True))
            live_state["zone2"]["updated_at"] = now_str

            c.execute('''INSERT INTO climate_telemetry 
                         (timestamp, temperature, humidity, light_level, fan, light, auto_mode)
                         VALUES (?, ?, ?, ?, ?, ?, ?)''',
                      (now_str, live_state["zone2"]["temperature"], live_state["zone2"]["humidity"],
                       live_state["zone2"]["light_level"], int(live_state["zone2"]["fan"]),
                       int(live_state["zone2"]["light"]), int(live_state["zone2"]["auto_mode"])))
            conn.commit()

        # Handle Zone 1: Entrance RFID & PIR Occupancy
        elif node == "zone1_entrance":
            live_state["zone1"]["updated_at"] = now_str
            if "occupied" in payload or "room_occupied" in payload:
                occ = payload.get("occupied", payload.get("room_occupied", False))
                live_state["zone1"]["occupied"] = bool(occ)

            if "badge_id" in payload:
                badge = payload.get("badge_id", "UNKNOWN")
                access = payload.get("access", "GRANTED" if payload.get("access_granted") else "DENIED")
                door = payload.get("door", payload.get("door_status", "LOCKED"))
                live_state["zone1"]["last_badge"] = badge
                live_state["zone1"]["last_access"] = access
                live_state["zone1"]["door"] = door

                c.execute('''INSERT INTO access_logs (timestamp, badge_id, access_status, door_state)
                             VALUES (?, ?, ?, ?)''', (now_str, badge, access, door))
                conn.commit()

        conn.close()
    except Exception as e:
        print(f"[MQTT Error] Failed to parse message: {e}")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect_async(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

# =====================================================================
# 4. REST API ENDPOINTS (Powers Web, Desktop & Mobile Apps)
# =====================================================================
class RpcCommand(BaseModel):
    method: str   # "setFan", "setLight", or "setAutoMode"
    params: bool  # True or False

@app.get("/api/telemetry/latest")
def get_latest_telemetry():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT timestamp, temperature, humidity, light_level FROM climate_telemetry ORDER BY id DESC LIMIT 15")
    history = [{"time": r[0], "temp": r[1], "hum": r[2], "ldr": r[3]} for r in reversed(c.fetchall())]

    c.execute("SELECT timestamp, badge_id, access_status, door_state FROM access_logs ORDER BY id DESC LIMIT 8")
    logs = [{"time": r[0], "badge": r[1], "status": r[2], "door": r[3]} for r in c.fetchall()]
    conn.close()

    return {
        "live": live_state,
        "history": history,
        "access_logs": logs
    }

@app.post("/api/rpc")
def send_rpc_command(cmd: RpcCommand):
    payload = json.dumps({"method": cmd.method, "params": cmd.params})
    mqtt_client.publish(TOPIC_RPC_PUB, payload)
    print(f"[RPC SENT TO WOKWI] {payload}")
    return {"status": "sent", "command": cmd.dict()}

# =====================================================================
# 5. BUILT-IN RESPONSIVE WEB & MOBILE DASHBOARD UI
# =====================================================================
@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Smart Office IoT Platform (Mini-ThingsBoard)</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body class="bg-slate-900 text-slate-100 min-h-screen font-sans p-4 md:p-8">
    <div class="max-w-6xl mx-auto">
        <!-- Header -->
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-6 border-b border-slate-700 pb-4">
            <div>
                <h1 class="text-2xl md:text-3xl font-bold text-cyan-400">🏢 Smart Office IoT Platform</h1>
                <p class="text-slate-400 text-sm">Custom Reverse-Engineered ThingsBoard Server • Wokwi Live Bridge</p>
            </div>
            <div class="mt-2 md:mt-0 text-xs bg-slate-800 px-3 py-2 rounded border border-slate-700">
                Broker: <span class="text-green-400 font-mono">broker.hivemq.com</span> |
                Zone 2 Sync: <span id="z2-time" class="text-amber-300 font-mono">--</span>
            </div>
        </div>

        <!-- Top Metric Cards -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-slate-400 text-xs uppercase">Temperature (Zone 2)</p>
                <p id="val-temp" class="text-3xl font-bold text-rose-400 mt-1">-- °C</p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-slate-400 text-xs uppercase">Humidity (Zone 2)</p>
                <p id="val-hum" class="text-3xl font-bold text-sky-400 mt-1">-- %</p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-slate-400 text-xs uppercase">Ambient Light (LDR)</p>
                <p id="val-ldr" class="text-3xl font-bold text-amber-400 mt-1">--</p>
            </div>
            <div class="bg-slate-800 p-4 rounded-xl border border-slate-700">
                <p class="text-slate-400 text-xs uppercase">Meeting Room (PIR)</p>
                <p id="val-pir" class="text-2xl font-bold text-emerald-400 mt-2">VACANT</p>
            </div>
        </div>

        <!-- Middle Control & Chart Grid -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
            <!-- RPC Actuator Control Panel -->
            <div class="bg-slate-800 p-5 rounded-xl border border-slate-700 flex flex-col justify-between">
                <div>
                    <h2 class="text-lg font-semibold text-white mb-1">⚡ Zone 2 Remote RPC Control</h2>
                    <p class="text-xs text-slate-400 mb-4">Click to send MQTT RPC command to Wokwi ESP32-S3</p>
                    
                    <div class="space-y-4">
                        <div class="flex items-center justify-between bg-slate-900 p-3 rounded-lg">
                            <div>
                                <p class="font-medium">Operation Mode</p>
                                <p id="status-mode" class="text-xs text-cyan-400">AUTO (Edge Logic)</p>
                            </div>
                            <button onclick="sendRpc('setAutoMode', true)" class="bg-cyan-600 hover:bg-cyan-500 text-xs px-3 py-2 rounded font-semibold">
                                Enable Auto
                            </button>
                        </div>

                        <div class="flex items-center justify-between bg-slate-900 p-3 rounded-lg">
                            <div>
                                <p class="font-medium">AC / Ventilation Fan</p>
                                <p id="status-fan" class="text-xs text-slate-400">State: OFF</p>
                            </div>
                            <div class="space-x-2">
                                <button onclick="sendRpc('setFan', true)" class="bg-blue-600 hover:bg-blue-500 text-xs px-3 py-2 rounded font-semibold">ON</button>
                                <button onclick="sendRpc('setFan', false)" class="bg-slate-700 hover:bg-slate-600 text-xs px-3 py-2 rounded font-semibold">OFF</button>
                            </div>
                        </div>

                        <div class="flex items-center justify-between bg-slate-900 p-3 rounded-lg">
                            <div>
                                <p class="font-medium">Workspace Lighting</p>
                                <p id="status-light" class="text-xs text-slate-400">State: OFF</p>
                            </div>
                            <div class="space-x-2">
                                <button onclick="sendRpc('setLight', true)" class="bg-amber-500 hover:bg-amber-400 text-slate-950 text-xs px-3 py-2 rounded font-semibold">ON</button>
                                <button onclick="sendRpc('setLight', false)" class="bg-slate-700 hover:bg-slate-600 text-xs px-3 py-2 rounded font-semibold">OFF</button>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="mt-4 pt-3 border-t border-slate-700 text-xs text-slate-400">
                    💡 Tip: Clicking ON/OFF switches Wokwi to MANUAL mode. Click "Enable Auto" to restore edge thresholds.
                </div>
            </div>

            <!-- Live Telemetry Chart -->
            <div class="bg-slate-800 p-5 rounded-xl border border-slate-700 lg:col-span-2">
                <h2 class="text-lg font-semibold text-white mb-2">📈 Real-Time Climate Telemetry</h2>
                <canvas id="telemetryChart" height="130"></canvas>
            </div>
        </div>

        <!-- Zone 1 RFID Access Log Table -->
        <div class="bg-slate-800 p-5 rounded-xl border border-slate-700">
            <div class="flex justify-between items-center mb-3">
                <h2 class="text-lg font-semibold text-white">🔐 Zone 1: RFID Door Access Logs</h2>
                <span id="z1-time" class="text-xs text-slate-400">Last event: --</span>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm">
                    <thead class="bg-slate-900 text-slate-400 uppercase text-xs">
                        <tr>
                            <th class="p-3">Time</th>
                            <th class="p-3">RFID Card UID</th>
                            <th class="p-3">Authorization</th>
                            <th class="p-3">Servo Door Lock</th>
                        </tr>
                    </thead>
                    <tbody id="rfid-table" class="divide-y divide-slate-700">
                        <tr><td colspan="4" class="p-3 text-slate-500">Tap an RFID card in Wokwi Zone 1 to log entry...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        const ctx = document.getElementById('telemetryChart').getContext('2d');
        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    { label: 'Temp (°C)', data: [], borderColor: '#fb7185', tension: 0.3 },
                    { label: 'Humidity (%)', data: [], borderColor: '#38bdf8', tension: 0.3 }
                ]
            },
            options: {
                responsive: true,
                scales: { y: { beginAtZero: false, grid: { color: '#334155' } }, x: { grid: { color: '#334155' } } },
                plugins: { legend: { labels: { color: '#e2e8f0' } } }
            }
        });

        async function sendRpc(method, params) {
            await fetch('/api/rpc', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ method, params })
            });
            fetchData();
        }

        async function fetchData() {
            try {
                const res = await fetch('/api/telemetry/latest');
                const data = await res.json();
                const z2 = data.live.zone2;
                const z1 = data.live.zone1;

                document.getElementById('val-temp').innerText = z2.temperature.toFixed(1) + ' °C';
                document.getElementById('val-hum').innerText = z2.humidity.toFixed(1) + ' %';
                document.getElementById('val-ldr').innerText = z2.light_level;
                document.getElementById('z2-time').innerText = z2.updated_at;
                document.getElementById('z1-time').innerText = 'Last sync: ' + z1.updated_at;

                const pirEl = document.getElementById('val-pir');
                pirEl.innerText = z1.occupied ? '🚨 OCCUPIED' : '✅ VACANT';
                pirEl.className = z1.occupied ? 'text-2xl font-bold text-amber-400 mt-2' : 'text-2xl font-bold text-emerald-400 mt-2';

                document.getElementById('status-mode').innerText = z2.auto_mode ? 'AUTO (Edge Logic)' : 'MANUAL OVERRIDE';
                document.getElementById('status-fan').innerText = 'State: ' + (z2.fan ? '🟢 ON' : '⚪ OFF');
                document.getElementById('status-light').innerText = 'State: ' + (z2.light ? '🟡 ON' : '⚪ OFF');

                // Update Chart
                chart.data.labels = data.history.map(h => h.time);
                chart.data.datasets[0].data = data.history.map(h => h.temp);
                chart.data.datasets[1].data = data.history.map(h => h.hum);
                chart.update('none');

                // Update RFID Table
                if (data.access_logs.length > 0) {
                    document.getElementById('rfid-table').innerHTML = data.access_logs.map(log => `
                        <tr>
                            <td class="p-3 font-mono text-xs">${log.time}</td>
                            <td class="p-3 font-mono text-cyan-300">${log.badge}</td>
                            <td class="p-3"><span class="px-2 py-1 rounded text-xs font-bold ${log.status === 'GRANTED' ? 'bg-emerald-900 text-emerald-300' : 'bg-rose-900 text-rose-300'}">${log.status}</span></td>
                            <td class="p-3 text-xs">${log.door}</td>
                        </tr>
                    `).join('');
                }
            } catch (e) { console.error(e); }
        }

        setInterval(fetchData, 500);
        fetchData();
    </script>
</body>
</html>
"""

import socket

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

@app.get("/mobile", response_class=HTMLResponse)
def serve_mobile_app():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="mobile-web-app-capable" content="yes">
    <title>Smart Office Mobile</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-black text-white min-h-screen flex flex-col justify-between select-none font-sans pb-16">
    <!-- Top App Bar -->
    <div class="bg-slate-900 px-5 py-4 border-b border-slate-800 flex justify-between items-center sticky top-0 z-10">
        <div>
            <h1 class="text-lg font-bold text-cyan-400">📱 SmartOffice Companion</h1>
            <p id="m-sync" class="text-[11px] text-slate-400">Syncing...</p>
        </div>
        <span id="m-mode" class="px-2.5 py-1 rounded-full text-xs font-bold bg-cyan-950 text-cyan-300 border border-cyan-700">AUTO</span>
    </div>

    <!-- Main Mobile Scroll Area -->
    <div class="p-4 space-y-4">
        <!-- Room Status Pill -->
        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex items-center justify-between">
            <div>
                <p class="text-xs text-slate-400 uppercase">Zone 1 Meeting Room</p>
                <p id="m-pir" class="text-xl font-bold text-emerald-400 mt-0.5">✅ VACANT</p>
            </div>
            <div class="text-right">
                <p class="text-xs text-slate-400">Last Badge</p>
                <p id="m-badge" class="text-sm font-mono text-cyan-300">None</p>
            </div>
        </div>

        <!-- Climate Grid -->
        <div class="grid grid-cols-3 gap-3">
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-3 text-center">
                <p class="text-[10px] text-slate-400 uppercase">Temp</p>
                <p id="m-temp" class="text-xl font-bold text-rose-400 mt-1">--°C</p>
            </div>
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-3 text-center">
                <p class="text-[10px] text-slate-400 uppercase">Humidity</p>
                <p id="m-hum" class="text-xl font-bold text-sky-400 mt-1">--%</p>
            </div>
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-3 text-center">
                <p class="text-[10px] text-slate-400 uppercase">Brightness</p>
                <p id="m-ldr" class="text-xl font-bold text-amber-400 mt-1">--</p>
            </div>
        </div>

        <!-- Touch Control Cards -->
        <h2 class="text-xs font-bold uppercase text-slate-400 tracking-wider pt-2">Quick Device Controls</h2>
        
        <div class="grid grid-cols-2 gap-4">
            <button onclick="toggleFan()" id="card-fan" class="p-5 rounded-2xl border border-slate-800 bg-slate-900 text-left transition active:scale-95">
                <div class="text-2xl mb-2">❄️</div>
                <p class="font-bold text-sm">AC Ventilation</p>
                <p id="m-fan-txt" class="text-xs text-slate-400 mt-1">Tap to toggle</p>
            </button>

            <button onclick="toggleLight()" id="card-light" class="p-5 rounded-2xl border border-slate-800 bg-slate-900 text-left transition active:scale-95">
                <div class="text-2xl mb-2">💡</div>
                <p class="font-bold text-sm">Office Lights</p>
                <p id="m-light-txt" class="text-xs text-slate-400 mt-1">Tap to toggle</p>
            </button>
        </div>

        <button onclick="rpc('setAutoMode', true)" class="w-full py-3.5 rounded-2xl bg-cyan-600 active:bg-cyan-500 font-bold text-sm shadow-lg">
            🤖 Enable Autonomous Edge Mode
        </button>
    </div>

    <!-- Bottom Navigation Bar -->
    <div class="fixed bottom-0 left-0 right-0 bg-slate-900 border-t border-slate-800 px-6 py-2 flex justify-around text-xs text-slate-400">
        <a href="/mobile" class="text-cyan-400 font-bold flex flex-col items-center"><span>🏠</span><span>Mobile App</span></a>
        <a href="/" class="flex flex-col items-center"><span>🖥️</span><span>Full Web View</span></a>
    </div>

    <script>
        let fanState = false;
        let lightState = false;

        async function rpc(method, params) {
            await fetch('/api/rpc', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({method, params})
            });
            sync();
        }
        function toggleFan() { rpc('setFan', !fanState); }
        function toggleLight() { rpc('setLight', !lightState); }

        async function sync() {
            try {
                const res = await fetch('/api/telemetry/latest');
                const d = await res.json();
                const z2 = d.live.zone2;
                const z1 = d.live.zone1;

                fanState = z2.fan;
                lightState = z2.light;

                document.getElementById('m-sync').innerText = 'Live • ' + z2.updated_at;
                document.getElementById('m-mode').innerText = z2.auto_mode ? 'AUTO EDGE' : 'MANUAL';
                document.getElementById('m-temp').innerText = z2.temperature.toFixed(1) + '°C';
                document.getElementById('m-hum').innerText = z2.humidity.toFixed(0) + '%';
                document.getElementById('m-ldr').innerText = z2.light_level;

                document.getElementById('m-pir').innerText = z1.occupied ? '🚨 OCCUPIED' : '✅ VACANT';
                document.getElementById('m-badge').innerText = z1.last_badge;

                const fCard = document.getElementById('card-fan');
                fCard.className = fanState ? 'p-5 rounded-2xl border border-blue-500 bg-blue-950/60 text-left transition active:scale-95' : 'p-5 rounded-2xl border border-slate-800 bg-slate-900 text-left transition active:scale-95';
                document.getElementById('m-fan-txt').innerText = fanState ? 'Status: ON' : 'Status: OFF';

                const lCard = document.getElementById('card-light');
                lCard.className = lightState ? 'p-5 rounded-2xl border border-amber-400 bg-amber-950/60 text-left transition active:scale-95' : 'p-5 rounded-2xl border border-slate-800 bg-slate-900 text-left transition active:scale-95';
                document.getElementById('m-light-txt').innerText = lightState ? 'Status: ON' : 'Status: OFF';
            } catch (e) {}
        }
        setInterval(sync, 500);
        sync();
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)