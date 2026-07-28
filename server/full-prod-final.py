#!/usr/bin/env python3
"""
Combined production server: runs the pipe-sensor server (receive_data.py,
port 8080) and the container/filtration server (container_final.py, port
8083) side by side in a single process, on a single asyncio event loop.

This exists so you only have to start one script instead of two separate
ones. The wire protocol on each port is untouched — ESP32 boards and the
Next.js frontend (pld-new/frontend/pld-display/app/page.tsx) keep talking to
ws://<host>:8080 for moisture/pressure ("Pipes View") and
ws://<host>:8083 for pH/pump/valve ("Filtration View") exactly as before, so
no frontend changes are required.

Run this on your laptop (192.168.50.193), on the same WiFi network
('Abuserin wifi6'):
    python3 full-prod-final.py
    Press 't' to toggle filtration simulation mode on/off.
    Press 'l' to increase simulated pH by 0.1.
    Press 'k' to decrease simulated pH by 0.1.
    Press 'r' to toggle pipe simulation mode on/off (same as pressing 'R' in
        the frontend's Pipes View — synthesizes readings for esp32-1/2/3
        without needing real hardware; see PIPE_SIM_TARGETS below).
    Press 'q' to quit.

Install dependency:
    pip install websockets
"""

import asyncio
import json
import random
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import websockets

if sys.platform == "win32":
    import msvcrt
else:
    import termios
    import tty

HOST = "0.0.0.0"  # listen on all interfaces

# ============================================================
# Pipe sensors (moisture + pressure) — formerly receive_data.py
# ============================================================

PIPES_PORT = 8080

DB_PATH = Path(__file__).parent / "sensor_data.db"

# Tracks the latest known device id per active connection, and the most
# recent payload received from each device.
pipe_connected_devices = {}
pipe_latest_data = {}

# Browser/frontend clients that registered as viewers via a "hello" with
# role "viewer". They receive a snapshot on connect and a broadcast of
# every subsequent device update.
pipe_viewers = set()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            device_id TEXT NOT NULL,
            sensor_type TEXT NOT NULL,  -- 'moisture' or 'pressure'
            position TEXT NOT NULL,     -- e.g. 'start', 'middle', 'end'
            raw REAL,
            status TEXT,
            ready INTEGER,
            value REAL          -- pressure: calibrated kPa (data-transfer-calibrated sketch); raw ADC count if using the uncalibrated sketch
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_payloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            device_id TEXT NOT NULL,
            payload TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_readings_lookup "
        "ON sensor_readings (device_id, sensor_type, position, ts)"
    )
    conn.commit()
    return conn


db_conn = init_db()


def log_reading(device_id: str, data: dict):
    ts = datetime.now().isoformat()
    rows = []

    for position, sensor in data.get("moisture", {}).items():
        rows.append((ts, device_id, "moisture", position, sensor.get("raw"), sensor.get("status"), None, None))

    for position, sensor in data.get("pressure", {}).items():
        ready = sensor.get("ready")
        rows.append((ts, device_id, "pressure", position, sensor.get("raw"), None, int(bool(ready)) if ready is not None else None, sensor.get("value")))

    if rows:
        db_conn.executemany(
            "INSERT INTO sensor_readings (ts, device_id, sensor_type, position, raw, status, ready, value) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    db_conn.execute(
        "INSERT INTO raw_payloads (ts, device_id, payload) VALUES (?, ?, ?)",
        (ts, device_id, json.dumps(data)),
    )
    db_conn.commit()


# ============================================================
# Pipe simulation mode — synthesizes esp32-1/2/3 readings so the demo
# doesn't depend on the physical rig. Targets below encode the three
# scenarios from the drilled-hole trials: pipe 1 healthy, pipe 2 leaking
# at every sensor (holes at all three points), pipe 3 leaking only at the
# middle zone (pressure/moisture upstream and downstream of the hole
# still read close to normal — the hole, not distance from it, is what
# makes a zone read as a leak).
# ============================================================

PIPE_SIM_INTERVAL = 2.5  # seconds between simulated readings
PRESSURE_JITTER = 8  # +/- kPa noise around each target
MOISTURE_DRY_RAW = 3200  # matches frontend's MOISTURE_DRY_RAW
MOISTURE_WET_RAW = 2200  # matches frontend's MOISTURE_WET_RAW
MOISTURE_JITTER = 150

PIPE_SIM_TARGETS = {
    "esp32-1": {"start": (35, "Dry"), "middle": (35, "Dry"), "end": (35, "Dry")},
    "esp32-2": {"start": (15, "Wet"), "middle": (15, "Wet"), "end": (15, "Wet")},
    "esp32-3": {"start": (36, "Dry"), "middle": (29, "Wet"), "end": (0, "Dry")},
}

pipe_simulation_mode = False
pipe_sim_task: asyncio.Task | None = None


def simulated_pipe_payload(device_id: str) -> dict:
    moisture = {}
    pressure = {}
    for position, (target_kpa, state) in PIPE_SIM_TARGETS[device_id].items():
        pressure_value = max(0.0, target_kpa + random.uniform(-PRESSURE_JITTER, PRESSURE_JITTER))
        moisture_ref = MOISTURE_WET_RAW if state == "Wet" else MOISTURE_DRY_RAW
        moisture_raw = moisture_ref + random.uniform(-MOISTURE_JITTER, MOISTURE_JITTER)
        pressure[position] = {"ready": True, "value": round(pressure_value, 1)}
        moisture[position] = {"raw": round(moisture_raw, 1), "status": state}
    return {"device": device_id, "moisture": moisture, "pressure": pressure, "simulated": True}


async def pipe_simulation_loop():
    try:
        while True:
            for device_id in PIPE_SIM_TARGETS:
                data = simulated_pipe_payload(device_id)
                pipe_latest_data[device_id] = data
                log_reading(device_id, data)
                if pipe_viewers:
                    websockets.broadcast(
                        pipe_viewers, json.dumps({"type": "update", "device": device_id, "data": data})
                    )
            await asyncio.sleep(PIPE_SIM_INTERVAL)
    except asyncio.CancelledError:
        raise


async def toggle_pipe_simulation():
    global pipe_simulation_mode, pipe_sim_task
    pipe_simulation_mode = not pipe_simulation_mode
    if pipe_simulation_mode:
        pipe_sim_task = asyncio.create_task(pipe_simulation_loop())
    elif pipe_sim_task is not None:
        pipe_sim_task.cancel()
        pipe_sim_task = None
    print(f"[pipes] [state] Simulation mode is now {'ON' if pipe_simulation_mode else 'OFF'}")


async def handle_pipe_client(websocket):
    remote = websocket.remote_address
    device_id = None
    print(f"[pipes] [+] New connection from {remote}")

    try:
        async for message in websocket:
            timestamp = datetime.now().strftime("%H:%M:%S")

            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                print(f"[pipes] [{timestamp}] Received non-JSON message from {remote}: {message}")
                continue

            if data.get("type") == "hello":
                if data.get("role") == "viewer":
                    pipe_viewers.add(websocket)
                    print(f"[pipes] [{timestamp}] Viewer connected from {remote}")
                    await websocket.send(json.dumps({"type": "snapshot", "devices": pipe_latest_data}))
                    continue

                device_id = data.get("device", f"unknown-{remote[0]}:{remote[1]}")
                pipe_connected_devices[websocket] = device_id
                print(f"[pipes] [{timestamp}] Handshake complete: '{device_id}' connected from {remote}")
                continue

            if data.get("type") == "command":
                command = data.get("command")
                if command == "PIPE_SIM_TOGGLE":
                    print(f"[pipes] [{timestamp}] [<] Command from viewer {remote}: {command}")
                    await toggle_pipe_simulation()
                continue

            device_id = data.get("device", device_id or "unknown")
            pipe_latest_data[device_id] = data
            log_reading(device_id, data)

            if pipe_viewers:
                websockets.broadcast(pipe_viewers, json.dumps({"type": "update", "device": device_id, "data": data}))

            moisture = data.get("moisture", {})
            pressure = data.get("pressure", {})

            print(f"\n[pipes] [{timestamp}] --- Update from {device_id} ---")
            for key, sensor in moisture.items():
                print(f"  Moisture {key}: raw={sensor.get('raw')} status={sensor.get('status')}")
            for key, sensor in pressure.items():
                ready = sensor.get("ready")
                value = sensor.get("value")
                print(f"  Pressure {key}: ready={ready} value={value}")

    except websockets.exceptions.ConnectionClosed as e:
        print(f"[pipes] [-] '{device_id or remote}' disconnected ({e.code}: {e.reason})")
    finally:
        pipe_connected_devices.pop(websocket, None)
        pipe_viewers.discard(websocket)


# ============================================================
# Container / filtration (pH, pump, valve) — formerly container_final.py
# ============================================================

CONTAINER_PORT = 8083

container_connected_devices = set()
filtration_viewers = set()

pump_is_on = False
valve_is_open = False
latest_ph = {"voltage": None, "value": None, "updated_at": None}

# device_simulated reflects what the ESP32 itself last reported via the
# "simulated" field in its JSON — this is the authoritative flag shown to
# frontend viewers. simulation_mode (below) is only our local guess of
# which way to toggle next on 't', used before any device confirmation
# arrives.
device_simulated = False
simulation_mode = False


def current_filtration_state() -> dict:
    return {
        "pump_on": pump_is_on,
        "valve_open": valve_is_open,
        "ph": latest_ph,
        "simulated": device_simulated,
    }


def broadcast_filtration_state():
    if filtration_viewers:
        websockets.broadcast(
            filtration_viewers,
            json.dumps({"type": "state", **current_filtration_state()}),
        )


async def handle_container_client(websocket):
    global pump_is_on, valve_is_open, device_simulated, simulation_mode

    remote = websocket.remote_address
    container_connected_devices.add(websocket)
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[container] [{timestamp}] [+] Device connected from {remote}")

    try:
        async for message in websocket:
            timestamp = datetime.now().strftime("%H:%M:%S")

            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                print(f"[container] [{timestamp}] [Device] {message}")
                continue

            if data.get("type") == "hello" and data.get("role") == "viewer":
                container_connected_devices.discard(websocket)
                filtration_viewers.add(websocket)
                print(f"[container] [{timestamp}] [+] Filtration viewer connected from {remote}")
                await websocket.send(json.dumps({"type": "snapshot", **current_filtration_state()}))
                continue

            if data.get("type") == "command":
                command = data.get("command")
                if command in ("SIM_ON", "SIM_OFF", "SIM_INC", "SIM_DEC"):
                    print(f"[container] [{timestamp}] [<] Command from viewer {remote}: {command}")
                    if command in ("SIM_ON", "SIM_OFF"):
                        simulation_mode = command == "SIM_ON"
                    await send_container_command(command)
                continue

            pH = data.get("pH")
            if pH is not None:
                latest_ph["voltage"] = pH.get("voltage")
                latest_ph["value"] = pH.get("value")
                latest_ph["updated_at"] = datetime.now().isoformat()

            if "pump_on" in data:
                pump_is_on = data["pump_on"]
            if "valve_open" in data:
                valve_is_open = data["valve_open"]
            if "simulated" in data:
                device_simulated = data["simulated"]
                simulation_mode = device_simulated  # stay in sync with the device

            print(
                f"[container] [{timestamp}] [pH] value={latest_ph['value']} "
                f"voltage={latest_ph['voltage']} "
                f"{'(SIMULATED)' if device_simulated else ''} "
                f"pump={'ON' if pump_is_on else 'OFF'} "
                f"valve={'OPEN' if valve_is_open else 'CLOSED'}"
            )
            broadcast_filtration_state()
    except websockets.exceptions.ConnectionClosed as e:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[container] [{timestamp}] [-] Device disconnected ({e.code}: {e.reason})")
    finally:
        container_connected_devices.discard(websocket)
        filtration_viewers.discard(websocket)


async def send_container_command(command: str):
    if not container_connected_devices:
        print("[container] No device connected yet.")
        return

    for websocket in list(container_connected_devices):
        try:
            await websocket.send(command)
            print(f"[container] [>] Sent: {command}")
        except websockets.exceptions.ConnectionClosed:
            container_connected_devices.discard(websocket)


# ============================================================
# Shared keyboard control loop (filtration simulation only)
# ============================================================


def read_key() -> str:
    """Blocking read of a single keypress from stdin, no Enter required."""
    if sys.platform == "win32":
        return msvcrt.getch().decode(errors="ignore")

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        return sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


async def toggle_simulation():
    global simulation_mode
    simulation_mode = not simulation_mode
    await send_container_command("SIM_ON" if simulation_mode else "SIM_OFF")
    print(f"[container] [state] Simulation mode is now {'ON' if simulation_mode else 'OFF'}")


async def step_simulated_ph(increase: bool):
    await send_container_command("SIM_INC" if increase else "SIM_DEC")
    print(f"[container] [state] Simulated pH {'increased' if increase else 'decreased'} by 0.1")
    if not simulation_mode:
        print("[container] [state] (note: not in simulation mode yet — press 't' first for this to take effect)")


async def command_loop():
    loop = asyncio.get_event_loop()
    print(
        "'t' to toggle filtration simulation mode, 'l' to raise simulated pH by 0.1, "
        "'k' to lower it by 0.1, 'r' to toggle pipe simulation mode. Press 'q' to quit."
    )

    while True:
        key = await loop.run_in_executor(None, read_key)

        if key.lower() == "t":
            await toggle_simulation()
        elif key.lower() == "l":
            await step_simulated_ph(True)
        elif key.lower() == "k":
            await step_simulated_ph(False)
        elif key.lower() == "r":
            await toggle_pipe_simulation()
        elif key.lower() == "q":
            print("Exiting...")
            return


async def main():
    print(f"Starting pipe-sensor server on ws://{HOST}:{PIPES_PORT}")
    print(f"Starting container/filtration server on ws://{HOST}:{CONTAINER_PORT}")
    async with websockets.serve(handle_pipe_client, HOST, PIPES_PORT), \
            websockets.serve(handle_container_client, HOST, CONTAINER_PORT):
        await command_loop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
