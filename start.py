"""
Railway start script — launches both the API server and the Telegram bot.
"""
import subprocess
import sys
import os
import signal
import time

procs = []

def shutdown(sig=None, frame=None):
    for p in procs:
        try:
            p.terminate()
        except Exception:
            pass
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

# Use Railway's PORT env var for the API server, default 5000
port = os.environ.get("PORT", "5000")
os.environ["PORT"] = port

# Start API server
api = subprocess.Popen([sys.executable, "api_server.py"])
procs.append(api)
print(f"[start] API server started (PID {api.pid}, port {port})")

# Give Flask a moment to bind the port
time.sleep(2)

# Point the bot at the local API server
os.environ.setdefault("SHIRO_API_URL", f"http://127.0.0.1:{port}")

# Start Telegram bot
bot = subprocess.Popen([sys.executable, "shiro.py"])
procs.append(bot)
print(f"[start] Bot started (PID {bot.pid})")

# Wait for either to exit — if one dies, kill the other
while True:
    for p in procs:
        ret = p.poll()
        if ret is not None:
            print(f"[start] Process {p.pid} exited with code {ret}, shutting down...")
            shutdown()
    time.sleep(1)
