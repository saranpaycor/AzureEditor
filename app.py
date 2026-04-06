"""
Azure SQL Editor — Web version (Flask + Microsoft Entra MFA)
Serves a browser UI at http://localhost:5000
"""

import json
import os
import queue
import re
import threading
import uuid
import datetime
import webbrowser

from flask import Flask, Response, jsonify, render_template, request

try:
    import pyodbc
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyodbc"])
    import pyodbc

app = Flask(__name__)

# ---------------------------------------------------------------------------
# ODBC helper
# ---------------------------------------------------------------------------

PREFERRED_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server",
]


def get_best_driver():
    available = pyodbc.drivers()
    for d in PREFERRED_DRIVERS:
        if d in available:
            return d
    return None


# ---------------------------------------------------------------------------
# Single-user in-process state  (localhost only — not for multi-user prod)
# ---------------------------------------------------------------------------

_state = {
    "connection": None,
    "connected": False,
    "server": "",
    "database": "",
}

_jobs: dict[str, queue.Queue] = {}


def _new_job() -> tuple[str, queue.Queue]:
    jid = str(uuid.uuid4())
    q: queue.Queue = queue.Queue()
    _jobs[jid] = q
    return jid, q


# ---------------------------------------------------------------------------
# Routes — pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html", driver=get_best_driver() or "Not found — install ODBC Driver 18")


# ---------------------------------------------------------------------------
# Routes — API
# ---------------------------------------------------------------------------

@app.route("/api/status")
def api_status():
    return jsonify({
        "connected": _state["connected"],
        "server":    _state["server"],
        "database":  _state["database"],
    })


@app.route("/api/browse")
def api_browse():
    """Open a native file/folder dialog on the local machine and return the path."""
    mode = request.args.get("mode", "file")
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        if mode == "file":
            path = filedialog.askopenfilename(
                title="Select SQL File",
                filetypes=[("SQL Files", "*.sql"), ("All Files", "*.*")],
                parent=root,
            )
        else:
            path = filedialog.askdirectory(
                title="Select Folder Containing .sql Files",
                parent=root,
            )
        root.destroy()
    except Exception as exc:
        return jsonify({"path": "", "error": str(exc)})
    return jsonify({"path": path or ""})


@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.get_json(force=True) or {}
    server   = (data.get("server")   or "").strip()
    database = (data.get("database") or "master").strip()
    username = (data.get("username") or "").strip()
    trust    = bool(data.get("trust_cert", False))

    if not server or not username:
        return jsonify({"error": "Server and Username are required"}), 400

    driver = get_best_driver()
    if not driver:
        return jsonify({"error": "No ODBC Driver for SQL Server found. Install ODBC Driver 18."}), 500

    # Normalise server — strip tcp: prefix, split host/port
    raw = server
    if raw.lower().startswith("tcp:"):
        raw = raw[4:]
    if "," in raw:
        srv_host, srv_port = raw.rsplit(",", 1)
        srv_port = srv_port.strip() or "1433"
    else:
        srv_host, srv_port = raw, "1433"
    if "." not in srv_host:
        srv_host = f"{srv_host}.database.windows.net"

    server_str     = f"tcp:{srv_host},{srv_port}"
    server_display = f"{srv_host},{srv_port}"

    conn_str = (
        f"Driver={{{driver}}};"
        f"Server={server_str};"
        f"Database={database};"
        f"UID={username};"
        "Authentication=ActiveDirectoryInteractive;"
        "Encrypt=yes;"
        f"TrustServerCertificate={'yes' if trust else 'no'};"
        "Connect Timeout=30;"
        "MARS_Connection=No;"
        "PacketSize=4096;"
    )

    jid, q = _new_job()

    def worker():
        q.put({"type": "log", "tag": "info",
               "text": f"Connecting to  {server_display}  /  {database}  as  {username} …"})
        q.put({"type": "log", "tag": "info",
               "text": "MFA browser / prompt will open on this machine …"})
        try:
            if _state["connection"]:
                try:
                    _state["connection"].close()
                except Exception:
                    pass
            conn = pyodbc.connect(conn_str, timeout=60)
            _state["connection"] = conn
            _state["connected"]  = True
            _state["server"]     = server_display
            _state["database"]   = database
            q.put({"type": "connected", "server": server_display, "database": database,
                   "text": f"Connected successfully to  {server_display}  |  {database}"})
        except Exception as ex:
            err = str(ex)
            q.put({"type": "log", "tag": "error", "text": f"Connection error: {err}"})
            if "18456" in err:
                q.put({"type": "log", "tag": "error",
                       "text": (
                           "Hint (18456): Login failed. Check:\n"
                           "  • Azure Portal › SQL Server › Networking — add your IP to firewall\n"
                           "  • DBA must run: CREATE USER [sar071023@paycor.net] FROM EXTERNAL PROVIDER"
                       )})
            if "10054" in err or "forcibly closed" in err.lower():
                q.put({"type": "log", "tag": "error",
                       "text": "Hint (10054): Try enabling 'Trust Server Certificate' and reconnecting."})
            q.put({"type": "error", "text": err})
        finally:
            q.put({"type": "done"})

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"job_id": jid})


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    if _state["connection"]:
        try:
            _state["connection"].close()
        except Exception:
            pass
    _state["connection"] = None
    _state["connected"]  = False
    _state["server"]     = ""
    _state["database"]   = ""
    return jsonify({"ok": True})


@app.route("/api/execute", methods=["POST"])
def api_execute():
    if not _state["connected"] or not _state["connection"]:
        return jsonify({"error": "Not connected to any server"}), 400

    data        = request.get_json(force=True) or {}
    mode        = data.get("mode", "file")          # "file" | "folder" | "text"
    path        = (data.get("path") or "").strip()
    sql_text    = data.get("sql_text", "")
    autocommit  = bool(data.get("autocommit", False))
    stop_on_err = bool(data.get("stop_on_error", False))

    # Build file list
    if mode == "text":
        items = [("(inline SQL)", sql_text)]
    elif mode == "file":
        if not os.path.isfile(path):
            return jsonify({"error": f"File not found: {path}"}), 400
        items = [(os.path.basename(path), path)]
    else:  # folder
        if not os.path.isdir(path):
            return jsonify({"error": f"Folder not found: {path}"}), 400
        files = sorted(
            os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith(".sql")
        )
        if not files:
            return jsonify({"error": "No .sql files found in that folder"}), 400
        items = [(os.path.basename(f), f) for f in files]

    jid, q = _new_job()

    def worker():
        conn       = _state["connection"]
        total_ok   = 0
        total_err  = 0
        aborted    = False

        q.put({"type": "log", "tag": "header",
               "text": (
                   "─" * 60 + "\n" +
                   f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]  "
                   f"Starting — {len(items)} file(s) to execute"
               )})

        for fname, payload in items:
            # payload = file path (str) or SQL content (str) when mode=='text'
            if mode == "text":
                sql = payload
            else:
                try:
                    with open(payload, "r", encoding="utf-8", errors="replace") as fh:
                        sql = fh.read()
                except OSError as exc:
                    q.put({"type": "log", "tag": "error",
                           "text": f"Cannot read {fname}: {exc}"})
                    total_err += 1
                    if stop_on_err:
                        aborted = True
                        break
                    continue

            q.put({"type": "log", "tag": "header", "text": f"── {fname}"})

            batches = [
                b.strip()
                for b in re.split(r"(?im)^\s*GO\s*(?:--[^\n]*)?\s*$", sql)
                if b.strip()
            ]
            if not batches:
                q.put({"type": "log", "tag": "info", "text": "  No executable batches."})
                continue

            cursor = conn.cursor()
            cursor.timeout = 0  # Command Timeout = 0

            for i, batch in enumerate(batches, 1):
                try:
                    cursor.execute(batch)
                    while True:
                        if cursor.description:
                            cols = [c[0] for c in cursor.description]
                            rows = cursor.fetchall()
                            truncated = len(rows) > 500
                            # Structured table message for rich HTML rendering
                            q.put({
                                "type":      "table",
                                "file":      fname,
                                "batch":     i,
                                "total":     len(batches),
                                "columns":   cols,
                                "rows":      [
                                    [None if v is None else str(v) for v in row]
                                    for row in rows[:500]
                                ],
                                "row_count": len(rows),
                                "truncated": truncated,
                            })
                            q.put({"type": "log", "tag": "data",
                                   "text": f"  Batch {i}/{len(batches)}: {len(rows)} row(s) returned."})
                        else:
                            rc = max(cursor.rowcount, 0)
                            q.put({
                                "type":  "affected",
                                "file":  fname,
                                "batch": i,
                                "total": len(batches),
                                "count": rc,
                            })
                            q.put({"type": "log", "tag": "info",
                                   "text": f"  Batch {i}/{len(batches)}: {rc} row(s) affected."})
                        if not cursor.nextset():
                            break
                    if autocommit:
                        conn.commit()
                    total_ok += 1
                except Exception as exc:
                    err_text = str(exc)
                    q.put({"type": "log", "tag": "error",
                           "text": f"  Batch {i}/{len(batches)} FAILED: {err_text}"})
                    # Also send structured error for Results panel inline display
                    q.put({"type": "execerror", "file": fname,
                           "batch": i, "total": len(batches), "text": err_text})
                    total_err += 1
                    if stop_on_err:
                        aborted = True
                        break

            if not autocommit:
                try:
                    conn.commit()
                except Exception:
                    pass

            if aborted:
                break

        tag     = "success" if total_err == 0 else "error"
        summary = (f"Finished — {len(items)} file(s), "
                   f"{total_ok + total_err} batch(es): "
                   f"{total_ok} OK, {total_err} failed.")
        if aborted:
            summary += "  [Stopped after first error]"
        q.put({"type": "log", "tag": tag, "text": summary})
        q.put({"type": "done"})

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"job_id": jid})


@app.route("/api/stream/<job_id>")
def api_stream(job_id):
    q = _jobs.get(job_id)
    if not q:
        return jsonify({"error": "Job not found"}), 404

    def generate():
        try:
            while True:
                try:
                    msg = q.get(timeout=120)
                    yield f"data: {json.dumps(msg)}\n\n"
                    if msg.get("type") in ("done", "error"):
                        break
                except queue.Empty:
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    break
        finally:
            _jobs.pop(job_id, None)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------

def _format_table(columns, rows, max_rows=500, max_col_width=60):
    if not rows:
        return "    (no rows)"
    display = rows[:max_rows]
    widths = [min(max_col_width, len(str(c))) for c in columns]
    for row in display:
        for i, v in enumerate(row):
            widths[i] = min(max_col_width, max(widths[i], len("NULL" if v is None else str(v))))
    sep = "─┼─".join("─" * w for w in widths)
    hdr = " │ ".join(str(c)[:widths[i]].ljust(widths[i]) for i, c in enumerate(columns))
    lines = ["    " + hdr, "    " + sep]
    for row in display:
        cells = " │ ".join(
            ("NULL" if v is None else str(v))[:widths[i]].ljust(widths[i])
            for i, v in enumerate(row)
        )
        lines.append("    " + cells)
    if len(rows) > max_rows:
        lines.append(f"    … {len(rows) - max_rows} more row(s) not displayed")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    url = "http://localhost:8000"
    print(f"\n  Azure SQL Editor → {url}\n  Press Ctrl+C to stop.\n")
    threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    app.run(host="127.0.0.1", port=8000, debug=False, threaded=True)
