"""
Azure SQL Editor — Microsoft Entra MFA
Connects to Azure SQL Server using Active Directory Interactive (MFA) auth,
loads a .sql file (or all .sql files in a folder), and executes them.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import os
import re
import datetime

try:
    import pyodbc
except ImportError:
    import sys
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyodbc"])
    import pyodbc


# ---------------------------------------------------------------------------
# Driver detection
# ---------------------------------------------------------------------------

PREFERRED_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server Native Client 11.0",
    "SQL Server",
]


def get_best_driver():
    available = pyodbc.drivers()
    for d in PREFERRED_DRIVERS:
        if d in available:
            return d
    return None


# ---------------------------------------------------------------------------
# Main application
# ---------------------------------------------------------------------------

class AzureEditorApp:

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Azure SQL Editor — Microsoft Entra MFA")
        self.root.geometry("1000x740")
        self.root.minsize(820, 580)
        self.connection = None
        self._build_ui()
        self._check_drivers()

    # ── UI construction ──────────────────────────────────────────────────────

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        wrap = ttk.Frame(self.root, padding=10)
        wrap.grid(row=0, column=0, sticky="nsew")
        wrap.columnconfigure(0, weight=1)
        wrap.rowconfigure(2, weight=1)   # output row expands

        # ── Connection ────────────────────────────────────────────────
        cf = ttk.LabelFrame(wrap, text=" Connection ", padding=8)
        cf.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        cf.columnconfigure(1, weight=1)
        cf.columnconfigure(3, weight=1)

        ttk.Label(cf, text="Server Name:").grid(row=0, column=0, sticky="w", padx=(0, 5))
        self.server_var = tk.StringVar()
        ttk.Entry(cf, textvariable=self.server_var).grid(
            row=0, column=1, sticky="ew", padx=(0, 14))

        ttk.Label(cf, text="Database:").grid(row=0, column=2, sticky="w", padx=(0, 5))
        self.database_var = tk.StringVar(value="master")
        ttk.Entry(cf, textvariable=self.database_var, width=22).grid(
            row=0, column=3, sticky="ew")

        ttk.Label(cf, text="Username:").grid(
            row=1, column=0, sticky="w", padx=(0, 5), pady=(6, 0))
        self.username_var = tk.StringVar(value="sar071023@paycor.net")
        ttk.Entry(cf, textvariable=self.username_var).grid(
            row=1, column=1, sticky="ew", padx=(0, 14), pady=(6, 0))

        btn_row = ttk.Frame(cf)
        btn_row.grid(row=1, column=2, columnspan=2, sticky="e", pady=(6, 0))

        self.status_dot = tk.Label(btn_row, text="●", fg="#888888", font=("Segoe UI", 14))
        self.status_dot.pack(side="left", padx=(0, 6))

        self.connect_btn = ttk.Button(
            btn_row, text="Connect  (MFA)", command=self._connect, width=16)
        self.connect_btn.pack(side="left", padx=(0, 4))

        self.disconnect_btn = ttk.Button(
            btn_row, text="Disconnect", command=self._disconnect,
            state="disabled", width=12)
        self.disconnect_btn.pack(side="left")

        # Trust server cert option (helps when cert chain validation drops the TCP conn)
        self.trust_cert_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            cf, text="Trust Server Certificate",
            variable=self.trust_cert_var,
        ).grid(row=2, column=0, columnspan=4, sticky="w", pady=(4, 0))

        # ── SQL Source ────────────────────────────────────────────────
        sf = ttk.LabelFrame(wrap, text=" SQL Source ", padding=8)
        sf.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        sf.columnconfigure(1, weight=1)

        self.source_mode = tk.StringVar(value="file")
        ttk.Radiobutton(
            sf, text="Single .sql file",
            variable=self.source_mode, value="file",
            command=self._on_mode_change,
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        ttk.Radiobutton(
            sf, text="Folder (execute all .sql files)",
            variable=self.source_mode, value="folder",
            command=self._on_mode_change,
        ).grid(row=0, column=1, columnspan=3, sticky="w", pady=(0, 4))

        self.autocommit_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            sf, text="Auto-commit each batch",
            variable=self.autocommit_var,
        ).grid(row=0, column=4, sticky="e")

        self.stop_on_error_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            sf, text="Stop on first error",
            variable=self.stop_on_error_var,
        ).grid(row=0, column=5, sticky="e", padx=(8, 0))

        ttk.Label(sf, text="Path:").grid(row=1, column=0, sticky="w", padx=(0, 5))
        self.path_var = tk.StringVar()
        ttk.Entry(sf, textvariable=self.path_var).grid(
            row=1, column=1, sticky="ew", padx=(0, 5))

        ttk.Button(sf, text="Browse…", command=self._browse, width=10).grid(
            row=1, column=2, padx=(0, 5))

        self.execute_btn = ttk.Button(
            sf, text="▶  Execute", command=self._execute,
            state="disabled", width=12)
        self.execute_btn.grid(row=1, column=3)

        # ── Output ────────────────────────────────────────────────────
        of = ttk.LabelFrame(wrap, text=" Output ", padding=8)
        of.grid(row=2, column=0, sticky="nsew", pady=(0, 6))
        of.columnconfigure(0, weight=1)
        of.rowconfigure(0, weight=1)

        self.output = scrolledtext.ScrolledText(
            of, wrap=tk.WORD, font=("Consolas", 9),
            state="disabled",
            background="#1e1e1e", foreground="#d4d4d4",
            selectbackground="#264f78",
            insertbackground="white",
        )
        self.output.grid(row=0, column=0, sticky="nsew")

        # Colour tags
        self.output.tag_config("error",   foreground="#f44747")
        self.output.tag_config("success", foreground="#4ec9b0")
        self.output.tag_config("info",    foreground="#9cdcfe")
        self.output.tag_config("header",  foreground="#dcdcaa")
        self.output.tag_config("data",    foreground="#ce9178")

        ttk.Button(of, text="Clear output", command=self._clear_output).grid(
            row=1, column=0, sticky="w", pady=(5, 0))

        # ── Status bar ────────────────────────────────────────────────
        self.status_var = tk.StringVar(value="Ready  —  Not connected")
        ttk.Label(
            wrap, textvariable=self.status_var,
            anchor="w", relief="sunken", padding=(6, 2),
        ).grid(row=3, column=0, sticky="ew")

        self.progress = ttk.Progressbar(wrap, mode="indeterminate")
        self.progress.grid(row=4, column=0, sticky="ew", pady=(3, 0))

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _log(self, message: str, tag: str = "info"):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self.output.config(state="normal")
        self.output.insert(tk.END, f"[{ts}]  {message}\n", tag)
        self.output.see(tk.END)
        self.output.config(state="disabled")

    def _clear_output(self):
        self.output.config(state="normal")
        self.output.delete("1.0", tk.END)
        self.output.config(state="disabled")

    def _set_status(self, msg: str):
        self.status_var.set(msg)
        self.root.update_idletasks()

    def _on_mode_change(self):
        self.path_var.set("")

    def _check_drivers(self):
        driver = get_best_driver()
        if driver:
            self._log(f"ODBC driver detected: {driver}", "success")
        else:
            self._log(
                "WARNING: No ODBC Driver for SQL Server detected. "
                "Install 'ODBC Driver 18 for SQL Server' from https://aka.ms/downloadmsodbcsql",
                "error",
            )
            messagebox.showwarning(
                "ODBC Driver Not Found",
                "No ODBC Driver for SQL Server was found.\n\n"
                "Please install  ODBC Driver 18 for SQL Server  from Microsoft:\n"
                "https://aka.ms/downloadmsodbcsql\n\n"
                "The application will not be able to connect without it.",
            )

    # ── Connection ───────────────────────────────────────────────────────────

    def _connect(self):
        server = self.server_var.get().strip()
        database = self.database_var.get().strip() or "master"
        username = self.username_var.get().strip()

        if not server:
            messagebox.showerror("Missing Input", "Please enter a Server Name.")
            return
        if not username:
            messagebox.showerror("Missing Input", "Please enter a Username.")
            return

        driver = get_best_driver()
        if not driver:
            messagebox.showerror(
                "Driver Error",
                "No ODBC Driver for SQL Server found.\n"
                "Install ODBC Driver 18: https://aka.ms/downloadmsodbcsql",
            )
            return

        # Normalize server: strip tcp: prefix, separate host and port
        raw = server.strip()
        if raw.lower().startswith("tcp:"):
            raw = raw[4:]
        if "," in raw:
            srv_host, srv_port = raw.rsplit(",", 1)
            srv_port = srv_port.strip() or "1433"
        else:
            srv_host, srv_port = raw, "1433"
        if "." not in srv_host:
            srv_host = f"{srv_host}.database.windows.net"
        server_str = f"tcp:{srv_host},{srv_port}"   # ODBC Server= value
        server_display = f"{srv_host},{srv_port}"   # label / status bar

        trust_cert = self.trust_cert_var.get()

        conn_str = (
            f"Driver={{{driver}}};"
            f"Server={server_str};"
            f"Database={database};"
            f"UID={username};"
            "Authentication=ActiveDirectoryInteractive;"
            "Encrypt=yes;"
            f"TrustServerCertificate={'yes' if trust_cert else 'no'};"
            "Connect Timeout=30;"
            "MARS_Connection=No;"
            "PacketSize=4096;"
        )

        self.connect_btn.config(state="disabled")
        self.progress.start(12)
        self._set_status("Connecting — MFA browser / prompt will open shortly…")
        self._log(f"Connecting to  {server_display}  /  {database}  as  {username} …", "info")

        def worker():
            try:
                conn = pyodbc.connect(conn_str, timeout=60)
                self.connection = conn
                self.root.after(0, self._on_connected, server_display, database)
            except Exception as ex:
                self.root.after(0, self._on_connect_fail, str(ex))

        threading.Thread(target=worker, daemon=True).start()

    def _on_connected(self, server: str, db: str):
        self.progress.stop()
        self.disconnect_btn.config(state="normal")
        self.execute_btn.config(state="normal")
        self.status_dot.config(fg="#4ec9b0")
        self._set_status(f"Connected  ●  {server}  /  {db}")
        self._log(f"Connected successfully to  {server}  |  {db}", "success")

    def _on_connect_fail(self, err: str):
        self.progress.stop()
        self.connect_btn.config(state="normal")
        self.status_dot.config(fg="#f44747")
        self._set_status("Connection failed — see output for details")
        self._log(f"Connection error: {err}", "error")

        # Emit targeted hints for common Azure MFA failure modes
        if "18456" in err:
            self._log(
                "Hint (18456): Login failed. Possible causes:\n"
                "  • The Azure SQL firewall may be blocking your client IP\n"
                "    → Azure Portal › SQL Server › Networking › Add client IP\n"
                "  • User lacks a SQL/AAD login in this database\n"
                "    → Ask a DBA to run: CREATE USER [sar071023@paycor.net] "
                "FROM EXTERNAL PROVIDER",
                "error",
            )
        if "10054" in err or "forcibly closed" in err.lower():
            self._log(
                "Hint (10054): Connection was reset. Try:\n"
                "  • Enable 'Trust Server Certificate' checkbox and reconnect\n"
                "  • Verify the server name / database name are correct",
                "error",
            )

        messagebox.showerror("Connection Failed",
            err + "\n\nSee the Output panel for troubleshooting hints.")

    def _disconnect(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
        self.connection = None
        self.connect_btn.config(state="normal")
        self.disconnect_btn.config(state="disabled")
        self.execute_btn.config(state="disabled")
        self.status_dot.config(fg="#888888")
        self._set_status("Disconnected")
        self._log("Disconnected.", "info")

    # ── Browse ───────────────────────────────────────────────────────────────

    def _browse(self):
        if self.source_mode.get() == "file":
            p = filedialog.askopenfilename(
                title="Select SQL File",
                filetypes=[("SQL Files", "*.sql"), ("All Files", "*.*")],
            )
        else:
            p = filedialog.askdirectory(title="Select Folder Containing .sql Files")
        if p:
            self.path_var.set(p)

    # ── Execute ──────────────────────────────────────────────────────────────

    def _execute(self):
        path = self.path_var.get().strip()
        if not path:
            messagebox.showerror("Missing Input", "Please select a file or folder.")
            return
        if not self.connection:
            messagebox.showerror("Not Connected", "Connect to a server first.")
            return

        if self.source_mode.get() == "file":
            if not os.path.isfile(path):
                messagebox.showerror("File Not Found", f"File not found:\n{path}")
                return
            files = [path]
        else:
            if not os.path.isdir(path):
                messagebox.showerror("Folder Not Found", f"Folder not found:\n{path}")
                return
            files = sorted(
                os.path.join(path, f)
                for f in os.listdir(path)
                if f.lower().endswith(".sql")
            )
            if not files:
                messagebox.showwarning(
                    "No SQL Files", "No .sql files found in the selected folder.")
                return

        self.execute_btn.config(state="disabled")
        self.progress.start(12)
        self._log(
            f"{'─' * 60}\n"
            f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]  "
            f"Starting — {len(files)} file(s) to execute",
            "header",
        )

        autocommit = self.autocommit_var.get()
        stop_on_error = self.stop_on_error_var.get()

        def worker():
            total_ok = total_err = 0
            aborted = False
            for filepath in files:
                ok, err = self._run_sql_file(filepath, autocommit, stop_on_error)
                total_ok += ok
                total_err += err
                if stop_on_error and err:
                    aborted = True
                    break

            tag = "success" if total_err == 0 else "error"
            summary = (
                f"Finished — {len(files)} file(s), "
                f"{total_ok + total_err} batch(es): "
                f"{total_ok} OK, {total_err} failed."
            )
            if aborted:
                summary += "  [Stopped after first error]"
            self.root.after(0, self._log, summary, tag)
            self.root.after(0, self._set_status, summary)
            self.root.after(0, self._finish_execute)

        threading.Thread(target=worker, daemon=True).start()

    def _finish_execute(self):
        self.progress.stop()
        self.execute_btn.config(state="normal")

    def _run_sql_file(
        self,
        filepath: str,
        autocommit: bool,
        stop_on_error: bool,
    ):
        fname = os.path.basename(filepath)
        self.root.after(0, self._log, f"── {fname}", "header")

        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
                sql = fh.read()
        except OSError as exc:
            self.root.after(0, self._log, f"Cannot read file: {exc}", "error")
            return 0, 1

        # Split on GO batch separator
        batches = [
            b.strip()
            for b in re.split(r"(?im)^\s*GO\s*(?:--[^\n]*)?\s*$", sql)
            if b.strip()
        ]

        if not batches:
            self.root.after(0, self._log, "  File contains no executable batches.", "info")
            return 0, 0

        cursor = self.connection.cursor()
        cursor.timeout = 0   # Command Timeout=0 — no statement timeout
        ok = err = 0

        for i, batch in enumerate(batches, 1):
            try:
                cursor.execute(batch)

                while True:
                    if cursor.description:
                        cols = [c[0] for c in cursor.description]
                        rows = cursor.fetchall()
                        table = self._format_table(cols, rows)
                        self.root.after(
                            0, self._log,
                            f"  Batch {i}/{len(batches)}: {len(rows)} row(s) returned\n{table}",
                            "data",
                        )
                    else:
                        rc = cursor.rowcount
                        self.root.after(
                            0, self._log,
                            f"  Batch {i}/{len(batches)}: {max(rc, 0)} row(s) affected.",
                            "info",
                        )
                    # Move to next result set, if any
                    if not cursor.nextset():
                        break

                if autocommit:
                    self.connection.commit()

                ok += 1

            except Exception as exc:
                self.root.after(
                    0, self._log,
                    f"  Batch {i}/{len(batches)} FAILED: {exc}",
                    "error",
                )
                err += 1
                if stop_on_error:
                    break

        # Final commit if not auto-committing per batch
        if not autocommit:
            try:
                self.connection.commit()
            except Exception:
                pass

        return ok, err

    # ── Result formatting ────────────────────────────────────────────────────

    @staticmethod
    def _format_table(
        columns: list,
        rows: list,
        max_rows: int = 500,
        max_col_width: int = 48,
    ) -> str:
        if not rows:
            return "    (no rows)"

        display = rows[:max_rows]

        widths = [min(max_col_width, len(str(c))) for c in columns]
        for row in display:
            for i, v in enumerate(row):
                cell = "NULL" if v is None else str(v)
                widths[i] = min(max_col_width, max(widths[i], len(cell)))

        sep = "─┼─".join("─" * w for w in widths)
        hdr = " │ ".join(
            str(c)[:widths[i]].ljust(widths[i]) for i, c in enumerate(columns)
        )
        lines = ["    " + hdr, "    " + sep]

        for row in display:
            cells = " │ ".join(
                ("NULL" if v is None else str(v))[:widths[i]].ljust(widths[i])
                for i, v in enumerate(row)
            )
            lines.append("    " + cells)

        if len(rows) > max_rows:
            lines.append(f"    … {len(rows) - max_rows} more rows not displayed")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    root = tk.Tk()
    root.configure(bg="#f0f0f0")
    style = ttk.Style(root)
    # Use a modern theme if available
    available = style.theme_names()
    for theme in ("vista", "clam", "alt"):
        if theme in available:
            style.theme_use(theme)
            break
    AzureEditorApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
