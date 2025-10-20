
import re
import sys
import socket
import threading
import queue
from tkinter import Tk, ttk, Text, BOTH, END, N, S, E, W, filedialog, messagebox, StringVar, Menu

APP_TITLE = "UDP Feed Parser + Live Listener (TICK | MBO | MBP | FEED)"
APP_GEOMETRY = "1200x720"

# ---------- Parsing Logic ----------

def parse_line(line: str):
    """
    Detect the message type and dispatch to a specialized parser.
    Returns: (msg_type, rows, columns)
    """
    if not isinstance(line, str):
        try:
            line = line.decode("utf-8", errors="ignore")
        except Exception:
            line = str(line)

    s = line.strip()
    s = "".join(ch for ch in s if ord(ch) >= 9)

    if s.startswith("TICK|"):
        return ("TICK", *parse_tick(s))
    if s.startswith("MBO|"):
        return ("MBO", *parse_mbo(s))
    if s.startswith("MBP|"):
        return ("MBP", *parse_mbp(s))
    if s.startswith("FEED|"):
        return ("FEED", *parse_feed(s))

    for prefix in ("TICK|", "MBO|", "MBP|", "FEED|"):
        i = s.find(prefix)
        if i >= 0:
            s2 = s[i:]
            if s2.startswith("TICK|"):
                return ("TICK", *parse_tick(s2))
            if s2.startswith("MBO|"):
                return ("MBO", *parse_mbo(s2))
            if s2.startswith("MBP|"):
                return ("MBP", *parse_mbp(s2))
            if s2.startswith("FEED|"):
                return ("FEED", *parse_feed(s2))

    return ("UNKNOWN", [], [])

def _split_safe(s, sep):
    parts = s.split(sep)
    if parts and parts[-1].endswith("|*"):
        parts[-1] = parts[-1].replace("|*", "")
    return parts

def parse_tick(s: str):
    payload = s[5:]
    fields = _split_safe(payload, ";")
    while len(fields) < 8:
        fields.append("")
    headers = ["Group/Market", "Board", "Symbol", "Side", "Qty", "Price", "TradeID", "Time"]
    row = fields[:8]
    return [row], headers

def parse_mbo(s: str):
    payload = s[4:]
    try:
        symbol, board, rest = payload.split(";", 2)
    except ValueError:
        return [], []

    parts = _split_safe(rest, ";")
    side_blocks = []
    i = 0
    while i < len(parts):
        side = parts[i].strip().upper()
        if side not in ("BUY", "SELL"):
            i += 1
            continue
        i += 1
        if i >= len(parts):
            break
        entries = parts[i]
        i += 1
        side_blocks.append((side, entries))

    rows = []
    for side, entries in side_blocks:
        if not entries:
            continue
        for chunk in entries.split("$"):
            if not chunk:
                continue
            cells = [c.strip() for c in chunk.split(",")]
            while len(cells) < 4:
                cells.append("")
            price, qty, level, oid = cells[:4]
            rows.append([symbol, board, side, level, price, qty, oid])

    headers = ["Symbol", "Board", "Side", "Level", "Price", "Qty", "OrderID"]
    return rows, headers

def parse_mbp(s: str):
    payload = s[4:]
    try:
        symbol, board, rest = payload.split(";", 2)
    except ValueError:
        return [], []

    parts = _split_safe(rest, ";")
    side_blocks = []
    i = 0
    while i < len(parts):
        side = parts[i].strip().upper()
        if side not in ("BUY", "SELL"):
            i += 1
            continue
        i += 1
        if i >= len(parts):
            break
        entries = parts[i]
        i += 1
        side_blocks.append((side, entries))

    rows = []
    for side, entries in side_blocks:
        if not entries:
            continue
        for chunk in entries.split("$"):
            if not chunk:
                continue
            cells = [c.strip() for c in chunk.split(",")]
            while len(cells) < 4:
                cells.append("")
            level, qty, price, rank = cells[:4]
            rows.append([symbol, board, side, level, price, qty, rank])

    headers = ["Symbol", "Board", "Side", "Level", "Price", "Qty", "Rank"]
    return rows, headers

def parse_feed(s: str):
    payload = s[5:]
    fields = _split_safe(payload, ";")
    symbol = fields[0] if len(fields) > 0 else ""
    board  = fields[1] if len(fields) > 1 else ""
    ftype  = fields[2] if len(fields) > 2 else ""
    extras = fields[3:]
    headers = ["Symbol", "Board", "Type"]
    row = [symbol, board, ftype]
    for idx, val in enumerate(extras, start=1):
        headers.append(f"Extra{idx}")
        row.append(val)
    return [row], headers

# ---------- Live UDP Listener ----------

class UDPListener(threading.Thread):
    def __init__(self, app, local_ip, ports):
        super().__init__(daemon=True)
        self.app = app
        self.local_ip = local_ip
        self.ports = ports
        self.sockets = []
        self._stop_evt = threading.Event()

    def stop(self):
        self._stop_evt.set()
        for s in self.sockets:
            try:
                s.close()
            except Exception:
                pass
        self.sockets = []

    def run(self):
        try:
            # Create/bind sockets
            for p in self.ports:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                except Exception:
                    pass
                try:
                    s.bind((self.local_ip, p))
                except OSError as e:
                    self.app.post_status(f"[Bind error] {self.local_ip}:{p} -> {e}")
                    continue
                s.settimeout(0.5)
                self.sockets.append(s)
                self.app.post_status(f"[Listening] {self.local_ip}:{p}")
        except Exception as e:
            self.app.post_status(f"[Listener init error] {e}")

        # Receive loop
        while not self._stop_evt.is_set():
            for s in list(self.sockets):
                try:
                    data, addr = s.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    # socket closed during stop()
                    continue
                except Exception as e:
                    self.app.post_status(f"[Recv error] {e}")
                    continue

                # Parse and enqueue to GUI
                msg_type, rows, cols = parse_line(data)
                if rows and cols:
                    self.app.enqueue_rows(rows, cols)
                else:
                    # Unknown line: show raw
                    self.app.post_status(f"[Unknown] from {addr}: {data[:120]!r}")

# ---------- GUI ----------

class ParserApp:
    def __init__(self, master):
        self.master = master
        master.title(APP_TITLE)
        master.geometry(APP_GEOMETRY)

        self.style = ttk.Style(master)
        try:
            self.style.theme_use('clam')
        except Exception:
            pass

        # Top toolbar
        toolbar = ttk.Frame(master, padding=(6, 6))
        toolbar.grid(row=0, column=0, sticky=(W, E))
        self.current_type = StringVar(value="Auto")
        ttk.Label(toolbar, text="Message Type:").grid(row=0, column=0, padx=(0,6))
        type_cb = ttk.Combobox(toolbar, textvariable=self.current_type, state="readonly",
                               values=["Auto", "TICK", "MBO", "MBP", "FEED"])
        type_cb.grid(row=0, column=1, padx=(0,12))

        parse_btn = ttk.Button(toolbar, text="Parse (Ctrl+Enter)", command=self.parse_input)
        parse_btn.grid(row=0, column=2, padx=(0,6))

        open_btn = ttk.Button(toolbar, text="Open Text File...", command=self.open_file)
        open_btn.grid(row=0, column=3, padx=(0,6))

        clear_btn = ttk.Button(toolbar, text="Clear Table", command=self.clear_table)
        clear_btn.grid(row=0, column=4, padx=(0,6))

        # Live listener controls
        live = ttk.LabelFrame(master, text="Live UDP Listener", padding=(6,6))
        live.grid(row=1, column=0, sticky=(W, E), padx=6)

        ttk.Label(live, text="Local IP (e.g., 0.0.0.0 or your NIC IP):").grid(row=0, column=0, sticky=W)
        self.local_ip_var = StringVar(value="0.0.0.0")
        ttk.Entry(live, textvariable=self.local_ip_var, width=24).grid(row=0, column=1, padx=(6,12))

        ttk.Label(live, text="Ports (comma-separated):").grid(row=0, column=2, sticky=W)
        self.ports_var = StringVar(value="7011,7012,7013,7015")
        ttk.Entry(live, textvariable=self.ports_var, width=24).grid(row=0, column=3, padx=(6,12))

        self.listen_btn = ttk.Button(live, text="Start Listening", command=self.toggle_listen)
        self.listen_btn.grid(row=0, column=4, padx=(6,6))

        # Input text for manual paste/parse
        self.input = Text(master, wrap='none', height=8)
        self.input.grid(row=2, column=0, sticky=(N, S, E, W), padx=6, pady=(6,0))
        self.input.insert(END, "Paste UDP payload lines here (TICK|, MBO|, MBP|, FEED|) then click Parse.\n")

        # Output Tree
        self.tree = ttk.Treeview(master, columns=(), show="headings")
        self.tree.grid(row=3, column=0, sticky=(N, S, E, W), padx=6, pady=(6, 6))

        # Status box
        self.status = Text(master, wrap='none', height=6)
        self.status.grid(row=4, column=0, sticky=(N, S, E, W), padx=6, pady=(0, 6))
        self.status.insert(END, "[Status]\n")

        # Scrollbars
        yscroll_in = ttk.Scrollbar(master, orient="vertical", command=self.input.yview)
        self.input.configure(yscrollcommand=yscroll_in.set)
        yscroll_in.place(in_=self.input, relx=1.0, rely=0, relheight=1.0, anchor="ne")

        yscroll_out = ttk.Scrollbar(master, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll_out.set)
        yscroll_out.place(in_=self.tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")

        yscroll_status = ttk.Scrollbar(master, orient="vertical", command=self.status.yview)
        self.status.configure(yscrollcommand=yscroll_status.set)
        yscroll_status.place(in_=self.status, relx=1.0, rely=0, relheight=1.0, anchor="ne")

        # Layout config
        master.grid_rowconfigure(2, weight=0)  # input
        master.grid_rowconfigure(3, weight=1)  # table
        master.grid_rowconfigure(4, weight=0)  # status
        master.grid_columnconfigure(0, weight=1)

        # Shortcuts
        master.bind_all("<Control-Return>", lambda e: self.parse_input())

        # Menu
        menubar = Menu(master)
        filemenu = Menu(menubar, tearoff=0)
        filemenu.add_command(label="Open Text File...", command=self.open_file)
        filemenu.add_separator()
        filemenu.add_command(label="Exit", command=master.quit)
        menubar.add_cascade(label="File", menu=filemenu)
        master.config(menu=menubar)

        # Queues and listener
        self.row_queue = queue.Queue()
        self.listener = None
        self.headers = None
        self.master.after(100, self._drain_queue)

    # ----- Status & queue helpers -----
    def post_status(self, msg: str):
        self.status.insert(END, msg + "\n")
        self.status.see(END)

    def enqueue_rows(self, rows, cols):
        self.row_queue.put(("rows", rows, cols))

    def _drain_queue(self):
        try:
            while True:
                kind, rows, cols = self.row_queue.get_nowait()
                if kind == "rows":
                    # Ensure headers
                    if (self.headers is None) or (len(cols) > len(self.headers)):
                        self._setup_tree(cols)
                        self.headers = cols
                    # Insert rows
                    for r in rows:
                        if len(r) < len(self.headers):
                            r = r + [""] * (len(self.headers) - len(r))
                        self.tree.insert("", END, values=r)
        except queue.Empty:
            pass
        self.master.after(100, self._drain_queue)

    # ----- UI actions -----
    def clear_table(self):
        self.tree.delete(*self.tree.get_children())

    def open_file(self):
        path = filedialog.askopenfilename(title="Open UDP Text Payload",
                                          filetypes=[("Text files", "*.txt *.log *.csv *.json *.dat"),
                                                     ("All files", "*.*")])
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                data = f.read()
            self.input.delete("1.0", END)
            self.input.insert(END, data)
            messagebox.showinfo("Loaded", f"Loaded {len(data)} characters from:\\n{path}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to open file:\\n{e}")

    def _setup_tree(self, headers):
        for col in self.tree["columns"]:
            self.tree.heading(col, text="")
            self.tree.column(col, width=0)
        self.tree["columns"] = headers
        for h in headers:
            self.tree.heading(h, text=h)
            self.tree.column(h, stretch=True, width=120)

    def parse_input(self):
        text = self.input.get("1.0", END)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        all_rows = []
        headers = None

        selected = self.current_type.get()

        for ln in lines:
            if selected != "Auto":
                if not ln.startswith(selected + "|"):
                    idx = ln.find(selected + "|")
                    if idx >= 0:
                        ln = ln[idx:]
            msg_type, rows, cols = parse_line(ln if selected == "Auto" else (ln if ln.startswith(selected + "|") else ln))
            if rows and cols:
                if headers is None or len(cols) > len(headers):
                    headers = cols
                for r in rows:
                    all_rows.append(r)

        if headers is None:
            messagebox.showwarning("No parsed data", "No recognizable TICK/MBO/MBP/FEED lines were found.")
            return

        self._setup_tree(headers)
        self.headers = headers

        for r in all_rows:
            if len(r) < len(headers):
                r = r + [""] * (len(headers) - len(r))
            self.tree.insert("", END, values=r)

    def toggle_listen(self):
        if self.listener is None:
            # Start
            ports_text = self.ports_var.get().strip()
            try:
                ports = [int(p.strip()) for p in ports_text.split(",") if p.strip()]
            except ValueError:
                messagebox.showerror("Error", "Ports must be comma-separated integers.")
                return
            local_ip = self.local_ip_var.get().strip() or "0.0.0.0"
            self.listener = UDPListener(self, local_ip, ports)
            self.listener.start()
            self.listen_btn.config(text="Stop Listening")
            self.post_status(f"Starting listener on {local_ip}:{ports}")
        else:
            # Stop
            self.listener.stop()
            self.listener = None
            self.listen_btn.config(text="Start Listening")
            self.post_status("Listener stopped.")

def main():
    root = Tk()
    app = ParserApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()
