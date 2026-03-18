"""
bxf_parser — minimal Tkinter GUI.

Launch with:
  python -m bxf_parser.gui
  python -m bxf_parser          (via __main__.py)
"""
from __future__ import annotations

import logging
import queue
import threading
import tkinter as tk
from tkinter import filedialog, scrolledtext, ttk

from .bxf_parser import discover_files, write_outputs
from .parsers import parse_file


# ---------------------------------------------------------------------------
# Queue-based logging handler so worker thread output appears in the GUI
# ---------------------------------------------------------------------------

class _QueueHandler(logging.Handler):
    """Send log records to a queue for the GUI to consume."""

    def __init__(self, log_queue: "queue.Queue[str]") -> None:
        super().__init__()
        self._queue = log_queue

    def emit(self, record: logging.LogRecord) -> None:
        self._queue.put(self.format(record))


# ---------------------------------------------------------------------------
# Main application window
# ---------------------------------------------------------------------------

class BxfParserApp(tk.Tk):
    """Minimal tkinter GUI for the BXF parser."""

    def __init__(self) -> None:
        super().__init__()
        self.title("BXF Parser")
        self.resizable(True, True)
        self.minsize(620, 480)

        self._log_queue: queue.Queue[str] = queue.Queue()
        self._running = False

        self._build_ui()
        self._setup_logging()
        self._poll_log_queue()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        pad = {"padx": 8, "pady": 4}

        # ---- Input path ----
        frm_input = ttk.LabelFrame(self, text="Input (file or folder)")
        frm_input.pack(fill="x", **pad)

        self._var_input = tk.StringVar()
        ttk.Entry(frm_input, textvariable=self._var_input).pack(
            side="left", fill="x", expand=True, padx=(6, 4), pady=6
        )
        ttk.Button(frm_input, text="Browse…", command=self._browse_input).pack(
            side="left", padx=(0, 6), pady=6
        )

        # ---- Output directory ----
        frm_output = ttk.LabelFrame(self, text="Output directory")
        frm_output.pack(fill="x", **pad)

        self._var_output = tk.StringVar(value="./bxf_output")
        ttk.Entry(frm_output, textvariable=self._var_output).pack(
            side="left", fill="x", expand=True, padx=(6, 4), pady=6
        )
        ttk.Button(frm_output, text="Browse…", command=self._browse_output).pack(
            side="left", padx=(0, 6), pady=6
        )

        # ---- Options ----
        frm_opts = ttk.LabelFrame(self, text="Options")
        frm_opts.pack(fill="x", **pad)

        # Output format
        ttk.Label(frm_opts, text="Output format:").grid(
            row=0, column=0, sticky="w", padx=8, pady=4
        )
        self._var_fmt = tk.StringVar(value="both")
        cb_fmt = ttk.Combobox(
            frm_opts,
            textvariable=self._var_fmt,
            values=["csv", "xlsx", "both"],
            state="readonly",
            width=8,
        )
        cb_fmt.grid(row=0, column=1, sticky="w", padx=4, pady=4)

        # Only key events
        self._var_only_key = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            frm_opts, text="Only key events", variable=self._var_only_key
        ).grid(row=0, column=2, sticky="w", padx=16, pady=4)

        # Flatten graphics under main
        self._var_flatten = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frm_opts, text="Flatten graphics under main", variable=self._var_flatten
        ).grid(row=0, column=3, sticky="w", padx=8, pady=4)

        # Include all key
        self._var_include_all = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frm_opts, text="Include all key events", variable=self._var_include_all
        ).grid(row=1, column=2, sticky="w", padx=16, pady=4)

        # Verbose
        self._var_verbose = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frm_opts, text="Verbose logging", variable=self._var_verbose
        ).grid(row=1, column=3, sticky="w", padx=8, pady=4)

        # ---- Run / Clear buttons ----
        frm_btns = ttk.Frame(self)
        frm_btns.pack(fill="x", padx=8, pady=6)

        self._btn_run = ttk.Button(frm_btns, text="▶  Run", command=self._on_run)
        self._btn_run.pack(side="left", padx=(0, 8))
        ttk.Button(frm_btns, text="Clear log", command=self._clear_log).pack(
            side="left"
        )

        # ---- Log output ----
        frm_log = ttk.LabelFrame(self, text="Log")
        frm_log.pack(fill="both", expand=True, **pad)

        self._log_text = scrolledtext.ScrolledText(
            frm_log, state="disabled", height=14, font=("Courier", 9)
        )
        self._log_text.pack(fill="both", expand=True, padx=4, pady=4)
        self._log_text.tag_config("ERROR", foreground="red")
        self._log_text.tag_config("WARNING", foreground="orange")

    # ------------------------------------------------------------------
    # File / folder dialogs
    # ------------------------------------------------------------------

    def _browse_input(self) -> None:
        choice = _ask_file_or_folder(self)
        if choice:
            self._var_input.set(choice)

    def _browse_output(self) -> None:
        path = filedialog.askdirectory(title="Select output directory", parent=self)
        if path:
            self._var_output.set(path)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _setup_logging(self) -> None:
        handler = _QueueHandler(self._log_queue)
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

    def _poll_log_queue(self) -> None:
        """Check the log queue periodically and append messages to the text widget."""
        try:
            while True:
                msg = self._log_queue.get_nowait()
                self._append_log(msg)
        except queue.Empty:
            pass
        self.after(100, self._poll_log_queue)

    def _append_log(self, msg: str) -> None:
        self._log_text.configure(state="normal")
        tag = ""
        upper = msg.upper()
        if upper.startswith("ERROR"):
            tag = "ERROR"
        elif upper.startswith("WARNING"):
            tag = "WARNING"
        self._log_text.insert("end", msg + "\n", tag)
        self._log_text.see("end")
        self._log_text.configure(state="disabled")

    def _clear_log(self) -> None:
        self._log_text.configure(state="normal")
        self._log_text.delete("1.0", "end")
        self._log_text.configure(state="disabled")

    # ------------------------------------------------------------------
    # Run action
    # ------------------------------------------------------------------

    def _on_run(self) -> None:
        if self._running:
            return

        input_str = self._var_input.get().strip()
        if not input_str:
            self._append_log("ERROR: Please select an input file or folder.")
            return

        out_str = self._var_output.get().strip() or "./bxf_output"

        params = {
            "input_path": input_str,
            "out_dir": out_str,
            "output_format": self._var_fmt.get(),
            "only_key_events": self._var_only_key.get(),
            "flatten_graphics": self._var_flatten.get(),
            "include_all_key": self._var_include_all.get(),
            "verbose": self._var_verbose.get(),
        }

        self._running = True
        self._btn_run.configure(state="disabled")
        thread = threading.Thread(target=self._run_worker, kwargs=params, daemon=True)
        thread.start()

    def _run_worker(
        self,
        input_path: str,
        out_dir: str,
        output_format: str,
        only_key_events: bool,
        flatten_graphics: bool,
        include_all_key: bool,
        verbose: bool,
    ) -> None:
        from pathlib import Path

        logger = logging.getLogger("bxf_parser.gui")

        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.INFO)

        try:
            in_path = Path(input_path)
            out_path = Path(out_dir)
            out_path.mkdir(parents=True, exist_ok=True)

            files = discover_files(in_path)
            if not files:
                logger.error("No files found at: %s", in_path)
                return

            logger.info("Found %d file(s) to process", len(files))
            all_rows = []

            for file_path in files:
                logger.info("Processing: %s", file_path)
                rows = parse_file(
                    file_path,
                    only_key_events=only_key_events,
                    flatten_graphics=flatten_graphics,
                    include_all_key=include_all_key,
                )
                if not rows:
                    logger.warning("No events extracted from %s", file_path.name)
                    continue

                all_rows.extend(rows)
                out_stem = out_path / file_path.name
                write_outputs(rows, out_stem, output_format)
                logger.info("Wrote %d events for %s", len(rows), file_path.name)

            if all_rows:
                combined_stem = out_path / "combined_all"
                write_outputs(all_rows, combined_stem, output_format)
                logger.info(
                    "Done — %d total events written to %s", len(all_rows), out_path
                )
            else:
                logger.warning("No events were extracted from any file.")
        except (SystemExit, KeyboardInterrupt, GeneratorExit):
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error("Unexpected error: %s", exc)
        finally:
            self.after(0, self._on_run_finished)

    def _on_run_finished(self) -> None:
        self._running = False
        self._btn_run.configure(state="normal")


# ---------------------------------------------------------------------------
# Helper: ask for file OR folder via a small dialog
# ---------------------------------------------------------------------------

def _ask_file_or_folder(parent: tk.Tk) -> str:
    """Show a small dialog letting the user pick a file or a folder."""
    win = tk.Toplevel(parent)
    win.title("Select input")
    win.resizable(False, False)
    win.grab_set()

    result = ""

    ttk.Label(win, text="Choose to open a single file or a whole folder:").pack(
        padx=16, pady=(14, 8)
    )

    def pick_file() -> None:
        nonlocal result
        path = filedialog.askopenfilename(
            title="Select schedule file",
            filetypes=[("XML / Schedule files", "*.xml *.sch"), ("All files", "*.*")],
            parent=win,
        )
        if path:
            result = path
        win.destroy()

    def pick_folder() -> None:
        nonlocal result
        path = filedialog.askdirectory(title="Select folder", parent=win)
        if path:
            result = path
        win.destroy()

    frm = ttk.Frame(win)
    frm.pack(padx=16, pady=(0, 14))
    ttk.Button(frm, text="Open file…", command=pick_file).pack(
        side="left", padx=8
    )
    ttk.Button(frm, text="Open folder…", command=pick_folder).pack(
        side="left", padx=8
    )
    ttk.Button(frm, text="Cancel", command=win.destroy).pack(side="left", padx=8)

    parent.wait_window(win)
    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    app = BxfParserApp()
    app.mainloop()


if __name__ == "__main__":
    main()
