"""
youtube_gui.py

Tkinter GUI YouTube Analyzer project.

All algorithms are assumed to read data directly from Neo4j

- Search tab: top-k queries (Spark + Neo4j via youtube_search.py)
- Network Aggregation tab: Neo4j-based aggregation (via network_aggregation.py)
- Influence Analysis tab: PageRank / influence (via influence_analysis.py)
- GO TO LINE 239 for algoirthm implementation
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from tkinter.scrolledtext import ScrolledText
import threading
import sys
import io
import os


class YouTubeAnalyzerGUI(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("YouTube Analyzer")
        self.geometry("900x600")

        self._create_widgets()

   
    def _create_widgets(self):
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # tabs
        self.search_frame = ttk.Frame(self.notebook)
        self.network_frame = ttk.Frame(self.notebook)
        self.influence_frame = ttk.Frame(self.notebook)

        self.notebook.add(self.search_frame, text="Search (Top-K)")
        self.notebook.add(self.network_frame, text="Network Aggregation")
        self.notebook.add(self.influence_frame, text="Influence Analysis")

        # tab UIs
        self._build_search_tab()
        self._build_network_tab()
        self._build_influence_tab()

        # output
        self.log_widget = ScrolledText(self, height=10, state="disabled")
        self.log_widget.pack(fill=tk.BOTH, expand=False, padx=10, pady=(0, 10))

    # search tab
    def _build_search_tab(self):
        frame = self.search_frame

        # Neo4j Connection (all algorithms use)
        conn_group = ttk.LabelFrame(frame, text="Neo4j Connection (used by all algorithms)")
        conn_group.pack(fill=tk.X, padx=10, pady=10)

        ttk.Label(conn_group, text="URI:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        ttk.Label(conn_group, text="User:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        ttk.Label(conn_group, text="Password:").grid(row=2, column=0, sticky="e", padx=5, pady=5)

        self.neo_uri_var = tk.StringVar(value="neo4j://127.0.0.1:7687")
        self.neo_user_var = tk.StringVar(value="neo4j")
        self.neo_pass_var = tk.StringVar(value="password")

        ttk.Entry(conn_group, textvariable=self.neo_uri_var, width=40).grid(
            row=0, column=1, sticky="w", padx=5, pady=5
        )
        ttk.Entry(conn_group, textvariable=self.neo_user_var, width=20).grid(
            row=1, column=1, sticky="w", padx=5, pady=5
        )
        ttk.Entry(conn_group, textvariable=self.neo_pass_var, width=20, show="*").grid(
            row=2, column=1, sticky="w", padx=5, pady=5
        )

        # -- Top-k Search -- #
        search_group = ttk.LabelFrame(frame, text="Search Algorithms (Spark + Neo4j)")
        search_group.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(search_group, text="k (Top-k):").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        self.k_var = tk.IntVar(value=10)
        ttk.Entry(search_group, textvariable=self.k_var, width=10).grid(
            row=0, column=1, sticky="w", padx=5, pady=5
        )

        ttk.Label(search_group, text="Metric:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        self.metric_var = tk.StringVar(value="views")

        metric_frame = ttk.Frame(search_group)
        metric_frame.grid(row=1, column=1, sticky="w", padx=5, pady=5)

        metrics = [
            ("Top-k Views", "views"),
            ("Top-k Rating Count", "rating"),
            ("Top-k Comment Count", "comments"),
            ("Top-k Categories", "categories"),
        ]
        for text, value in metrics:
            ttk.Radiobutton(metric_frame, text=text, value=value, variable=self.metric_var).pack(anchor="w")

        ttk.Button(search_group, text="Run Search", command=self._on_run_search).grid(
            row=2, column=0, columnspan=2, pady=10
        )

        ttk.Label(
            search_group,
            text="(Wire this button to your functions in youtube_search.py where marked TODO.)",
            foreground="gray",
        ).grid(row=3, column=0, columnspan=2, sticky="w", padx=5, pady=(0, 5))

    # -- Network Aggregation -- #
    def _build_network_tab(self):
        frame = self.network_frame

        agg_group = ttk.LabelFrame(frame, text="Network Aggregation (Spark + Neo4j)")
        agg_group.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(
            agg_group,
            text=(
                "This algorithm should read the YouTube graph directly from Neo4j\n"
                "(using the same Neo4j URI / user / password from the Search tab),\n"
                "then run your global + category-level metrics via Spark."
            ),
            justify="left",
        ).grid(row=0, column=0, columnspan=3, sticky="w", padx=5, pady=(5, 10))

        # output folder
        ttk.Label(agg_group, text="Output folder:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        self.out_dir_var = tk.StringVar(value="agg_out")
        ttk.Entry(agg_group, textvariable=self.out_dir_var, width=40).grid(
            row=1, column=1, sticky="w", padx=5, pady=5
        )

        ttk.Button(agg_group, text="Browse...", command=self._browse_out_dir).grid(
            row=1, column=2, sticky="w", padx=5, pady=5
        )

        ttk.Button(frame, text="Run Network Aggregation", command=self._on_run_network).pack(
            pady=15
        )

        ttk.Label(
            frame,
            text=(
                "(Expected: implement network_aggregation.run_from_neo4j(uri, user, password, out_dir)\n"
            ),
            foreground="gray",
            justify="left",
        ).pack(anchor="w", padx=15, pady=(0, 10))

    # -- Influence Analysis  -- #
    def _build_influence_tab(self):
        frame = self.influence_frame

        info_lbl = ttk.LabelFrame(frame, text="Influence Analysis (PageRank)")
        info_lbl.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(
            info_lbl,
            text=(
                "This tab is for influence analysis.\n"
                "influence_analysis.py may run everything at import.\n\n"
            ),
            justify="left",
        ).pack(anchor="w", padx=5, pady=5)

        ttk.Button(info_lbl, text="Run PageRank", command=self._on_run_influence).pack(
            pady=15
        )

        ttk.Label(
            info_lbl,
            text="(After you add run_pagerank(...), update _run_influence_work().)",
            foreground="gray",
        ).pack(anchor="w", padx=5, pady=(0, 5))

    # button callbacks 
    def _on_run_search(self):
        try:
            k = self.k_var.get()
            if k <= 0:
                raise ValueError
        except Exception:
            messagebox.showerror("Invalid k", "Please enter a positive integer for k.")
            return

        metric = self.metric_var.get()
        uri = self.neo_uri_var.get()
        user = self.neo_user_var.get()
        pwd = self.neo_pass_var.get()

        self.log(f"Running search: metric={metric}, k={k}, neo4j={uri} ...")
        t = threading.Thread(
            target=self._run_search_work, args=(metric, k, uri, user, pwd), daemon=True
        )
        t.start()

    def _on_run_network(self):
        uri = self.neo_uri_var.get()
        user = self.neo_user_var.get()
        pwd = self.neo_pass_var.get()
        out_dir = self.out_dir_var.get()

        if not uri or not user:
            messagebox.showerror(
                "Missing Neo4j info",
                "Please fill in the Neo4j URI and user in the Search tab.",
            )
            return

        if not out_dir:
            messagebox.showerror("Missing output folder", "Please choose an output folder.")
            return

        self.log(
            f"Running Neo4j-based network aggregation with:\n"
            f"  uri={uri}\n  user={user}\n  out_dir={out_dir}"
        )
        t = threading.Thread(
            target=self._run_network_work, args=(uri, user, pwd, out_dir), daemon=True
        )
        t.start()

    def _on_run_influence(self):
        uri = self.neo_uri_var.get()
        user = self.neo_user_var.get()
        pwd = self.neo_pass_var.get()

        self.log(f"Running influence analysis (PageRank) using Neo4j: {uri} ...")
        t = threading.Thread(target=self._run_influence_work, args=(uri, user, pwd), daemon=True)
        t.start()

    # Worker functions # PLUG IN ALGORITHMS HERE!!!!!!!
    def _run_search_work(self, metric, k, uri, user, pwd):
        try:
            import youtube_search 


        except ImportError as e:
            self.log(f"ERROR: Could not import youtube_search.py: {e}")

    def _run_network_work(self, uri, user, pwd, out_dir):
        """

            def run_from_neo4j(uri: str, user: str, password: str, out_dir: str) -> None:
                # 1. Create SparkSession
                # 2. Read the graph from Neo4j (videos + edges) into DataFrames
                # 3. Compute:
                #       - Global metrics (num videos, num edges, avg in/out-degree)
                #       - Category-level metrics
                # 4. Print results and/or write them to out_dir

            if __name__ == "__main__":
                # optionally keep a CLI entry point
                run_from_neo4j("neo4j://...", "neo4j", "password", "agg_out")

        This worker calls run_from_neo4j(...) and logs stdout to the GUI.
        """
        try:
            repo_dir = os.path.dirname(os.path.abspath(__file__))
            if repo_dir not in sys.path:
                sys.path.insert(0, repo_dir)

            import network_aggregation

            # Capture stdout from the Spark job
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf

            try:
                # <-- call your Neo4j-based aggregation here
                network_aggregation.run_from_neo4j(uri, user, pwd, out_dir)
            finally:
                sys.stdout = old_stdout

            output = buf.getvalue()
            if output.strip():
                self.log(output)
            self.log("Network aggregation finished.")

        except AttributeError:
            self.log(
                "ERROR: network_aggregation.run_from_neo4j(...) is not defined yet.\n"
                "Please implement it in network_aggregation.py."
            )
        except Exception as e:
            self.log(f"ERROR while running network aggregation: {e}")

    def _run_influence_work(self, uri, user, pwd):
        
        try:
            repo_dir = os.path.dirname(os.path.abspath(__file__)) # find algorithm
            if repo_dir not in sys.path:
                sys.path.insert(0, repo_dir)

            import influence_analysis

        except Exception as e:
            self.log(f"ERROR while running influence analysis: {e}")

    def _browse_out_dir(self):
        path = filedialog.askdirectory(title="Select output directory")
        if path:
            self.out_dir_var.set(path)

    # Logging helper
    def log(self, text: str):
        self.log_widget.configure(state="normal")
        self.log_widget.insert(tk.END, text + "\n")
        self.log_widget.see(tk.END)
        self.log_widget.configure(state="disabled")


if __name__ == "__main__":
    app = YouTubeAnalyzerGUI()
    app.mainloop()