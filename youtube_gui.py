"""
youtube_gui.py

Tkinter GUI YouTube Analyzer project.

All algorithms are assumed to read data directly from Neo4j

- Search tab: top-k queries (Spark + Neo4j via youtube_search.py)
- Network Aggregation tab: Neo4j-based aggregation (via network_aggregation.py)
- Influence Analysis tab: PageRank / influence (via influence_analysis.py)
- GO TO LINE 239 for algoirthm implementations
"""

from neo4j import GraphDatabase
from pathlib import Path
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

        #Start spark cluster
        from start_spark_cluster import start_cluster_win
        start_cluster_win()

        self._create_widgets()

   
    def _create_widgets(self):
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # tabs
        self.setup_frame = ttk.Frame(self.notebook)
        self.search_frame = ttk.Frame(self.notebook)
        self.network_frame = ttk.Frame(self.notebook)
        self.influence_frame = ttk.Frame(self.notebook)

        self.notebook.add(self.setup_frame, text = "Setup")
        self.notebook.add(self.search_frame, text="Search (Top-K)")
        self.notebook.add(self.network_frame, text="Network Aggregation")
        self.notebook.add(self.influence_frame, text="Influence Analysis")

        # tab UIs
        self._build_setup_tab()
        self._build_search_tab()
        self._build_network_tab()
        self._build_influence_tab()
        

        # output
        self.log_widget = ScrolledText(self, height=10, state="disabled")
        self.log_widget.pack(fill=tk.BOTH, expand=False, padx=10, pady=(0, 10))

    #Setup tab
    def _build_setup_tab(self):
        frame = self.setup_frame

        # Neo4j Connection (all algorithms use)
        conn_group = ttk.LabelFrame(frame, text="Neo4j Connection (used by all algorithms)")
        conn_group.pack(fill=tk.X, padx=10, pady=10)

        ttk.Label(conn_group, text="URI:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        ttk.Label(conn_group, text="User:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        ttk.Label(conn_group, text="Password:").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        ttk.Label(conn_group, text="Results database:").grid(row=3, column=0, sticky="e", padx=5, pady=5)

        self.neo_uri_var = tk.StringVar(value="neo4j://127.0.0.1:7687")
        self.neo_user_var = tk.StringVar(value="neo4j")
        self.neo_pass_var = tk.StringVar(value="password")
        self.neo_dbname_var = tk.StringVar(value="results")

        ttk.Entry(conn_group, textvariable=self.neo_uri_var, width=40).grid(
            row=0, column=1, sticky="w", padx=5, pady=5
        )
        ttk.Entry(conn_group, textvariable=self.neo_user_var, width=20).grid(
            row=1, column=1, sticky="w", padx=5, pady=5
        )
        ttk.Entry(conn_group, textvariable=self.neo_pass_var, width=20, show="*").grid(
            row=2, column=1, sticky="w", padx=5, pady=5
        )
        ttk.Entry(conn_group, textvariable=self.neo_dbname_var, width=20).grid(
            row=3, column=1, sticky="w", padx=5, pady=5
        )

        #Run tsv to csv
        import_group = ttk.LabelFrame(frame, text="Import (imports all tsvs in the input dir to neo4j)")
        import_group.pack(fill=tk.X, padx=10, pady=10)

        ttk.Label(
            import_group,
            text=r"Folder should have a path like C:\Users\{username}\.Neo4jDesktop2\Data\dbmss\{database_id}\import",
            foreground="gray",
        ).grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=5)

        ttk.Label(import_group, text="Neo4j Import Folder:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        self.neo_import_folder_var = tk.StringVar(value=r"C:\Users\monke\.Neo4jDesktop2\Data\dbmss\dbms-efaee50c-20eb-41cd-99c0-a556ef2ad97f\import")
        ttk.Entry(import_group, textvariable=self.neo_import_folder_var, width=60).grid(row=1, column=1, sticky="w", padx=5, pady=5)

        ttk.Button(import_group, text="Run Import", command=self._on_run_import).grid(
            row=2, column=0, columnspan=2, pady=15
        )
        

    # search tab
    def _build_search_tab(self):
        frame = self.search_frame

        
        search_group = ttk.LabelFrame(frame, text="Search Algorithms (Spark + Neo4j)")
        search_group.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(search_group, text="Category:").grid(row=0, column=0, sticky="w", padx=5, pady=(0,5))
        self.metric_var = tk.StringVar(value="views")

        metric_frame = ttk.Frame(search_group)
        metric_frame.grid(row=1, column=0, sticky="w", padx=5, pady=5)

        metrics = [
            ("Views", "views"),
            ("Rating Count", "ratingCount"),
            ("Comment Count", "commentCount"),
            ("Categories", "category"),
            ("Oldest", "age"),
            ("Length", "length")
        ]
        for text, value in metrics:
            ttk.Radiobutton(metric_frame, text=text, value=value, variable=self.metric_var).pack(anchor="w")
        # -- Top-k Search -- #
        ttk.Label(
            search_group,
            text="Top K",
            foreground="black",
        ).grid(row=2, column=0, columnspan=2, sticky="w", padx=5, pady=(0, 5))

        ttk.Label(search_group, text="k (Top-k):").grid(row=3, column=0, sticky="e", padx=5, pady=5)
        self.k_var = tk.IntVar(value=10)
        ttk.Entry(search_group, textvariable=self.k_var, width=10).grid(
            row=3, column=1, sticky="w", padx=5, pady=5
        )

        ttk.Button(search_group, text="Run Search", command=self._on_run_search).grid(
            row=5, column=0, columnspan=2, pady=10
        )

        # -- Find in Range -- #

        ttk.Label(
            search_group,
            text="Find in Range",
            foreground="black",
        ).grid(row=6, column=0, columnspan=2, sticky="w", padx=5, pady=(0, 5))

        ttk.Label(search_group, text="Start:").grid(row=7, column=0, sticky="e", padx=5, pady=5)
        self.start_var = tk.IntVar(value=10)
        ttk.Entry(search_group, textvariable=self.start_var, width=10).grid(
            row=7, column=1, sticky="w", padx=5, pady=5
        )

        ttk.Label(search_group, text="End:").grid(row=8, column=0, sticky="e", padx=5, pady=5)
        self.end_var = tk.IntVar(value=1000)
        ttk.Entry(search_group, textvariable=self.end_var, width=10).grid(
            row=8, column=1, sticky="w", padx=5, pady=5
        )

        ttk.Label(search_group, text="Size:").grid(row=9, column=0, sticky="e", padx=5, pady=5)
        self.table_size_var = tk.IntVar(value=10)
        ttk.Entry(search_group, textvariable=self.table_size_var, width=10).grid(
            row=9, column=1, sticky="w", padx=5, pady=5
        )

        ttk.Button(search_group, text="Run Search", command=self._on_run_search_range).grid(
            row=10, column=0, columnspan=2, pady=10
        )

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
                "The file influence_analysis.py should be run before attempting to retreive the results.\n"
                "Be sure to set the database parameters there.\n\n"
            ),
            justify="left",
        ).pack(anchor="w", padx=5, pady=5)

        ttk.Button(info_lbl, text="Get PageRank Results", command=self._on_run_influence).pack(
            pady=15
        )

        ttk.Label(
            info_lbl,
            text="(After you add run_pagerank(...), update _run_influence_work().)",
            foreground="gray",
        ).pack(anchor="w", padx=5, pady=(0, 5))

    # button callbacks 
    def _on_run_import(self):
        uri = self.neo_uri_var.get()
        user = self.neo_user_var.get()
        pwd = self.neo_pass_var.get()
        import_folder = self.neo_import_folder_var.get()

        if not uri or not user:
            messagebox.showerror(
                "Missing Neo4j info",
                "Please fill in the Neo4j URI and user in the Setup tab.",
            )
            return
        
        self.log(f"Importing tsvs to Neo4j using uri: {uri} ...\n")
        t = threading.Thread(target=self._run_setup_work, args=(uri, user, pwd, import_folder), daemon=True)
        t.start()

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
        dbname = self.neo_dbname_var.get()

        self.log(f"Running top k search: metric={metric}, k={k}, neo4j={uri} ...")
        t = threading.Thread(
            target=self._run_search_work, args=(metric, k, uri, user, pwd, dbname), daemon=True
        )
        t.start()

    def _on_run_search_range(self):
        try:
            start = self.start_var.get()
            end = self.end_var.get()
            size = self.table_size_var.get()
            if start > end or start == end or start < 0 or end < 0 or size < 0:
                raise ValueError
        except Exception:
            messagebox.showerror("Inavlid entry for start or end", "Please enter a valid integer amount for start and end (ex. 1-100)")
            return
        
        metric = self.metric_var.get()
        uri = self.neo_uri_var.get()
        user = self.neo_user_var.get()
        pwd = self.neo_pass_var.get()

        self.log(f"Running find in range search: metric={metric}, start={start}, end={end}, neo4j={uri} ...")
        t = threading.Thread(
            target=self._run_search_work_range, args=(metric, start, end, uri, user, pwd, size), daemon=True
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
        dbname = self.neo_dbname_var.get()

        self.log(f"Obtaining results from influence analysis (PageRank) using Neo4j: {uri} ...\n")
        t = threading.Thread(target=self._run_influence_work, args=(uri, user, pwd, dbname), daemon=True)
        t.start()

    # Worker functions # PLUG IN ALGORITHMS HERE!!!!!!!
    def _run_setup_work(self, uri, user, pwd, import_folder):
        try:
            import to_neo_csv

            #Run the tsv to csv 
            try:
                driver = GraphDatabase.driver(uri, auth=(user, pwd))

                driver.close()
                videos = f"{Path(import_folder).as_posix()}/videos.csv"
                related = f"{Path(import_folder).as_posix()}/related.csv"
                to_neo_csv.main(videos, related)


            except Exception as e:
                self.log(f"Error while running the import: {e}")


            #Import csvs in output dir to neo4j cluster
            try:
                driver = GraphDatabase.driver(uri, auth=(user, pwd))

                params = {
                    "file_0":  f"file:///videos.csv",
                    'file_1':  f"file:///related.csv",
                    'idsToSkip': []
                }

                query = open("neo4j_importer_cypher_script.cypher").read()

                statements = [s.strip() for s in query.split(";") if s.strip()]

                with driver.session() as session:
                    for st in statements:
                        session.run(st, params)

                self.log(f"The files in {Path(os.getcwd()).as_posix()}/input were uploaded to {uri}\n")
                driver.close()

            except Exception as e:
                self.log(f"ERROR: Could not import the files from {Path(import_folder).as_posix()} with exception: {e}")


        except ImportError as e:
            self.log(f"ERROR: Could not import to_neo_csv.py: {e}")

    def _run_search_work(self, metric, k, uri, user, pwd, dbname):
        try:
            import youtube_search

            # Capture stdout from the Spark job
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf
            try:
                # --SWITCH "videodata2" TO YOUR LOCAL DATABASE NAME-- #
                youtube_search.run_top_k(uri, user, pwd, "videodata2", k, metric)
            finally:
                sys.stdout = old_stdout

            output = buf.getvalue()
            if output.strip():
                self.log(output)

        except ImportError as e:
            self.log(f"ERROR: Could not import youtube_search.py: {e}")

    def _run_search_work_range(self, metric, start, end, uri, user, pwd, size):
        try:
            import youtube_search

            # Capture stdout from the Spark job
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf
            try:
                # --SWITCH "videodata2" TO YOUR LOCAL DATABASE NAME-- #
                youtube_search.run_find_in_range(uri, user, pwd, "videodata2", start, end, metric, size)
            finally:
                sys.stdout = old_stdout

            output = buf.getvalue()
            if output.strip():
                self.log(output)

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

    def _run_influence_work(self, uri, user, pwd, dbname):
        
        try:
            repo_dir = os.path.dirname(os.path.abspath("influence_analysis.py")) # find algorithm
            if repo_dir not in sys.path:
                sys.path.insert(0, repo_dir)

            import influence_analysis

            # Capture stdout from the Spark job
            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf

            try:
                influence_analysis.run_pagerank(uri, user, pwd, dbname)
            finally:
                sys.stdout = old_stdout

            output = buf.getvalue()
            if output.strip():
                self.log(output)
            self.log("Page Rank results have been fetched.")
            

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