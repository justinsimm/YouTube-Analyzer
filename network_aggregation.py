from __future__ import annotations

from typing import Optional

import argparse
import os
import sys

from neo4j import GraphDatabase
from pyspark.sql import SparkSession, functions as F


# ---------------------------
# Spark helpers
# ---------------------------
def _build_spark(app_name: str = "YouTubeNetworkAggregation") -> SparkSession:
    """
    Create (or reuse) a SparkSession.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------
# Core aggregation logic
# ---------------------------
def _compute_and_output(
    spark: SparkSession,
    videos_df,
    edges_df,
    out_dir: Optional[str] = None,
) -> None:
    """
    Core logic: given a videos DataFrame and an edges DataFrame, compute:

        1. Global metrics
        2. Category-level aggregation table
        3. Per-video node-degree table

    Optionally writes CSVs under out_dir.
    """

    print("Normalizing input DataFrames...")

    required_video_cols = {"videoId", "category", "views"}
    required_edge_cols = {"srcVideoId", "dstVideoId"}

    missing_v = required_video_cols - set(videos_df.columns)
    missing_e = required_edge_cols - set(edges_df.columns)

    if missing_v:
        raise ValueError(f"videos_df is missing columns: {missing_v}")
    if missing_e:
        raise ValueError(f"edges_df is missing columns: {missing_e}")

    videos_df = videos_df.withColumn("views", F.col("views").cast("long"))
    videos_df = videos_df.fillna({"category": "Unknown"})

    print("Caching videos and edges DataFrames...")
    videos_df = videos_df.cache()
    edges_df = edges_df.cache()

    num_videos = videos_df.count()
    num_edges = edges_df.count()
    print(f"Input sizes: {num_videos} videos, {num_edges} edges")

    # ---------------------------
    # Degree computations
    # ---------------------------
    print("Computing in/out-degrees per video...")

    out_deg = (
        edges_df
        .groupBy("srcVideoId")
        .agg(F.count("*").alias("out_degree"))
    )

    in_deg = (
        edges_df
        .groupBy("dstVideoId")
        .agg(F.count("*").alias("in_degree"))
    )

    v = videos_df.alias("v")

    node_degrees = (
        v
        .join(out_deg, v.videoId == out_deg.srcVideoId, how="left")
        .join(in_deg, v.videoId == in_deg.dstVideoId, how="left")
        .drop("srcVideoId", "dstVideoId")
        .fillna({"in_degree": 0, "out_degree": 0})
    )

    # ---------------------------
    # Global metrics
    # ---------------------------
    print("Computing global metrics...")

    global_metrics_df = node_degrees.agg(
        F.count("*").alias("num_videos"),
        F.lit(num_edges).alias("num_edges"),
        F.avg("in_degree").alias("avg_in_degree"),
        F.avg("out_degree").alias("avg_out_degree"),
    )

    # ---------------------------
    # Category-level metrics
    # ---------------------------
    print("Computing category-level aggregation...")

    category_agg = (
        node_degrees
        .groupBy("category")
        .agg(
            F.count("*").alias("num_videos"),
            F.sum("views").alias("total_views"),
            F.avg("views").alias("avg_views"),
            F.avg("out_degree").alias("avg_out_degree"),
            F.max("out_degree").alias("max_out_degree"),
            F.avg("in_degree").alias("avg_in_degree"),
            F.max("in_degree").alias("max_in_degree"),
        )
        .orderBy(F.desc("total_views"))
    )

    # ---------------------------
    # Show results 
    # ---------------------------
    print("\n=== Global Network Metrics ===")
    global_metrics_df.show(truncate=False)

    print("\n=== Category-Level Network Aggregation ===")
    category_agg.show(truncate=False)

    print("\n=== Per-Video Node Degree Table (sample) ===")
    node_degrees.select(
        "videoId", "category", "views", "in_degree", "out_degree"
    ).show(20, truncate=False)

    # ---------------------------
    # CSV output
    # ---------------------------
    if out_dir:
        print(f"\nWriting CSV outputs to: {out_dir}")
        (
            global_metrics_df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(os.path.join(out_dir, "global_metrics"))
        )
        (
            category_agg.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(os.path.join(out_dir, "category_agg"))
        )
        (
            node_degrees.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(os.path.join(out_dir, "node_degrees"))
        )

        print(
            f"\nWrote CSV outputs under:\n"
            f"  {out_dir}/global_metrics\n"
            f"  {out_dir}/category_agg\n"
            f"  {out_dir}/node_degrees\n"
        )


# ---------------------------
# Neo4j entrypoint 
# ---------------------------
def run_from_neo4j(uri: str, user: str, password: str, out_dir: str) -> None:
    """
    This is what the Tkinter GUI calls.

    It:
      1. Connects to Neo4j using the provided uri/user/password.
      2. Pulls the YouTube graph:
            (v:Videos)
            (s:Videos)-[:Related_Videos]->(t:Videos)
      3. Builds Spark DataFrames from the query results.
      4. Runs the same aggregation logic as the CSV version.
    """

    print(f"Connecting to Neo4j at {uri} as {user}...")
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        print("Pulling video nodes from Neo4j...")
        video_records = session.run(
            """
            MATCH (v:Videos)
            RETURN
                v.videoId AS videoId,
                v.category AS category,
                v.views AS views
            """
        ).data()
        print(f"Fetched {len(video_records)} videos.")

        print("Pulling Related_Videos relationships from Neo4j...")
        edge_records = session.run(
            """
            MATCH (s:Videos)-[:Related_Videos]->(t:Videos)
            RETURN
                s.videoId AS srcVideoId,
                t.videoId AS dstVideoId
            """
        ).data()
        print(f"Fetched {len(edge_records)} edges.")

    driver.close()

    if not video_records:
        print("No Videos nodes found in Neo4j. Aborting network aggregation.")
        return

    if not edge_records:
        print("No Related_Videos relationships found in Neo4j. Aborting network aggregation.")
        return

    print("Starting Spark job for Neo4j-based network aggregation...")
    spark = _build_spark("YouTubeNetworkAggregationNeo4j")

    videos_df = spark.createDataFrame(video_records)
    edges_df = spark.createDataFrame(edge_records)

    _compute_and_output(spark, videos_df, edges_df, out_dir)
    print("Neo4j-based network aggregation completed.")
    spark.stop()


# ---------------------------
# CSV entrypoint 
# ---------------------------
def run_from_csv(videos_path: str, related_path: str, out_dir: str) -> None:
    """
    Run the aggregation starting from CSV files (videos.csv, related.csv).

    This is useful for your prof/TA's automated tests and for manual
    spark-submit runs.
    """
    print(f"Loading CSVs:\n  videos={videos_path}\n  related={related_path}")
    spark = _build_spark("YouTubeNetworkAggregationCSV")

    videos_df = (
        spark.read
        .option("header", "true")
        .csv(videos_path)
    )
    edges_df = (
        spark.read
        .option("header", "true")
        .csv(related_path)
    )

    _compute_and_output(spark, videos_df, edges_df, out_dir)
    print("CSV-based network aggregation completed.")
    spark.stop()


# ---------------------------
# CLI
# ---------------------------
def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="YouTube Network Aggregation (Spark + Neo4j or CSV)."
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--from-csv",
        action="store_true",
        help="Read videos/related data from CSV files.",
    )
    mode.add_argument(
        "--from-neo4j",
        action="store_true",
        help="Read videos/edges directly from Neo4j.",
    )

    # CSV inputs
    parser.add_argument("--videos", help="Path to videos.csv (when using --from-csv).")
    parser.add_argument("--related", help="Path to related.csv (when using --from-csv).")

    # Neo4j inputs
    parser.add_argument("--neo4j-uri", help="Neo4j URI, e.g. bolt://localhost:7687")
    parser.add_argument("--neo4j-user", help="Neo4j username")
    parser.add_argument("--neo4j-password", help="Neo4j password")

    # Output dir
    parser.add_argument(
        "--out",
        required=True,
        help="Output directory for CSV results.",
    )

    return parser.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)

    if args.from_csv:
        if not args.videos or not args.related:
            raise SystemExit("ERROR: --videos and --related are required with --from-csv.")
        run_from_csv(args.videos, args.related, args.out)

    elif args.from_neo4j:
        if not (args.neo4j_uri and args.neo4j_user and args.neo4j_password):
            raise SystemExit("ERROR: --neo4j-uri, --neo4j-user, and --neo4j-password are required with --from-neo4j.")
        run_from_neo4j(args.neo4j_uri, args.neo4j_user, args.neo4j_password, args.out)


if __name__ == "__main__":
    main()