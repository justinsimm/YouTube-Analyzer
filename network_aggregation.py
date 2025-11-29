from typing import Optional

from neo4j import GraphDatabase
from pyspark.sql import SparkSession, functions as F


# -- Spark helpers -- #
def _build_spark(app_name: str = "YouTubeNetworkAggregation") -> SparkSession:
    """
    Create (or reuse) a SparkSession. You can add extra .config(...) calls
    here if you need HADOOP_HOME or other options.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


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

    # -- Normalize Input -- #
 
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

    # -- Degree computations -- #
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

    # -- Global metrics -- #
    num_edges = edges_df.count()

    global_metrics_df = node_degrees.agg(
        F.count("*").alias("num_videos"),
        F.lit(num_edges).alias("num_edges"),
        F.avg("in_degree").alias("avg_in_degree"),
        F.avg("out_degree").alias("avg_out_degree"),
    )

    # -- Category-level metric -- #
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

    # -- Print to stdout -- #
    print("=== Global Network Metrics ===")
    global_metrics_df.show(truncate=False)

    print("\n=== Category-Level Network Aggregation ===")
    category_agg.show(truncate=False)

    print("\n=== Per-Video Node Degree Table (sample) ===")
    node_degrees.select(
        "videoId", "category", "views", "in_degree", "out_degree"
    ).show(20, truncate=False)

    # -- CSV output -- #
    if out_dir:
        (
            global_metrics_df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(f"{out_dir}/global_metrics")
        )
        (
            category_agg.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(f"{out_dir}/category_agg")
        )
        (
            node_degrees.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(f"{out_dir}/node_degrees")
        )

        print(
            f"\nWrote CSV outputs under:\n"
            f"  {out_dir}/global_metrics\n"
            f"  {out_dir}/category_agg\n"
            f"  {out_dir}/node_degrees\n"
        )

#  -- Neo4j input -- #
def run_from_neo4j(uri: str, user: str, password: str, out_dir: str) -> None:
    """
    This is what the Tkinter GUI calls.

    It:
      1. Connects to Neo4j using the provided uri/user/password.
      2. Pulls the YouTube graph:
            (v:Videos)
            (s:Videos)-[:Related_Videos]->(t:Videos)
      3. Builds Spark DataFrames from the query results.
      4. Runs exactly the same aggregation logic as the CSV version.
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        
        video_records = session.run(
            """
            MATCH (v:Videos)
            RETURN
                v.videoId AS videoId,
                v.category AS category,
                v.views AS views
            """
        ).data()

        edge_records = session.run(
            """
            MATCH (s:Videos)-[:Related_Videos]->(t:Videos)
            RETURN
                s.videoId AS srcVideoId,
                t.videoId AS dstVideoId
            """
        ).data()

    driver.close()

    if not video_records:
        print("No Videos nodes found in Neo4j. Aborting network aggregation.")
        return

    if not edge_records:
        print("No Related_Videos relationships found in Neo4j. Aborting network aggregation.")
        return

    
    spark = _build_spark("YouTubeNetworkAggregationNeo4j")

    videos_df = spark.createDataFrame(video_records)
    edges_df = spark.createDataFrame(edge_records)

    _compute_and_output(spark, videos_df, edges_df, out_dir)
    spark.stop()
