import argparse
import os
import sys
import traceback

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import expr


# ---------------------------
# Logging helper 
# ---------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------
# Core aggregation logic
# ---------------------------

def _compute_aggregation(spark, videos, edges, out_dir: str) -> None:
    """
    Core aggregation:
      - computes in/out degrees
      - global metrics
      - category-level metrics
      - writes CSVs into out_dir
    """

    log("[network_aggregation] Computing in/out degrees...")

    out_deg = (
        edges.groupBy("srcVideoId")
        .agg(F.count("*").alias("outDegree"))
        .withColumnRenamed("srcVideoId", "videoId")
    )

    in_deg = (
        edges.groupBy("dstVideoId")
        .agg(F.count("*").alias("inDegree"))
        .withColumnRenamed("dstVideoId", "videoId")
    )

    node_deg = (
        videos
        .join(out_deg, on="videoId", how="left")
        .join(in_deg, on="videoId", how="left")
        .fillna({"outDegree": 0, "inDegree": 0})
    )

    log("=== VIDEOS SCHEMA ===")
    videos.printSchema()
    log("=== EDGES SCHEMA ===")
    edges.printSchema()
    log("=== NODE_DEG SAMPLE ===")
    node_deg.show(5, truncate=False)

    log("[network_aggregation] Computing global metrics...")

    num_videos = node_deg.count()
    num_edges = edges.count()

    global_metrics = spark.createDataFrame(
        [
            (
                num_videos,
                num_edges,
                # average degree from node_deg (safer than recomputing)
                node_deg.agg(F.avg("outDegree")).first()[0],
                node_deg.agg(F.avg("inDegree")).first()[0],
            )
        ],
        schema="numVideos LONG, numEdges LONG, avgOutDegree DOUBLE, avgInDegree DOUBLE",
    )

    log("=== GLOBAL METRICS ===")
    global_metrics.show(truncate=False)

    log("[network_aggregation] Computing category-level metrics...")

    category_summary = (
        node_deg
        .groupBy("category")
        .agg(
            F.count("*").alias("numVideos"),
            F.sum("views").alias("totalViews"),
            F.avg("views").alias("avgViews"),
            F.avg("rating").alias("avgRating"),
            F.avg("outDegree").alias("avgOutDegree"),
            F.avg("inDegree").alias("avgInDegree"),
        )
        .orderBy(F.desc("totalViews"))
    )

    log("=== CATEGORY SUMMARY (FIRST 20 ROWS) ===")
    category_summary.show(20, truncate=False)

    log(f"[network_aggregation] Writing outputs to {out_dir} ...")

    (
        global_metrics
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{out_dir}/global_metrics")
    )

    (
        category_summary
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{out_dir}/category_summary")
    )

    (
        node_deg
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{out_dir}/node_degrees")
    )

    log("[network_aggregation] Finished writing CSV outputs.")


# ---------------------------
# CSV entry
# ---------------------------

def main(videos_path: str, related_path: str, out_dir: str) -> None:
    """
    CLI entry point: CSV-based aggregation, used by spark-submit.
    """
    log("[network_aggregation] Starting CSV-based aggregation...")
    log(f"[network_aggregation] videos={videos_path}")
    log(f"[network_aggregation] related={related_path}")
    log(f"[network_aggregation] out_dir={out_dir}")

    spark = (
        SparkSession.builder
        .appName("YouTubeNetworkAggregationCSV")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log("[network_aggregation] Loading CSVs...")

    videos_raw = (
        spark.read
        .option("header", "true")
        .csv(videos_path)
    )

    edges_raw = (
        spark.read
        .option("header", "true")
        .csv(related_path)
    )

    # 2. Clean 
    log("[network_aggregation] Cleaning / casting CSV columns...")

    videos = (
        videos_raw.select(
            "videoId",
            "uploader",
            "categoryId",
            "category",
            expr("try_cast(metric1 as double)").alias("metric1"),
            expr("try_cast(views as double)").alias("views"),
            expr("try_cast(rating as double)").alias("rating"),
            expr("try_cast(ratingCount as double)").alias("ratingCount"),
            expr("try_cast(commentCount as double)").alias("commentCount"),
        )
    )

    edges = (
        edges_raw
        .select(
            F.col("srcVideoId"),
            F.col("dstVideoId"),
        )
        .where(
            F.col("srcVideoId").isNotNull()
            & F.col("dstVideoId").isNotNull()
        )
    )

    _compute_aggregation(spark, videos, edges, out_dir)

    spark.stop()
    log("[network_aggregation] CSV-based aggregation complete.")


# ---------------------------
# Neo4j entry
# ---------------------------

DEFAULT_NEO4J_DB = "neo4j"  # db name


def run_from_neo4j(uri: str, user: str, password: str, out_dir: str, dbname: str = DEFAULT_NEO4J_DB) -> None:
    """
    Entry point for the GUI: reads the full graph directly from Neo4j
    and runs the same aggregation as the CSV-based version.

    Called from youtube_gui.py as:
        network_aggregation.run_from_neo4j(uri, user, pwd, out_dir)
    """

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    log("[network_aggregation] Starting Neo4j-based aggregation...")
    log(f"[network_aggregation] Neo4j URI={uri} DB={dbname}")
    log(f"[network_aggregation] out_dir={out_dir}")

    spark = None
    try:
        # Build Spark session with Neo4j connector
        spark = (
            SparkSession.builder
            .appName("YouTubeNetworkAggregationNeo4j")
            .master("spark://68.234.244.60:7077")
            .config(
                "spark.jars.packages",
                "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3",
            )
            .config(
                "spark.jars.repositories",
                "https://repo1.maven.org/maven2/,https://s01.oss.sonatype.org/content/repositories/releases/"
            )
            .getOrCreate()
        )

        # ---- Load Videos from Neo4j ----
        log("[network_aggregation] Loading :Videos nodes from Neo4j via Spark connector...")

        videos_raw = (
            spark.read.format("org.neo4j.spark.DataSource")
            .option("url", uri)
            .option("authentication.basic.username", user)
            .option("authentication.basic.password", password)
            .option("database", dbname)
            .option("labels", "Videos")
            .option("partitions", "10")  
            .option("batch.size", "1000") 
            .option("transaction.timeout", "600s") 
            .load()
        )

        videos_count = videos_raw.count()
        log(f"[network_aggregation] Loaded {videos_count} videos")

        # ---- Load Related_Videos from Neo4j ----
        log("[network_aggregation] Loading :Related_Videos relationships from Neo4j via Spark connector...")

        edges_raw = (
            spark.read.format("org.neo4j.spark.DataSource")
            .option("url", uri)
            .option("authentication.basic.username", user)
            .option("authentication.basic.password", password)
            .option("database", dbname)
            .option("relationship", "Related_Videos")
            .option("relationship.source.labels", "Videos")
            .option("relationship.target.labels", "Videos")
            .option("partitions", "10")  
            .option("batch.size", "1000") 
            .option("transaction.timeout", "600s") 
            .load()
        )

        edges_count = edges_raw.count()
        log(f"[network_aggregation] Loaded {edges_count} edges")

        if videos_count == 0:
            log("[network_aggregation] No videos found in Neo4j. Aborting.")
            return
        if edges_count == 0:
            log("[network_aggregation] No edges found in Neo4j. Aborting.")
            return

       
        log("[network_aggregation] Shaping columns for aggregation...")

        videos = (
            videos_raw.select(
                "videoId",
                "uploader",
                "category",
                expr("try_cast(views as double)").alias("views"),
                expr("try_cast(rating as double)").alias("rating"),
                expr("try_cast(ratingCount as double)").alias("ratingCount"),
                expr("try_cast(commentCount as double)").alias("commentCount"),
            )
        )

        edges = (
            edges_raw
            .select(
                F.col("`source.VideoId`").alias("srcVideoId"),
                F.col("`target.videoId`").alias("dstVideoId"),
            )
            .where(
                F.col("`source.VideoId`").isNotNull()
                & F.col("`target.videoId`").isNotNull()
            )
        )

    
        log("[network_aggregation] Running aggregation on Neo4j data...")

        _compute_aggregation(spark, videos, edges, out_dir)

        log("[network_aggregation] Neo4j-based network aggregation completed.")

    except Exception:
        log("[network_aggregation] ERROR during Neo4j aggregation:")
        traceback.print_exc()
    finally:
        if spark is not None:
            log("[network_aggregation] Stopping SparkSession...")
            spark.stop()
            log("[network_aggregation] SparkSession stopped.")


# ---------------------------
# CLI 
# ---------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--videos", required=True, help="Path to videos.csv")
    parser.add_argument("--related", required=True, help="Path to related.csv")
    parser.add_argument("--out", required=True, help="Output directory")
    args = parser.parse_args()

    main(args.videos, args.related, args.out)