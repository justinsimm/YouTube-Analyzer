import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import expr


def main(videos_path: str, related_path: str, out_dir: str) -> None:
    spark = (
        SparkSession.builder
        .appName("YouTubeNetworkAggregation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------------------------------------
    # 1. Load CSVs
    # ---------------------------------------------------------
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

    # ---------------------------------------------------------
    # 2. Clean 
    # ---------------------------------------------------------
    videos = (
        videos_raw.select(
            "videoId",
            "uploader",
            "categoryId",
            "category",
            # safe numeric casts – bad values → NULL, no crash
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

    # ---------------------------------------------------------
    # 3. Compute in-degree / out-degree
    # ---------------------------------------------------------
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

    # Debug prints (safe)
    print("=== VIDEOS SCHEMA ===")
    videos.printSchema()
    print("=== EDGES SCHEMA ===")
    edges.printSchema()
    print("=== NODE_DEG SAMPLE ===")
    node_deg.show(5, truncate=False)

    # ---------------------------------------------------------
    # 4. Global metrics
    # ---------------------------------------------------------
    num_videos = node_deg.count()
    num_edges = edges.count()

    global_metrics = spark.createDataFrame(
        [
            (
                num_videos,
                num_edges,
                # average degree from node_deg (safer than recomputing from edges)
                node_deg.agg(F.avg("outDegree")).first()[0],
                node_deg.agg(F.avg("inDegree")).first()[0],
            )
        ],
        schema="numVideos LONG, numEdges LONG, avgOutDegree DOUBLE, avgInDegree DOUBLE",
    )

    print("=== GLOBAL METRICS ===")
    global_metrics.show(truncate=False)

    # ---------------------------------------------------------
    # 5. Category summary
    # ---------------------------------------------------------
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

    print("=== CATEGORY SUMMARY (FIRST 20 ROWS) ===")
    category_summary.show(20, truncate=False)

    # ---------------------------------------------------------
    # 6. Write outputs
    # ---------------------------------------------------------
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

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--videos", required=True, help="Path to videos.csv")
    parser.add_argument("--related", required=True, help="Path to related.csv")
    parser.add_argument("--out", required=True, help="Output directory")
    args = parser.parse_args()

    main(args.videos, args.related, args.out)