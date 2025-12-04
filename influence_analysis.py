import os
import numpy
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Replace with the actual connection URI and credentials
"""
url = "neo4j://127.0.0.1:7687"
username = "neo4j"
password = "T27saj39"
dbname = "neo4j"
results_db = "results"
"""
os.environ["HADOOP_HOME"] = r"C:\hadoop"

def run_pagerank(uri, username, password, dbname):
    #Start a Spark session using the required neo4j connector
    spark =  (
        SparkSession.builder
        .master("spark://68.234.244.60:7077")
        .config("neo4j.url", "neo4j://127.0.0.1:7687")
        .config("neo4j.authentication.basic.username", "neo4j")
        .config("neo4j.authentication.basic.password", "password")
        .config(
            "spark.jars.packages",
            "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3,io.graphframes:graphframes-spark4_2.13:0.10.0"
        )
        .config(
            "spark.jars.repositories",
            "https://repo1.maven.org/maven2/,https://s01.oss.sonatype.org/content/repositories/releases/"
        )
        .getOrCreate()
    )


    from graphframes import GraphFrame



    """
    Run the page Rank algorithm using pyspark's method in graphframes
    requires: an edge set and node set to construct the graphframe
    """

    start_time = time.time()

    
    #Extract the edges into a dataframe
    df = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", uri)
        .option("authentication.basic.username", username)
        .option("authentication.basic.password", password)
        .option("database", dbname)
        .option("relationship", "Related_Videos")
        .option("relationship.source.labels", "Videos")
        .option("relationship.target.labels", "Videos")
        .option("query", """
            MATCH (source:Videos)-[r:Related_Videos]->(target:Videos)
            RETURN id(source) as src, 
                id(target) as dst,
                r.* as relationship_properties
        """)
        .option("partitions", "10")  
        .option("batch.size", "1000") 
        .option("transaction.timeout", "600s") 
        .load()
    ) 



    #reduce the df to just the source and target and prep for graphframe call
    #relationships = df.select(col("`source.VideoId`").alias("src"), col("`target.VideoId`").alias("dst"))
    relationships = df.select(col("src"), col("dst"))

    #Extract the nodes into a dataframe
    vertices = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", uri)
        .option("authentication.basic.username", username)
        .option("authentication.basic.password", password)
        .option("database", dbname)
        .option("labels", "Videos")
        .option("partitions", "10")  
        .option("batch.size", "1000") 
        .option("transaction.timeout", "600s") 
        .load()
    ) 

    #change VideeoID to id for the graphframes call and select the original columns
    vertices = vertices.withColumnRenamed("VideoID", "id")
    vertices = vertices.select(col('Views'), col('id'), col('Uploader'), col('RatingCount'), col('Rating'), col('Length'), col('CommentCount'), col('Category'), col('Age'))


    # Create GraphFrame and run PageRank
    g = GraphFrame(vertices, relationships)
    pagerank_result = g.pageRank(resetProbability=0.15, tol=0.01)
    pagerank_result.vertices.show()

    #Output execution time for page rank
    end_time = time.time()
    print(f"PageRank computation time: {end_time - start_time} seconds")

    #Write the page rank results to a different database for later fetching
    (
        pagerank_result.vertices.write.format("org.neo4j.spark.DataSource")
        .option("url", uri)
        .option("authentication.basic.username", username)
        .option("authentication.basic.password", password)
        .option("database", "results")
        .mode("Overwrite")
        .option("node.keys", "id")
        .option("labels", ":Video")
        .option("partitions", "5")  
        .option("batch.size", "10000")
        .option("transaction.timeout", "600s")
        .save()
    )


