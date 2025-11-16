
import os
import numpy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Replace with the actual connection URI and credentials
url = "neo4j://127.0.0.1:7687"
username = "neo4j"
password = "Fhglala34"
dbname = "neo4j"

#Start a Spark session using the required neo4j connector
spark =  (
    SparkSession.builder
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

#Extract the edges into a dataframe
df = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("url", url)
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", dbname)
    .option("relationship", "Related_ID")
    .option("relationship.source.labels", "Video")
    .option("relationship.target.labels", "Video")
    .load()
) 

#reduce the df to just the source and target and prep for graphframe call
relationships = df.select(col("`source.VideoID`").alias("src"), col("`target.VideoID`").alias("dst"))


#Extract the nodes into a dataframe
vertices = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("url", url)
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", dbname)
    .option("labels", "Video")
    .load()
) 

#change VideeoID to id for the graphframes call and select the original columns
vertices = vertices.withColumnRenamed("VideoID", "id")
vertices = vertices.select(col('Views'), col('id'), col('Uploader'), col('RatingCount'), col('Rating'), col('Length'), col('CommentCount'), col('Category'), col('Age'))


# Create GraphFrame and run PageRank
g = GraphFrame(vertices, relationships)
pagerank_result = g.pageRank(resetProbability=0.15, tol=0.01)
pagerank_result.vertices.show()
