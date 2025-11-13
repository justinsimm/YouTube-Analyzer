
import os
import numpy
from pyspark.sql import SparkSession


# Replace with the actual connection URI and credentials
url = "neo4j://127.0.0.1:7687"
username = "neo4j"
password = ""
dbname = "Test"

#Start a Spark session using the required neo4j connector
spark =  (
    SparkSession.builder
    .config(
        "spark.jars.packages",
        "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3")
    .getOrCreate()
)

#Extract the database into a dataframe to operate on
df = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("url", url)
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", dbname)
    .option("labels", "Person")
    .load()
    .show()
) 

#Run page Rank algorithm using spark's built in page rank alg

