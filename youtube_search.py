from neo4j import GraphDatabase
from pyspark.sql import SparkSession
import time
import os
import sys

python_executable=sys.executable
os.environ["PYSPARK_PYTHON"] = python_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
#.config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3") \
os.environ["HADOOP_HOME"] = r"C:\hadoop"
spark = SparkSession.builder \
    .master("spark://192.168.1.41:7077") \
    .config("neo4j.url", "neo4j://127.0.0.1:7687") \
    .config("neo4j.authentication.basic.username", "neo4j") \
    .config("neo4j.authentication.basic.password", "password") \
    .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.10_for_spark_3") \
    .config("spark.master", "local[1]") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()
#.config( "spark.jars.repositories", "https://repo1.maven.org/maven2/,https://s01.oss.sonatype.org/content/repositories/releases/") \
    #, io.graphframes:graphframes-spark4_2.13:0.10.0
#from graphframes import GraphFrame

def top_k(k, dataframe, field):
    start = time.time()
    newDF = dataframe
    if field == "category":
        newDF.groupBy(field).count().orderBy("count", ascending=False).show(k)
    else:
        newDF.sort(field, ascending=False).show(k)
    end = time.time()
    length = end - start
    print(f"The process took {round(length, 2)} seconds")

def find_in_range(start, end, field, dataframe, n):
    newDF = dataframe
    newDF.filter(f"{field} > {start} AND {field} < {end}").orderBy(field, ascending=False).show(n)

def run_top_k(uri, user, password, database, k, searchField):
    vertices = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", uri)
        .option("authentication.basic.username", user)
        .option("authentication.basic.password", password)
        .option("database", database)
        .option("labels", "Videos")
        .load()
    )
    theDF = vertices.dropna(how="any")
    top_k(k, theDF, searchField)

def run_find_in_range(uri, user, password, database, start, end, searchField, n):
    vertices = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", uri)
        .option("authentication.basic.username", user)
        .option("authentication.basic.password", password)
        .option("database", database)
        .option("labels", "Videos")
        .load()
    )
    theDF = vertices.dropna(how="any")
    find_in_range(start, end, searchField, theDF, n)



def main():
    #nodes = fetchNodes("neo4j://127.0.0.1:7687", "neo4j", "password")
    #print(nodes)
    #edges = fetchEdges("neo4j://127.0.0.1:7687", "neo4j", "password")
    #verticesDataFrame, edgesDataFrame = createDataFrame(nodes, edges)

    vertices = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", "neo4j://127.0.0.1:7687")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "password")
        .option("database", "videodata2")
        .option("labels", "Videos")
        .load()
    )
    newDF = vertices.dropna(how="any")
    go = True
    while(go):
        print("Enter Selection")
        print("1. Top k Rated Videos")
        print("2. Top k Viewed Videos")
        print("3. Top k Commented Videos")
        print("4. Top k Most Uploaded Categories")
        print("5. Find Videos Within a Range in Category")
        print("6. Quit")
        choice = int(input("Enter Input: "))
        if choice < 5:
            k = int(input("Enter k amount: "))
        if choice == 1:
            top_k(k, newDF, "ratingCount")
        elif choice == 2:
            start_time = time.time()
            top_k(k, newDF, "views")
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(elapsed_time)
        elif choice == 3:
            top_k(k, newDF, "commentCount")
        elif choice == 4:
            top_k(k, newDF, "category")
        elif choice == 5:
            field = input("Enter category to search: ")
            start = int(input("Enter start of range: "))
            end = int(input("Enter end of range: "))
            find_in_range(start, end, field, newDF)
        elif choice == 6:
            go = False
    #newDF.sort("views", ascending=False).show(10)
    #graph = createGraphFrame(nodes, edges)
    #graph.vertices.orderBy("views", ascending=False).show(10)

if __name__ == "__main__":
    main()