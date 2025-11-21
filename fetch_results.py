import neo4j
from neo4j import GraphDatabase
import pandas as pd


def run_pagerank(uri, username, password, dbname):

    driver = neo4j.GraphDatabase.driver(uri, auth=(username, password))

    data = driver.execute_query(
        "MATCH (n:Video) RETURN properties(n) AS video",
        database_ = dbname,
        result_transformer_ = neo4j.Result.to_df
    )

    #Flatten into table format
    data = data['video'].apply(pd.Series)

    #Get the top 10 by pagerank along with desired columns
    pd.set_option('display.max_columns', None)
    data = data.sort_values(by='pagerank', ascending=False)
    print(data.head(10))

    