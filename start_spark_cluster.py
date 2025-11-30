import subprocess
import os

def start_cluster_win():

    master = subprocess.Popen(["spark-class", "org.apache.spark.deploy.master.Master"], shell=True)
    worker1 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://192.168.1.41:7077"], shell =True)
    worker2 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://192.168.1.41:7077"], shell =True)
    worker3 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://192.168.1.41:7077"], shell =True)

