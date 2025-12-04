import subprocess
import os

def start_cluster_win():

    master = subprocess.Popen(["spark-class", "org.apache.spark.deploy.master.Master"], shell=True)
    worker1 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://68.234.244.60:7077"], shell =True)
    worker2 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://68.234.244.60:7077"], shell =True)
    worker3 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://68.234.244.60:7077"], shell =True)
    worker4 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://68.234.244.60:7077"], shell =True)
    worker5 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://68.234.244.60:7077"], shell =True)
    worker6 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://68.234.244.60:7077"], shell =True)
    worker7 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://68.234.244.60:7077"], shell =True)
    worker8 = subprocess.Popen(["spark-class", "org.apache.spark.deploy.worker.Worker", "spark://68.234.244.60:7077"], shell =True)


