import subprocess
import os

def start_cluster_win():

    master = subprocess.Popen("spark-class org.apache.spark.deploy.master.Master", shell=True)
    worker1 = subprocess.Popen("spark-class org.apache.spark.deploy.worker.Worker spark://68.234.244.60:7077 -c 3 -m 1500M", shell=True)
    worker2 = subprocess.Popen("spark-class org.apache.spark.deploy.worker.Worker spark://68.234.244.60:7077 -c 3 -m 1500M", shell=True)
    worker3 = subprocess.Popen("spark-class org.apache.spark.deploy.worker.Worker spark://68.234.244.60:7077 -c 3 -m 1500M", shell=True)
    


