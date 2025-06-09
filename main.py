import sys
from operator import add
from random import random

from kagglehub import kagglehub

from pyspark.sql import SparkSession

from fraudDetectionPipeline import FraudDetectionPipeline

if __name__ == "__main__":
    """
    Usage: pi [partitions]
    
    spark = SparkSession.builder.appName("FraudDetection").getOrCreate()


    print("Path to dataset files:", path)

    df = spark.read.csv(f"{path}/Synthetic_Financial_datasets_log.csv", header=True, inferSchema=True)

  

"""
    path = kagglehub.dataset_download("sriharshaeedala/financial-fraud-detection-dataset")
    spark = SparkSession.builder.appName("FraudDetection").config("spark.executor.memory", "2g").config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    #print(spark.sparkContext.getConf().getAll())

    pipeline = FraudDetectionPipeline(spark, path)
    pipeline.load_data()
    pipeline.preprocess()
    #pipeline.balance_data()
    pipeline.train_and_evaluate_models()
    pipeline.export_results(
       csv_path="output/resultados_metricas.csv",
        sheet_url="https://docs.google.com/spreadsheets/d/1GWfUrWNkHEDb9XCGwjisNzfnoXPT8WXrOCdv4cvAC4w/edit?usp=sharing",
        creds_path="./credenciales.json"
    )



