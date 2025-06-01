import sys
from operator import add
from random import random
from kagglehub import kagglehub

from pyspark.sql import SparkSession

from FraudDetectionPipeline import FraudDetectionPipeline

if __name__ == "__main__":
    """
    Usage: pi [partitions]
    
    spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

    # Download latest version
    path = kagglehub.dataset_download("sriharshaeedala/financial-fraud-detection-dataset")

    print("Path to dataset files:", path)

    df = spark.read.csv(f"{path}/Synthetic_Financial_datasets_log.csv", header=True, inferSchema=True)

    # Asegúrate de usar el path correcto al CSV dentro del dataset

    df.printSchema()
    df.show(5)
    import os

    print("Files in dataset folder:")
    print(os.listdir(path))
"""
    path = kagglehub.dataset_download("sriharshaeedala/financial-fraud-detection-dataset")
    spark = SparkSession.builder.appName("FraudDetection").config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    print(spark.sparkContext.getConf().getAll())

    pipeline = FraudDetectionPipeline(spark, path)
    pipeline.load_data()
    pipeline.preprocess()
    pipeline.train_model()
    pipeline.evaluate()
    #self.df.printSchema()


