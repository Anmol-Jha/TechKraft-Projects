# Databricks notebook source
from pyspark.sql import SparkSession

# Read data from DBFS
df = spark.read.option("header", True).csv("/FileStore/tables/sample_pii_data-1.csv")
df.show()
df.printSchema()