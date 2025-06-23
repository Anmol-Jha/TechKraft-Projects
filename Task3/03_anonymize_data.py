# Databricks notebook source
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import hashlib

def hash_value(val):
    return hashlib.sha256(val.encode()).hexdigest() if val else ""

hash_udf = udf(hash_value, StringType())

df = spark.read.option("header", True).csv("/FileStore/tables/sample_pii_data-1.csv")
df = df.withColumn("ssn", hash_udf(col("ssn")))
df = df.withColumn("email", hash_udf(col("email")))
df.write.format("delta").mode("overwrite").save("/tmp/delta/pii_anonymized")
df.show()