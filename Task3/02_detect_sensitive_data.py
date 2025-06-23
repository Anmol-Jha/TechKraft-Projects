# Databricks notebook source
import re
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

email_pattern = r'[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+'

def detect_email(text):
    return bool(re.search(email_pattern, text)) if text else False

detect_email_udf = udf(detect_email, BooleanType())

df = spark.read.option("header", True).csv("/FileStore/tables/sample_pii_data-1.csv")
df = df.withColumn("email_detected", detect_email_udf(col("email")))
df.show()