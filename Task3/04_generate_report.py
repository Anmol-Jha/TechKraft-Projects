# Databricks notebook source
df = spark.read.format("delta").load("/tmp/delta/pii_anonymized")
df.createOrReplaceTempView("anonymized_data")

report = spark.sql("""
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT ssn) as unique_ssn
FROM anonymized_data
""")
report.show()