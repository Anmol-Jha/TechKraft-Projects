# Databricks notebook source
# MAGIC %pip install cryptography

# COMMAND ----------

from cryptography.fernet import Fernet

# Generate a key (run once and store securely!)
key = Fernet.generate_key().decode()  # convert to string for serialization
print("🔑 Encryption key (store this!):", key)


# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet

# Define encryption function that creates Fernet locally
def encrypt_value(val, key=key):
    if val:
        cipher = Fernet(key.encode())  # reconstruct Fernet from key
        return cipher.encrypt(val.encode()).decode()
    return None

# Define decryption function
def decrypt_value(val, key=key):
    if val:
        cipher = Fernet(key.encode())
        return cipher.decrypt(val.encode()).decode()
    return None

# Register UDFs
encrypt_udf = udf(lambda x: encrypt_value(x), StringType())
decrypt_udf = udf(lambda x: decrypt_value(x), StringType())


# COMMAND ----------

# Load the original CSV
df = spark.read.option("header", True).csv("/FileStore/tables/sample_pii_data-3.csv")

# Encrypt sensitive columns
df_encrypted = df.withColumn("ssn", encrypt_udf(col("ssn"))) \
                 .withColumn("email", encrypt_udf(col("email")))

# Save encrypted data
df_encrypted.write.format("delta").mode("overwrite").save("/tmp/delta/pii_encrypted")

print('PHI - Protected health information dataset   | PII - Personally Identifiable Information')
print('PHI - disease description                    | PII - name, email, phone, ssn, address')
print('...........................................................................................')
print('')

df_encrypted.show()

# COMMAND ----------

df_encrypted = spark.read.format("delta").load("/tmp/delta/pii_encrypted")

df_decrypted = df_encrypted.withColumn("ssn", decrypt_udf(col("ssn"))) \
                           .withColumn("email", decrypt_udf(col("email")))

print('PHI - Protected health information dataset   | PII - Personally Identifiable Information')
print('PHI - disease description                    | PII - name, email, phone, ssn, address')
print('...........................................................................................')
print('')

df_decrypted.show()
