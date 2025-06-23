# Databricks notebook source
from cryptography.fernet import Fernet
import json
import logging
from datetime import datetime
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.types import StringType
from datetime import datetime
from zoneinfo import ZoneInfo


# Generate Key
key = Fernet.generate_key().decode()
print("🔑 Encryption key (store this securely!):", key)
print("")

#  Identify Current User
try:
    current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
except:
    current_user = "unknown_user"

# Time
local_timezone = ZoneInfo("Asia/Kathmandu")

local_time = datetime.now(local_timezone)

formatted_local_time = local_time.isoformat()

print(f" User: {current_user}")
print(f" Login Time ({local_timezone}):{formatted_local_time}")
login_time = datetime.utcnow().isoformat() + "Z"

print("")

#  Audit Logger Class
class AuditLogger:
    def __init__(self, user="system", log_file='audit_log.jsonl'):
        self.user = user
        self.logger = logging.getLogger("AuditLogger")
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(handler)

    def log_event(self, event_type, status, record_id, fields_affected, details):
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user": self.user,
            "event_type": event_type,
            "status": status,
            "record_id": record_id,
            "fields_affected": fields_affected,
            "details": details
        }
        self.logger.info(json.dumps(entry))

# Initialize logger with user context
audit_logger = AuditLogger(user=current_user)

# Encrypt/Decrypt Functions
def encrypt_value(val, record_id=None, field=None):
    if val:
        try:
            cipher = Fernet(key.encode())
            encrypted = cipher.encrypt(val.encode()).decode()
            audit_logger.log_event("encryption", "success", record_id, [field], f"Encrypted value for field '{field}'")
            return encrypted
        except Exception as e:
            audit_logger.log_event("encryption", "failure", record_id, [field], str(e))
    return None

def decrypt_value(val, record_id=None, field=None):
    if val:
        try:
            cipher = Fernet(key.encode())
            decrypted = cipher.decrypt(val.encode()).decode()
            audit_logger.log_event("decryption", "success", record_id, [field], f"Decrypted value for field '{field}'")
            return decrypted
        except Exception as e:
            audit_logger.log_event("decryption", "failure", record_id, [field], str(e))
    return None

#  Define UDFs
encrypt_ssn_udf = udf(lambda x, row_id: encrypt_value(x, record_id=str(row_id), field="ssn"), StringType())
encrypt_email_udf = udf(lambda x, row_id: encrypt_value(x, record_id=str(row_id), field="email"), StringType())
encrypt_phone_udf = udf(lambda x, row_id: encrypt_value(x, record_id=str(row_id), field="phone"), StringType())
decrypt_ssn_udf = udf(lambda x, row_id: decrypt_value(x, record_id=str(row_id), field="ssn"), StringType())
decrypt_email_udf = udf(lambda x, row_id: decrypt_value(x, record_id=str(row_id), field="email"), StringType())

#  Load Original Data
df = spark.read.option("header", True).csv("/FileStore/tables/sample_pii_data-3.csv")
df = df.withColumn("row_id", monotonically_increasing_id())

# Encrypt Columns
df_encrypted = df.withColumn("ssn", encrypt_ssn_udf(col("ssn"), col("row_id"))) \
                 .withColumn("email", encrypt_email_udf(col("email"), col("row_id"))) \
                    .withColumn("phone", encrypt_email_udf(col("phone"), col("row_id")))

df_encrypted.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save("/tmp/delta/pii_encrypted")

print("Encryption complete.")

df_encrypted.show()

# Decrypt Columns
df_encrypted = spark.read.format("delta").load("/tmp/delta/pii_encrypted")

df_decrypted = df_encrypted.withColumn("ssn", decrypt_ssn_udf(col("ssn"), col("row_id"))) \
                           .withColumn("email", decrypt_email_udf(col("email"), col("row_id")))

print("Decryption complete.")

df_decrypted.show()
