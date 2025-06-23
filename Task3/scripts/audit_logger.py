# Databricks notebook source
import json
from datetime import datetime

def log_action(record_id, field, original_value, method, path="/dbfs/logs/audit_log.json"):
    log_entry = {
        "record_id": record_id,
        "field": field,
        "original_value_hash": hashlib.sha256(original_value.encode()).hexdigest() if original_value else None,
        "anonymized": True,
        "method": method,
        "timestamp": datetime.utcnow().isoformat()
    }
    with open(path, "a") as log_file:
        log_file.write(json.dumps(log_entry) + "\n")