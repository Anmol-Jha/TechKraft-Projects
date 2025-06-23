# Databricks notebook source
import hashlib

def hash_value(value):
    return hashlib.sha256(value.encode()).hexdigest() if value else ""

def mask_email(email):
    if email and "@" in email:
        name, domain = email.split("@")
        return name[0] + "***@" + domain[0] + "****"
    return "[MASKED]"