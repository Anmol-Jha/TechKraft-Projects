# Databricks notebook source
rules = {
    'email': r'[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+',
    'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
    'phone': r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
}