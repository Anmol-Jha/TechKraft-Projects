# Load all tables
t1 = spark.table("diagnosis_details_csv")
t2 = spark.table("emergency_details_14_csv")
t3 = spark.table("health_details_13_csv")
t4 = spark.table("medical_details_csv")
t5 = spark.table("member_details_csv")
t6 = spark.table("procedure_details_csv")

# Perform joins on Member_id
joined_df = (
    t5
    .join(t2, on="Member_id", how="inner")
    .join(t3, on="Member_id", how="inner")
    .join(t4, on="Member_id", how="inner")
    .join(t1, on="Diagnosis_code", how="inner")
    .join(t6, on="Procedure_code", how="inner")
)

# Show result
joined_df.select("Member_id", "first_name", "last_name", "insurance_provider", "insurance_policy_number","Diagnosis_code", "Diagnosis_description").show()

filtered_df = joined_df.filter(joined_df.Diagnosis_description == "Wheezing")

# Join with members table to get names
result_df = filtered_df.join(members_df, on="Member_id", how="inner")

# Show result
result_df.select("Member_id", "Diagnosis_code", "Diagnosis_description").show()