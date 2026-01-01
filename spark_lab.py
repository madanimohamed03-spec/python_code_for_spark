#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, get_json_object, col

spark = SparkSession.builder \
    .appName("PostgreSQL Data Analysis") \
    .getOrCreate()

# Read data from directories
employee_df = spark.read.csv("hdfs:///user/hduser/datasets/employees_dir", 
                             header=True, 
                             inferSchema=True)

department_df = spark.read.csv("hdfs:///user/hduser/datasets/departments_dir", 
                               header=True, 
                               inferSchema=True)

project_df = spark.read.csv("hdfs:///user/hduser/datasets/projects_dir", 
                            header=True, 
                            inferSchema=True)

assignment_df = spark.read.csv("hdfs:///user/hduser/datasets/assignments_dir", 
                               header=True, 
                               inferSchema=True)

print("Data loaded successfully!")
print(f"Employee records: {employee_df.count()}")
print(f"Project records: {project_df.count()}")

# Create temporary views for SQL queries
employee_df.createOrReplaceTempView("employees")
department_df.createOrReplaceTempView("departments")
project_df.createOrReplaceTempView("projects")
assignment_df.createOrReplaceTempView("assignments")

# 1. Project Effort Analysis (Required)
print("\n=== Project Effort Analysis ===")
effort_df = spark.sql("""
    SELECT p.proj_id, p.proj_name, SUM(a.hours_per_week) AS total_hours
    FROM projects p
    JOIN assignments a ON p.proj_id = a.proj_id
    GROUP BY p.proj_id, p.proj_name
    ORDER BY total_hours DESC
""")

effort_df.show()

# Save result
effort_df.write.mode("overwrite").csv(
    "hdfs:///user/hduser/output/project_effort", 
    header=True
)

# 2. Mini-Experiment: Parse JSON and Arrays
print("\n=== Mini-Experiment: Parsing JSON and Phone Numbers ===")

# Parse pipe-separated phone numbers
employee_with_phones = employee_df.withColumn(
    "phone_array", 
    split(col("phone_numbers"), "\|")
)

# Extract JSON field (experience)
employee_with_exp = employee_with_phones.withColumn(
    "experience_years", 
    get_json_object(col("profilejson"), "$.experience")
)

# Show results
employee_with_exp.select(
    "emp_id", 
    "full_name", 
    "phone_array", 
    "experience_years"
).show(10, truncate=False)

# 3. Convert to Parquet (Advanced)
print("\n=== Converting to Parquet Format ===")
employee_with_exp.write.mode("overwrite").parquet(
    "hdfs:///user/hduser/parquet/employees"
)

print("Parquet file saved to: hdfs:///user/hduser/parquet/employees")

# Verify HDFS output
print("\n=== HDFS Output Directories ===")
import subprocess
result = subprocess.run(["hdfs", "dfs", "-ls", "/user/hduser/output"], 
                       capture_output=True, text=True)
print(result.stdout)

spark.stop()
