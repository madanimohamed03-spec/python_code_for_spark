#!/usr/bin/env python3
# ============================================
# LAB 04: PostgreSQL Data Analysis with Spark
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import subprocess
import sys

def main():
    print("🚀 Starting Lab 04 Spark Script")
    
    # 1. Create Spark Session
    spark = SparkSession.builder \
        .appName("PostgreSQL_Hadoop_Lab04") \
        .getOrCreate()
    
    print("✅ Spark session created")
    
    # 2. Read CSV files from HDFS
    print("\n📁 Reading CSV files from HDFS...")
    
    try:
        projects_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("hdfs:///user/hduser/datasets/projects_dir/part-00000.csv")
        
        assignments_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("hdfs:///user/hduser/datasets/assignments_dir/part-00000.csv")
        
        employees_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("hdfs:///user/hduser/datasets/employees_dir/part-00000.csv")
        
        departments_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("hdfs:///user/hduser/datasets/departments_dir/part-00000.csv")
        
        print("✅ CSV files loaded")
    except Exception as e:
        print(f"❌ Error reading files: {e}")
        print("Checking HDFS...")
        subprocess.run(["hdfs", "dfs", "-ls", "-R", "/user/hduser/datasets/"])
        sys.exit(1)
    
    # 3. Create temporary views
    projects_df.createOrReplaceTempView("projects")
    assignments_df.createOrReplaceTempView("assignments")
    employees_df.createOrReplaceTempView("employees")
    departments_df.createOrReplaceTempView("departments")
    
    print("✅ Temporary views created")
    
    # 4. ===== STEP 5: Verification Queries =====
    print("\n" + "="*50)
    print("STEP 5: Verification & Project Effort Query")
    print("="*50)
    
    print("\n🔢 Row counts:")
    print("Employees count:")
    spark.sql("SELECT COUNT(*) as employee_count FROM employees").show()
    
    print("Assignments count:")
    spark.sql("SELECT COUNT(*) as assignment_count FROM assignments").show()
    
    print("\n📊 Project Effort Aggregation (Total hours per project):")
    project_effort = spark.sql("""
    SELECT 
      p.proj_id, 
      p.proj_name, 
      SUM(a.hours_per_week) AS total_hours 
    FROM projects p
    JOIN assignments a ON p.proj_id = a.proj_id
    GROUP BY p.proj_id, p.proj_name
    ORDER BY total_hours DESC
    """)
    project_effort.show()
    
    # Save result
    print("\n💾 Saving project effort results to HDFS...")
    project_effort.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs:///user/hduser/output/project_effort")
    
    # 5. ===== STEP 6: Mini-Experiment =====
    print("\n" + "="*50)
    print("STEP 6: Mini-Experiment - JSON Parsing & Phone Split")
    print("="*50)
    
    # a) Split phone numbers
    print("\n📱 Splitting phone numbers (pipe-separated):")
    employees_with_phones = employees_df.withColumn(
        "phone_array", 
        split(col("phone_numbers"), "\\|")
    )
    employees_with_phones.select("emp_id", "full_name", "phone_array").show(5, truncate=False)
    
    # b) Parse JSON
    print("\n📄 Parsing JSON from profilejson:")
    
    print("Sample JSON structure:")
    employees_df.select("profilejson").limit(1).show(truncate=False)
    
    # JSON schema - adjust based on your actual JSON
    json_schema = StructType([
        StructField("experience", IntegerType(), True),
        StructField("skills", ArrayType(StringType()), True)
    ])
    
    # Parse JSON
    employees_parsed = employees_df.withColumn(
        "profile_parsed", 
        from_json(col("profilejson"), json_schema)
    )
    
    # Extract fields
    employees_parsed = employees_parsed \
        .withColumn("experience", col("profile_parsed.experience")) \
        .withColumn("skills", col("profile_parsed.skills"))
    
    print("\n✅ Extracted fields from JSON:")
    employees_parsed.select("emp_id", "full_name", "experience", "skills").show(10, truncate=False)
    
    # 6. Save as Parquet
    print("\n" + "="*50)
    print("Saving employees_parsed as Parquet format")
    print("="*50)
    
    employees_parsed.write \
        .mode("overwrite") \
        .parquet("hdfs:///user/hduser/parquet/employees_parsed")
    
    print("✅ Parquet file saved to HDFS")
    
    # 7. Final HDFS check
    print("\n" + "="*50)
    print("Final HDFS Output Check")
    print("="*50)
    
    print("📂 Checking HDFS outputs...")
    subprocess.run(["hdfs", "dfs", "-ls", "/user/hduser/output/"])
    subprocess.run(["hdfs", "dfs", "-ls", "/user/hduser/parquet/"])
    
    print("\n" + "🎉" * 20)
    print("🎉 Lab 04 completed with Spark!")
    print("🎉" * 20)
    print("\n📋 Deliverables ready:")
    print("1. CSV export SQL ✓")
    print("2. Spark code (this script) ✓")
    print("3. Screenshots of HDFS listings")
    print("4. Project effort query output ✓")
    print("5. Mini-experiment output ✓")
    
    spark.stop()

if __name__ == "__main__":
    main()
