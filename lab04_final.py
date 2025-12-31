#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

def main():
    print("=== LAB 04: PostgreSQL Data Analysis with Spark ===")
    
    # 1. Create Spark session
    spark = SparkSession.builder.appName("Lab04").getOrCreate()
    print("[OK] Spark session created")
    
    # 2. Read CSV files from HDFS
    print("\nReading CSV files from HDFS...")
    try:
        projects = spark.read.option("header","true").option("inferSchema","true").csv("hdfs:///user/hduser/datasets/projects_dir/part-00000.csv")
        assignments = spark.read.option("header","true").option("inferSchema","true").csv("hdfs:///user/hduser/datasets/assignments_dir/part-00000.csv")
        employees = spark.read.option("header","true").option("inferSchema","true").csv("hdfs:///user/hduser/datasets/employees_dir/part-00000.csv")
        print("[OK] CSV files loaded")
    except Exception as e:
        print(f"[ERROR] Cannot read files: {e}")
        return
    
    # 3. Create SQL views
    projects.createOrReplaceTempView("projects")
    assignments.createOrReplaceTempView("assignments")
    employees.createOrReplaceTempView("employees")
    
    # 4. STEP 5: Run queries
    print("\n" + "="*50)
    print("STEP 5: Verification Queries")
    print("="*50)
    
    print("Row count - Employees:")
    spark.sql("SELECT COUNT(*) FROM employees").show()
    
    print("Row count - Assignments:")
    spark.sql("SELECT COUNT(*) FROM assignments").show()
    
    print("\nProject Effort (total hours per project):")
    result = spark.sql("""
    SELECT p.proj_id, p.proj_name, SUM(a.hours_per_week) AS total_hours
    FROM projects p
    JOIN assignments a ON p.proj_id = a.proj_id
    GROUP BY p.proj_id, p.proj_name
    ORDER BY total_hours DESC
    """)
    result.show()
    
    # 5. STEP 6: Mini-experiment
    print("\n" + "="*50)
    print("STEP 6: Mini-Experiment")
    print("="*50)
    
    print("1. Splitting phone numbers:")
    emp_with_phones = employees.withColumn("phone_array", split(col("phone_numbers"), "\\|"))
    emp_with_phones.select("emp_id", "phone_array").show(5, truncate=False)
    
    print("\n2. Parsing JSON from profilejson:")
    json_schema = StructType([
        StructField("experience", IntegerType(), True),
        StructField("skills", ArrayType(StringType()), True)
    ])
    emp_with_json = employees.withColumn("profile", from_json(col("profilejson"), json_schema))
    emp_with_json.select("emp_id", "full_name", col("profile.experience"), col("profile.skills")).show(5, truncate=False)
    
    print("\n" + "="*50)
    print("LAB 04 COMPLETED SUCCESSFULLY!")
    print("="*50)
    
    spark.stop()

if __name__ == "__main__":
    main()
