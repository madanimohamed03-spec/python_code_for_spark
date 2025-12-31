# Delete the problematic file
rm ~/lab04_spark_clean.py

# Create clean Python file
cat > ~/lab04_spark_final.py << 'PYCODE'
#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

def main():
    print("=== Starting Lab 04 Spark Script ===")
    
    spark = SparkSession.builder.appName("Lab04").getOrCreate()
    print("[OK] Spark ready")
    
    # Read data
    print("Reading HDFS files...")
    projects = spark.read.option("header","true").option("inferSchema","true").csv("hdfs:///user/hduser/datasets/projects_dir/part-00000.csv")
    assignments = spark.read.option("header","true").option("inferSchema","true").csv("hdfs:///user/hduser/datasets/assignments_dir/part-00000.csv")
    employees = spark.read.option("header","true").option("inferSchema","true").csv("hdfs:///user/hduser/datasets/employees_dir/part-00000.csv")
    
    print("[OK] Files loaded")
    
    # Create views
    projects.createOrReplaceTempView("projects")
    assignments.createOrReplaceTempView("assignments")
    employees.createOrReplaceTempView("employees")
    
    # STEP 5: Queries
    print("\n" + "="*50)
    print("STEP 5: Queries")
    print("="*50)
    
    print("Row counts:")
    spark.sql("SELECT COUNT(*) FROM employees").show()
    spark.sql("SELECT COUNT(*) FROM assignments").show()
    
    print("Project Effort:")
    spark.sql("""
    SELECT p.proj_id, p.proj_name, SUM(a.hours_per_week) AS total_hours 
    FROM projects p JOIN assignments a ON p.proj_id = a.proj_id
    GROUP BY p.proj_id, p.proj_name
    ORDER BY total_hours DESC
    """).show()
    
    # STEP 6: Mini-experiment
    print("\n" + "="*50)
    print("STEP 6: Mini-Experiment")
    print("="*50)
    
    print("Phone split:")
    employees.withColumn("phone_array", split(col("phone_numbers"), "\\|")).select("emp_id", "phone_array").show(5, False)
    
    print("JSON parse:")
    json_schema = StructType([
        StructField("experience", IntegerType(), True),
        StructField("skills", ArrayType(StringType()), True)
    ])
    employees.withColumn("profile", from_json(col("profilejson"), json_schema)) \
             .select("emp_id", "full_name", col("profile.experience"), col("profile.skills")) \
             .show(5, False)
    
    print("\n" + "="*50)
    print("LAB 04 COMPLETE!")
    print("="*50)
    
    spark.stop()

if __name__ == "__main__":
    main()
PYCODE
