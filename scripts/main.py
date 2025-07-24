from pyspark.sql import SparkSession
# Import necessary libraries
from pyspark.sql.functions import broadcast, col, avg, count, sum, min, max, when, lit, row_number

# Create a Spark session 
spark = SparkSession.builder \
        .appName("Spark SQL Example") \
        .config("spark.sql.shuffle.partitions","2") \
        .master("local[*]") \
        .getOrCreate()
        
emp_df = spark.read.csv("/project/workspace/sources/employees_1.csv", header=True, inferSchema=True)

emp_df.printSchema()
#emp_df.show(5)

emp_df.createOrReplaceTempView("emp")

# SQL query to find the average salary of employees
avg_sal = "Select avg(salary) as avg_sal from emp"

avg_sal_df = spark.sql(avg_sal)

avg_sal_df.show()



