from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Processing") \
    .getOrCreate()

# Define schemas for datasets
employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("state", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("age", IntegerType(), True)
])

department_schema = StructType([
    StructField("dept_id", StringType(), True),
    StructField("dept_name", StringType(), True)
])

country_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)
])

# Define datasets
employee_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]

department_data = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")
]

country_data = [
    ("ny", "newyork"),
    ("ca", "California"),
    ("uk", "Russia")
]

# Create data frames
employee_df = spark.createDataFrame(employee_data, schema=employee_schema)
department_df = spark.createDataFrame(department_data, schema=department_schema)
country_df = spark.createDataFrame(country_data, schema=country_schema)

# 2. Find avg salary of each department
avg_salary_by_dept = employee_df.groupBy("department").avg("salary")

# 3. Find employee's name and department name whose name starts with 'm'
m_starting_names = employee_df.filter(employee_df["employee_name"].startswith("m")) \
    .join(department_df, employee_df["department"] == department_df["dept_id"]) \
    .select("employee_name", "dept_name")

# 4. Create another new column in employee_df as bonus by multiplying employee salary * 2
employee_df_with_bonus = employee_df.withColumn("bonus", employee_df["salary"] * 2)

# 5. Reorder the column names of employee_df
employee_df_reordered = employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department")

# 6. Inner join, left join, and right join between employee_df and department_df
inner_join_result = employee_df.join(department_df, employee_df["department"] == department_df["dept_id"], "inner")
left_join_result = employee_df.join(department_df, employee_df["department"] == department_df["dept_id"], "left")
right_join_result = employee_df.join(department_df, employee_df["department"] == department_df["dept_id"], "right")

# 7. Derive a new data frame with country_name instead of State in employee_df
employee_df_with_country_name = employee_df.join(country_df, employee_df["state"] == country_df["country_code"]) \
    .select("employee_id", "employee_name", "department", "country_name", "salary", "age")

# Show results
avg_salary_by_dept.show()
m_starting_names.show()
employee_df_with_bonus.show()
employee_df_reordered.show()
inner_join_result.show()
left_join_result.show()
right_join_result.show()
employee_df_with_country_name.show()
