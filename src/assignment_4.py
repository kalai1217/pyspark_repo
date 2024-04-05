from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, explode_outer, posexplode, current_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("JSON Processing") \
    .getOrCreate()

# 1. Read JSON file
json_path = r"resources/nested_json_file.json"
json_df = spark.read.option("multiline", "true").json(json_path)

# 2. Flatten the data frame
json_df_flattened = json_df.selectExpr("id", "properties.*", "explode(employees) as employees")

# 3. Find record count before and after flattening
record_count_before = json_df.count()
record_count_after = json_df_flattened.count()

# 4. Differentiate using explode, explode outer, posexplode functions
json_df_explode = json_df.withColumn("employees", explode("employees"))
json_df_explode_outer = json_df.withColumn("employees", explode_outer("employees"))
json_df_posexplode = json_df.selectExpr("id", "properties", "posexplode(employees) as (pos, employee)")

# 5. Filter id equal to 0001
filtered_df = json_df_flattened.filter(col("id") == 1001)

# 6. Convert column names from camel case to snake case
snake_case_df = json_df_flattened.toDF(*[col_name.lower() for col_name in json_df_flattened.columns])

# 7. Add a new column named load_date with the current date
date_df = snake_case_df.withColumn("load_date", current_date())

# 8. Create year, month, and day columns from load_date
date_df_with_split = date_df.withColumn("year", date_df["load_date"].substr(1, 4))
date_df_with_split = date_df_with_split.withColumn("month", date_df["load_date"].substr(6, 2))
date_df_with_split = date_df_with_split.withColumn("day", date_df["load_date"].substr(9, 2))

# Show results
date_df_with_split.show()
