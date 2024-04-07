from pyspark_repo.src.assignment_4.utils import *

spark = spark_session()

# Loading JSON file
df = read_json(spark, json_path)
df.show()

# Flattening the DataFrame
flattened_df = flatten_df(df)
flattened_df.show()

# Displaying record count before and after flattening
count_before_after_flatten(df, flattened_df)

# Demonstrating differences using explode, explode outer, posexplode functions
diff_explode_outer_posexplode(spark)

# Filtering the DataFrame for empId == 1001
filtered_df = filter_employee_with_id(flattened_df, 1001)
filtered_df.show()

# Converting column names from camel case to snake case
snake_case_df = toSnakeCase(flattened_df)
snake_case_df.show()

# Adding a new column named load_date with the current date
load_date_df = add_load_date_with_current_date(snake_case_df)
load_date_df.show()

# Creating 3 new columns for year, month, and day from the load_date column
year_month_day_df = add_year_month_day(load_date_df)
year_month_day_df.show()
