# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, when, lower, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Function to initialize SparkSession
def spark_session():
    """
    Initialize a SparkSession instance for the application
    """
    spark = SparkSession.builder.appName('spark-assignment').getOrCreate()
    return spark


# Define schemas for dataframes dynamically
employee_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("State", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])

department_schema = StructType([
    StructField("dept_id", StringType(), False),
    StructField("dept_name", StringType(), True)
])

country_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)
])

# Data for employee_df
employee_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]

# Data for department_df
department_data = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")
]

# Data for country_df
country_data = [
    ("ny", "newyork"),
    ("ca", "California"),
    ("uk", "Russia")
]

# Function to create DataFrame
def create_df(spark, schema, data):
    """
    Create a DataFrame using the given schema and data
    """
    df = spark.createDataFrame(data, schema)
    return df


# Function to find average salary of each department
def find_avg_salary_employee(df):
    """
    Calculate the average salary for each department
    """
    avg_salary = df.groupby("department").avg("salary").alias("Average Salary")
    return avg_salary


# Function to find employee names and department names starting with 'm'
def find_employee_name_starts_with_m(employee_df, department_df):
    """
    Retrieve the employee names and department names where the name starts with 'm'
    """
    employee_starts_with_m = employee_df.filter(employee_df.employee_name.startswith('m'))
    starts_with_m = employee_starts_with_m.join(department_df,
                                                employee_starts_with_m["department"] == department_df["dept_id"],
                                                "inner") \
        .select(employee_starts_with_m.employee_name, department_df.dept_name)
    return starts_with_m


# Function to add bonus column by doubling the employee salary
def add_bonus_times_2(employee_df):
    """
    Create a new column 'bonus' in employee_df by multiplying the employee salary by 2
    """
    employee_bonus_df = employee_df.withColumn("bonus", employee_df.salary * 2)
    return employee_bonus_df


# Function to rearrange column order
def rearrange_columns_employee_df(employee_df):
    """
    Rearrange the column order in employee_df to (employee_id, employee_name, salary, State, Age, department)
    """
    employee_df = employee_df.select("employee_id", "employee_name", "salary", "State", "Age", "department")
    return employee_df


# Function to perform dynamic join
def dynamic_join(df1, df2, how):
    """
    Provide the results of an inner join, left join, and right join when dynamically joining employee_df with department_df
    """
    return df1.join(df2, df1.department == df2.dept_id, how)


# Function to update State to country_name
def update_country_name(dataframe):
    """
    Derive a new dataframe with country_name instead of State in employee_df
    """
    country_dataframe = dataframe.withColumn("State", when(dataframe["State"] == "uk", "United Kingdom")
                                             .when(dataframe["State"] == "ny", "New York")
                                             .when(dataframe["State"] == "ca", "Canada"))
    new_df = country_dataframe.withColumnRenamed("State", "country_name")
    return new_df


# Function to convert column names to lowercase and add load_date column
def column_to_lower(dataframe):
    """
    Convert all the column names to lowercase from the result of question 7, and add the load_date column with the current date
    """
    for column in dataframe.columns:
        dataframe = dataframe.withColumnRenamed(column, column.lower())
    return dataframe
