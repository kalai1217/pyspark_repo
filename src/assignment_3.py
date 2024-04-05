from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, date_format, to_date, countDistinct

# 1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field
data = [(1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')]

schema = StructType([
    StructField("log_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("user_activity", StringType(), nullable=False),
    StructField("time_stamp", StringType(), nullable=False)
])

df = spark.createDataFrame(data, schema=schema)

# 2. Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
df = df.withColumnRenamed("log_id", "log_id") \
       .withColumnRenamed("user_id", "user_id") \
       .withColumnRenamed("user_activity", "user_activity") \
       .withColumnRenamed("time_stamp", "time_stamp")

# 3. Write a query to calculate the number of actions performed by each user in the last 7 days
from pyspark.sql.functions import date_format, to_date
current_date = spark.sql("select current_date()").collect()[0][0]
last_7_days = to_date(col("time_stamp")) >= (current_date - 7)

user_activity_count = df.filter(last_7_days) \
                        .groupBy("user_id", "user_activity") \
                        .agg(countDistinct("log_id").alias("count")) \
                        .orderBy("user_id", "user_activity")

user_activity_count.show()

# 4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
df = df.withColumn("login_date", to_date(col("time_stamp")))
df.printSchema()
