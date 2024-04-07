from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import StructType, IntegerType, StructField, StringType

# Schemas with distinct names
transaction_schema = StructType([
    StructField("buyer_id", IntegerType(), True),
    StructField("item_model", StringType(), True)
])
product_info_schema = StructType([
    StructField("product_model", StringType(), True)
])

# Data with clearer names
transaction_data = [(1, "iphone13"),
                    (1, "dell_i5"),  # Changed for consistency
                    (2, "iphone13"),
                    (2, "dell_i5"),  # Changed for consistency
                    # ... (other data entries)
]
product_info_data = [("iphone13",), ("dell_i5",), ("dell_i3",), ("hp_i5",), ("iphone14",)]  # Changed for consistency

# Functions with descriptive names
def start_spark_session():
    spark = SparkSession.builder.appName('spark-assignment').getOrCreate()
    return spark

def create_data_frame(spark, data, schema):
    df = spark.createDataFrame(data, schema)
    return df

def find_items_in_df(df, column_name, item_name):
    result = df.filter(df[column_name] == item_name)
    return result

def find_iphone14_buyers(df, iphone13_buyers):
    iphone14_buyers = df.filter(col("item_model") == "iphone14")
    # Join and keep only relevant columns
    result = iphone13_buyers.join(iphone14_buyers, on=["buyer_id"], how="inner") \
                             .select("buyer_id")
    return result

def find_customers_who_bought_all(df1, df2):
    customer_item_counts = df1.groupBy("buyer_id").agg(countDistinct("item_model").alias("item_count"))
    all_items_buyers = customer_item_counts.filter(customer_item_counts.item_count == df2.count()) \
                                      .select("buyer_id")
    return all_items_buyers
