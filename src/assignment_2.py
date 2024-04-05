from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# 1. Create a Dataframe as credit_card_df with different read methods
data = [("1234567891234567",), ("5678912345671234",), ("9123456712345678",), ("1234567812341122",), ("1234567812341342",)]
credit_card_df = spark.createDataFrame(data, ["card_number"])

# 2. Print the number of partitions
print(f"Number of partitions: {credit_card_df.rdd.getNumPartitions()}")

# 3. Increase the partition size to 5
credit_card_df = credit_card_df.repartition(5)
print(f"Number of partitions after repartition: {credit_card_df.rdd.getNumPartitions()}")

# 4. Decrease the partition size back to its original partition size
credit_card_df = credit_card_df.coalesce(1)
print(f"Number of partitions after coalesce: {credit_card_df.rdd.getNumPartitions()}")

# 5. Create a UDF to print only the last 4 digits marking the remaining digits as *
@udf(returnType=StringType())
def mask_card_number(card_number):
    return "*" * (len(card_number) - 4) + card_number[-4:]

# 6. Output should have 2 columns as card_number, masked_card_number
credit_card_df = credit_card_df.withColumn("masked_card_number", mask_card_number(col("card_number")))
credit_card_df.show()
