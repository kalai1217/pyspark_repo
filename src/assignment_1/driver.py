from pyspark_repo.src.assignment_1.utils import *  # Assuming the modified functions are in this module
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

spark = start_spark_session()  # Using updated function name

logging.info("Creating DataFrames")
transaction_df = create_data_frame(spark, transaction_data, transaction_schema)
logging.info("Transaction data")
transaction_df.show(truncate=False)
product_info_df = create_data_frame(spark, product_info_data, product_info_schema)
logging.info("Product information data")
product_info_df.show(truncate=False)

iphone13_buyers_df = find_items_in_df(transaction_df, "item_model", "iphone13")
logging.info("2. Find customers who have bought only iPhone13")
iphone13_buyers_df.show()

iphone14_upgrade_buyers = find_iphone14_buyers(transaction_df, iphone13_buyers_df)
logging.info("3. Find customers who upgraded from iPhone13 to iPhone14")
iphone14_upgrade_buyers.show()

bought_all_items_customers = find_customers_who_bought_all(transaction_df, product_info_df)
logging.info("4. Find customers who have bought all models in the product data")
bought_all_items_customers.show(truncate=False)
