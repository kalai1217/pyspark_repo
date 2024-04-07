
from pyspark_repo.src.assignment_1.utils import *  # Assuming updated functions are in this module
import unittest


class TestAssignment1(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("PySpark Assignment").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_create_dataframe(self):
        test_transaction_schema = StructType([
            StructField("buyer_id", IntegerType(), True),
            StructField("item_model", StringType(), True)
        ])
        test_transaction_data = [(1, "iphone13"), (1, "dell_i5"), (2, "iphone13"), (2, "dell_i5"),
                                  (3, "iphone13"), (3, "dell_i5"), (1, "dell_i3"), (1, "hp_i5"),
                                  (1, "iphone14"), (3, "iphone14"), (4, "iphone13")]
        test_product_info_schema = StructType([StructField("product_model", StringType(), True)])
        test_product_info_data = [("iphone13",), ("dell_i5",), ("dell_i3",), ("hp_i5",), ("iphone14",)]
        test_transaction_df = create_data_frame(self.spark, test_transaction_data, test_transaction_schema)
        test_product_info_df = create_data_frame(self.spark, test_product_info_data, test_product_info_schema)

        self.assertEqual(test_transaction_df.count(), 11)
        self.assertEqual(test_product_info_df.count(), 5)

if __name__ == '__main__':
    unittest.main()
