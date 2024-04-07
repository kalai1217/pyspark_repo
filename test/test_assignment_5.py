# Import modules for testing and Spark functions
import unittest
from pyspark_repo.src.assignment_5.utils import *

# Test class for Spark function unit tests
class TestSparkFunctions(unittest.TestCase):

    # Set up Spark session and sample DataFrames before tests
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.employee_df = create_df(cls.spark, employee_schema, employee_data)
        cls.department_df = create_df(cls.spark, department_schema, department_data)
        cls.country_df = create_df(cls.spark, country_schema, country_data)

    # Test average salary calculation
    def test_find_avg_salary_employee(self):
        avg_salary = find_avg_salary_employee(self.employee_df)
        self.assertTrue(avg_salary.count() > 0)  # Check for non-empty result

    # Test filtering and joining employees by name
    def test_find_employee_name_starts_with_m(self):
        employees_starts_with_m = find_employee_name_starts_with_m(self.employee_df, self.department_df)
        self.assertTrue(employees_starts_with_m.count() > 0)  # Check for filtered/joined results

    # Test adding a bonus column
    def test_add_bonus_times_2(self):
        employee_bonus_df = add_bonus_times_2(self.employee_df)
        self.assertTrue("bonus" in employee_bonus_df.columns)  # Check for bonus column existence

    # Test rearranging columns
    def test_rearrange_columns_employee_df(self):
        rearranged_employee_df = rearrange_columns_employee_df(self.employee_df)
        self.assertEqual(rearranged_employee_df.columns, expected_column_order)  # Verify expected order

    # Test performing various joins
    def test_dynamic_join(self):
        inner_join_result, left_join_result, right_join_result = dynamic_join(..., ...)  # Use ellipsis for brevity
        self.assertTrue(all(result.count() > 0 for result in [inner_join_result, left_join_result, right_join_result]))  # Check for non-empty join results

    # Test replacing a column
    def test_update_country_name(self):
        updated_employee_df = update_country_name(self.employee_df)
        self.assertTrue("country_name" in updated_employee_df.columns)  # Check for new column

    # Test converting column names to lowercase
    def test_column_to_lower(self):
        lower_case_column_df = column_to_lower(self.employee_df)
        self.assertTrue(all(col.lower() in lower_case_column_df.columns for col in self.employee_df.columns))  # Check for lowercase conversion

    # Test adding a date column with current date (shortened for brevity)
    def test_add_load_date_with_current_date(self):
        date_df = column_to_lower(self.employee_df).withColumn("load_date", current_date())
        self.assertTrue("load_date" in date_df.columns)  # Check for load_date column