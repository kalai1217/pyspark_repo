
# PySpark_Repo

- _This repo contains PySpark assignments._


## Project_Structure

 - **resources**

     - `data` files that had been used in the assignments. 

 - **src**
 
     - `driver.py` contains function call.
     - `utils.py` contains function declaration and definition.
 - **test**

     - `test.py` contains test cases for the function.
 - **.gitignore**

     - ignore unwanted files/directory like `.idea` when committing project to the GitHub repository.
 - **README.md**

    - *Overview of the project*
## Question_1

**Functions used:**

- `from pyspark.sql` import SparkSession, StructType, StructField, IntegerType, StringType`: Imports necessary classes for SparkSession and DataFrame creation.
- `spark_session`: Function to create a SparkSession (assumed to be defined elsewhere).
- `create_dataframe`: Function to create a DataFrame from data and schema (assumed to be defined elsewhere).
- `filter`: Filters DataFrame rows based on a condition.
- `col`: Refers to a DataFrame column by name.
- `join`: Performs a join operation between two DataFrames.
- `select`: Selects specific columns from a DataFrame.
- `groupBy`: Groups rows in a DataFrame based on a specified column.
- `agg`: Applies aggregation functions to grouped DataFrame.
- `alias`: Assigns an alias to a column expression.
- `count`: Counts the number of elements in a column.
- `countDistinct`: Counts the number of distinct elements in a column.

---

### Apply DataFrame Operations:

1. **Filtering Data:**

    - Use the `filter` function to filter DataFrame rows based on specific conditions.

2. **Selecting Columns:**

    - Use the `select` function to select specific columns from a DataFrame.

3. **Joining DataFrames:**

    - Utilize the `join` function to perform join operations between two DataFrames.

4. **Grouping Data:**

    - Use the `groupBy` function to group rows in a DataFrame based on a specified column.

5. **Aggregating Data:**

    - Apply aggregation functions using the `agg` function on grouped DataFrames.

6. **Alias and Count:**

    - Assign aliases to column expressions using the `alias` function and count the number of elements or distinct elements in a column using `count` and `countDistinct`, respectively.



**Summary:**


Create Spark DataFrames:

1. **purchase_data_df:** DataFrame containing customer purchase data.
2. **product_data_df:** DataFrame containing unique product models.

---

Find Customers who Bought Only iPhone 13:

1. Filter `purchase_data_df` for rows where `product_model` is "iphone13".

---

Find Customers Upgraded from iPhone 13 to iPhone 14:

1. Filter `purchase_data_df` for "iphone13" and "iphone14" purchases separately.
2. Perform an inner join on both DataFrames using "customer" ID.
3. Select only the "customer" column from the joined DataFrame.

---

Find Customers who Bought All Models:

1. Group `purchase_data_df` by "customer".
2. Use `countDistinct` with an alias ("model_count") to count distinct "product_model" items purchased by each customer.
3. Filter the grouped DataFrame for customers with `model_count` equal to the total number of distinct models in `product_data_df`.
4. Select only the "customer" column from the filtered DataFrame.

##  Question_2

**Summary**



**Find Customers who Bought Only iPhone 13:**

1. Filter `purchase_data_df` for rows where `product_model` is "iphone13".

---

**Find Customers Upgraded from iPhone 13 to iPhone 14:**

1. Filter `purchase_data_df` for "iphone13" and "iphone14" purchases separately.
2. Perform an inner join on both DataFrames using "customer" ID.
3. Select only the "customer" column from the joined DataFrame.

---

**Find Customers who Bought All Models:**

1. Group `purchase_data_df` by "customer".
2. Use `countDistinct` with an alias ("model_count") to count distinct "product_model" items purchased by each customer.
3. Filter the grouped DataFrame for customers with `model_count` equal to the total number of distinct models in `product_data_df`.
4. Select only the "customer" column from the filtered DataFrame.
##  Question_3

**Summary**

### 1. Custom Schema:
- Column names: log_id, user_id, user_activity, time_stamp

### 2. Create DataFrame with Dynamic Column Names:

Column names should be:
- log_id
- user_id
- user_activity
- time_stamp

### 3. Query to Calculate Number of Actions in Last 7 Days:

Write a query to calculate the number of actions performed by each user in the last 7 days.

### 4. Convert Timestamp Column to Login Date:

Convert the timestamp column to the login_date column with the format YYYY-MM-DD and date type as its data type.
##  Question_4

**Summary**

### 1. Read JSON File Using Dynamic Function:

Read the JSON file provided in the attachment using a dynamic function.

### 2. Flatten the DataFrame with Custom Schema:

Flatten the DataFrame obtained from the JSON file, which has a custom schema.

### 3. Record Count Comparison:

Find out the record count when the DataFrame is flattened and when it's not flattened. Analyze the difference in count.

### 4. Differentiate Using Explode Functions:

Differentiate the difference using `explode`, `explode_outer`, and `posexplode` functions.

### 5. Filter ID Equals to 0001:

Filter the DataFrame to find records where the ID is equal to "0001".

### 6. Convert Column Names:

Convert the column names from camel case to snake case.

### 7. Add New Column:

Add a new column named `load_date` with the current date.

### 8. Create Year, Month, and Day Columns:

Create three new columns as `year`, `month`, and `day` from the `load_date` column.
##  Question_5

**Summary**


### 1. Create DataFrames with Custom Schema:
- Create employee_df, department_df, country_df with custom schemas defined dynamically.

### 2. Average Salary of Each Department:
- Find the average salary of each department.

### 3. Employees Whose Name Starts with 'M':
- Find the employee's name and department name whose name starts with 'M'.

### 4. Create Bonus Column:
- Create another new column in employee_df as a bonus by multiplying employee salary by 2.

### 5. Reorder Column Names:
- Reorder the column names of employee_df columns as (employee_id, employee_name, salary, State, Age, department).

### 6. Join Operations:
- Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way.

### 7. Replace State with Country Name:
- Derive a new DataFrame with country_name instead of State in employee_df.

### 8. Convert Column Names to Lowercase and Add Load Date Column:
- Convert all the column names into lowercase from the result of question 7 in a dynamic way.
- Add the load_date column with the current date.
