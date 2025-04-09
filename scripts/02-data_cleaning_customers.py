#!/usr/bin/env python3
# PySpark job based on 02-data_cleaning_customers.ipynb

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, regexp_replace, col, when, length

def main():
    # Initialize Spark Session 
    print("\n Initializing Spark Session")
    spark = SparkSession \
        .builder \
        .config('spark.shuffle.useOldFetchProtocol', 'true') \
        .enableHiveSupport() \
        .master('yarn') \
        .getOrCreate()
    
    print("Spark session created.")

    try:
        #  Define customer schema
        print("\n Defining customer schema")
        customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, addr_state string, zip_code string, country string, grade string, sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float, verification_status_joint string'
        print(f"Customer schema defined.")
        
        # Load data
        print("\nLoading customer data")
        customers_raw_df = spark.read \
            .format("csv") \
            .option("header", True) \
            .schema(customer_schema) \
            .load("gs://lending_ara/data/raw/customers_data_csv")
        print("customers_raw_df created.")
        
        # Display customers_raw_df
        print("\nDisplaying customers_raw_df (first 20 rows)")
        customers_raw_df.show()
        
        # Print schema of customers_raw_df
        print("\nPrinting schema of customers_raw_df")
        customers_raw_df.printSchema()
        
        # Rename columns
        print("\nRenaming columns")
        customer_df_renamed = customers_raw_df.withColumnRenamed("annual_inc", "annual_income") \
            .withColumnRenamed("addr_state", "address_state") \
            .withColumnRenamed("zip_code", "address_zipcode") \
            .withColumnRenamed("country", "address_country") \
            .withColumnRenamed("tot_hi_cred_lim", "total_high_credit_limit") \
            .withColumnRenamed("annual_inc_joint", "join_annual_income")
        print("Columns renamed.")
        
        # Display customer_df_renamed
        print("\nDisplaying customer_df_renamed (first 20 rows)")
        customer_df_renamed.show()
        
        # Add ingestion date column
        print("\nAdding ingest_date column")
        customers_df_ingestd = customer_df_renamed.withColumn("ingest_date", current_timestamp())
        print("ingest_date column added.")

        # Display customers_df_ingestd
        print("\nDisplaying customers_df_ingestd (first 20 rows)")
        customers_df_ingestd.show()
        
        # Count rows before distinct
        print("\nCounting rows before distinct")
        count_before_distinct = customers_df_ingestd.count()
        print(f"Count before distinct: {count_before_distinct}")
        
        # Apply distinct
        print("\nApplying distinct")
        customers_distinct = customers_df_ingestd.distinct()
        print("Distinct applied.")
        
        # Count rows after distinct
        print("\nCounting rows after distinct")
        count_after_distinct = customers_distinct.count()
        print(f"Count after distinct: {count_after_distinct}")
        
        # Create temporary view 'customers'
        print("\nCreating temporary view 'customers'")
        customers_distinct.createOrReplaceTempView("customers")
        print("Temporary view 'customers' created.")
        
        # SQL query: Select all from customers view
        print("\nExecuting SQL: SELECT * FROM customers (showing first 20 rows)")
        spark.sql("select * from customers").show()
        
        # SQL query: Count null annual_income
        print("\nExecuting SQL: SELECT count(*) FROM customers WHERE annual_income IS NULL")
        spark.sql("select count(*) as null_income_count from customers where annual_income is null").show()
        
        # Filter out null annual_income
        print("\nFiltering out rows where annual_income is null")
        customers_income_filtered = spark.sql("select * from customers where annual_income is not null")
        print("Null income rows filtered.")
        
        # Replace temporary view 'customers' with filtered data
        print("\nReplacing temporary view 'customers' with filtered data")
        customers_income_filtered.createOrReplaceTempView("customers")
        print("Temporary view 'customers' updated.")
        
        # SQL query: Verify null annual_income count is 0
        print("\nExecuting SQL: SELECT count(*) FROM customers WHERE annual_income IS NULL")
        spark.sql("select count(*) as null_income_count_after_filter from customers where annual_income is null").show()
        
        # SQL query: Select distinct emp_length
        print("\nExecuting SQL: SELECT distinct(emp_length) FROM customers")
        spark.sql("select distinct(emp_length) from customers").show()
        
        # Clean emp_length (remove non-digit characters)
        print("\nCleaning emp_length column (removing non-digits)")
        customers_emplength_cleaned = customers_income_filtered.withColumn("emp_length", regexp_replace(col("emp_length"), "(\\D)", ""))
        print("emp_length cleaned.")
        
        # Display customers_emplength_cleaned
        print("\nDisplaying customers_emplength_cleaned (first 20 rows)")
        customers_emplength_cleaned.show()
        
        # Print schema of customers_emplength_cleaned
        print("\nPrinting schema of customers_emplength_cleaned")
        customers_emplength_cleaned.printSchema()
        
        # Cast emp_length to integer
        print("\nCasting emp_length to integer")
        customers_emplength_casted = customers_emplength_cleaned.withColumn("emp_length", customers_emplength_cleaned.emp_length.cast('int'))
        print("emp_length casted to integer.")
        
        # Display customers_emplength_casted
        print("\nDisplaying customers_emplength_casted (first 20 rows)")
        customers_emplength_casted.show()
        
        # Print schema of customers_emplength_casted
        print("\nPrinting schema of customers_emplength_casted")
        customers_emplength_casted.printSchema()
        
        # Count null emp_length
        print("\nCounting null emp_length")
        null_emp_length_count = customers_emplength_casted.filter("emp_length is null").count()
        print(f"Count of null emp_length: {null_emp_length_count}")
        
        # Create temporary view 'customers' again
        print("\nCreating temporary view 'customers'")
        customers_emplength_casted.createOrReplaceTempView("customers")
        print("Temporary view 'customers' created.")
        
        # Calculate average emp_length
        print("\nCalculating average emp_length")
        avg_emp_length = spark.sql("select floor(avg(emp_length)) as avg_emp_length from customers").collect()
        print(f"Average emp_length result: {avg_emp_length}")
        
        # Extract average emp_length value
        print("\nExtracting average emp_length value")
        avg_emp_duration = avg_emp_length[0]['avg_emp_length']
        print(f"Average emp_duration: {avg_emp_duration}")
        
        # Fill null emp_length with average
        print("\nFilling null emp_length with average")
        customers_emplength_replaced = customers_emplength_casted.na.fill(avg_emp_duration, subset=['emp_length'])
        print("Null emp_length replaced.")
        
        # Display customers_emplength_replaced
        print("\nDisplaying customers_emplength_replaced (first 20 rows)")
        customers_emplength_replaced.show()
        
        # Verify null emp_length count is 0
        print("\nVerifying null emp_length count after replacement")
        null_emp_length_count_after = customers_emplength_replaced.filter("emp_length is null").count()
        print(f"Count of null emp_length after replacement: {null_emp_length_count_after}")
        
        # Create temporary view 'customers' again
        print("\nCreating temporary view 'customers'")
        customers_emplength_replaced.createOrReplaceTempView("customers")
        print("Temporary view 'customers' created.")
        
        # SQL query: Select distinct address_state
        print("\nExecuting SQL: SELECT distinct(address_state) FROM customers")
        spark.sql("select distinct(address_state) from customers").show()
        
        # SQL query: Count address_state with length > 2
        print("\nExecuting SQL: SELECT count(address_state) FROM customers WHERE length(address_state) > 2")
        spark.sql("select count(address_state) as invalid_state_count from customers where length(address_state) > 2").show()
        
        # Clean address_state (replace > 2 length with 'NA')
        print("\nCleaning address_state column")
        customers_state_cleaned = customers_emplength_replaced.withColumn(
            "address_state",
            when(length(col("address_state")) > 2, "NA").otherwise(col("address_state"))
        )
        print("address_state cleaned.")
        
        # Display customers_state_cleaned
        print("\nDisplaying customers_state_cleaned (first 20 rows)")
        customers_state_cleaned.show()
        
        # Select distinct address_state after cleaning
        print("\nSelecting distinct address_state after cleaning")
        customers_state_cleaned.select("address_state").distinct().show()
        
        # Write cleaned data to Parquet
        print("\nWriting cleaned customer data to Parquet")
        output_path_parquet = "gs://lending_ara/data/raw/cleaned/customers_parquet"
        customers_state_cleaned.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", output_path_parquet) \
            .save()
        print(f"Cleaned customer data saved to Parquet at {output_path_parquet}")
        
        # Write cleaned data to CSV
        print("\nWriting cleaned customer data to CSV")
        output_path_csv = "gs://lending_ara/data/raw/cleaned/customers_csv"
        customers_state_cleaned.write \
            .option("header", True) \
            .format("csv") \
            .mode("overwrite") \
            .option("path", output_path_csv) \
            .save()
        print(f"Cleaned customer data saved to CSV at {output_path_csv}")

    finally:
        # Stop Spark session
        print("\nStopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main() 