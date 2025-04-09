#!/usr/bin/env python3
# PySpark job based on 04-LendingClub_DataCleaning_S3.ipynb - Loan Repayment Data Cleaning

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, when, col

# initialize Spark Session

spark = SparkSession. \
    builder. \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

print("Spark session created.")

try:
    # read loans repayment raw data (inferring schema initially)
    print("\nreading loans repayment raw data (initial load)")
    loans_repay_raw_df_initial = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("gs://lending_ara/data/raw/loans_repayments_csv")
    print("Initial loans_repay_raw_df loaded.")

    # display initial raw DataFrame
    print("\ndisplaying initial raw DataFrame (first 20 rows)")
    loans_repay_raw_df_initial.show()

    # print schema of initial raw DataFrame
    print("\nPrinting schema of initial raw DataFrame")
    loans_repay_raw_df_initial.printSchema()

    # define loans repayment schema
    print("\nDefining loans repayment schema")
    loans_repay_schema = 'loan_id string, total_principal_received float, total_interest_received float, total_late_fee_received float, total_payment_received float, last_payment_amount float, last_payment_date string, next_payment_date string'
    print(f"Loans repayment schema: {loans_repay_schema}")

    # Read loans repayment raw data with defined schema
    print("\nReading loans repayment raw data with defined schema")
    loans_repay_raw_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(loans_repay_schema) \
        .load("gs://lending_ara/data/raw/loans_repayments_csv")
    print("loans_repay_raw_df with schema created.")
    
    # print schema of DataFrame with defined schema
    print("\nPrinting schema of DataFrame with defined schema")
    loans_repay_raw_df.printSchema()
    
    # add ingestion date column
    print("\nAdding ingest_date column")
    loans_repay_df_ingestd = loans_repay_raw_df.withColumn("ingest_date", current_timestamp())
    print("ingest_date column added.")

    # Display DataFrame with ingestion date
    print("\nDisplaying DataFrame with ingestion date (first 20 rows)")
    loans_repay_df_ingestd.show()

    # Print schema of DataFrame with ingestion date
    print("\nPrinting schema of DataFrame with ingestion date")
    loans_repay_df_ingestd.printSchema()

    # Count total rows
    print("\nCounting total rows")
    total_rows = loans_repay_df_ingestd.count()
    print(f"Total rows: {total_rows}")

    # Create temporary view 'loan_repayments'
    print("\nCreating temporary view 'loan_repayments'")
    loans_repay_df_ingestd.createOrReplaceTempView("loan_repayments")
    print("Temporary view 'loan_repayments' created.")

    # SQL query: Count rows with null total_principal_received
    print("\nExecuting SQL: SELECT count(*) FROM loan_repayments WHERE total_principal_received IS NULL")
    spark.sql("select count(*) as null_principal_count from loan_repayments where total_principal_received is null").show()

    # Define columns to check for nulls
    print("\nDefining columns to check for nulls")
    columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]
    print(f"Columns to check: {columns_to_check}")

    # Drop rows with nulls in specified columns
    print("\nDropping rows with nulls in specified columns")
    loans_repay_filtered_df = loans_repay_df_ingestd.na.drop(subset=columns_to_check)
    print("Rows with nulls dropped.")

    # Count rows after filtering
    print("\nCounting rows after filtering")
    count_after_filtering = loans_repay_filtered_df.count()
    print(f"Count after filtering: {count_after_filtering}")

    # Replace temporary view 'loan_repayments' with filtered data
    print("\nReplacing temporary view 'loan_repayments' with filtered data")
    loans_repay_filtered_df.createOrReplaceTempView("loan_repayments")
    print("Temporary view 'loan_repayments' updated.")

    # SQL query: Count rows where total_payment_received is 0.0
    print("\nExecuting SQL: SELECT count(*) FROM loan_repayments WHERE total_payment_received = 0.0")
    spark.sql("select count(*) as zero_payment_count from loan_repayments where total_payment_received = 0.0").show()

    # SQL query: Count rows where total_payment_received is 0.0 BUT total_principal_received is not 0.0
    print("\nExecuting SQL: SELECT count(*) FROM loan_repayments WHERE total_payment_received = 0.0 AND total_principal_received != 0.0")
    spark.sql("select count(*) as inconsistent_zero_payment_count from loan_repayments where total_payment_received = 0.0 and total_principal_received != 0.0").show()

    # SQL query: Show inconsistent rows
    print("\nExecuting SQL: SELECT * FROM loan_repayments WHERE total_payment_received = 0.0 AND total_principal_received != 0.0")
    spark.sql("select * from loan_repayments where total_payment_received = 0.0 and total_principal_received != 0.0").show()

    # Fix inconsistent total_payment_received
    print("\nFixing inconsistent total_payment_received")
    loans_payments_fixed_df = loans_repay_filtered_df.withColumn(
        "total_payment_received",
        when(
            (col("total_principal_received") != 0.0) & (col("total_payment_received") == 0.0),
            col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
        ).otherwise(col("total_payment_received"))
    )
    print("Inconsistent total_payment_received fixed.")

    # Display loans_payments_fixed_df
    print("\nDisplaying loans_payments_fixed_df (first 20 rows)")
    loans_payments_fixed_df.show()

    # Filter for a specific loan_id to verify fix
    print("\nFiltering for loan_id '1064185' to verify fix")
    loans_payments_fixed_df.filter("loan_id == '1064185'").show()

    # Filter out rows where total_payment_received is still 0.0
    print("\nFiltering out rows where total_payment_received is 0.0")
    loans_payments_fixed2_df = loans_payments_fixed_df.filter("total_payment_received != 0.0")
    print("Filtered out zero payment rows.")

    # Count rows where last_payment_date is 0.0
    print("\nCounting rows where last_payment_date is 0.0")
    count_last_payment_zero = loans_payments_fixed2_df.filter("last_payment_date = 0.0").count()
    print(f"Count where last_payment_date is 0.0: {count_last_payment_zero}")

    # Count rows where next_payment_date is 0.0
    print("\nCounting rows where next_payment_date is 0.0")
    count_next_payment_zero = loans_payments_fixed2_df.filter("next_payment_date = 0.0").count()
    print(f"Count where next_payment_date is 0.0: {count_next_payment_zero}")

    # Count rows where last_payment_date is null
    print("\nCounting rows where last_payment_date is null")
    count_last_payment_null = loans_payments_fixed2_df.filter("last_payment_date is null").count()
    print(f"Count where last_payment_date is null: {count_last_payment_null}")

    # Count rows where next_payment_date is null
    print("\nCounting rows where next_payment_date is null")
    count_next_payment_null = loans_payments_fixed2_df.filter("next_payment_date is null").count()
    print(f"Count where next_payment_date is null: {count_next_payment_null}")

    # Fix last_payment_date where it is 0.0
    print("\nFixing last_payment_date where it is 0.0")
    loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn(
        "last_payment_date",
        when(
            (col("last_payment_date") == 0.0),
            None
        ).otherwise(col("last_payment_date"))
    )
    print("Fixed last_payment_date.")

    # Fix next_payment_date where it is 0.0
    print("\nFixing next_payment_date where it is 0.0")
    loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn(
        "next_payment_date",
        when(
            (col("next_payment_date") == 0.0),
            None
        ).otherwise(col("next_payment_date"))
    )
    print("Fixed next_payment_date.")

    # Verify count where last_payment_date is 0.0 after fix
    print("\nVerifying count where last_payment_date is 0.0 after fix")
    count_last_payment_zero_after = loans_payments_ndate_fixed_df.filter("last_payment_date = 0.0").count()
    print(f"Count where last_payment_date is 0.0 after fix: {count_last_payment_zero_after}")

    # Verify count where next_payment_date is 0.0 after fix
    print("\nVerifying count where next_payment_date is 0.0 after fix")
    count_next_payment_zero_after = loans_payments_ndate_fixed_df.filter("next_payment_date = 0.0").count()
    print(f"Count where next_payment_date is 0.0 after fix: {count_next_payment_zero_after}")

    # Write cleaned data to Parquet
    print("\nWriting cleaned loan repayments data to Parquet")
    output_path_parquet = "gs://lending_ara/data/raw/cleaned/loans_repayments_parquet"
    loans_payments_ndate_fixed_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", output_path_parquet) \
        .save()
    print(f"Cleaned loan repayments data saved to Parquet at {output_path_parquet}")

    # Write cleaned data to CSV
    print("\nWriting cleaned loan repayments data to CSV")
    output_path_csv = "gs://lending_ara/data/raw/cleaned/loans_repayments_csv"
    loans_payments_ndate_fixed_df.write \
        .option("header", True) \
        .format("csv") \
        .mode("overwrite") \
        .option("path", output_path_csv) \
        .save()
    print(f"Cleaned loan repayments data saved to CSV at {output_path_csv}")

finally:
    # Stop Spark session
    print("\nStopping Spark session.")
    spark.stop()