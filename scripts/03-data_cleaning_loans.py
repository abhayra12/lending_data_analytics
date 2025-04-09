#!/usr/bin/env python3
# PySpark job based on 03-LendingClub_DataCleaning_S2.ipynb - Loan Data Cleaning

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, regexp_replace, col, when, count

    # Initialize Spark Session

spark = SparkSession. \
    builder. \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

print("Spark session created.")

try:
    # Define loans schema
    print("\nDefining loans schema")
    loans_schema = 'loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string, interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string, loan_title string'
    print(f"Loans schema: {loans_schema}")

    # Read loans raw data
    print("\nReading loans raw data")
    loans_raw_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(loans_schema) \
        .load("gs://lending_ara/data/raw/loans_data_csv")
    print("loans_raw_df created.")

    # Display loans_raw_df
    print("\nDisplaying loans_raw_df (first 20 rows)")
    loans_raw_df.show()

    # Print schema of loans_raw_df
    print("\nPrinting schema of loans_raw_df")
    loans_raw_df.printSchema()

    # Add ingestion date column
    print("\nAdding ingest_date column")
    loans_df_ingestd = loans_raw_df.withColumn("ingest_date", current_timestamp())
    print("ingest_date column added.")

    # Display loans_df_ingestd
    print("\nDisplaying loans_df_ingestd (first 20 rows)")
    loans_df_ingestd.show()

    # Create temporary view 'loans'
    print("\nCreating temporary view 'loans'")
    loans_df_ingestd.createOrReplaceTempView("loans")
    print("Temporary view 'loans' created.")

    # SQL query: Count rows in loans view
    print("\nExecuting SQL: SELECT count(*) FROM loans")
    spark.sql("select count(*) as total_rows from loans").show()

    # SQL query: Select rows with null loan_amount
    print("\nExecuting SQL: SELECT * FROM loans WHERE loan_amount IS NULL")
    spark.sql("select * from loans where loan_amount is null").show()

    # Define columns to check for nulls
    print("\nDefining columns to check for nulls")
    columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]
    print(f"Columns to check: {columns_to_check}")

    # Drop rows with nulls in specified columns
    print("\nDropping rows with nulls in specified columns")
    loans_filtered_df = loans_df_ingestd.na.drop(subset=columns_to_check)
    print("Rows with nulls dropped.")

    # Count rows after filtering
    print("\nCounting rows after filtering")
    count_after_filtering = loans_filtered_df.count()
    print(f"Count after filtering: {count_after_filtering}")

    #  Create temporary view 'loans' with filtered data
    print("\nReplacing temporary view 'loans' with filtered data")
    loans_filtered_df.createOrReplaceTempView("loans")
    print("Temporary view 'loans' updated.")

    #  Display loans_filtered_df
    print("\nDisplaying loans_filtered_df (first 20 rows)")
    loans_filtered_df.show()

    #  Modify loan_term_months to loan_term_years
    print("\nModifying loan_term_months to loan_term_years")
    loans_term_modified_df = loans_filtered_df.withColumn("loan_term_months", (regexp_replace(col("loan_term_months"), " months", "").cast("int") / 12).cast("int")) \
        .withColumnRenamed("loan_term_months", "loan_term_years")
    print("loan_term column modified.")

    #  Display loans_term_modified_df
    print("\nDisplaying loans_term_modified_df (first 20 rows)")
    loans_term_modified_df.show()

    #  Print schema of loans_term_modified_df
    print("\nPrinting schema of loans_term_modified_df")
    loans_term_modified_df.printSchema()

    #  Create temporary view 'loans' again
    print("\nCreating temporary view 'loans'")
    loans_term_modified_df.createOrReplaceTempView("loans")
    print("Temporary view 'loans' created.")

    #  SQL query: Select distinct loan_purpose
    print("\nExecuting SQL: SELECT distinct(loan_purpose) FROM loans")
    spark.sql("select distinct(loan_purpose) from loans").show()

    #  SQL query: Group by loan_purpose and count
    print("\nExecuting SQL: SELECT loan_purpose, count(*) FROM loans GROUP BY loan_purpose ORDER BY count DESC")
    spark.sql("select loan_purpose, count(*) as total from loans group by loan_purpose order by total desc").show()

    #  Define loan purpose lookup list
    print("\nDefining loan purpose lookup list")
    loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]
    print(f"Loan purpose lookup: {loan_purpose_lookup}")

    #  Clean loan_purpose column
    print("\nCleaning loan_purpose column")
    loans_purpose_modified = loans_term_modified_df.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other"))
    print("loan_purpose cleaned.")

    #  Create temporary view 'loans' with purpose-modified data
    print("\nReplacing temporary view 'loans' with purpose-modified data")
    loans_purpose_modified.createOrReplaceTempView("loans")
    print("Temporary view 'loans' updated.")

    #  SQL query: Verify loan_purpose counts after cleaning
    print("\nExecuting SQL: SELECT loan_purpose, count(*) FROM loans GROUP BY loan_purpose ORDER BY count DESC")
    spark.sql("select loan_purpose, count(*) as total from loans group by loan_purpose order by total desc").show()

    #  DataFrame API: Verify loan_purpose counts after cleaning
    print("\nVerifying loan_purpose counts using DataFrame API")
    loans_purpose_modified.groupBy("loan_purpose").agg(count("*").alias("total")).orderBy(col("total").desc()).show()

    #  Write cleaned data to Parquet
    print("\nWriting cleaned loans data to Parquet")
    output_path_parquet = f"gs://lending_ara/data/raw/cleaned/loans_parquet"
    loans_purpose_modified.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", output_path_parquet) \
        .save()
    print(f"Cleaned loans data saved to Parquet at {output_path_parquet}")

    #  Write cleaned data to CSV
    print("\nWriting cleaned loans data to CSV")
    output_path_csv = f"gs://lending_ara/data/raw/cleaned/loans_csv"
    loans_purpose_modified.write \
        .option("header", True) \
        .format("csv") \
        .mode("overwrite") \
        .option("path", output_path_csv) \
        .save()
    print(f"Cleaned loans data saved to CSV at {output_path_csv}")

finally:
    # Stop Spark session
    print("\nStopping Spark session.")
    spark.stop() 