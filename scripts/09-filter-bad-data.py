
# Customer Data Analysis and Deduplication

from pyspark.sql import SparkSession
import getpass
import os

def main():
    # Initialize Spark Session with BigQuery connector 
    username = getpass.getuser()
    spark = SparkSession. \
        builder. \
        config('spark.ui.port','0'). \
        config('spark.shuffle.useOldFetchProtocol', 'true'). \
        config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0'). \
        config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem'). \
        config('spark.hadoop.fs.gs.auth.service.account.enable', 'true'). \
        config('spark.hadoop.fs.gs.auth.service.account.json.keyfile', 'gs://lending_ara/code/gcp-creds.json'). \
        master('yarn'). \
        getOrCreate()
    
    spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-south1-877288389327-qnexfrfb')
    
    # Set your GCP project ID and BigQuery dataset
    GCP_PROJECT_ID = "eastern-amp-449614-e1"
    GCP_DATASET = "lending_db"
    GCS_BUCKET = "lending_ara"
    GCS_DATA_PATH = f"gs://{GCS_BUCKET}/data"
    
    print("Spark session created with BigQuery connector.")
    
    # Read tables from BigQuery first
    print("\nReading BigQuery tables into DataFrames")
    customers_df = spark.read \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.customers") \
        .load()
    print("Customers table loaded")
    
    loans_defaulters_delinq_df = spark.read \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_delinq") \
        .load()
    print("Loans defaulters delinq table loaded")
    
    loans_defaulters_detail_rec_enq_df = spark.read \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_detail_rec_enq") \
        .load()
    print("Loans defaulters detail rec enq table loaded")
    
    # Register DataFrames as temporary views for SQL operations
    customers_df.createOrReplaceTempView("customers")
    loans_defaulters_delinq_df.createOrReplaceTempView("loans_defaulters_delinq")
    loans_defaulters_detail_rec_enq_df.createOrReplaceTempView("loans_defaulters_detail_rec_enq")
    
    # Now we can use regular SQL without referencing BigQuery directly
    
    # Check for duplicate member_ids in customers table
    print("\nChecking for duplicate member_ids in customers table:")
    customers_dupes = spark.sql("""
        SELECT member_id, COUNT(*) as total 
        FROM customers
        GROUP BY member_id 
        ORDER BY total DESC
    """)
    customers_dupes.show(20)
    
    # Examine duplicate records for a specific member_id
    print("\nExamining duplicate records for member_id 'e4c167053d5418230':")
    spark.sql("""
        SELECT * FROM customers 
        WHERE member_id LIKE 'e4c167053d5418230%'
    """).show()
    
    # Check for duplicate member_ids in loans_defaulters_delinq table
    print("\nChecking for duplicate member_ids in loans_defaulters_delinq table:")
    delinq_dupes = spark.sql("""
        SELECT member_id, COUNT(*) as total 
        FROM loans_defaulters_delinq
        GROUP BY member_id 
        ORDER BY total DESC
    """)
    delinq_dupes.show(20)
    
    # Examine duplicate records for a specific member_id in loans_defaulters_delinq
    print("\nExamining duplicate records in loans_defaulters_delinq:")
    spark.sql("""
        SELECT * FROM loans_defaulters_delinq
        WHERE member_id LIKE 'e4c167053d5418230%'
    """).show()
    
    # Check for duplicate member_ids in loans_defaulters_detail_rec_enq table
    print("\nChecking for duplicate member_ids in loans_defaulters_detail_rec_enq table:")
    detail_rec_dupes = spark.sql("""
        SELECT member_id, COUNT(*) as total 
        FROM loans_defaulters_detail_rec_enq
        GROUP BY member_id 
        ORDER BY total DESC
    """)
    detail_rec_dupes.show(20)
    
    # Examine duplicate records for a specific member_id in loans_defaulters_detail_rec_enq
    print("\nExamining duplicate records in loans_defaulters_detail_rec_enq:")
    spark.sql("""
        SELECT * FROM loans_defaulters_detail_rec_enq
        WHERE member_id LIKE 'e4c167053d5418230%'
    """).show()
    
    # Create DataFrame for bad data in customers table
    print("\nCreating DataFrame for customers with duplicates:")
    bad_data_customer_df = spark.sql("""
        SELECT member_id FROM (
            SELECT member_id, COUNT(*) as total 
            FROM customers
            GROUP BY member_id 
            HAVING total > 1
        )
    """)
    
    # Count bad data in customers
    print("\nCounting customers with duplicates:")
    bad_customer_count = bad_data_customer_df.count()
    print(f"Number of customers with duplicates: {bad_customer_count}")
    bad_data_customer_df.show(20)
    
    #  Create DataFrame for bad data in loans_defaulters_delinq table
    print("\n Creating DataFrame for loans_defaulters_delinq with duplicates:")
    bad_data_loans_defaulters_delinq_df = spark.sql("""
        SELECT member_id FROM (
            SELECT member_id, COUNT(*) as total 
            FROM loans_defaulters_delinq
            GROUP BY member_id 
            HAVING total > 1
        )
    """)
    
    #  Count bad data in loans_defaulters_delinq
    print("\n Counting loans_defaulters_delinq with duplicates:")
    bad_delinq_count = bad_data_loans_defaulters_delinq_df.count()
    print(f"Number of loans_defaulters_delinq with duplicates: {bad_delinq_count}")
    bad_data_loans_defaulters_delinq_df.show(20)
    
    #  Create DataFrame for bad data in loans_defaulters_detail_rec_enq table
    print("\n Creating DataFrame for loans_defaulters_detail_rec_enq with duplicates:")
    bad_data_loans_defaulters_detail_rec_enq_df = spark.sql("""
        SELECT member_id FROM (
            SELECT member_id, COUNT(*) as total 
            FROM loans_defaulters_detail_rec_enq
            GROUP BY member_id 
            HAVING total > 1
        )
    """)
    
    #  Count bad data in loans_defaulters_detail_rec_enq
    print("\n Counting loans_defaulters_detail_rec_enq with duplicates:")
    bad_detail_rec_count = bad_data_loans_defaulters_detail_rec_enq_df.count()
    print(f"Number of loans_defaulters_detail_rec_enq with duplicates: {bad_detail_rec_count}")
    bad_data_loans_defaulters_detail_rec_enq_df.show(20)
    
    #  Save bad customers data to GCS
    print("\n Saving bad customers data to GCS:")
    bad_data_customer_df.repartition(1).write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .option("path", f"{GCS_DATA_PATH}/bad/bad_data_customers") \
        .save()
    print(f"Saved to {GCS_DATA_PATH}/bad/bad_data_customers")
    
    #  Save bad loans_defaulters_delinq data to GCS
    print("\n Saving bad loans_defaulters_delinq data to GCS:")
    bad_data_loans_defaulters_delinq_df.repartition(1).write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .option("path", f"{GCS_DATA_PATH}/bad/bad_data_loans_defaulters_delinq") \
        .save()
    print(f"Saved to {GCS_DATA_PATH}/bad/bad_data_loans_defaulters_delinq")
    
    #  Save bad loans_defaulters_detail_rec_enq data to GCS
    print("\n Saving bad loans_defaulters_detail_rec_enq data to GCS:")
    bad_data_loans_defaulters_detail_rec_enq_df.repartition(1).write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .option("path", f"{GCS_DATA_PATH}/bad/bad_data_loans_defaulters_detail_rec_enq") \
        .save()
    print(f"Saved to {GCS_DATA_PATH}/bad/bad_data_loans_defaulters_detail_rec_enq")
    
    #  Combine all bad data into one DataFrame
    print("\n Combining all bad data into one DataFrame:")
    bad_customer_data_df = bad_data_customer_df.select("member_id") \
        .union(bad_data_loans_defaulters_delinq_df.select("member_id")) \
        .union(bad_data_loans_defaulters_detail_rec_enq_df.select("member_id"))
    
    #  Remove duplicates from the combined bad data
    print("\n Removing duplicates from the combined bad data:")
    bad_customer_data_final_df = bad_customer_data_df.distinct()
    
    #  Count total unique bad member_ids
    print("\n Counting total unique bad member_ids:")
    bad_customer_total = bad_customer_data_final_df.count()
    print(f"Total unique member_ids with duplicates: {bad_customer_total}")
    
    #  Save combined bad data to GCS
    print("\n Saving combined bad data to GCS:")
    bad_customer_data_final_df.repartition(1).write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .option("path", f"{GCS_DATA_PATH}/bad/bad_customer_data_final") \
        .save()
    print(f"Saved to {GCS_DATA_PATH}/bad/bad_customer_data_final")
    
    #  Create a temporary view for the bad data
    print("\n Creating a temporary view for the bad data:")
    bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")
    
    #  Filter out bad data from customers table
    print("\n Filtering out bad data from customers table:")
    filtered_customers_df = spark.sql("""
        SELECT * FROM customers
        WHERE member_id NOT IN (SELECT member_id FROM bad_data_customer)
    """)
    
    #  Save cleaned customers data to GCS
    print("\n Saving cleaned customers data to GCS:")
    filtered_customers_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", f"{GCS_DATA_PATH}/cleaned_new/customers_parquet") \
        .save()
    print(f"Saved to {GCS_DATA_PATH}/cleaned_new/customers_parquet")
    
    #  Filter out bad data from loans_defaulters_delinq table
    print("\n Filtering out bad data from loans_defaulters_delinq table:")
    filtered_loans_defaulters_delinq_df = spark.sql("""
        SELECT * FROM loans_defaulters_delinq
        WHERE member_id NOT IN (SELECT member_id FROM bad_data_customer)
    """)
    
    #  Save cleaned loans_defaulters_delinq data to GCS
    print("\n Saving cleaned loans_defaulters_delinq data to GCS:")
    filtered_loans_defaulters_delinq_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", f"{GCS_DATA_PATH}/cleaned_new/loans_defaulters_delinq_parquet") \
        .save()
    print(f"Saved to {GCS_DATA_PATH}/cleaned_new/loans_defaulters_delinq_parquet")
    
    #  Filter out bad data from loans_defaulters_detail_rec_enq table
    print("\n Filtering out bad data from loans_defaulters_detail_rec_enq table:")
    filtered_loans_defaulters_detail_rec_enq_df = spark.sql("""
        SELECT * FROM loans_defaulters_detail_rec_enq
        WHERE member_id NOT IN (SELECT member_id FROM bad_data_customer)
    """)
    
    #  Save cleaned loans_defaulters_detail_rec_enq data to GCS
    print("\n Saving cleaned loans_defaulters_detail_rec_enq data to GCS:")
    filtered_loans_defaulters_detail_rec_enq_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", f"{GCS_DATA_PATH}/cleaned_new/loans_defaulters_detail_rec_enq_parquet") \
        .save()
    print(f"Saved to {GCS_DATA_PATH}/cleaned_new/loans_defaulters_detail_rec_enq_parquet")
    
    #  Writing cleaned customers data to BigQuery
    print("\n Writing cleaned customers data to BigQuery")
    filtered_customers_df.write \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.customers_new") \
        .option("temporaryGcsBucket", "dataproc-temp-asia-south1-877288389327-qnexfrfb") \
        .mode("overwrite") \
        .save()
    print(f"Table {GCP_PROJECT_ID}.{GCP_DATASET}.customers_new created.")
    
    #  Writing cleaned loans_defaulters_delinq data to BigQuery
    print("\n Writing cleaned loans_defaulters_delinq data to BigQuery")
    filtered_loans_defaulters_delinq_df.write \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_delinq_new") \
        .option("temporaryGcsBucket", "dataproc-temp-asia-south1-877288389327-qnexfrfb") \
        .mode("overwrite") \
        .save()
    print(f"Table {GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_delinq_new created.")
    
    #  Writing cleaned loans_defaulters_detail_rec_enq data to BigQuery
    print("\n Writing cleaned loans_defaulters_detail_rec_enq data to BigQuery")
    filtered_loans_defaulters_detail_rec_enq_df.write \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_detail_rec_enq_new") \
        .option("temporaryGcsBucket", "dataproc-temp-asia-south1-877288389327-qnexfrfb") \
        .mode("overwrite") \
        .save()
    print(f"Table {GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_detail_rec_enq_new created.")
    
    #  Load the new customers table and check
    print("\n Verifying deduplication in customers_new:")
    customers_new_df = spark.read \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.customers_new") \
        .load()
    customers_new_df.createOrReplaceTempView("customers_new")
    
    spark.sql("""
        SELECT member_id, COUNT(*) as total 
        FROM customers_new
        GROUP BY member_id 
        ORDER BY total DESC
    """).show(20)
    
    print("Processing completed successfully.")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main() 