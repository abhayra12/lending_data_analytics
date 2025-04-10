#!/usr/bin/env python3
# PySpark job based on 07-LendingClub_S2_DP.ipynb - Customer Data Processing with BigQuery

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

    print("Spark session created with BigQuery connector.")

    # Set your GCP project ID and BigQuery dataset
    GCP_PROJECT_ID = "eastern-amp-449614-e1"  # Updated project ID
    GCP_DATASET = "lending_db"  # Your BigQuery dataset name
    GCS_BUCKET = "lending_ara"  # Your GCS bucket name

    try:
        # Read customers data from GCS
        print("\nReading customers data from GCS")
        customers_df = spark.read \
            .format("parquet") \
            .load(f"gs://{GCS_BUCKET}/data/raw/cleaned/customers_parquet")
        
        # Show customers data schema
        print("\nShowing customers data schema")
        customers_df.printSchema()
        
        # Create BigQuery dataset (if it doesn't exist)
        print(f"\nCreating BigQuery dataset {GCP_DATASET}")
        # Note: Dataset will be created automatically when creating tables
        print(f"BigQuery dataset {GCP_DATASET} will be created with the first table.")
        
        # Create table for customers in BigQuery
        print("\nCreating table for customers in BigQuery")
        customers_df.write \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.customers") \
            .option("temporaryGcsBucket", "dataproc-temp-asia-south1-877288389327-qnexfrfb") \
            .mode("overwrite") \
            .save()
        print(f"Table {GCP_PROJECT_ID}.{GCP_DATASET}.customers created.")
        
        # Show customers table data
        print(f"\nShowing sample data from customers_df")
        customers_df.show(5)
        
        # Read and create table for loans in BigQuery
        print("\nReading and creating table for loans in BigQuery")
        loans_df = spark.read \
            .format("parquet") \
            .load(f"gs://{GCS_BUCKET}/data/raw/cleaned/loans_parquet")

        loans_df.write \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans") \
            .option("temporaryGcsBucket", "dataproc-temp-asia-south1-877288389327-qnexfrfb") \
            .mode("overwrite") \
            .save()
        print(f"Table {GCP_PROJECT_ID}.{GCP_DATASET}.loans created.")
        
        # Show loans table data
        print(f"\nShowing sample data from loans_df")
        loans_df.show(5)
        
        # Read and create table for loans_repayments in BigQuery
        print("\nReading and creating table for loans_repayments in BigQuery")
        repayments_df = spark.read \
            .format("parquet") \
            .load(f"gs://{GCS_BUCKET}/data/raw/cleaned/loans_repayments_parquet")
        
        repayments_df.write \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_repayments") \
            .option("temporaryGcsBucket", "dataproc-temp-asia-south1-877288389327-qnexfrfb") \
            .mode("overwrite") \
            .save()
        print(f"Table {GCP_PROJECT_ID}.{GCP_DATASET}.loans_repayments created.")
        
        #  Show loans_repayments table data
        print(f"\n Showing sample data from repayments_df")
        repayments_df.show(5)
        
        #  Read and create table for loans_defaulters_delinq in BigQuery
        print("\n Reading and creating table for loans_defaulters_delinq in BigQuery")
        defaulters_delinq_df = spark.read \
            .format("parquet") \
            .load(f"gs://{GCS_BUCKET}/data/raw/cleaned/loans_defaulters_deling_parquet")
        
        defaulters_delinq_df.write \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_delinq") \
            .option("temporaryGcsBucket", "dataproc-temp-asia-south1-877288389327-qnexfrfb") \
            .mode("overwrite") \
            .save()
        print(f"Table {GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_delinq created.")
        
        #  Show loans_defaulters_delinq table data
        print(f"\n Showing sample data from defaulters_delinq_df")
        defaulters_delinq_df.show(5)
        
        #  Read and create table for loans_defaulters_detail_rec_enq in BigQuery
        print("\n Reading and creating table for loans_defaulters_detail_rec_enq in BigQuery")
        defaulters_detail_df = spark.read \
            .format("parquet") \
            .load(f"gs://{GCS_BUCKET}/data/raw/cleaned/loans_def_detail_records_enq_df_parquet")
        
        defaulters_detail_df.write \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_detail_rec_enq") \
            .option("temporaryGcsBucket", "dataproc-temp-asia-south1-877288389327-qnexfrfb") \
            .mode("overwrite") \
            .save()
        print(f"Table {GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_detail_rec_enq created.")
        
        #  Show loans_defaulters_detail_rec_enq table data
        print(f"\n Showing sample data from defaulters_detail_df")
        defaulters_detail_df.show(5)

    finally:
        # Stop Spark session
        print("\nStopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main()  