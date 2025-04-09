#!/usr/bin/env python3
# PySpark job based on 08-create_unified_view.ipynb - Creating unified customer loan view and table

from pyspark.sql import SparkSession
import getpass

def main():
    # Cell 1: Initialize Spark Session with BigQuery connector (from 07-create_bq_ext_tables.py)
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
    GCP_PROJECT_ID = "eastern-amp-449614-e1"
    GCP_DATASET = "lending_db"

    try:
        # Cell 2: Read all the tables from BigQuery
        print("\nCell 2: Reading tables from BigQuery")
        
        customers_df = spark.read \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.customers") \
            .load()
        print("Customers table loaded")
        
        loans_df = spark.read \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans") \
            .load()
        print("Loans table loaded")
        
        repayments_df = spark.read \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_repayments") \
            .load()
        print("Loans repayments table loaded")
        
        defaulters_delinq_df = spark.read \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_delinq") \
            .load()
        print("Loans defaulters delinq table loaded")
        
        defaulters_detail_df = spark.read \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_defaulters_detail_rec_enq") \
            .load()
        print("Loans defaulters detail table loaded")
        
        # Cell 3: Join the dataframes to create the unified view
        print("\nCell 3: Creating unified customer loan dataframe")
        unified_df = customers_df \
            .join(loans_df, customers_df.member_id == loans_df.member_id, "left") \
            .join(repayments_df, loans_df.loan_id == repayments_df.loan_id, "left") \
            .join(defaulters_delinq_df, customers_df.member_id == defaulters_delinq_df.member_id, "left") \
            .join(defaulters_detail_df, customers_df.member_id == defaulters_detail_df.member_id, "left") \
            .select(
                loans_df.loan_id,
                customers_df.member_id,
                customers_df.emp_title,
                customers_df.emp_length,
                customers_df.home_ownership,
                customers_df.annual_income,
                customers_df.address_state,
                customers_df.address_zipcode,
                customers_df.address_country,
                customers_df.grade,
                customers_df.sub_grade,
                customers_df.verification_status,
                customers_df.total_high_credit_limit,
                customers_df.application_type,
                customers_df.join_annual_income,
                customers_df.verification_status_joint,
                loans_df.loan_amount,
                loans_df.funded_amount,
                loans_df.loan_term_years,
                loans_df.interest_rate,
                loans_df.monthly_installment,
                loans_df.issue_date,
                loans_df.loan_status,
                loans_df.loan_purpose,
                repayments_df.total_principal_received,
                repayments_df.total_interest_received,
                repayments_df.total_late_fee_received,
                repayments_df.last_payment_date,
                repayments_df.next_payment_date,
                defaulters_delinq_df.delinq_2yrs,
                defaulters_delinq_df.delinq_amnt,
                defaulters_delinq_df.mths_since_last_delinq,
                defaulters_detail_df.pub_rec,
                defaulters_detail_df.pub_rec_bankruptcies,
                defaulters_detail_df.inq_last_6mths
            )
        
        # Cell 4: Show the unified dataframe
        print("\nCell 4: Showing sample data from the unified dataframe")
        unified_df.show(5)
        
        # Cell 5: Create a materialized table from the unified dataframe
        print("\nCell 5: Creating table from the unified dataframe")
        unified_df.write \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.customers_loan_t") \
            .option("temporaryGcsBucket", "dataproc-temp-asia-south1-877288389327-qnexfrfb") \
            .mode("overwrite") \
            .save()
        print(f"Table {GCP_PROJECT_ID}.{GCP_DATASET}.customers_loan_t created.")
        
        # Note: BigQuery views need to be created directly in BigQuery, not through Spark
        print("\nNote: To create the view customers_loan_v, please use the BigQuery console or bq command-line tool with the following SQL:")
        view_sql = f"""
        CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{GCP_DATASET}.customers_loan_v` AS 
        SELECT * FROM `{GCP_PROJECT_ID}.{GCP_DATASET}.customers_loan_t`
        """
        print(view_sql)

    finally:
        # Stop Spark session
        print("\nStopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main() 