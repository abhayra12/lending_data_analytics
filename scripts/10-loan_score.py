#!/usr/bin/env python3
# PySpark job based on 10-LendingClub_S7.ipynb - Loan Score Calculation

from pyspark.sql import SparkSession
import getpass
import os

def main():
    # Cell 1: Initialize Spark Session with BigQuery connector
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

    try:
        # Cell 2-3: Configure scoring points for loan scoring system
        # Points for different ratings
        spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
        spark.conf.set("spark.sql.very_bad_rated_pts", 100)
        spark.conf.set("spark.sql.bad_rated_pts", 250)
        spark.conf.set("spark.sql.good_rated_pts", 500)
        spark.conf.set("spark.sql.very_good_rated_pts", 650)
        spark.conf.set("spark.sql.excellent_rated_pts", 800)
        
        # Points for different grades
        spark.conf.set("spark.sql.unacceptable_grade_pts", 750)
        spark.conf.set("spark.sql.very_bad_grade_pts", 1000)
        spark.conf.set("spark.sql.bad_grade_pts", 1500)
        spark.conf.set("spark.sql.good_grade_pts", 2000)
        spark.conf.set("spark.sql.very_good_grade_pts", 2500)
        
        # Load tables from BigQuery first
        print("\nLoading tables from BigQuery")
        loans_repayments_df = spark.read \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans_repayments") \
            .load()
        print("Loans repayments table loaded")

        loans_df = spark.read \
            .format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{GCP_DATASET}.loans") \
            .load()
        print("Loans table loaded")

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
        loans_repayments_df.createOrReplaceTempView("loans_repayments")
        loans_df.createOrReplaceTempView("loans")
        customers_df.createOrReplaceTempView("customers")
        loans_defaulters_delinq_df.createOrReplaceTempView("loans_defaulters_delinq")
        loans_defaulters_detail_rec_enq_df.createOrReplaceTempView("loans_defaulters_detail_rec_enq")
        
        # Cell 4-5: Load bad customer data
        print("\nLoading bad customer data")
        bad_customer_data_final_df = spark.read \
            .format("csv") \
            .option("header", True) \
            .option("inferSchema", True) \
            .load(f"gs://{GCS_BUCKET}/data/bad/bad_customer_data_final")
        
        bad_customer_data_final_df.createOrReplaceTempView("bad_data_customer")
        
        # Cell 6-7: Calculate Payment History (ph) points
        print("\nCalculating Payment History (ph) points")
        ph_df = spark.sql("""
            SELECT 
                c.member_id, 
                CASE 
                    WHEN p.last_payment_amount < (c.monthly_installment * 0.5) THEN ${spark.sql.very_bad_rated_pts} 
                    WHEN p.last_payment_amount >= (c.monthly_installment * 0.5) AND 
                         p.last_payment_amount < c.monthly_installment THEN ${spark.sql.very_bad_rated_pts} 
                    WHEN (p.last_payment_amount = (c.monthly_installment)) THEN ${spark.sql.good_rated_pts} 
                    WHEN p.last_payment_amount > (c.monthly_installment) AND 
                         p.last_payment_amount <= (c.monthly_installment * 1.50) THEN ${spark.sql.very_good_rated_pts} 
                    WHEN p.last_payment_amount > (c.monthly_installment * 1.50) THEN ${spark.sql.excellent_rated_pts} 
                    ELSE ${spark.sql.unacceptable_rated_pts} 
                END AS last_payment_pts, 
                CASE 
                    WHEN p.total_payment_received >= (c.funded_amount * 0.50) THEN ${spark.sql.very_good_rated_pts} 
                    WHEN p.total_payment_received < (c.funded_amount * 0.50) AND 
                         p.total_payment_received > 0 THEN ${spark.sql.good_rated_pts} 
                    WHEN p.total_payment_received = 0 OR 
                         (p.total_payment_received) IS NULL THEN ${spark.sql.unacceptable_rated_pts} 
                END AS total_payment_pts 
            FROM loans_repayments p 
            INNER JOIN loans c 
            ON c.loan_id = p.loan_id 
            WHERE member_id NOT IN (SELECT member_id FROM bad_data_customer)
        """)
        
        ph_df.createOrReplaceTempView("ph_pts")
        print("Payment History Points sample:")
        ph_df.show(5)
        
        # Cell 9-10: Loan Defaulters History(ldh)
        print("\nCalculating Loan Defaulters History (ldh) points")
        ldh_ph_df = spark.sql("""
            SELECT p.*, 
            CASE 
            WHEN d.delinq_2yrs = 0 THEN ${spark.sql.excellent_rated_pts} 
            WHEN d.delinq_2yrs BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} 
            WHEN d.delinq_2yrs BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} 
            WHEN d.delinq_2yrs > 5 OR d.delinq_2yrs IS NULL THEN ${spark.sql.unacceptable_grade_pts} 
            END AS delinq_pts, 
            CASE 
            WHEN l.pub_rec = 0 THEN ${spark.sql.excellent_rated_pts} 
            WHEN l.pub_rec BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} 
            WHEN l.pub_rec BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} 
            WHEN l.pub_rec > 5 OR l.pub_rec IS NULL THEN ${spark.sql.very_bad_rated_pts} 
            END AS public_records_pts, 
            CASE 
            WHEN l.pub_rec_bankruptcies = 0 THEN ${spark.sql.excellent_rated_pts} 
            WHEN l.pub_rec_bankruptcies BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} 
            WHEN l.pub_rec_bankruptcies BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} 
            WHEN l.pub_rec_bankruptcies > 5 OR l.pub_rec_bankruptcies IS NULL THEN ${spark.sql.very_bad_rated_pts} 
            END as public_bankruptcies_pts, 
            CASE 
            WHEN l.inq_last_6mths = 0 THEN ${spark.sql.excellent_rated_pts} 
            WHEN l.inq_last_6mths BETWEEN 1 AND 2 THEN ${spark.sql.bad_rated_pts} 
            WHEN l.inq_last_6mths BETWEEN 3 AND 5 THEN ${spark.sql.very_bad_rated_pts} 
            WHEN l.inq_last_6mths > 5 OR l.inq_last_6mths IS NULL THEN ${spark.sql.unacceptable_rated_pts} 
            END AS enq_pts 
            FROM loans_defaulters_detail_rec_enq l 
            INNER JOIN loans_defaulters_delinq d ON d.member_id = l.member_id  
            INNER JOIN ph_pts p ON p.member_id = l.member_id 
            WHERE l.member_id NOT IN (SELECT member_id FROM bad_data_customer)
        """)
        
        ldh_ph_df.createOrReplaceTempView("ldh_ph_pts")
        print("Loan Defaulters History Points sample:")
        ldh_ph_df.show(5)
        
        # Cell 12-13: Financial Health
        print("\nCalculating Financial Health points")
        fh_ldh_ph_df = spark.sql("""
            SELECT ldef.*, 
            CASE 
            WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN ${spark.sql.excellent_rated_pts} 
            WHEN LOWER(l.loan_status) LIKE '%current%' THEN ${spark.sql.good_rated_pts} 
            WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN ${spark.sql.bad_rated_pts} 
            WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' THEN ${spark.sql.very_bad_rated_pts} 
            WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN ${spark.sql.unacceptable_rated_pts} 
            ELSE ${spark.sql.unacceptable_rated_pts} 
            END AS loan_status_pts, 
            CASE 
            WHEN LOWER(a.home_ownership) LIKE '%own' THEN ${spark.sql.excellent_rated_pts} 
            WHEN LOWER(a.home_ownership) LIKE '%rent' THEN ${spark.sql.good_rated_pts} 
            WHEN LOWER(a.home_ownership) LIKE '%mortgage' THEN ${spark.sql.bad_rated_pts} 
            WHEN LOWER(a.home_ownership) LIKE '%any' OR LOWER(a.home_ownership) IS NULL THEN ${spark.sql.very_bad_rated_pts} 
            END AS home_pts, 
            CASE 
            WHEN l.funded_amount <= (a.total_high_credit_limit * 0.10) THEN ${spark.sql.excellent_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 0.10) AND l.funded_amount <= (a.total_high_credit_limit * 0.20) THEN ${spark.sql.very_good_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 0.20) AND l.funded_amount <= (a.total_high_credit_limit * 0.30) THEN ${spark.sql.good_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 0.30) AND l.funded_amount <= (a.total_high_credit_limit * 0.50) THEN ${spark.sql.bad_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 0.50) AND l.funded_amount <= (a.total_high_credit_limit * 0.70) THEN ${spark.sql.very_bad_rated_pts} 
            WHEN l.funded_amount > (a.total_high_credit_limit * 0.70) THEN ${spark.sql.unacceptable_rated_pts} 
            ELSE ${spark.sql.unacceptable_rated_pts} 
            END AS credit_limit_pts, 
            CASE 
            WHEN (a.grade) = 'A' and (a.sub_grade)='A1' THEN ${spark.sql.excellent_rated_pts} 
            WHEN (a.grade) = 'A' and (a.sub_grade)='A2' THEN (${spark.sql.excellent_rated_pts} * 0.95) 
            WHEN (a.grade) = 'A' and (a.sub_grade)='A3' THEN (${spark.sql.excellent_rated_pts} * 0.90) 
            WHEN (a.grade) = 'A' and (a.sub_grade)='A4' THEN (${spark.sql.excellent_rated_pts} * 0.85) 
            WHEN (a.grade) = 'A' and (a.sub_grade)='A5' THEN (${spark.sql.excellent_rated_pts} * 0.80) 
            WHEN (a.grade) = 'B' and (a.sub_grade)='B1' THEN (${spark.sql.very_good_rated_pts}) 
            WHEN (a.grade) = 'B' and (a.sub_grade)='B2' THEN (${spark.sql.very_good_rated_pts} * 0.95) 
            WHEN (a.grade) = 'B' and (a.sub_grade)='B3' THEN (${spark.sql.very_good_rated_pts} * 0.90) 
            WHEN (a.grade) = 'B' and (a.sub_grade)='B4' THEN (${spark.sql.very_good_rated_pts} * 0.85) 
            WHEN (a.grade) = 'B' and (a.sub_grade)='B5' THEN (${spark.sql.very_good_rated_pts} * 0.80) 
            WHEN (a.grade) = 'C' and (a.sub_grade)='C1' THEN (${spark.sql.good_rated_pts}) 
            WHEN (a.grade) = 'C' and (a.sub_grade)='C2' THEN (${spark.sql.good_rated_pts} * 0.95) 
            WHEN (a.grade) = 'C' and (a.sub_grade)='C3' THEN (${spark.sql.good_rated_pts} * 0.90) 
            WHEN (a.grade) = 'C' and (a.sub_grade)='C4' THEN (${spark.sql.good_rated_pts} * 0.85) 
            WHEN (a.grade) = 'C' and (a.sub_grade)='C5' THEN (${spark.sql.good_rated_pts} * 0.80) 
            WHEN (a.grade) = 'D' and (a.sub_grade)='D1' THEN (${spark.sql.bad_rated_pts}) 
            WHEN (a.grade) = 'D' and (a.sub_grade)='D2' THEN (${spark.sql.bad_rated_pts} * 0.95) 
            WHEN (a.grade) = 'D' and (a.sub_grade)='D3' THEN (${spark.sql.bad_rated_pts} * 0.90) 
            WHEN (a.grade) = 'D' and (a.sub_grade)='D4' THEN (${spark.sql.bad_rated_pts} * 0.85) 
            WHEN (a.grade) = 'D' and (a.sub_grade)='D5' THEN (${spark.sql.bad_rated_pts} * 0.80) 
            WHEN (a.grade) = 'E' and (a.sub_grade)='E1' THEN (${spark.sql.very_bad_rated_pts}) 
            WHEN (a.grade) = 'E' and (a.sub_grade)='E2' THEN (${spark.sql.very_bad_rated_pts} * 0.95) 
            WHEN (a.grade) = 'E' and (a.sub_grade)='E3' THEN (${spark.sql.very_bad_rated_pts} * 0.90) 
            WHEN (a.grade) = 'E' and (a.sub_grade)='E4' THEN (${spark.sql.very_bad_rated_pts} * 0.85) 
            WHEN (a.grade) = 'E' and (a.sub_grade)='E5' THEN (${spark.sql.very_bad_rated_pts} * 0.80) 
            WHEN (a.grade) in ('F', 'G') THEN (${spark.sql.unacceptable_rated_pts}) 
            END AS grade_pts 
            FROM ldh_ph_pts ldef 
            INNER JOIN loans l ON ldef.member_id = l.member_id 
            INNER JOIN customers a ON a.member_id = ldef.member_id 
            WHERE ldef.member_id NOT IN (SELECT member_id FROM bad_data_customer)
        """)
        
        fh_ldh_ph_df.createOrReplaceTempView("fh_ldh_ph_pts")
        print("Financial Health Points sample:")
        fh_ldh_ph_df.show(5)
        
        # Cell 14-15: Final loan score calculation with weights
        # 1. Payment History = 20%
        # 2. Loan Defaults = 45%
        # 3. Financial Health = 35%
        print("\nCalculating final loan score with weights")
        loan_score = spark.sql("""
            SELECT member_id, 
            ((last_payment_pts+total_payment_pts)*0.20) as payment_history_pts, 
            ((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) as defaulters_history_pts, 
            ((loan_status_pts + home_pts + credit_limit_pts + grade_pts)*0.35) as financial_health_pts 
            FROM fh_ldh_ph_pts
        """)
        
        print("Loan Score components:")
        loan_score.show(5)
        
        # Cell 16-17: Add total loan score column
        final_loan_score = loan_score.withColumn('loan_score', 
                                              loan_score.payment_history_pts + 
                                              loan_score.defaulters_history_pts + 
                                              loan_score.financial_health_pts)
        
        final_loan_score.createOrReplaceTempView("loan_score_eval")
        
        # Cell 18-19: Assign loan grades based on score
        print("\nAssigning loan grades based on score")
        loan_score_final = spark.sql("""
            SELECT ls.*, 
            CASE 
            WHEN loan_score > ${spark.sql.very_good_grade_pts} THEN 'A' 
            WHEN loan_score <= ${spark.sql.very_good_grade_pts} AND loan_score > ${spark.sql.good_grade_pts} THEN 'B' 
            WHEN loan_score <= ${spark.sql.good_grade_pts} AND loan_score > ${spark.sql.bad_grade_pts} THEN 'C' 
            WHEN loan_score <= ${spark.sql.bad_grade_pts} AND loan_score  > ${spark.sql.very_bad_grade_pts} THEN 'D' 
            WHEN loan_score <= ${spark.sql.very_bad_grade_pts} AND loan_score > ${spark.sql.unacceptable_grade_pts} THEN 'E'  
            WHEN loan_score <= ${spark.sql.unacceptable_grade_pts} THEN 'F' 
            END as loan_final_grade 
            FROM loan_score_eval ls
        """)
        
        loan_score_final.createOrReplaceTempView("loan_final_table")
        
        # Show sample of final loan scores
        print("Final loan scores sample:")
        loan_score_final.show(5)
        
        # Cell 22: Save results to GCS
        print("\nSaving loan score results to GCS")
        loan_score_final.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", f"gs://{GCS_BUCKET}/data/processed/loan_score") \
            .save()
        
        print(f"Loan score data saved to gs://{GCS_BUCKET}/data/processed/loan_score")

    finally:
        # Stop Spark session
        print("\nStopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main() 