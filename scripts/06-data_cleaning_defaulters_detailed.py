#!/usr/bin/env python3
# PySpark job based on 06-LendingClub_S1.ipynb

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor

def main():
    #  Initialize Spark Session
    spark = SparkSession. \
        builder. \
        config('spark.shuffle.useOldFetchProtocol', 'true'). \
        enableHiveSupport(). \
        master('yarn'). \
        getOrCreate()

    print("Spark session created.")

    try:
        #  Read loans defaulters raw data (inferring schema initially)
        print("\nReading loans defaulters raw data (initial load)")
        loans_def_raw_df_initial = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("gs://lending_ara/data/raw/loans_defaulters_csv")
        print("Initial loans_def_raw_df loaded.")

        #  Display initial raw DataFrame
        print("\nDisplaying initial raw DataFrame (first 20 rows)")
        loans_def_raw_df_initial.show()

        #  Print schema of initial raw DataFrame
        print("\nPrinting schema of initial raw DataFrame")
        loans_def_raw_df_initial.printSchema()

        #  Create temporary view 'loan_defaulters' from initial raw data
        print("\nCreating temporary view 'loan_defaulters'")
        loans_def_raw_df_initial.createOrReplaceTempView("loan_defaulters")
        print("Temporary view 'loan_defaulters' created.")

        #  SQL query: Select distinct delinq_2yrs
        print("\nquery: Select distinct delinq_2yrs")
        spark.sql("select distinct(delinq_2yrs) from loan_defaulters").show()

        #  SQL query: Group by delinq_2yrs and count (Show top 40)
        print("\nquery: Group by delinq_2yrs and count (Show top 40)")
        spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)

        #  Define loan defaulters schema
        print("\nDefining loan defaulters schema")
        loan_defaulters_schema = "member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float,inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"
        print(f"Loan defaulters schema: {loan_defaulters_schema}")

        #  Read loans defaulters raw data with defined schema
        print("\nReading loans defaulters raw data with defined schema")
        loans_def_raw_df = spark.read \
            .format("csv") \
            .option("header", True) \
            .schema(loan_defaulters_schema) \
            .load("gs://lending_ara/data/raw/loans_defaulters_csv")
        print("loans_def_raw_df with schema created.")

        # Replace temporary view 'loan_defaulters' with schema-applied data
        print("\nReplacing temporary view 'loan_defaulters' with schema-applied data")
        loans_def_raw_df.createOrReplaceTempView("loan_defaulters")
        print("Temporary view 'loan_defaulters' updated.")

        #  SQL query: Group by delinq_2yrs and count again (Show top 40)
        print("\n SQL query: Group by delinq_2yrs and count again (Show top 40)")
        spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)

        # Process delinq_2yrs (cast to integer, fill nulls with 0)
        print("\nProcess delinq_2yrs (cast to integer, fill nulls with 0)")
        loans_def_processed_df = loans_def_raw_df.withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")).fillna(0, subset=["delinq_2yrs"])
        print("delinq_2yrs processed.")

        # : Replace temporary view 'loan_defaulters' with processed data
        print("\nReplacing temporary view 'loan_defaulters' with processed data")
        loans_def_processed_df.createOrReplaceTempView("loan_defaulters")
        print("Temporary view 'loan_defaulters' updated.")

        # SQL query: Count null delinq_2yrs after processing
        print("\nSQL query: Count null delinq_2yrs after processing")
        spark.sql("select count(*) as null_delinq_count from loan_defaulters where delinq_2yrs is null").show()

        # SQL query: Group by delinq_2yrs and count after processing (Show top 40)
        print("\nSQL query: Group by delinq_2yrs and count after processing (Show top 40)")
        spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)

        # SQL query: Select delinquent members
        print("\nSQL query: Select delinquent members")
        loans_def_delinq_df = spark.sql("select member_id, delinq_2yrs, delinq_amnt, int(mths_since_last_delinq) as mths_since_last_delinq from loan_defaulters where delinq_2yrs > 0 or mths_since_last_delinq > 0")
        print("Selected delinquent members.")

        # Display delinquent members DataFrame
        print("\nDisplay delinquent members DataFrame")
        loans_def_delinq_df.show()

        # Count delinquent members
        print("\nCounting delinquent members")
        delinquent_count = loans_def_delinq_df.count()
        print(f"Delinquent members count: {delinquent_count}")

        # SQL query: Select members with public records/bankruptcies/inquiries
        print("\nSQL query: Select members with public records/bankruptcies/inquiries")
        loans_def_records_enq_df = spark.sql("select member_id from loan_defaulters where pub_rec > 0.0 or pub_rec_bankruptcies > 0.0 or inq_last_6mths > 0.0")
        print("Selected members with public records/bankruptcies/inquiries.")

        # Display members with records/inquiries DataFrame
        print("\nDisplaying members with records/inquiries DataFrame (first 20 rows)")
        loans_def_records_enq_df.show()

        # Count members with records/inquiries
        print("\nCounting members with records/inquiries")
        records_enq_count = loans_def_records_enq_df.count()
        print(f"Members with records/inquiries count: {records_enq_count}")

        # Write delinquent data to CSV
        print("\nWriting delinquent data to CSV")
        output_path_deling_csv = f"gs://lending_ara/data/raw/cleaned/loans_defaulters_deling_csv"
        loans_def_delinq_df.write \
            .option("header", True) \
            .format("csv") \
            .mode("overwrite") \
            .option("path", output_path_deling_csv) \
            .save()
        print(f"Delinquent data saved to CSV at {output_path_deling_csv}")

        # Write delinquent data to Parquet
        print("\nWriting delinquent data to Parquet")
        output_path_deling_parquet = f"gs://lending_ara/data/raw/cleaned/loans_defaulters_deling_parquet"
        loans_def_delinq_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", output_path_deling_parquet) \
            .save()
        print(f"Delinquent data saved to Parquet at {output_path_deling_parquet}")

        # Write records/enquiries data to CSV
        print("\nWriting records/enquiries data to CSV")
        output_path_records_csv = f"gs://lending_ara/data/raw/cleaned/loans_defaulters_records_enq_csv"
        loans_def_records_enq_df.write \
            .option("header", True) \
            .format("csv") \
            .mode("overwrite") \
            .option("path", output_path_records_csv) \
            .save()
        print(f"Records/enquiries data saved to CSV at {output_path_records_csv}")

        # Write records/enquiries data to Parquet
        print("\nWriting records/enquiries data to Parquet")
        output_path_records_parquet = f"gs://lending_ara/data/raw/cleaned/loans_defaulters_records_enq_parquet"
        loans_def_records_enq_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", output_path_records_parquet) \
            .save()
        print(f"Records/enquiries data saved to Parquet at {output_path_records_parquet}")

        #  Process pub_rec (cast to integer, fill nulls with 0)
        print("\nProcessing pub_rec column")
        loans_def_p_pub_rec_df = loans_def_processed_df.withColumn("pub_rec", col("pub_rec").cast("integer")).fillna(0, subset=["pub_rec"])
        print("pub_rec processed.")

        # Process pub_rec_bankruptcies (cast to integer, fill nulls with 0)
        print("\nProcessing pub_rec_bankruptcies column")
        loans_def_p_pub_rec_bankruptcies_df = loans_def_p_pub_rec_df.withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("integer")).fillna(0, subset=["pub_rec_bankruptcies"])
        print("pub_rec_bankruptcies processed.")

        # Process inq_last_6mths (cast to integer, fill nulls with 0)
        print("\nProcessing inq_last_6mths column")
        loans_def_p_inq_last_6mths_df = loans_def_p_pub_rec_bankruptcies_df.withColumn("inq_last_6mths", col("inq_last_6mths").cast("integer")).fillna(0, subset=["inq_last_6mths"])
        print("inq_last_6mths processed.")

        # Replace temporary view 'loan_defaulters' with fully processed data
        print("\nReplacing temporary view 'loan_defaulters' with fully processed data")
        loans_def_p_inq_last_6mths_df.createOrReplaceTempView("loan_defaulters")
        print("Temporary view 'loan_defaulters' updated.")

        # SQL query: Select detailed records/enquiries data
        print("\nSQL query: Select detailed records/enquiries data")
        loans_def_detail_records_enq_df = spark.sql("select member_id, pub_rec, pub_rec_bankruptcies, inq_last_6mths from loan_defaulters")
        print("Selected detailed records/enquiries data.")

        # Display detailed records/enquiries DataFrame
        print("\nDisplaying detailed records/enquiries DataFrame (first 20 rows)")
        loans_def_detail_records_enq_df.show()

        # Write detailed records/enquiries data to CSV
        print("\n Writing detailed records/enquiries data to CSV")
        output_path_detail_csv = f"gs://lending_ara/data/raw/cleaned/loans_def_detail_records_enq_df_csv"
        loans_def_detail_records_enq_df.write \
            .option("header", True) \
            .format("csv") \
            .mode("overwrite") \
            .option("path", output_path_detail_csv) \
            .save()
        print(f"Detailed records/enquiries data saved to CSV at {output_path_detail_csv}")

        # : Write detailed records/enquiries data to Parquet
        print("\n Writing detailed records/enquiries data to Parquet")
        output_path_detail_parquet = f"gs://lending_ara/data/raw/cleaned/loans_def_detail_records_enq_df_parquet"
        loans_def_detail_records_enq_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", output_path_detail_parquet) \
            .save()
        print(f"Detailed records/enquiries data saved to Parquet at {output_path_detail_parquet}")

    finally:
    # Stop Spark session
        print("\nStopping Spark session.")
    spark.stop()

if __name__ == "__main__":
    main() 