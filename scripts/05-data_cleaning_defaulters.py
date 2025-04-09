#!/usr/bin/env python3
# PySpark job based on 05-data_cleaning_defaulters.ipynb - Loan Defaulters Data Cleaning

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


def main():
    # Initialize Spark Session 
    
    spark = SparkSession \
        .builder \
        .config('spark.shuffle.useOldFetchProtocol', 'true') \
        .enableHiveSupport() \
        .master('yarn') \
        .getOrCreate()
    
    print("Spark session created.")

    try:
        # Load loan defaulters data with inferSchema 
        print("Loading loan defaulters data with inferSchema...")
        loans_def_raw_df_initial = spark.read \
            .format("csv") \
            .option("header", True) \
            .option("inferSchema", True) \
            .load("gs://lending_ara/data/raw/loans_defaulters_csv")
        
        # Display dataframe info 
        print("\nInitial dataframe info:")
        print(loans_def_raw_df_initial)
        print("\nInitial schema:")
        loans_def_raw_df_initial.printSchema()
        
        # Create temporary view 
        loans_def_raw_df_initial.createOrReplaceTempView("loan_defaulters")
        
        # Execute SQL to check delinq_2yrs values 
        print("\nDistinct delinq_2yrs values:")
        spark.sql("select distinct(delinq_2yrs) from loan_defaulters").show()
        
        print("\ndelinq_2yrs value counts:")
        spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)
        
        # Define proper schema for loan defaulters data 
        loan_defaulters_schema = "member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float, inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"
        
        # Load data with proper schema 
        print("\nLoading data with proper schema...")
        loans_def_raw_df = spark.read \
            .format("csv") \
            .option("header", True) \
            .schema(loan_defaulters_schema) \
            .load("gs://lending_ara/data/raw/loans_defaulters_csv")     
        
        # Create temporary view with schema-defined data 
        loans_def_raw_df.createOrReplaceTempView("loan_defaulters")
        
        # Check delinq_2yrs values again 
        print("\ndelinq_2yrs value counts after schema definition:")
        spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)
        
        # Process delinq_2yrs column - cast to integer and fill nulls 
        loans_def_processed_df = loans_def_raw_df.withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")).fillna(0, subset=["delinq_2yrs"])
        loans_def_processed_df.createOrReplaceTempView("loan_defaulters")
        
        # Check for null values
        print("\nCount of null values in delinq_2yrs:")
        spark.sql("select count(*) from loan_defaulters where delinq_2yrs is null").show()
        
        # Check delinq_2yrs distribution after processing 
        print("\ndelinq_2yrs value counts after processing:")
        spark.sql("select delinq_2yrs, count(*) as total from loan_defaulters group by delinq_2yrs order by total desc").show(40)
        
        # Create delinquency dataframe 
        loans_def_delinq_df = spark.sql("select member_id, delinq_2yrs, delinq_amnt, int(mths_since_last_delinq) from loan_defaulters where delinq_2yrs > 0 or mths_since_last_delinq > 0")
        print("\nDelinquency dataframe:")
        print(loans_def_delinq_df)
        print(f"Delinquency count: {loans_def_delinq_df.count()}")
        
        # Create records and enquiries dataframe 
        loans_def_records_enq_df = spark.sql("select member_id from loan_defaulters where pub_rec > 0.0 or pub_rec_bankruptcies > 0.0 or inq_last_6mths > 0.0")
        print("\nRecords and enquiries dataframe:")
        print(loans_def_records_enq_df)
        print(f"Records and enquiries count: {loans_def_records_enq_df.count()}")
        
        # Save delinquency data as CSV
        print("\nSaving delinquency data as CSV...")
        loans_def_delinq_df.write \
            .option("header", True) \
            .format("csv") \
            .mode("overwrite") \
            .option("path", "gs://lending_ara/data/raw/cleaned/loans_defaulters_deling_csv") \
            .save()
        
        # Save delinquency data as Parquet 
        print("\nSaving delinquency data as Parquet...")
        loans_def_delinq_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", "gs://lending_ara/data/raw/cleaned/loans_defaulters_deling_parquet") \
            .save()
        
        # Save records and enquiries data as CSV 
        print("\nSaving records and enquiries data as CSV...")
        loans_def_records_enq_df.write \
            .option("header", True) \
            .format("csv") \
            .mode("overwrite") \
            .option("path", "gs://lending_ara/data/raw/cleaned/loans_defaulters_records_enq_csv") \
            .save()
        
        # Save records and enquiries data as Parquet 
        print("\nSaving records and enquiries data as Parquet...")
        loans_def_records_enq_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", "gs://lending_ara/data/raw/cleaned/loans_defaulters_records_enq_parquet") \
            .save()
        
        print("Process completed successfully")
        
        # Stop Spark session
        spark.stop()

    except Exception as e:
        print(f"Error occurred: {e}")
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main() 