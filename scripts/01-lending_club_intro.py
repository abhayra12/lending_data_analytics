from pyspark.sql import SparkSession

spark=SparkSession. \
    builder. \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

raw_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load("gs://lending_ara/data/accepted_2007_to_2018Q4.csv")

raw_df.createOrReplaceTempView("lending_club_data")

# Display the data 
spark.sql("select * from lending_club_data").show()

from pyspark.sql.functions import sha2, concat_ws

# Create a new dataframe with the SHA2 hash
new_df = raw_df.withColumn("name_sha2", sha2(concat_ws("||", *["emp_title", "emp_length", "home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]), 256))

new_df.createOrReplaceTempView("newtable")

# Count total records
spark.sql("select count(*) from newtable").show()

# Count distinct customers
spark.sql("select count(distinct(name_sha2)) from newtable").show()

# Find customers with multiple records
spark.sql("""select name_sha2, count(*) as total_cnt 
from newtable group by name_sha2 having total_cnt>1 order by total_cnt desc""").show()

# Check specific customer
spark.sql("select * from newtable where name_sha2 like 'e3b0c44298fc1c149%'").show()

# Create customers data
spark.sql("""select name_sha2 as member_id,emp_title,emp_length,home_ownership,annual_inc,addr_state,zip_code,'USA' as country,grade,sub_grade,
verification_status,tot_hi_cred_lim,application_type,annual_inc_joint,verification_status_joint from newtable
""").repartition(1).write \
.option("header","true")\
.format("csv") \
.mode("overwrite") \
.option("path", f"gs://lending_ara/data/raw/customers_data_csv") \
.save()

customers_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load(f"gs://lending_ara/data/raw/customers_data_csv")

# Display customers data
customers_df.show()

# Create loans data
spark.sql("""select id as loan_id, name_sha2 as member_id,loan_amnt,funded_amnt,term,int_rate,installment,issue_d,loan_status,purpose,
title from newtable""").repartition(1).write \
.option("header",True)\
.format("csv") \
.mode("overwrite") \
.option("path", f"gs://lending_ara/data/raw/loans_data_csv") \
.save()

loans_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load(f"gs://lending_ara/data/raw/loans_data_csv")

# Display loans data
loans_df.show()

# Create loans repayments data
spark.sql("""select id as loan_id,total_rec_prncp,total_rec_int,total_rec_late_fee,total_pymnt,last_pymnt_amnt,last_pymnt_d,next_pymnt_d from newtable""").repartition(1).write \
.option("header",True)\
.format("csv") \
.mode("overwrite") \
.option("path", f"gs://lending_ara/data/raw/loans_repayments_csv") \
.save()

loans_repayments_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load(f"gs://lending_ara/data/raw/loans_repayments_csv")

# Display loans repayments data
loans_repayments_df.show()

# Create loans defaulters data
spark.sql("""select name_sha2 as member_id,delinq_2yrs,delinq_amnt,pub_rec,pub_rec_bankruptcies,inq_last_6mths,total_rec_late_fee,mths_since_last_delinq,mths_since_last_record from newtable""").repartition(1).write \
.option("header",True)\
.format("csv") \
.mode("overwrite") \
.option("path", f"gs://lending_ara/data/raw/loans_defaulters_csv") \
.save()

loans_defaulters_df = spark.read \
.format("csv") \
.option("InferSchema","true") \
.option("header","true") \
.load(f"gs://lending_ara/data/raw/loans_defaulters_csv")

# Display loans defaulters data
loans_defaulters_df.show()

# Stop the Spark session
spark.stop()


