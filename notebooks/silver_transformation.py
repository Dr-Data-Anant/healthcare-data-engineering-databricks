BRONZE_BASE_PATH = "abfss://raw@mimicrawanant01.dfs.core.windows.net/bronze/"
SILVER_BASE_PATH = "abfss://raw@mimicrawanant01.dfs.core.windows.net/silver/"
storage_account_name = "mimicrawanant01"
storage_account_key = "E1AjTg0fxr6sLrMoqa1Jzx101+LwmObyqXh0KPXqbCurewjxmR9xfCECL/6to+w/vtFaKemw8k+o+AStGsJaMA=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
patients_bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_BASE_PATH + "patients")
)

patients_bronze_df.printSchema()
patients_bronze_df.display(5)
from pyspark.sql.functions import col, to_date

patients_silver_df = (
    patients_bronze_df
    .withColumn("dob", to_date(col("dob")))
    .withColumn("dod", to_date(col("dod")))
    .withColumn("dod_hosp", to_date(col("dod_hosp")))
    .withColumn("dod_ssn", to_date(col("dod_ssn")))
    .select(
        "subject_id",
        "gender",
        "dob",
        "dod",
        "expire_flag",
        "_ingestion_time",
        "_source_file"
    )
)
dbutils.fs.rm(SILVER_BASE_PATH + "patients", recurse=True)
(
    patients_silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_BASE_PATH + "patients")
)
spark.read.format("delta").load(SILVER_BASE_PATH + "patients").printSchema()
admissions_bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_BASE_PATH + "admissions")
)

admissions_bronze_df.printSchema()
admissions_bronze_df.display(5)
from pyspark.sql.functions import col, to_date

admissions_silver_df = (
    admissions_bronze_df
    .withColumn("admittime", to_date(col("admittime")))
    .withColumn("dischtime", to_date(col("dischtime")))
    .withColumn("deathtime", to_date(col("deathtime")))
    .select(
        "hadm_id",
        "subject_id",
        "admittime",
        "dischtime",
        "admission_type",
        "admission_location",
        "discharge_location",
        "insurance",
        "language",
        "religion",
        "marital_status",
        "hospital_expire_flag",
        "_ingestion_time",
        "_source_file"
    )
)
dbutils.fs.rm(SILVER_BASE_PATH + "admissions", recurse=True)
(
    admissions_silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_BASE_PATH + "admissions")
)
spark.read.format("delta").load(SILVER_BASE_PATH + "admissions").display(5)
icustays_bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_BASE_PATH + "icustays")
)

icustays_bronze_df.printSchema()
icustays_bronze_df.display(5)
from pyspark.sql.functions import col, to_date

icustays_silver_df = (
    icustays_bronze_df
    .withColumn("intime", to_date(col("intime")))
    .withColumn("outtime", to_date(col("outtime")))
    .select(
        "icustay_id",
        "subject_id",
        "hadm_id",
        "first_careunit",
        "last_careunit",
        "intime",
        "outtime",
        "los",
        "_ingestion_time",
        "_source_file"
    )
)
dbutils.fs.rm(SILVER_BASE_PATH + "icustays", recurse=True)
(
    icustays_silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_BASE_PATH + "icustays")
)
spark.read.format("delta").load(SILVER_BASE_PATH + "icustays").display(5)
diagnoses_bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_BASE_PATH + "diagnoses_icd")
)

diagnoses_bronze_df.printSchema()
diagnoses_bronze_df.display(5)
from pyspark.sql.functions import col, trim

diagnoses_silver_df = (
    diagnoses_bronze_df
    .withColumn("icd9_code", trim(col("icd9_code")))
    .select(
        "subject_id",
        "hadm_id",
        "icd9_code",
        "seq_num",
        "_ingestion_time",
        "_source_file"
    )
)
dbutils.fs.rm(SILVER_BASE_PATH + "diagnoses_icd", recurse=True)
(
    diagnoses_silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_BASE_PATH + "diagnoses_icd")
)
spark.read.format("delta").load(SILVER_BASE_PATH + "diagnoses_icd").display(5)
procedures_bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_BASE_PATH + "procedures_icd")
)

procedures_bronze_df.printSchema()
procedures_bronze_df.display(5)
from pyspark.sql.functions import col, trim

procedures_silver_df = (
    procedures_bronze_df
    .withColumn("icd9_code", trim(col("icd9_code")))
    .select(
        "subject_id",
        "hadm_id",
        "icd9_code",
        "seq_num",
        "_ingestion_time",
        "_source_file"
    )
)
dbutils.fs.rm(SILVER_BASE_PATH + "procedures_icd", recurse=True)
(
    procedures_silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_BASE_PATH + "procedures_icd")
)
spark.read.format("delta").load(SILVER_BASE_PATH + "procedures_icd").display(5)
prescriptions_bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_BASE_PATH + "prescriptions")
)

prescriptions_bronze_df.printSchema()
prescriptions_bronze_df.display(5)
from pyspark.sql.functions import col, trim, to_timestamp

prescriptions_silver_df = (
    prescriptions_bronze_df
    .withColumn("drug", trim(col("drug")))
    .withColumn("startdate", to_timestamp(col("startdate")))
    .withColumn("enddate", to_timestamp(col("enddate")))
    .select(
        "subject_id",
        "hadm_id",
        "drug",
        "drug_type",
        "startdate",
        "enddate",
        "dose_val_rx",
        "dose_unit_rx",
        "_ingestion_time",
        "_source_file"
    )
)
dbutils.fs.rm(SILVER_BASE_PATH + "prescriptions", recurse=True)
(
    prescriptions_silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_BASE_PATH + "prescriptions")
)
spark.read.format("delta").load(SILVER_BASE_PATH + "prescriptions").display(5)
