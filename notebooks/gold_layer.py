SILVER_PATH = "abfss://raw@mimicrawanant01.dfs.core.windows.net/silver"
GOLD_PATH   = "abfss://raw@mimicrawanant01.dfs.core.windows.net/gold"
spark.conf.set(
    "fs.azure.account.key.mimicrawanant01.dfs.core.windows.net",
    "E1AjTg0fxr6sLrMoqa1Jzx101+LwmObyqXh0KPXqbCurewjxmR9xfCECL/6to+w/vtFaKemw8k+o+AStGsJaMA=="
)
silver_admissions = spark.read.format("delta").load(f"{SILVER_PATH}/admissions")

silver_patients = spark.read.format("delta").load(f"{SILVER_PATH}/patients")

silver_icustays = spark.read.format("delta").load(f"{SILVER_PATH}/icustays")

silver_diagnoses = spark.read.format("delta").load(f"{SILVER_PATH}/diagnoses_icd")

silver_procedures = spark.read.format("delta").load(f"{SILVER_PATH}/procedures_icd")

silver_prescriptions = spark.read.format("delta").load(f"{SILVER_PATH}/prescriptions")
display(silver_admissions)
from pyspark.sql.functions import *

gold_dashboard_df = (
    silver_admissions.alias("a")
    .join(silver_patients.alias("p"), col("a.subject_id") == col("p.subject_id"), "left")
    .join(silver_icustays.alias("i"), col("a.hadm_id") == col("i.hadm_id"), "left")
    .join(silver_diagnoses.alias("d"), col("a.hadm_id") == col("d.hadm_id"), "left")
    .join(silver_procedures.alias("pr"), col("a.hadm_id") == col("pr.hadm_id"), "left")
    .join(silver_prescriptions.alias("rx"), col("a.hadm_id") == col("rx.hadm_id"), "left")
)
gold_dashboard_df = gold_dashboard_df.select(

    col("a.subject_id"),
    col("a.hadm_id"),

    col("p.gender"),
    col("p.dob"),

    col("a.admission_type"),
    col("a.admission_location"),
    col("a.discharge_location"),

    col("a.hospital_expire_flag"),
    col("a.length_of_stay"),

    col("i.los").alias("icu_los"),

    col("d.icd9_code").alias("diagnosis_code"),

    col("rx.drug")
)
silver_admissions.printSchema()
from pyspark.sql.functions import *

silver_admissions = silver_admissions.withColumn(
    "length_of_stay",
    datediff(col("dischtime"), col("admittime"))
)
display(silver_admissions)
gold_dashboard_df = (
    silver_admissions.alias("a")
    .join(silver_patients.alias("p"), col("a.subject_id") == col("p.subject_id"), "left")
    .join(silver_icustays.alias("i"), col("a.hadm_id") == col("i.hadm_id"), "left")
    .join(silver_diagnoses.alias("d"), col("a.hadm_id") == col("d.hadm_id"), "left")
    .join(silver_procedures.alias("pr"), col("a.hadm_id") == col("pr.hadm_id"), "left")
    .join(silver_prescriptions.alias("rx"), col("a.hadm_id") == col("rx.hadm_id"), "left")
)
gold_dashboard_df = gold_dashboard_df.select(

    col("a.subject_id"),
    col("a.hadm_id"),

    col("p.gender"),
    col("p.dob"),

    col("a.admission_type"),
    col("a.admission_location"),
    col("a.discharge_location"),

    col("a.length_of_stay"),
    col("a.hospital_expire_flag"),

    col("i.los").alias("icu_los"),

    col("d.icd9_code").alias("diagnosis_code"),

    col("rx.drug")
)
display(gold_dashboard_df)
gold_dashboard_kpi = (
    gold_dashboard_df
    .groupBy("admission_type","gender")
    .agg(
        countDistinct("subject_id").alias("total_patients"),
        countDistinct("hadm_id").alias("total_admissions"),
        avg("length_of_stay").alias("avg_los"),
        avg("icu_los").alias("avg_icu_los"),
        sum("hospital_expire_flag").alias("total_deaths")
    )
)
display(gold_dashboard_kpi)
%sql
SHOW CATALOGS;
%sql
SHOW SCHEMAS IN internship;
%sql
USE CATALOG internship;
USE SCHEMA healthcare_gold;
gold_dashboard_kpi.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("internship.healthcare_gold.gold_dashboard_joined")
%sql
SELECT *
FROM internship.healthcare_gold.gold_dashboard_joined
LIMIT 20;

master_gold_df = patients_silver.join(
    diagnoses_silver, 
    ["SUBJECT_ID", "HADM_ID"], 
    "inner"
)

display(master_gold_df.limit(5))
master_gold_df.write.format("delta").mode("overwrite") \
    .saveAsTable("internship.gold.master_health_report")

print(" SUCCESS: internship.gold.master_health_report is now ready!")
