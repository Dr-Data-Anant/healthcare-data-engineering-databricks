RAW_BASE_PATH = "abfss://raw@mimicrawanant01.dfs.core.windows.net/mimic_iii/2026-01-27/"
BRONZE_BASE_PATH = "abfss://raw@mimicrawanant01.dfs.core.windows.net/bronze/"
storage_account_name = "mimicrawanant01"
storage_account_key = "E1AjTg0fxr6sLrMoqa1Jzx101+LwmObyqXh0KPXqbCurewjxmR9xfCECL/6to+w/vtFaKemw8k+o+AStGsJaMA=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
files = dbutils.fs.ls(RAW_BASE_PATH)

csv_files = [f.path for f in files if f.path.lower().endswith(".csv")]

print(f"Total CSV files found: {len(csv_files)}")
csv_files
import os
from pyspark.sql.functions import current_timestamp, col

for file_path in csv_files:
    table_name = os.path.basename(file_path).replace(".csv", "").lower()

    print(f"🔄 Ingesting {table_name}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(BRONZE_BASE_PATH + table_name)
    )

print("✅ Bronze ingestion completed")
dbutils.fs.ls(BRONZE_BASE_PATH)
df_patients = spark.read.format("delta").load(BRONZE_BASE_PATH + "patients")
df_patients.display(5)
