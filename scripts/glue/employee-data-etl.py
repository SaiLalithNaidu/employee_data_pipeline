import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. Initialize Glue / Spark
# ---------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# 2. S3 paths
# ---------------------------------------------------------

input_path = (
    "s3://employee-data-platform-sai-2026/"
    "raw-bucket/employees/"
)

processed_path = (
    "s3://employee-data-platform-sai-2026/"
    "processed-bucket/employees/"
)

rejected_path = (
    "s3://employee-data-platform-sai-2026/"
    "rejected-bucket/employees/"
)


# ---------------------------------------------------------
# 3. Read CSV from S3
# ---------------------------------------------------------

print("========================================")
print("Reading employee data from S3")
print("========================================")

print(f"Input path: {input_path}")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

input_count = df.count()

print(f"Input record count: {input_count}")

df.printSchema()


# ---------------------------------------------------------
# 4. Remove duplicate employee records
# ---------------------------------------------------------

df = df.dropDuplicates(["employee_id"])

deduplicated_count = df.count()

print(f"Record count after duplicate removal: {deduplicated_count}")


# ---------------------------------------------------------
# 5. Convert data types
# ---------------------------------------------------------

df = (
    df
    .withColumn(
        "employee_id",
        F.col("employee_id").cast("integer")
    )
    .withColumn(
        "salary",
        F.col("salary").cast("double")
    )
    .withColumn(
        "joining_date",
        F.to_date(
            F.col("joining_date"),
            "yyyy-MM-dd"
        )
    )
)


# ---------------------------------------------------------
# 6. Validate email
# ---------------------------------------------------------

email_valid = F.col("email").rlike(
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)


# ---------------------------------------------------------
# 7. Define valid records
# ---------------------------------------------------------

valid_condition = (
    F.col("employee_id").isNotNull()
    & F.col("employee_name").isNotNull()
    & F.col("email").isNotNull()
    & email_valid
    & F.col("department").isNotNull()
    & F.col("salary").isNotNull()
    & (F.col("salary") > 0)
    & F.col("joining_date").isNotNull()
    & F.col("team").isNotNull()
    & F.col("location").isNotNull()
    & F.col("status").isNotNull()
)


# ---------------------------------------------------------
# 8. Split valid and rejected records
# ---------------------------------------------------------

valid_df = df.filter(valid_condition)

rejected_df = df.filter(~valid_condition)


valid_count = valid_df.count()
rejected_count = rejected_df.count()

print(f"Valid records: {valid_count}")
print(f"Rejected records: {rejected_count}")


# ---------------------------------------------------------
# 9. Add processing timestamp
# ---------------------------------------------------------

valid_df = valid_df.withColumn(
    "processed_timestamp",
    F.current_timestamp()
)


# ---------------------------------------------------------
# 10. Write valid records as Parquet
# ---------------------------------------------------------

print("========================================")
print("Writing valid records")
print("========================================")

(
    valid_df
    .write
    .mode("overwrite")
    .parquet(processed_path)
)


# ---------------------------------------------------------
# 11. Write rejected records as CSV
# ---------------------------------------------------------

print("========================================")
print("Writing rejected records")
print("========================================")

(
    rejected_df
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(rejected_path)
)


# ---------------------------------------------------------
# 12. Complete Glue job
# ---------------------------------------------------------

print("========================================")
print("Employee ETL completed successfully")
print("========================================")

job.commit()
