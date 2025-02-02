import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initialize Spark and Glue Context
spark = SparkSession.builder.appName("GlueNotebook").getOrCreate()
glueContext = GlueContext(spark)
spark_context = glueContext.spark_session.sparkContext

# S3 Input and Output Paths
input_path = "s3://your-bucket/input/"
output_path = "s3://your-bucket/output/"

# Read CSV Data from S3
print("Reading data from S3...")
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [input_path]},
)

# Convert DynamicFrame to DataFrame
df = dynamic_frame.toDF()

# Transform Data (Example: Drop nulls, Rename Column, Filter Data)
df = df.dropna()
df = df.withColumnRenamed("old_column_name", "new_column_name")
df = df.filter(df["column_name"] > 100)  # Example filter condition

# Convert DataFrame back to DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(df, glueContext)

# Write Transformed Data to S3 in Parquet Format
print("Writing transformed data to S3...")
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)

print("ETL process completed successfully!")
