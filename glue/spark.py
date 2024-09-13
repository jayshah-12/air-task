from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, expr

# Initialize SparkSession with PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("RelianceBalanceSheet") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# Read the CSV file
df = spark.read.csv("reliance_balance_sheet_transposed.csv", header=True, inferSchema=True)

# Print schema and show initial data
df.printSchema()
df.show(truncate=False)

# Rename columns
renamed_df = df \
    .withColumnRenamed("Total Assets", "total_assets") \
    .withColumnRenamed("Total Liabilities", "total_liabilities") \
    .withColumnRenamed("Equity Capital", "shareholders_equity")

# Clean and convert columns
cleaned_df = renamed_df \
    .withColumn("total_assets", regexp_replace(col("total_assets"), ",", "").cast("decimal(20,2)")) \
    .withColumn("total_liabilities", regexp_replace(col("total_liabilities"), ",", "").cast("decimal(20,2)")) \
    .withColumn("shareholders_equity", regexp_replace(col("shareholders_equity"), ",", "").cast("decimal(20,2)"))
cleaned_df = cleaned_df.withColumn("Year", regexp_replace(col("Year"), "Mar ", "").cast("integer"))
# Compute new fields
final_df = cleaned_df \
    .withColumn("debt_to_equity_ratio", when(col("shareholders_equity") != 0,
        col("total_liabilities") / col("shareholders_equity")).otherwise(None))

# Add company ID
company_id = "RELIANCE"
final_df = final_df.withColumn("company_id", expr(f"'{company_id}'"))

# Select desired columns
final_df = final_df.select("company_id", "Year", "total_assets", "total_liabilities",
                           "shareholders_equity", "debt_to_equity_ratio")

# Show final DataFrame
final_df.show(truncate=False)

# Define PostgreSQL connection properties
url = "jdbc:postgresql://192.168.3.112:5432/my_db"
properties = {
    "user": "root",
    "password": "root2",
    "driver": "org.postgresql.Driver"
}

# Save DataFrame to PostgreSQL table
final_df.write.jdbc(url=url, table="balance_sheet", mode="overwrite", properties=properties)

# Stop Spark session
spark.stop()
