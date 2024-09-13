from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, regexp_replace, when

spark = SparkSession.builder \
    .appName("RelianceBalanceSheet") \
    .getOrCreate()

df = spark.read.csv("reliance_balance_sheet.csv", header=True, inferSchema=True)

unpivoted_df = df.selectExpr("Narration", 
                             "stack(12, 'Mar 2013', `Mar 2013`, 'Mar 2014', `Mar 2014`, 'Mar 2015', `Mar 2015`, " +
                             "'Mar 2016', `Mar 2016`, 'Mar 2017', `Mar 2017`, 'Mar 2018', `Mar 2018`, " +
                             "'Mar 2019', `Mar 2019`, 'Mar 2020', `Mar 2020`, 'Mar 2021', `Mar 2021`, " +
                             "'Mar 2022', `Mar 2022`, 'Mar 2023', `Mar 2023`, 'Mar 2024', `Mar 2024`) " +
                             "as (Year, Value)")


unpivoted_df = unpivoted_df.withColumn("Year", regexp_replace(col("Year"), "Mar ", "").cast("integer"))


pivoted_df = unpivoted_df.groupBy("Year").pivot("Narration").agg(expr("first(Value)"))


renamed_df = pivoted_df \
    .withColumnRenamed("Total Assets", "total_assets") \
    .withColumnRenamed("Total Liabilities", "total_liabilities") \
    .withColumnRenamed("Equity Capital", "shareholders_equity")


cleaned_df = renamed_df \
    .withColumn("total_assets", regexp_replace(col("total_assets"), ",", "").cast("decimal(20,2)")) \
    .withColumn("total_liabilities", regexp_replace(col("total_liabilities"), ",", "").cast("decimal(20,2)")) \
    .withColumn("shareholders_equity", regexp_replace(col("shareholders_equity"), ",", "").cast("decimal(20,2)"))


final_df = cleaned_df \
    .withColumn("debt_to_equity_ratio", when(col("shareholders_equity") != 0, 
        col("total_liabilities") / col("shareholders_equity")).otherwise(None))

company_id = "RELIANCE"  
final_df = final_df.withColumn("company_id", expr(f"'{company_id}'"))

final_df = final_df.select("company_id", "Year", "total_assets", "total_liabilities", 
                           "shareholders_equity", "debt_to_equity_ratio")

final_df.show(truncate=False)

spark.stop()
