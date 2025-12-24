from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg
from pyspark.sql.window import Window


spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


df = spark.read.csv(
    "/opt/spark/work-dir/population.csv",
    header=True,
    inferSchema=True
)


w = Window.partitionBy("Country Name").orderBy("Year")


df_growth = (
    df
    .withColumn("prev_population", lag(col("Value")).over(w))
    .withColumn("growth", col("Value") - col("prev_population"))
)


df_avg_growth = (
    df_growth
    .filter((col("Year") >= 1990) & (col("Year") <= 2018))
    .groupBy("Country Name")
    .agg(
        avg(col("growth")).alias("trend")
    )
)


result = (
    df_avg_growth
    .filter(col("trend") < 0)
    .orderBy(col("trend").asc())
)


result.show(truncate=False)

spark.stop()
