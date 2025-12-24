from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count


spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


df = spark.read.csv(
    "/opt/spark/work-dir/world-cities.csv",
    header=True,
    inferSchema=True
)


df.printSchema()


result = (
    df
    .groupBy(col("country"))
    .agg(
        countDistinct(col("subcountry")).alias("subcountry"),
        count("*").alias("cnt")
    )
    .orderBy(col("cnt").desc())
)


result.show(20, truncate=False)

spark.stop()
