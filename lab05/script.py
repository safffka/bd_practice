from pyspark.sql import SparkSession
from pyspark.sql.functions import col, last, get_json_object, to_json, coalesce
from pyspark.sql.window import Window

from pyspark.sql.functions import when

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


df = spark.read.json("/accounts/*")


df = (
    df
    .withColumn("data_json", to_json(col("data")))
    .withColumn("set_json", to_json(col("set")))
)


df_norm = df.select(
    col("ts"),
    col("id").alias("global_id"),

    get_json_object(col("data_json"), "$.account_id").alias("account_id"),

    get_json_object(col("set_json"), "$.address").alias("address_set"),
    get_json_object(col("data_json"), "$.address").alias("address_data"),

    get_json_object(col("set_json"), "$.email").alias("email_set"),
    get_json_object(col("data_json"), "$.email").alias("email_data"),

    get_json_object(col("set_json"), "$.name").alias("name_set"),
    get_json_object(col("data_json"), "$.name").alias("name_data"),

    get_json_object(col("set_json"), "$.phone_number").alias("phone_set"),
    get_json_object(col("data_json"), "$.phone_number").alias("phone_data"),

    get_json_object(col("set_json"), "$.card_id").alias("card_id_set"),
    get_json_object(col("data_json"), "$.card_id").alias("card_id_data"),

    get_json_object(col("set_json"), "$.savings_account_id").alias("savings_set"),
    get_json_object(col("data_json"), "$.savings_account_id").alias("savings_data"),
)


w = (
    Window
    .partitionBy("global_id")
    .orderBy("ts")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)


history = df_norm.select(
    col("ts"),
    last("account_id", ignorenulls=True).over(w).alias("account_id"),

    coalesce(
        last("address_set", ignorenulls=True).over(w),
        last("address_data", ignorenulls=True).over(w)
    ).alias("address"),

    coalesce(
        last("email_set", ignorenulls=True).over(w),
        last("email_data", ignorenulls=True).over(w)
    ).alias("email"),

    coalesce(
        last("name_set", ignorenulls=True).over(w),
        last("name_data", ignorenulls=True).over(w)
    ).alias("name"),

    coalesce(
        last("phone_set", ignorenulls=True).over(w),
        last("phone_data", ignorenulls=True).over(w)
    ).alias("phone_number"),

    when(
        last("card_id_set", ignorenulls=True).over(w) == "",
        None
    ).otherwise(
        coalesce(
            last("card_id_set", ignorenulls=True).over(w),
            last("card_id_data", ignorenulls=True).over(w)
        )
    ).alias("card_id"),

    coalesce(
        last("savings_set", ignorenulls=True).over(w),
        last("savings_data", ignorenulls=True).over(w)
    ).alias("savings_account_id"),
).orderBy("account_id", "ts")


history.show(truncate=False)

spark.stop()
