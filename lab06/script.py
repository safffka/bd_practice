from pyspark.sql import SparkSession
import sqlite3

spark = SparkSession.builder \
    .appName("SQLite to Postgres ETL") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


sqlite_path = "/opt/spark/work-dir/chinook.db"
sqlite_url = f"jdbc:sqlite:{sqlite_path}"

conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table'
  AND name NOT LIKE 'sqlite_%'
""")

tables = [row[0] for row in cursor.fetchall()]
conn.close()

print("Tables found in SQLite:", tables)


pg_url = "jdbc:postgresql://postgres-source:5432/postgres"
pg_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


for table in tables:
    print(f"Processing table: {table}")

    df = (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:sqlite:/opt/spark/work-dir/chinook.db")
        .option("dbtable", table)
        .option("driver", "org.sqlite.JDBC")
        .load()
    )

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres-source:5432/postgres") \
        .option("dbtable", table) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

spark.stop()
