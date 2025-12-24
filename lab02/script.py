from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


rdd = spark.sparkContext.textFile("/opt/spark/work-dir/u.data")




pairRDD = (
    rdd
    .map(lambda x: x.split("\t"))
    .map(lambda x: (int(x[1]), int(x[2])))
)




filmRatingCounts = (
    pairRDD
    .map(lambda x: ((x[0], x[1]), 1))
    .reduceByKey(lambda a, b: a + b)
)


filmStats = filmRatingCounts.map(
    lambda x: (x[0][0], (x[0][1], x[1]))
)


aggPairRDD = filmStats.aggregateByKey(
    {},
    lambda acc, x: {**acc, x[0]: x[1]},
    lambda acc1, acc2: {**acc1, **acc2}
)

def printStat(inp):
    ind, marks_dict = inp
    marks = [marks_dict.get(i, 0) for i in range(1, 6)]
    print(
        f"Marks for film {ind}: "
        f"1 -> {marks[0]}, 2 -> {marks[1]}, 3 -> {marks[2]}, "
        f"4 -> {marks[3]}, 5 -> {marks[4]}"
    )


for item in aggPairRDD.collect():
    printStat(item)



allMarks = (
    pairRDD
    .map(lambda x: (x[1], 1))
    .reduceByKey(lambda a, b: a + b)
    .collectAsMap()
)

print(
    "Marks for films ALL: "
    f"1 -> {allMarks.get(1, 0)}, "
    f"2 -> {allMarks.get(2, 0)}, "
    f"3 -> {allMarks.get(3, 0)}, "
    f"4 -> {allMarks.get(4, 0)}, "
    f"5 -> {allMarks.get(5, 0)}"
)

spark.stop()
