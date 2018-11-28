import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark_app_01.py <input-file> <output-file>")
        sys.exit()

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    tweetDF = spark.read.json(sys.argv[1])
    t1 = tweetDF.withColumn("documents",tweetDF.documents)
    t1.printSchema()

    t1DF = t1.select(explode("documents").alias("tweet"))
    t1DF.select("tweet.Author","tweet.FollowersCount").write.csv("/output/t1DF", encoding="utf-8")

    # namesDF = tweetDF.select("firstName","lastName")
    # namesDF.write.option("header","true").csv(sys.argv[2])

    spark.stop()