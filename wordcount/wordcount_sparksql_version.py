from pyspark.sql import SparkSession
import pyspark.sql.functions as f

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder.master("local").appName("wordCount_RDD_ver").getOrCreate()

    df = ss.read.text("data/words.txt")

    # transformation
    df = (
        df.withColumn("word", f.explode(f.split(f.col("value"), " ")))
        .withColumn("count", f.lit(1))
        .groupBy("word")
        .agg(f.sum("count").alias("count"))
        .orderBy(f.desc("count"))
    )

    # action
    df.show()
