from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder.master("local").appName("wordCount_RDD_ver").getOrCreate()

    sc: SparkContext = ss.sparkContext

    # 분산 환경에서도 코드는 똑같음, 위 설정만 바꾸면 됨
    # load data
    text_file: RDD[str] = sc.textFile("data/words.txt")

    # transformation
    counts = (
        text_file.flatMap(lambda line: line.split(" "))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda count1, count2: count1 + count2)
    )

    # action
    output = counts.collect()

    for word, count in output:
        print(f"{word}: {count}")
