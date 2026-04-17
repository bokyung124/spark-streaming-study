from pyspark import SparkContext
from pyspark.sql import SparkSession


def load_data(from_file, sc):
    if from_file:
        return load_data_from_file(sc)
    else:
        return load_data_from_inmemory(sc)


def load_data_from_file(sc):
    return sc.textFile("data/user_visits.txt").map(lambda v: v.split(",")), sc.textFile("data/user_names.txt").map(
        lambda v: v.split(",")
    )


def load_data_from_inmemory(sc: SparkContext):
    user_visits = [(1, 10), (2, 27), (3, 2), (4, 5), (5, 88), (6, 1), (7, 5)]
    user_names = [(1, "John"), (2, "Jane"), (3, "Jim"), (4, "Jill"), (5, "Jack"), (6, "Jill"), (7, "Jim")]

    return sc.parallelize(user_visits), sc.parallelize(user_names)


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder.master("local").appName("rdd join ex").getOrCreate()
    sc: SparkContext = ss.sparkContext

    user_visits_rdd, user_names_rdd = load_data(True, sc)

    # 같은 key에 대해 name과 visit count가 튜플로 묶임
    joined_rdd = user_names_rdd.join(user_visits_rdd).sortByKey()
    # print(joined_rdd.take(5))

    result = joined_rdd.filter(lambda row: row[1][0] == "Jane").collect()
    # print(result)

    # inner, left outer join, right outer join, full outer join
    inner_join = user_names_rdd.join(user_visits_rdd)
    left_outer_join = user_names_rdd.leftOuterJoin(user_visits_rdd)
    right_outer_join = user_names_rdd.rightOuterJoin(user_visits_rdd)
    full_outer_join = user_names_rdd.fullOuterJoin(user_visits_rdd)

    print(inner_join.take(5))
    print(left_outer_join.take(5))
    print(right_outer_join.take(5))
    print(full_outer_join.take(5))
