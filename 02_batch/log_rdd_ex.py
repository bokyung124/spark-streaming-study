from typing import List, Tuple
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark import SparkContext, RDD

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder.master("local").appName("log_rdd_ex").getOrCreate()

    sc: SparkContext = ss.sparkContext

    log_rdd: RDD[str] = sc.textFile("data/log.txt")

    # a) map
    # a-1) 각 행을 List[str] 로 받아오기

    def parse_line(row: str):
        return row.split(" | ")

    parsed_log_rdd: RDD[List[str]] = log_rdd.map(parse_line)
    # parsed_log_rdd.foreach(lambda v: print(v))

    # b) filter
    # b-1) status_code = 404인 로그만 필터링

    def get_only_404(row: List[str]) -> bool:
        status_code = row[-1]
        return status_code == "404"

    rdd_404 = parsed_log_rdd.filter(get_only_404)
    # rdd_404.foreach(print)

    # b-2) status_code가 정상인 로그만 필터링 (2XX)

    def get_only_2xx(row: List[str]) -> bool:
        status_code = row[-1]
        return status_code.startswith("2")

    rdd_normal = parsed_log_rdd.filter(get_only_2xx)
    # rdd_normal.foreach(print)

    # b-3) post 요청 & /playbooks로 끝나는 로그만 필터링

    def get_post_and_playbooks_api(row: List[str]) -> bool:
        log = row[2].replace('"', "")
        return log.startswith("POST") and "/playbooks" in log

    rdd_post_playbooks = parsed_log_rdd.filter(get_post_and_playbooks_api)
    # rdd_post_playbooks.foreach(print)

    # c) reduce
    # c-1) API method 별 개수 출력

    def extract_api_method(row: List[str]) -> Tuple[str, int]:
        log = row[2].replace('"', "")
        api_method = log.split(" ")[0]
        return api_method, 1

    rdd_count_by_api_method = parsed_log_rdd.map(extract_api_method).reduceByKey(lambda c1, c2: c1 + c2).sortByKey()
    # rdd_count_by_api_method.foreach(print)

    # c-2) 분 단위 별 요청 횟수

    def extract_hour_and_minute(row: List[str]) -> Tuple[str, int]:
        timestamp = row[1].replace("[", "").replace("]", "")
        date_format = "%d/%b/%Y:%H:%M:%S"
        date_time_obj = datetime.strptime(timestamp, date_format)
        return f"{date_time_obj.hour}:{date_time_obj.minute}", 1

    rdd_count_by_hour_min = parsed_log_rdd.map(extract_hour_and_minute).reduceByKey(lambda c1, c2: c1 + c2).sortByKey()
    # rdd_count_by_hour_min.foreach(print)

    # d) group by
    # d-1) status code, api method 별 ip list 출력

    def extract_cols(row: List[str]) -> Tuple[str, str, str]:
        ip = row[0]
        status_code = row[-1]
        api_method = row[2].replace('"', "").split(" ")[0]

        return status_code, api_method, ip

    rdd_ip_list = parsed_log_rdd.map(extract_cols).map(lambda x: ((x[0], x[1]), x[2])).groupByKey().mapValues(list)
    # rdd_ip_list.foreach(print)

    # reductByKey - 일반적으로 데이터 크기가 클 때 reduceByKey가 groupByKey보다 더 성능이 좋음
    result_2 = (
        parsed_log_rdd.map(extract_cols)
        .map(lambda x: ((x[0], x[1]), x[2]))
        .reduceByKey(lambda i1, i2: f"{i1},{i2}")
        .map(lambda row: (row[0], row[1].split(",")))
    )
    result_2.collect()
    # result_2.foreach(print)

    while True:
        pass