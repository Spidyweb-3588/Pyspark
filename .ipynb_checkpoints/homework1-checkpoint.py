# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sample").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
# -

emp = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/emp.csv")
amt_sal = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/금액제급여.csv")
hobong_sal = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/호봉제급여.csv")
yearly_sal = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/연봉제급여.csv")
sal_pay = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/급여지급.csv")
bonus_sal = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/기타급여.csv")
iss_hist = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/발령이력.csv")
dept = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/부서.csv")
pjt = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/프로젝트.csv")
pjt_use_hist = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/프로젝트투입이력.csv")
vc_use_hist = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/실습문제_20210324/data/휴가사용이력.csv")

# 1.부서별 연봉제/호봉제/금액제 급여를 받는 사원 수   '- 부서, 사원, 연봉제급여, 호봉제급여, 금액제급여 데이터 사용

from pyspark.sql.functions import *
from pyspark.sql.window import *

homework1_1 =\
emp.join(dept,dept["부서코드"] == emp["부서코드"],"inner")\
   .join(yearly_sal,yearly_sal["사원번호"] == emp["사원번호"],"left_outer")\
   .join(amt_sal,amt_sal["사원번호"] == emp["사원번호"],"left_outer")\
   .join(hobong_sal,hobong_sal["사원번호"] == emp["사원번호"],"left_outer")\
   .groupBy(emp["부서코드"],dept["부서명"])\
   .agg(count(yearly_sal["사원번호"]).alias("연봉제사원수")\
       ,count(amt_sal["사원번호"]).alias("금액제사원수")\
       ,count(hobong_sal["사원번호"]).alias("호봉제사원수"))\
   .orderBy(emp["부서코드"]) #사원테이블기준으로 부서,연봉제급여,호봉제급여,금액제급여테이블과 조인하여 각각 부서명,사원수를 출력

homework1_1

# 2.지금까지 가장 많은 급여(누적)를 받은 사원의 최초 직급   '- 급여지급, 사원, 발령이력 데이터 사용

min_seq = iss_hist.groupBy(col("사원번호"))\
                  .agg(min("이력순번").alias("최초이력순번")) #이력순번이 최소인 데이터프레임 

min_rank =iss_hist.join(min_seq,(iss_hist["사원번호"] == min_seq["사원번호"]) & (iss_hist["이력순번"] == min_seq["최초이력순번"]),"inner")\
                  .select(iss_hist["사원번호"],iss_hist["직급"]) #이력순번이 최소인 사원번호와 직급을 출력하는 데이터프레임

total_sum = sal_pay.groupBy(col("사원번호"))\
                   .agg(sum("월급여").alias("총급여")) #사원번호의 총급여를 출력하는 데이터프레임

no1_result = emp.join(min_rank,emp["사원번호"] == min_rank["사원번호"],"inner")\#이력순번이 최소인 데이터프레임과 사원번호로 조인
                .join(total_sum,emp["사원번호"] == total_sum["사원번호"],"inner")\#사원의 총급여 데이터프레임과 사원번호로 조인
                .select(emp["사원번호"],emp["사원명"],min_rank["직급"],total_sum["총급여"].cast("integer"))\
                .orderBy(total_sum["총급여"].desc())#총급여를 기준으로 내림차순 정렬

homework1_2 = no1_result.select("*").limit(1) #결과중에서 가장 위엣줄출력

homework1_2

# 3.각 사원의 직급별 받은 급여의 총 합계 '- 발령이력, 사원, 급여지급 데이터 사용

# +

rank_smonth_emonth = \
iss_hist.withColumn("발령시작월",translate(substring("발령일자",1,7),'/',''))\
        .withColumn("발령종료월",coalesce(floor(translate(substring(lead(col("발령일자"))\
                                                               .over(Window.partitionBy("사원번호")\
                                                                           .orderBy("발령일자")),1,7),'/','')-1),lit('999912')))
#발령시작월과 발령종료월을 구하는 데이터프레임
# -

rank_smonth_emonth

homework1_3 =\
emp.join(rank_smonth_emonth,emp["사원번호"] == rank_smonth_emonth["사원번호"])\
   .join(sal_pay,(emp["사원번호"] == sal_pay["사원번호"]) & (sal_pay["급여월"].between(rank_smonth_emonth["발령시작월"],rank_smonth_emonth["발령종료월"])))\
   .groupBy(emp["사원번호"],emp["사원명"],rank_smonth_emonth["직급"],rank_smonth_emonth["발령시작월"],rank_smonth_emonth["발령종료월"])\
   .agg(sum(sal_pay["월급여"]).cast("integer").alias("직급별급여"))\
   .orderBy(emp["사원번호"],rank_smonth_emonth["직급"].desc())
"""
1.발령일자 데이터프레임과 사원번호로 조인
2.급여지급 데이터프레임과 사원번호로 조인,그리고 급여월이 발령시작월과 발령종료월사이에 있는 월급여의 합을 구한다.
"""

homework1_3
