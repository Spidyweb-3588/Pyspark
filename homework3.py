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

from pyspark.sql.functions import *
from pyspark.sql.window import *

# [1] 부서별 사원수, 전체사원수, 참여프로젝트수, 가장 마지막으로 입사한 사원의 이름
#    - 참여프로젝트수: 부서에 속한 사원이 참여한 모든 프로젝트의 개수 (동일 프로젝트는 1건으로 집계)
#    - 최종입사자: 각 부서별 가장 마지막으로 입사한 사원의 이름 (입사일자가 동일한 경우는 사원번호가 가장 빠른 사원이 노출되도록 함)
#

num_name = emp.withColumn("RN",row_number().over(Window.partitionBy("부서코드").orderBy(desc("입사일자"),"사원번호")))\
              .withColumn("최종입사자사원명",when(col("RN") == 1,emp["사원명"]))\
              .join(dept,emp["부서코드"] == dept["부서코드"],"inner")\
              .groupBy(emp["부서코드"],dept["부서명"]).agg(count(emp["사원번호"]).alias("전체사원수"),min("최종입사자사원명").alias("최종입사자사원명"))
#부서별 최종입사자명과 사원수를 구하는 데이터프레임

num_name

pjt_num = emp.join(pjt_use_hist,emp["사원번호"] == pjt_use_hist["사원번호"],"left_outer")\
             .groupBy(emp["부서코드"]).agg(countDistinct(pjt_use_hist["프로젝트번호"]).alias("프로젝트수"))
#부서별 프로젝트 수를 구하는 데이터프레임

pjt_num

homework3_1 = num_name.join(pjt_num,num_name["부서코드"] == pjt_num["부서코드"],"inner")\
                      .select(num_name["부서코드"],num_name["부서명"],num_name["전체사원수"],pjt_num["프로젝트수"],num_name["최종입사자사원명"])
#부서코드로 조인하여 원하는 결과를 얻어내는 데이터프레임

homework3_1

# [2] (1번의 결과 데이터를 기준으로) 각 부서 중, 전체사원수가 가장 많은 상위 2개 부서만 노출
#

homework3_2 = homework3_1.withColumn("rnk",row_number().over(Window.orderBy(desc("전체사원수"))))\
                         .select("부서코드","부서명","전체사원수","프로젝트수","최종입사자사원명")\
                         .orderBy("rnk")\
                         .limit(2)

homework3_2

# [3] 월별 사원별 휴가사용일수 및 투입 프로젝트
#     - 조회 대상은 휴가사용이력에 발생된 데이터를 기준으로 함
#     - 월+사원 별 휴가 사용 일수 집계
#     - 해당 사원이 해당 월에 투입되었던 프로젝트명을 포함 (투입 프로젝트가 없는 경우, '없음'으로 표기)
#

name_vc_num = vc_use_hist.join(emp,vc_use_hist["사원번호"] == emp["사원번호"],"inner")\
                         .groupBy(substring(vc_use_hist["휴가사용일자"],1,6).alias("휴가사용월"),vc_use_hist["사원번호"],emp["사원명"])\
                         .agg(count(emp["사원번호"]).alias("휴가사용일수"))
#휴가사용월별 사원번호,사원명,휴가사용일수를 구하는 데이터프레임

name_vc_num

pjt_name = pjt_use_hist.join(pjt,pjt_use_hist["프로젝트번호"] == pjt["프로젝트번호"],"inner")
#프로젝트 명을 구하는 데이터프레임

pjt_name

homework3_3 = name_vc_num.join(pjt_name,(name_vc_num["사원번호"] == pjt_name["사원번호"]) & \
                              (name_vc_num["휴가사용월"]).between(pjt_name["투입월"],pjt_name["종료월"]),"left_outer")\
                         .select(name_vc_num["휴가사용월"],name_vc_num["사원번호"],name_vc_num["사원명"],name_vc_num["휴가사용일수"],\
                                 coalesce(pjt_name["프로젝트명"],lit("없음")).alias("프로젝트명"))\
                         .orderBy(name_vc_num["휴가사용월"])
#2데이터프레임을 조인해서 원하는 결과를 얻고 프로젝트명이 없을 시 없음으로 표기

homework3_3

# [4] 사원별 가장 마지막으로 투입된 프로젝트, 해당 프로젝트 기간 동안 받은 총 급여액
#    - 대상은 사원 테이블의 모든 사원을 대상으로 함
#    - 각 사원 별 가장 마지막으로 투입되었던 프로젝트명을 포함 (투입 프로젝트가 없는 경우, '없음'으로 표기)
#    - 마지막 투입 프로젝트 기간 동안 받은 총 급여액 합계 포함 (프로젝트 투입 이력이 없는 경우, 급여지급을 기준으로 사원 별 총 급여지급액을 합산하여 반영)
#

pjt_name_sal =\
pjt_use_hist.withColumn("rn",row_number().over(Window.partitionBy("사원번호").orderBy("종료월")))\
            .join(pjt,(pjt_use_hist["프로젝트번호"] == pjt["프로젝트번호"]) & (col("rn") == 1),"inner")\
            .join(sal_pay,(pjt_use_hist["사원번호"] == sal_pay["사원번호"]) &\
                 (sal_pay["급여월"]).between(pjt_use_hist["투입월"],pjt_use_hist["종료월"]),"inner")\
            .groupBy(pjt_use_hist["사원번호"],pjt["프로젝트명"]).agg(sum(sal_pay["월급여"]).cast("int").alias("투입기간급여액"))
#사원번호별 최종종료월을 구해서 해당하는 프로젝트명을 구한다. 이후 급여지급 데이터프레임에서 급여월이 투입월과 종료월사이에 있는 급여들의 합을 구한다.

pjt_name_sal

total_sal = sal_pay.groupBy("사원번호").agg(sum("월급여").cast("int").alias("총급여"))
#위의 데이터프레임에 해당하지않을 시 총 급여를 구한다.

total_sal

homework3_4 =\
emp.join(pjt_name_sal,emp["사원번호"] == pjt_name_sal["사원번호"],"left_outer")\
   .join(total_sal,emp["사원번호"] == total_sal["사원번호"],"left_outer")\
   .select(emp["사원번호"],emp["사원명"],coalesce(pjt_name_sal["프로젝트명"],lit("없음")).alias("프로젝트명"),coalesce(pjt_name_sal["투입기간급여액"],total_sal["총급여"]).alias("급여"))\
   .orderBy(emp["사원번호"])
#프로젝트명이 없을 시 없음 표기 프로젝트에 투입되지않은 직원은 총급여를 표시하는 데이터프레임

homework3_4
