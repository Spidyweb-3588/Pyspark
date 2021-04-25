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

# [1]
# -- 프로젝트 / 투입직원수 / 투입직원들의 프로젝트 기간 내 받은 총 급여 / 프로젝트 기간 내 휴가 사용 직원 수 / 휴가 사용 일수
# -- 발령이력을 기준으로 최종 직급이 과장 이상(과장, 차장)인 직원은 집계 대상에서 제외
#

final_rn = iss_hist.withColumn("DESC_RN",row_number().over(Window.partitionBy("사원번호").orderBy(desc("이력순번"))))

final_rn

no_gwa_cha = emp.join(final_rn,(emp["사원번호"] == final_rn["사원번호"]) & (final_rn["DESC_RN"] == 1),"left_outer")\
                .where(~final_rn["직급"].isin("과장","차장"))\
                .select(final_rn["사원번호"])

no_gwa_cha

term_sal = pjt_use_hist.join(sal_pay,(pjt_use_hist["사원번호"] == sal_pay["사원번호"]) & (sal_pay["급여월"].between(pjt_use_hist["투입월"],pjt_use_hist["종료월"])))\
                       .groupBy(sal_pay["사원번호"]).agg(sum(sal_pay["월급여"]).cast("int").alias("기간내급여"))


term_sal

vc_month_usecnt = vc_use_hist.groupBy(col("사원번호"),substring(col("휴가사용일자"),1,6).alias("휴가사용월"))\
                            .agg(count("휴가사용일자").alias("휴가사용일수"))

vc_month_usecnt

homework5_1 =\
pjt_use_hist.join(no_gwa_cha,pjt_use_hist["사원번호"] == no_gwa_cha["사원번호"])\
            .join(term_sal,term_sal["사원번호"] == pjt_use_hist["사원번호"])\
            .join(vc_month_usecnt,(vc_month_usecnt["사원번호"] == pjt_use_hist["사원번호"]) & (vc_month_usecnt["휴가사용월"].between(pjt_use_hist["투입월"],pjt_use_hist["종료월"])),"left_outer")\
            .groupBy(pjt_use_hist["프로젝트번호"])\
            .agg(count(pjt_use_hist["사원번호"]).alias("투입직원수"),sum(term_sal["기간내급여"]).alias("총급여"),\
                 count(vc_month_usecnt["사원번호"]).alias("휴가사용직원수"),sum(vc_month_usecnt["휴가사용일수"]).alias("휴가사용일수"))\
            .orderBy(pjt_use_hist["프로젝트번호"])


homework5_1

# [2]
# -- 각 사원별 직급별 급여합계 (단, 급여지급 내역이 존재하지 않는 사원도 포함되어야 함
#

iss_smonth_emonth = iss_hist.withColumn("발령시작일자",translate(substring(col("발령일자"),1,7),'/',''))\
                            .withColumn("발령종료일자",coalesce(translate(substring(lead("발령일자").over(Window.partitionBy("사원번호").orderBy("발령시작일자")),1,7),'/','')-1,lit(999912)).cast("int").cast("string"))

iss_smonth_emonth

homework5_2 = iss_smonth_emonth.join(sal_pay,(iss_smonth_emonth["사원번호"] == sal_pay["사원번호"]) &\
                                    (sal_pay["급여월"].between(iss_smonth_emonth["발령시작일자"],iss_smonth_emonth["발령종료일자"])))\
                               .withColumn("사원급여",when(iss_smonth_emonth["직급"] == '사원',sal_pay["월급여"]))\
                               .withColumn("대리급여",when(iss_smonth_emonth["직급"] == '대리',sal_pay["월급여"]))\
                               .withColumn("과장급여",when(iss_smonth_emonth["직급"] == '과장',sal_pay["월급여"]))\
                               .withColumn("차장급여",when(iss_smonth_emonth["직급"] == '차장',sal_pay["월급여"]))\
                               .groupBy(iss_smonth_emonth["사원번호"])\
                               .agg(sum("사원급여").cast("int").alias("사원_급여합"),sum("대리급여").cast("int").alias("대리_급여합"),\
                                    sum("과장급여").cast("int").alias("과장_급여합"),sum("차장급여").cast("int").alias("차장_급여합"))\
                               .orderBy("사원번호")

homework5_2
