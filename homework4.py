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
#  - 각 사원 별 프로젝트투입횟수, 휴가사용일수를 구하시오.
#  - 프로젝트에 한번이라도 투입되었거나, 휴가를 한번이라도 사용한 사원만 포함함 (휴가사용, 프로젝트 투입 경험이 없는 사원은 조회 결과에서 제외)
#  - 단, 사원 테이블은 사용하지 않고 작성 (프로젝트투입이력, 휴가사용이력 테이블만 사용)
#

pjt_num = pjt_use_hist.groupBy("사원번호").agg(count("프로젝트번호").alias("프로젝트투입수"))

pjt_num

vc_num = vc_use_hist.groupBy("사원번호").agg(count("휴가사용일자").alias("휴가사용일"))

vc_num

homework4_1 =\
pjt_num.join(vc_num,pjt_num["사원번호"] == vc_num["사원번호"],"full_outer")\
       .where(~((pjt_num["프로젝트투입수"] == 0) & (vc_num["사원번호"] == 0)))\
       .withColumn("프로젝트경험여부",when(col("프로젝트투입수") >= 1,"Y").otherwise("N"))\
       .withColumn("휴가사용여부",when(col("휴가사용일") >= 1,"Y").otherwise("N"))\
       .select(coalesce(pjt_num["사원번호"],vc_num["사원번호"]).alias("사원번호"),pjt_num["프로젝트투입수"],vc_num["휴가사용일"],"프로젝트경험여부","휴가사용여부")
#두 개의 데이터프레임을 사원번호로 full_outer 조인하고 pjt_num 사원번호가 null인부분을 vc_num사원번호로채움

homework4_1

# [2]
#  - 아래 프로젝트에 투입되었던 사원을 제외한 모든 사원을 조회
#    . 제외 대상 프로젝트번호: A00, A01
#  - 조회 SQL은 3가지 방법으로 각각 작성
#   1) 조인 활용
#

no_00_01 = pjt_use_hist.where(col("프로젝트번호").isin("A00","A01"))

no_00_01

homework4_2 = emp.join(no_00_01,emp["사원번호"] == no_00_01["사원번호"],"left_anti")\
                 .select(emp["사원번호"],emp["사원명"])
#프로젝트번호 A00,A01이 아닌 사원번호들이 출력되게 하는 데이터프레임

homework4_2
