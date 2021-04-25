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

# - 월별 평균 급여지급 합계를 구하시오. (단, 이미 퇴사한 직원의 급여는 계산에서 제외)
# - 월별 퇴사자/입사자 수를 구하시오.
# - 월별 휴가를 사용한 사원의 수를 구하시오. (사원 1명이 휴가 3일 사용한 경우에도 건수는 1로 집계)
#

retired= emp.withColumnRenamed("퇴사일자","end_date").where(expr("end_date is not null"))#퇴사자의 사원번호를 얻을 수 있는 데이터프레임

retired

with_no_retired = sal_pay.join(retired,sal_pay["사원번호"] == retired["사원번호"],"left_anti")#퇴사자가 포함되지않은 급여지급 데이터프레임

with_no_retired

avg_sal_no_retired = with_no_retired.groupBy("급여월").agg(round(avg("월급여"),0).alias("평균급여액")) #퇴사자가 포함되지 않은 월별 평균급여액

retired_num = emp.withColumn("퇴사일자",translate(substring(col("퇴사일자"),1,7),'/',''))\
                 .groupBy(col("퇴사일자")).agg(count("*").alias("퇴사자수"))
#월별 퇴사자수를 구하는 데이터프레임

monthly_vc_use_hist = vc_use_hist.withColumn("휴가사용월",substring(col("휴가사용일자"),1,6))\
                                 .groupBy(col("휴가사용월"),col("사원번호")).agg(countDistinct("사원번호").alias("휴가사용사원수"))
#월별 휴가사용사원수를 구하는 데이터프레임

monthly_vc_use_hist

homework2_1 = avg_sal_no_retired.join(retired_num,avg_sal_no_retired["급여월"] == retired_num["퇴사일자"],"left_outer")\
                                .join(monthly_vc_use_hist,avg_sal_no_retired["급여월"] == monthly_vc_use_hist["휴가사용월"],"left_outer")\
                                .groupBy(avg_sal_no_retired["급여월"],avg_sal_no_retired["평균급여액"],retired_num["퇴사자수"])\
                                .agg(sum(monthly_vc_use_hist["휴가사용사원수"]).alias("휴가사용사원수"))\
                                .orderBy(avg_sal_no_retired["급여월"])
#급여월별로 휴가사용한 사원수를 모두 더해서 휴가사용사원수를 구한다.

homework2_1

# - 사원별 휴가 사용일수를 구하시오. (휴가를 사용하지 않은 사원도 포함)
# - 호봉제 사원의 경우, 호봉도 포함
# - 각 사원별 최초/최종 직급을 포함
# - 각 사원별 최초 지급된 급여액을 포함
#

#필요한 데이터프레임 사원,부서,호봉제급여,휴가사용이력,발령이력,급여지급(emp,dept,hobong_sal,vc_use_hist,iss_hist,sal_pay)
bseq_fseq = iss_hist.withColumn("RN",row_number().over(Window.partitionBy("사원번호").orderBy("이력순번")))\
                    .withColumn("DESC_RN",row_number().over(Window.partitionBy("사원번호").orderBy(desc("이력순번"))))
#최초순번 최종순번을 포함한 데이터프레임

bseq_lseq

brank_frank = bseq_fseq.withColumn("최초직급",when(col("RN") == 1,col("직급")))\
                       .withColumn("최종직급",when(col("DESC_RN") == 1,col("직급")))\
                       .groupBy(col("사원번호")).agg(min("최초직급").alias("최초직급"),min("최종직급").alias("최종직급"))
#sql에서는 min(case when rn = 1 then 직급 end) 인 구문을 pyspark에서는 withcolumn으로 when(col("RN") == 1,col("직급"))처럼 열을 추가시켜서
#group by.agg에서 min으로 묶어준다.

brank_frank

bsal = sal_pay.groupBy(col("사원번호")).agg(min("급여월").alias("최초급여월"),min("월급여").alias("최초월급여"))
#급여지급 데이터프레임에서 사원번호별 최초급여월의 급여를 얻어내기위해 사원번호별,최초급여월,월급여를 구하는 데이터프레임

bsal

vc_use_num = vc_use_hist.groupBy(col("사원번호")).agg(count("휴가사용일자").alias("휴가사용일수"))
#휴가사용이력 데이터프레임에서 사우너번호별 휴가사용일수를 구하는 데이터프레임

vc_use_num

homework2_2 = emp.join(dept,emp["부서코드"] == dept["부서코드"],"inner")\
                 .join(hobong_sal,emp["사원번호"] == hobong_sal["사원번호"],"left_outer")\
                 .join(brank_frank,emp["사원번호"] == brank_frank["사원번호"],"inner")\
                 .join(bsal,emp["사원번호"] == bsal["사원번호"],"left_outer")\
                 .join(vc_use_num,emp["사원번호"] == vc_use_num["사원번호"],"left_outer")\
                 .select(emp["사원명"],dept["부서명"],hobong_sal["호봉"],vc_use_num["휴가사용일수"],brank_frank["최초직급"],brank_frank["최종직급"],bsal["최초월급여"])
#각 데이터프레임을 사원데이터프레임의 사원번호로 조인한다.

homework2_2
