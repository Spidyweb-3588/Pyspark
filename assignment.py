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

cnts_prod_mst_20210205 = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/homework_2/cnts_prod_mst_20210205.csv")
cnts_prod_mst_20210206 = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/homework_2/cnts_prod_mst_20210206.csv")
cnts_prod_mst_20210207 = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/homework_2/cnts_prod_mst_20210207.csv")
cnts_prod_mst_20210208 = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/homework_2/cnts_prod_mst_20210208.csv")
vod_wat_day_20210206 = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/homework_2/vod_wat_day_20210206.csv")
vod_wat_day_20210207 = spark.read.format("csv").option("header","true").load("C:/Users/b2en/Downloads/Spark 실습/homework_2/vod_wat_day_20210207.csv")

from pyspark.sql.functions import *
from pyspark.sql.window import *

cnts_prod_mst_20210205.withColumn("DATA_END_DT",coalesce(lead(col("STRD_DT")).over(Window.orderBy("STRD_DT")),lit('99991231')))\
                      .withColumn("DATA_STA_DT",col("STRD_DT"))\
                      .select("CNTS_PROD_ID","DATA_STA_DT","DATA_END_DT","CNTS_PROD_NM","CNTS_PROD_TYP_CD","PROD_PRC","SALE_PRC","USE_YN")

cnts_prod_mst_20210205.union(cnts_prod_mst_20210206)\
                      .withColumn("DATA_END_DT",coalesce(lead(col("STRD_DT")).over(Window.orderBy("STRD_DT")),lit('99991231')))\
                      .withColumn("DATA_STA_DT",col("STRD_DT"))\
                      .select("CNTS_PROD_ID","DATA_STA_DT","DATA_END_DT","CNTS_PROD_NM","CNTS_PROD_TYP_CD","PROD_PRC","SALE_PRC","USE_YN")

cnts_prod_mst_20210205.union(cnts_prod_mst_20210206)\
                      .union(cnts_prod_mst_20210207)\
                      .withColumn("DATA_END_DT",coalesce(lead(col("STRD_DT")).over(Window.orderBy("STRD_DT")),lit('99991231')))\
                      .withColumn("DATA_STA_DT",col("STRD_DT"))\
                      .select("CNTS_PROD_ID","DATA_STA_DT","DATA_END_DT","CNTS_PROD_NM","CNTS_PROD_TYP_CD","PROD_PRC","SALE_PRC","USE_YN")

vod_wat_day_20210206.join(cnts_prod_mst_20210206,vod_wat_day_20210206["CNTS_PROD_ID"] == cnts_prod_mst_20210206["CNTS_PROD_ID"],"inner")\
                    .withColumn("PYFR_YN",when(cnts_prod_mst_20210206["SALE_PRC"] >= 0,lit('Y')).otherwise(lit('N')))\
                    .select(vod_wat_day_20210206["STRD_DT"],vod_wat_day_20210206["SVC_MGMT_NUM"],vod_wat_day_20210206["CNTS_PROD_ID"],\
                            cnts_prod_mst_20210206["CNTS_PROD_NM"],vod_wat_day_20210206["WAT_CNT"],col("PYFR_YN"),cnts_prod_mst_20210206["SALE_PRC"])


