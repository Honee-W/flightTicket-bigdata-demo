#!/user/bin/python
# -*- coding:UTF-8 -*-
import os
from pyspark.sql import SparkSession
from config import MYSQL_USER,MYSQL_PWD,MYSQL_CONN,MYSQL_DRIVER

os.environ['SPARK_HOME'] = r"D:\Apache\spark-2.4.3-bin-hadoop2.7"

spark = SparkSession \
    .builder \
    .master("spark://192.168.23.136:7077") \
    .appName("flight info analyze") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
sc = spark.sparkContext


df = spark.read.load(
    "hdfs://master:9000/flight/西安到全国热门城市.csv",
    format="csv", sep=",", inferSchema="true", header="true",  encoding="gbk"
)

# 根据表创建临时视图
df.createGlobalTempView("ticket")


# 数据写入数据库
def writeIntoDB():
    test_df = spark.sql("select * from global_temp.ticket")
    conn_param = {}
    conn_param['user'] = MYSQL_USER
    conn_param['password'] = MYSQL_PWD
    conn_param['driver'] = MYSQL_DRIVER
    test_df.write\
        .jdbc(MYSQL_CONN, 'ArriveTicket', 'overwrite', conn_param)
    print('执行完毕')


# 统计总票数
def total():
    totalDF = spark.sql("select count(*) as total from global_temp.ticket")
    totalDF.show()

# 不同航司航班次数
def companyFlightCount():
    GH_df = spark.sql("select count(*) as total from global_temp.ticket where company = '中国国航'")
    GH_df.show()
    DH_df = spark.sql("select count(*) as total from global_temp.ticket where company = '东方航空'")
    DH_df.show()
    NH_df = spark.sql("select count(*) as total from global_temp.ticket where company = '南方航空'")
    NH_df.show()
    HN_df = spark.sql("select count(*) as total from global_temp.ticket where company = '海南航空'")
    HN_df.show()
    SZ_df = spark.sql("select count(*) as total from global_temp.ticket where company = '深圳航空'")
    SZ_df.show()
    XM_df = spark.sql("select count(*) as total from global_temp.ticket where company = '厦门航空'")
    XM_df.show()
    SC_df = spark.sql("select count(*) as total from global_temp.ticket where company = '四川航空'")
    SC_df.show()


# 不同航司最高及最低准点率
def punctualRate():
    GH_df = spark.sql("select punctualRate from global_temp.ticket where company = '中国国航' order by punctualRate*0.01 desc")
    GH_df.show()
    DH_df = spark.sql("select punctualRate from global_temp.ticket where company = '东方航空' order by punctualRate*0.01 desc")
    DH_df.show()
    XM_df = spark.sql("select punctualRate from global_temp.ticket where company = '厦门航空' order by punctualRate*0.01 desc")
    XM_df.show()
    SC_df = spark.sql("select punctualRate from global_temp.ticket where company = '四川航空' order by punctualRate*0.01 desc")
    SC_df.show()
    NF_df = spark.sql("select punctualRate from global_temp.ticket where company = '南方航空' order by punctualRate*0.01 desc")
    NF_df.show()
    SZ_df = spark.sql("select punctualRate from global_temp.ticket where company = '深圳航空' order by punctualRate*0.01 desc")
    SZ_df.show()
    HN_df = spark.sql("select punctualRate from global_temp.ticket where company = '海南航空' order by punctualRate*0.01 desc")
    HN_df.show()
    CL_df = spark.sql("select punctualRate from global_temp.ticket where company = '长龙航空' order by punctualRate*0.01 desc")
    CL_df.show()
    CA_df = spark.sql("select punctualRate from global_temp.ticket where company = '长安航空' order by punctualRate*0.01 desc")
    CA_df.show()
    HB_df = spark.sql("select punctualRate from global_temp.ticket where company = '河北航空' order by punctualRate*0.01 desc")
    HB_df.show()
    SD_df = spark.sql("select punctualRate from global_temp.ticket where company = '山东航空' order by punctualRate*0.01 desc")
    SD_df.show()
    KM_df = spark.sql("select punctualRate from global_temp.ticket where company = '昆明航空' order by punctualRate*0.01 desc")
    KM_df.show()


# 票价分布百分比
def priceDistribute():
    df_1 = spark.sql("select (count(case when lowestPrice < 500 then 1 end)/count(*))*100 as pct from global_temp.ticket")
    df_1.show()
    df_2 = spark.sql("select (count(case when lowestPrice >= 500 and lowestPrice < 1000 then 1 end)/count(*))*100 as pct from global_temp.ticket")
    df_2.show()
    df_3 = spark.sql("select (count(case when lowestPrice >= 1000 and lowestPrice < 1500 then 1 end)/count(*))*100 as pct from global_temp.ticket")
    df_3.show()
    df_4 = spark.sql("select (count(case when lowestPrice >= 1500 and lowestPrice < 2000 then 1 end)/count(*))*100 as pct from global_temp.ticket")
    df_4.show()
    df_5 = spark.sql("select (count(case when lowestPrice >= 2000 and lowestPrice < 2500 then 1 end)/count(*))*100 as pct from global_temp.ticket")
    df_5.show()
    df_6 = spark.sql("select (count(case when lowestPrice >= 2500 and lowestPrice < 3000 then 1 end)/count(*))*100 as pct from global_temp.ticket")
    df_6.show()
    df_7 = spark.sql("select (count(case when lowestPrice >= 3000 then 1 end)/count(*))*100 as pct from global_temp.ticket")
    df_7.show()


# 不同飞行时间机票统计
def differentTimeCount():
    print(spark.sql("select * from global_temp.ticket"))
    time_1 = spark.sql("select count(*) from global_temp.ticket where leaveTime >= 5 and leaveTime < 9")
    time_1.show()
    time_2 = spark.sql("select count(*) from global_temp.ticket where leaveTime >= 9 and leaveTime < 13")
    time_2.show()
    time_3 = spark.sql("select count(*) from global_temp.ticket where leaveTime >= 13 and leaveTime < 17")
    time_3.show()
    time_4 = spark.sql("select count(*) from global_temp.ticket where leaveTime >= 17 and leaveTime < 21")
    time_4.show()
    time_5 = spark.sql("select count(*) from global_temp.ticket where leaveTime >= 21 and leaveTime < 24")
    time_5.show()


total()
writeIntoDB()
companyFlightCount()
punctualRate()
priceDistribute()
differentTimeCount()
