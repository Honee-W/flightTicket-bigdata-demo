# flightTicket-bigdata-demo

## a simple python-flask web project using mysql, spark, hdfs and E-charts.

To learn some basic applications about bigdata framework, l use VMware to build 3 virtual machines which serve as database as well as nodes for spark and hdfs.

### data requiring ---  crawlers
use regular expressions to match and collect data from static HTML

### data storing --- hdfs and mysql
data is stored in CSV and uploaded to remote hdfs
use spark to read data from hdfs and write into mysql

### data analyzing --- spark, spark-ml
use rdd and dataframe to analyse flight ticket information === Regression, Clustering,etc.

### data visualization --- E-charts
use JS and templates to show the charts

### ticket recommendation
show the tickets according to the specific requests

### screen shots:
![截图1](https://github.com/Honee-W/flightTicket-bigdata-demo/blob/master/screenshots/img1.jpg "截图1")
![截图2](https://github.com/Honee-W/flightTicket-bigdata-demo/blob/master/screenshots/img2.jpg "截图2")
![截图3](https://github.com/Honee-W/flightTicket-bigdata-demo/blob/master/screenshots/img3.jpg "截图3")
![截图4](https://github.com/Honee-W/flightTicket-bigdata-demo/blob/master/screenshots/img4.jpg "截图4")

## 基于flask框架的简单大数据项目
### --- 爬虫获取数据  （添加IP代理池功能，使用https://github.com/Python3WebSpider/ProxyPool )
### --- spark_rdd处理并写入数据
### --- 数据存储在mysql
### --- 运用JS模板对数据进行简单展示
