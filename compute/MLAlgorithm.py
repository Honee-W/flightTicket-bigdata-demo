#!/user/bin/python
# -*- coding:UTF-8 -*-
from pyspark import Row
from pyspark.ml import Pipeline
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.feature import RegexTokenizer, FeatureHasher, Word2Vec, StringIndexer, VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.stat import Correlation
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from pyspark.ml.clustering import BisectingKMeans, GaussianMixture
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import SparkSession

# 创建SparkSession
from pyspark.sql.functions import regexp_replace

from MLTesting.MLBasic import featureHasher

spark = SparkSession\
    .builder\
    .appName("MLAlgorithm")\
    .getOrCreate()

# 对机票价格进行聚类，采用二分K均值算法
def priceClustering():
    # dataframe读取CSV文件可直接设置option("header", "true"), 将首行作为表信息
    rawData = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("../data/arrive/全国热门城市到西安.csv")
    rawData = rawData.withColumn("lowestPrice", rawData["lowestPrice"].cast('Integer'))
    rawData = spark.createDataFrame(rawData.select("id", "lowestPrice").collect(), ["id", "features"]).rdd

    #处理数据，将dataframe转换为libsvm格式用于机器学习算法
    data = rawData.map(lambda line: LabeledPoint(line[0], line[1:]))
    print(data)
    MLUtils.saveAsLibSVMFile(data, "../data/arrive/price")

    for i in range(0, 8):
        with open('../data/arrive/price/part-0000{}'.format(i)) as f :
            file = open("../data/arrive/price.txt", "a+")
            for line in f.readlines():
                file.write(line)

    # 加载数据
    dataset = spark.read.format("libsvm").load("../data/arrive/price.txt")

    # 训练二分K均值模型
    bkm = BisectingKMeans().setK(8).setSeed(1)
    model = bkm.fit(dataset)

    # 做预测
    predictions = model.transform(dataset)

    # 评估预测结果
    evaluator = ClusteringEvaluator()

    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))

    # 展示结果
    print("Cluster Centers: ")
    centers = model.clusterCenters()
    for center in centers:
        print(center)


# 对准点率进行聚类，采用高斯混合模型算法 GMM
def punctualRateClustering():
    # 读取并处理数据
    rawData = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(
        "../data/leave/西安到全国热门城市.csv")
    rawData = rawData.withColumn("punctualRate", regexp_replace(str="punctualRate", pattern="%", replacement=""))
    rawData = spark.createDataFrame(rawData.select("id", "punctualRate").collect(), ["id", "features"]).rdd

    #处理数据，将dataframe转换为libsvm格式用于机器学习算法
    data = rawData.map(lambda line: LabeledPoint(line[0], line[1:]))
    MLUtils.saveAsLibSVMFile(data, "../data/leave/punctualRate")

    for i in range(0, 8):
        with open("../data/leave/punctualRate/part-0000{}".format(i)) as f:
            file = open("../data/leave/punctualRate.txt", "a+")
            for line in f.readlines():
                file.write(line)

    # 加载数据
    dataset = spark.read.format("libsvm").load("../data/leave/punctualRate.txt")
    # 训练GMM模型
    gmm = GaussianMixture().setK(5).setSeed(1)
    model = gmm.fit(dataset)
    # 做预测
    predictions = model.transform(dataset)
    predictions.select("features", "prediction", "probability").show()

# 对起飞时间进行聚类，采用二分K均值算法
def leaveTimeClustering():
    # 读取并处理数据
    rawData = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("../data/leave/西安到全国热门城市.csv")
    rawData = rawData.withColumn("leaveTime", regexp_replace(str="leaveTime", pattern=":", replacement=""))
    rawData = rawData.withColumn("leaveTime", rawData["leaveTime"].cast('Integer'))
    rawData = rawData.select("id", "leaveTime")
    # rawData.show(truncate=False)

    #处理数据，将dataframe转换为libsvm格式用于机器学习算法
    data = rawData.rdd.map(lambda line: LabeledPoint(line[0], line[1:]))
    MLUtils.saveAsLibSVMFile(data, "../data/leave/leaveTime")

    with open("../data/leave/leaveTime/part-00000") as f :
        file = open("../data/leave/leaveTime.txt", "a+")
        for line in f.readlines():
            file.write(line)

    # 读取数据
    dataset = spark.read.format("libsvm").load("../data/leave/leaveTime.txt")
    dataset.show()
    # 训练模型
    bmk = BisectingKMeans().setK(6).setSeed(1)
    model = bmk.fit(dataset)
    # 做预测
    predictions = model.transform(dataset)
    # 评估模型
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))
    # 展示结果
    print("Cluster Centers: ")
    centers = model.clusterCenters()
    for center in centers:
        print(center)

# 对落地时间进行聚类，采用二分K均值算法
def arriveTimeClustering():
    # 读取并处理数据
    rawData = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("../data/leave/西安到全国热门城市.csv")
    rawData = rawData.withColumn("arriveTime", regexp_replace(str="arriveTime", pattern=":", replacement=""))
    rawData = rawData.withColumn("arriveTime", rawData["arriveTime"].cast('Integer'))
    rawData = rawData.select("id", "arriveTime")
    # rawData.show(truncate=False)

    #处理数据，将dataframe转换为libsvm格式用于机器学习算法
    data = rawData.rdd.map(lambda line: LabeledPoint(line[0], line[1:]))
    MLUtils.saveAsLibSVMFile(data, "../data/leave/arriveTime")

    with open("../data/leave/arriveTime/part-00000") as f :
        file = open("../data/leave/arriveTime.txt", "a+")
        for line in f.readlines():
            file.write(line)

    # 读取数据
    dataset = spark.read.format("libsvm").option("numFeatures", 1).load("../data/leave/arriveTime.txt")
    dataset.show()
    # 训练模型
    bmk = BisectingKMeans().setK(6).setSeed(1)
    model = bmk.fit(dataset)
    # 做预测
    predictions = model.transform(dataset)
    # 评估模型
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))
    # 展示结果
    print("Cluster Centers: ")
    centers = model.clusterCenters()
    for center in centers:
        print(center)

# 航空公司和准点率做回归分析，采用MLPC回归算法
def cpRegression():
    # 数据加载和处理
    rawData = spark.read.format("csv").option("delimiter", ",").option("encoding", "GBK").option("header", "true").load("../data/leave/西安到全国热门城市.csv")
    rawData = rawData.withColumn("punctualRate",regexp_replace(str="punctualRate", pattern="%", replacement=""))
    rawData = rawData.select("company", "punctualRate")

    # 将航空公司编码为索引 默认按出现频率降序排列
    indexer = StringIndexer(inputCol="company", outputCol="companyIndex")
    indexed = indexer.fit(rawData).transform(rawData)
    # indexed.show()

    # 处理数据并以libsvm格式保存
    rawData = spark.createDataFrame(indexed.select("companyIndex", "punctualRate").collect(), ["companyIndex", "features"]).rdd
    rawData = rawData.map(lambda line: LabeledPoint(line[0], line[1:]))
    MLUtils.saveAsLibSVMFile(rawData, "../data/leave/cpRegression")

    for i in range(0, 8):
        with open("../data/leave/cpRegression/part-0000{}".format(i)) as f:
            file = open("../data/leave/cpRegression.txt", "a+")
            for line in f.readlines():
                file.write(line)

    dataset = spark.read.format("libsvm").load("../data/leave/cpRegression.txt")
    (trainingData, testData) = dataset.randomSplit([0.7, 0.3], 1234)

    # 输入层为1（特征），中间层为4，1，输出层为30（分30类）--30个不同航司
    layers = [1, 4, 1, 30]
    # 创建模型，设置参数
    trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=128, seed=1234)
    # 训练模型
    model = trainer.fit(trainingData)
    # 在测试集上验证准确率
    result = model.transform(testData)
    predictionAndLabels = result.select("prediction", "label")
    predictionAndLabels.show()
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))

# 航空公司和机票价格做回归分析，采用MLPC回归算法
def clRegression():
    # 数据加载和处理
    rawData = spark.read.format("csv").option("delimiter", ",").option("encoding", "GBK").option("header", "true").load("../data/leave/西安到全国热门城市.csv")
    rawData = rawData.select("company", "lowestPrice")

    # 将航空公司编码为索引 默认按出现频率降序排列
    indexer = StringIndexer(inputCol="company", outputCol="companyIndex")
    indexed = indexer.fit(rawData).transform(rawData)
    # indexed.show()

    # 处理数据并以libsvm格式保存
    rawData = spark.createDataFrame(indexed.select("companyIndex", "lowestPrice").collect(), ["companyIndex", "features"]).rdd
    rawData = rawData.map(lambda line: LabeledPoint(line[0], line[1:]))
    MLUtils.saveAsLibSVMFile(rawData, "../data/leave/clRegression")

    for i in range(0, 8):
        with open("../data/leave/clRegression/part-0000{}".format(i)) as f:
            file = open("../data/leave/clRegression.txt", "a+")
            for line in f.readlines():
                file.write(line)

    dataset = spark.read.format("libsvm").load("../data/leave/clRegression.txt")
    (trainingData, testData) = dataset.randomSplit([0.7, 0.3], 1234)

    # 输入层为1（特征），中间层为4，1，输出层为30（分30类）
    layers = [1, 4, 1, 30]
    # 创建模型，设置参数
    trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=128, seed=1234)
    # 训练模型
    model = trainer.fit(trainingData)
    # 在测试集上验证准确率
    result = model.transform(testData)
    predictionAndLabels = result.select("prediction", "label")
    predictionAndLabels.show()
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))

# 城市和机票价格做回归分析，采用MLPC回归算法
def cityPriceRegression():
    # 数据加载和处理
    rawData = spark.read.format("csv").option("delimiter", ",").option("encoding", "GBK").option("header", "true").load("../data/leave/西安到全国热门城市.csv")
    rawData = rawData.select("destination", "lowestPrice")

    # 将城市编码为索引 默认按出现频率降序排列
    indexer = StringIndexer(inputCol="destination", outputCol="destinationIndex")
    indexed = indexer.fit(rawData).transform(rawData)
    indexed.show()

    # 处理数据并以libsvm格式保存
    rawData = spark.createDataFrame(indexed.select("destinationIndex", "lowestPrice").collect(), ["destinationIndex", "features"]).rdd
    rawData = rawData.map(lambda line: LabeledPoint(line[0], line[1:]))
    MLUtils.saveAsLibSVMFile(rawData, "../data/leave/cityPriceRegression")

    for i in range(0, 8):
        with open("../data/leave/cityPriceRegression/part-0000{}".format(i)) as f:
            file = open("../data/leave/cityPriceRegression.txt", "a+")
            for line in f.readlines():
                file.write(line)

    dataset = spark.read.format("libsvm").load("../data/leave/cityPriceRegression.txt")
    (trainingData, testData) = dataset.randomSplit([0.7, 0.3], 1234)

    # 输入层为1（特征），中间层为4，1，输出层为27（分27类）27个不同城市
    layers = [1, 4, 1, 27]
    # 创建模型，设置参数
    trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=128, seed=1234)
    # 训练模型
    model = trainer.fit(trainingData)
    # 在测试集上验证准确率
    result = model.transform(testData)
    predictionAndLabels = result.select("prediction", "label")
    predictionAndLabels.show()
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))

def correlation():
    # 加载并处理数据
    rawData = spark.read.format("csv").option("delimiter", ",").option("encoding", "GBK").option("header", "true").load("../data/leave/西安到全国热门城市.csv")
    rawData = rawData.withColumn("leaveTime", regexp_replace("leaveTime", ":", ""))
    rawData = rawData.withColumn("leaveTime", rawData["leaveTime"].cast("Integer"))
    rawData = rawData.withColumn("lowestPrice", rawData["lowestPrice"].cast("Integer"))
    rawData = rawData.withColumn("punctualRate", regexp_replace("punctualRate", "%", ""))
    rawData = rawData.withColumn("punctualRate", rawData["punctualRate"].cast("Integer"))


    # 将航空公司编码为索引 默认按元素出现频率降序排列
    indexer = StringIndexer(inputCol="company", outputCol="companyIndex")
    indexed = indexer.fit(rawData).transform(rawData)

    # 计算起飞时间与机票价格的相关性
    print("起飞时间与机票价格的样本协方差: "+ str(indexed.cov("leaveTime", "lowestPrice")))
    print("起飞时间与机票价格的Pearson相关系数: "+ str(indexed.stat.corr("leaveTime", "lowestPrice", "pearson")))

    # 计算航空公司与机票价格的相关性
    print("航空公司与机票价格的样本协方差: "+ str(indexed.cov("companyIndex", "lowestPrice")))
    print("航空公司与机票价格的Pearson相关系数: "+ str(indexed.stat.corr("companyIndex", "lowestPrice", "pearson")))

    # 计算航空公司与准点率的相关性
    print("航空公司与准点率的样本协方差: "+ str(indexed.cov("companyIndex", "punctualRate")))
    print("航空公司与准点率的Pearson相关系数: "+ str(indexed.stat.corr("companyIndex", "punctualRate", "pearson")))

    # 计算城市和机票价格的相关性
    # 将城市编码为索引 默认按元素出现频率降序排列
    indexer = StringIndexer(inputCol="destination", outputCol="cityIndex")
    indexed = indexer.fit(indexed).transform(indexed)
    print("城市与机票价格的样本协方差: "+ str(indexed.cov("cityIndex", "lowestPrice")))
    print("城市与机票价格的Pearson相关系数: "+ str(indexed.stat.corr("cityIndex", "lowestPrice", "pearson")))



spark.stop()