from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf=SparkConf().setAppName("frugalops").setMaster("local[*]")
sc=SparkContext(conf=conf)

sparkSession = SparkSession.builder.appName("frugalops").master("local[*]").getOrCreate()