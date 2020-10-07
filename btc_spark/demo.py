#spark_config
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf=SparkConf().setAppName("frugalops").setMaster("local[*]")
sc=SparkContext(conf=conf)

sparkSession = SparkSession.builder.appName("frugalops").master("local[*]").getOrCreate()
#===========================================================================================


#loader
def loadCSVfrom(file_path:str):
	df=sparkSession.read.csv(file_path,header=True,sep=",")
	return df

# df=loadCSVfrom(file_path="/home/pan/Documents/tutorial/btc_spark/data/btc.csv")
# df.show();
#===========================================================================================


#reduce_service
import os
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as func

def aggregateBlocks(df:DataFrame):
	df.agg(func.sum("blockCount")).show()

df=loadCSVfrom(file_path="data/btc.csv")
print("total record count: %d" % (df.count()))

aggregateBlocks(df)
#==========================================================================================


#figure out blocksize were traded in each month in 2018 and 2019
def blocksize_month(df:DataFrame):
	res = df.groupBy('NEW_DATE').agg(func.sum("blocksize"))
	res.show()
	return res 

df=loadCSVfrom(file_path="data/btc.csv")
df1 = df.withColumn("NEW_DATE", df['date'].substr(1, 7))
df1.show()

print("total record count: %d" % (df.count()))

df2=blocksize_month(df1)
df2.filter("NEW_DATE LIKE '2018%' OR NEW_DATE LIKE '2019%'").show()