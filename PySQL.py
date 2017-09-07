from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.functions import col
import pyspark.sql.functions as sqlfn

config=SparkConf()
config.setMaster("local")
sc=SparkContext(conf=config)
sql=SQLContext(sc)

records = sc.textFile("ratings.txt")\
.map(lambda record: record.split("|"))

DF1 = sql.createDataFrame(records)

#print("--- DF1.printSchema")
#DF1.printSchema()

#print("--- DF1.show()")
#]DF1.show(20) 									#Prints 20 records
#print(DF1.count()) 								#Returns total records
#DF1.select("_1", "_2").show() 					#Shows Column _1 and _1
#DF1.select(DF1._1, DF1._2).show()				#Does the same
#DF1.select(DF1._1).filter(DF1._1>100).show()	#Shows results where column _1 is >100. Automatically parsed from String

DF1\
.select(DF1._3.alias("Rating"), DF1._2.alias("Film"))\
.filter(DF1._2 == 90)\
.show()