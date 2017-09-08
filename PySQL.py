from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

config=SparkConf()
config.setMaster("local")
sc=SparkContext(conf=config)
sql=SQLContext(sc)

movies = sc.textFile("file:///home/cloudera/Desktop/Python/item.txt")\
	.map(lambda record: record.split("|"))\
	.map(lambda record: [int(record[0]), record[1]]).collect()

records = sc.textFile("file:///home/cloudera/Desktop/Python/ratings.txt")\
.map(lambda record: record.split("|"))\
.map(lambda record: [int(record[0]), int(record[1]), int(record[2]), int(record[3])])

#Creates the DataFrame, parsing all elements to the types specified, naming columns also.
schema=StructType([\
StructField("userid",LongType(),True),\
StructField("filmid", LongType(),True),\
StructField("rating", LongType(),True),\
StructField("timestamp", LongType(),True)])

#Can be used instead, just to label the columns
#columnnames = ["userid","filmid","rating","timestamp"]

movieschema=StructType([
StructField("filmid", LongType(), True),
StructField("filmname", StringType(), True)])

DF1 = sql.createDataFrame(records, schema)
DF2 = sql.createDataFrame(movies, movieschema)

#print("--- DF1.show()")
#DF1.show(20) 							#Prints 20 records
#print(DF1.count()) 						#Returns total records
#DF1.select("userid", "filmid").show() 				#Shows Column _1 and _2
#DF1.select(DF1.userid, DF1.filmid).show()			#Does the same
#DF1.select(DF1.userid).filter(DF1.userid>100).show()		#Shows results where column _1 is >100. Automatically parsed from String

#Counts all the ratings of film 90, Blade Runner
RatingsCount =(DF1.select(DF1.rating, DF1.filmid)	#Reduces the columns to just rating and filmid
	.filter(DF1.filmid == 90)			#filters where the filmid is 90, this is "Blade Runner"
	.groupBy("rating")				#Groups by rating, the below is required to make anything meaningful of this.
	.count())					#Counts the occurrence of each rating

RatingsCount.show()					#Displays the transformed data

#Counts how many distinct users and films there are
print("Distinct Users: " + str(DF1.select(DF1.userid)
		.distinct()
		.count()))

print("Distinct Films: " + str(DF1.select(DF1.filmid)
		.distinct()
		.count()))

#Counts all the ratings of films
AverageRatings =(DF1.select(DF1.rating, DF1.filmid)		#Reduces the columns to just rating and filmid
		.groupBy("filmid")				#Groups by rating, the below is required to make anything meaningful of this.
		.avg("rating")					#Counts the occurrence of each rating
		.join(DF2, DF1.filmid == DF2.filmid, 'inner')	#Joins the two created dataframes together, using filmid for reference
		.drop("filmid")					#Drops both duplicate versions of filmid, dropDuplicates() removes duplicate rows, not duplicate columns
		.sort(desc("avg(rating)")))			#Orders By Rating

AverageRatings.show(20)						#Displays the transformed data AverageRatings.count()



