from pyspark import SparkConf, SparkContext

config=SparkConf()
config.setMaster("local")
sc=SparkContext(conf=config)

ratingsRDD = sc.textFile("ratings.txt")\
.map(lambda record: record.split("|"))\
.filter(lambda record: record[1] == "2") # and record[2] == "5" in the parentheses filters by five stars

result = ratingsRDD.collect()

ratings = [0, 0, 0, 0, 0]

for record in result:
	ratings[(int(record[2])-1)] += 1 

print(ratings)
