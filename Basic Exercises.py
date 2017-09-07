from pyspark import SparkConf, SparkContext

config=SparkConf()
config.setMaster("local")
sc=SparkContext(conf=config)


#list = [1,2,3,4,5, 6, 7, 8, 9, 10]
#RDD = sc.parallelize(list)
#data = RDD.collect()

#print(data)

def genderise(record):
	splitted = record.split("|")
	if splitted[2] == "M":
		splitted[2] = "male"
	else:
		splitted[2] = "female"
	return splitted
	
def removefield(record, indice):
	del record[indice]
	return record

RDD = sc.textFile("people.txt").map(genderise).map(lambda record: removefield(record, 2))

data = RDD.collect()

for record in data:
	print(record)
	
# RDD.union(anotherRDD) merges two rdds
# RDD.intersection(anotherRDD) returns an RDD with only elements that exist in both
# RDD.subtract(anotherRDD) return all values in the first that is not contained in the second.