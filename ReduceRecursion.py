from pyspark import SparkConf, SparkContext

config=SparkConf()
config.setMaster("local")
sc=SparkContext(conf=config)

#RDD = sc.parallelize([1,4,7,2,7,2,4,7,2,9,10,11,3,5])

#print("Count by value: " + str(RDD.countByValue()))

#print("Max: " + str(RDD.max()))

#print("Sum: " + str(RDD.sum()))

#def addition(x,y):
	#return x+y

#print("Reduced Add: " + str(RDD.reduce(addition)))

def addition(x, y):
	return x+y

gradesRDD = sc.textFile("grades.txt")\
.map(lambda record: record.split("|"))\
.map(lambda record: [record[0], int(record[2])])\
.reduceByKey(addition)

print(type(gradesRDD))

studentsRDD = sc.textFile("students.txt")\
.map(lambda record: record.split("|"))\
.map(lambda record: [record[0], record[1]])

grades = gradesRDD.collect()
students = studentsRDD.collect()
#print(type(grades))
for record in grades:
	print record
for record in students:
	print record
#for record in result:
	#print record
	
#need the end result's rows to look like: ID, Name of student, Pass description
#0 exams failed: Pass
#1 exams failed: Repeat Exam
#2 exams failed: Repeat Course
#3 exams failed: Go Home