from pyspark import SparkConf, SparkContext

config=SparkConf()
config.setMaster("local")
sc=SparkContext(conf=config)

uniqueNums = {0: "Zero", 1: "One", 2: "Two", 3: "Three", 4: "Four", 5: "Five", 6: "Six", 7: "Seven", 8: "Eight", 9: "Nine"}
teens = {11: "Eleven", 12: "Twelve", 13: "Thirteen", 14: "Fourteen", 15: "Fifteen", 16: "Sixteen", 17: "Seventeen", 18: "Eighteen", 19: "Nineteen"}
tens = {0: "Zero", 00: "Zero", 000: "Zero", 10: "Ten", 20: "Twenty", 30: "Thirty", 40: "Forty", 50: "Fifty", 60: "Sixty", 70: "Seventy", 80: "Eighty", 90: "Ninety"}


def numerify(number):
	def hundreds(num):
		if num == ("100"):
			return "One Hundred"
		else:
			return (uniqueNums[int(num[-3])] + " Hundred and " + doubleDigits(num[1:]))
	
	def doubleDigits(num):
		if len(num) == 1 or int(num[-2]) == 0:
			return uniqueNums[int(num)]
		elif int(num[-1]) == 0:
			return tens[int(num)]
		elif int(num[-2]) == 1:
			return teens[int(num)]
		else:
			ten = int(num) - int(num[-1])
			return(tens[ten] + " " + uniqueNums[int(num[-1])])
			
	if len(number) <= 2:
		return doubleDigits(number)
	else:
		return hundreds(number[-3:])

def splitRecord(record):
	return record.split("|")

def removefield(record, indice):
	del record[indice]
	return record
	
def percentageCalc(number):
	return round((float(number)/150)*100)
	
def passFail(percent):
	if percent >= 60:
		return True
	else:
		return False


RDD = sc.textFile("students.txt")\
.map(splitRecord)\
.map(lambda record: removefield(record, 2))\
.map(lambda record: (record[0], record[1], record[2], numerify(record[2]), percentageCalc(record[2]), passFail(percentageCalc(record[2]))))


data = RDD.collect()

for record in data:
	print(record)