from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

config=SparkConf()
config.setMaster("local")
sc=SparkContext(conf=config)
sql=SQLContext(sc)

categoriesrdd = sc.textFile("file:///home/cloudera/Desktop/Python/products/category.txt")\
.map(lambda record: record.split("|"))

subcategoriesrdd = sc.textFile("file:///home/cloudera/Desktop/Python/products/subcategory.txt")\
.map(lambda record: record.split("|"))

productsrdd = sc.textFile("file:///home/cloudera/Desktop/Python/products/products.txt")\
.map(lambda record: record.split("|"))

salesrdd = sc.textFile("file:///home/cloudera/Desktop/Python/products/sales.txt")\
.map(lambda record: record.split("|"))\
.map(lambda record: [record[0], record[1], int(record[2]), float(record[3])])

categoriesSchema=StructType([
	StructField("catid", StringType()),
	StructField("catname", StringType())
])

subcategoriesSchema=StructType([
	StructField("catid", StringType()),
	StructField("subcatid", StringType()),
	StructField("subcatname", StringType())
])

productsSchema=StructType([
	StructField("prodid", StringType()),
	StructField("prodname", StringType()),
	StructField("subcatid", StringType())
])

salesSchema=StructType([
	StructField("salesid", StringType()),
	StructField("prodid", StringType()),
	StructField("quantity", LongType()),
	StructField("posale", FloatType())
])

categories = sql.createDataFrame(categoriesrdd, categoriesSchema)
subcategories = sql.createDataFrame(subcategoriesrdd, subcategoriesSchema)
products = sql.createDataFrame(productsrdd, productsSchema)
sales = sql.createDataFrame(salesrdd, salesSchema)

categories.registerTempTable("categories")
subcategories.registerTempTable("subcategories")
products.registerTempTable("products")
sales.registerTempTable("sales")

sql.sql("select prodname,\
	 count(quantity) as totalsales, sum(quantity) as totalsold, round(sum(posale), 2) as totalincome from sales INNER JOIN products ON sales.prodid=products.prodid	group by prodname order by totalsales desc").show()
