from pyspark.sql import SparkSession

# build a spark session
spark = Sparksession.builder.appName('dataprocessing').getOrCreate()

# stop the spark session
spark.stop()

# read from csv file
df = spark.read.csv("employess.csv",header=True,inferSchema=True)
# count no. of rows
df.count()
# count no. of columns
len(df.columns)
# print schema
df.printSchema() 
# show first 5 rows
df.show(5)
# select only 2 columns
df.select(['age','name']).show()
# Filter rows on bases of condition 
df.filter(df['age']>30).show() #table 
df.filter(df['age']>30).collect() #list
df.count() #total entries
# groupby and aggregate functions
df.groupby('Age').agg("Salary":"sum").show()
# order data in descending 
df.orderby('age',ascending=False).show()
df.groupby('Cities').agg("Salary":"sum").orderby('sum(Salary)',ascending=False).show()
#distinct values
df.select('age').distinct().show() 
# add new calculated columns 
df.withColumn('age_after_10_years',df['age']+10).show()
# run sql query
df.createOrReplaceTempView('employee')
spark.sql("select * from employee where age>30").show()
# drop columns
df.drop('age').show()
# drop duplicates 
df.dropDuplicates().show()

# Functionalities of udf(user defined function)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,IntegerType,DoubleType
def age_group(age):
  if age<30:
    return 'young'
  elif age>=30 and age<60:
    return 'middle'
  else:
    return 'old'
age_group_udf = udf(age_group,StringType())
df.withColumn('age_group',age_group_udf(df['age'])).show()


# create a rdd
rdd = spark.sparkContext.parallelize([1,2,4,3,1])
rdd.collect()
rdd.count()
# save rdd to text file 
rdd.saveAsTextFile("rdd.txt")
# create rdd from text file 
rdd_text = spark.sparkContext.textFile("rdd.txt")
rdd_text.collect()

