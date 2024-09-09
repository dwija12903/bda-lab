//managing structed data in rdd was difficult so dataframes were introduced
// all the features of rdd are available in dataframe
//data in dataframe is same as that in rdd but in structed format

val rdd = sc.parallize(List(1 to 5)).map(x => (x,"Dwija"))
rdd.collect 
//output: Array[(Int, String)] = Array((1,Dwija), (2,Dwija), (3,Dwija), (4,Dwija), (5,Dwija))
rdd.collect.foreach(println) 
//output: 
//(1,Dwija)
//(2,Dwija)
//(3,Dwija) 
//(4,Dwija) 
//(5,Dwija)

val df = rdd.toDF("SrNo","Name")
//to see the schema of dataframe
df.show 
//output: 
//+----+-----+
//|SrNo| Name|
//+----+-----+
//|   1|Dwija|
//|   2|Dwija|
//|   3|Dwija|
//|   4|Dwija|
//|   5|Dwija|
//+----+-----+

df.printSchema() //to see the schema of dataframe
//output:
//root
// |-- SrNo: integer (nullable = false)
// |-- Name: string (nullable = true)

df.schema //to see the schema of dataframe
//output:
//StructType(StructField(SrNo,IntegerType,false), StructField(Name,StringType,true))

//to load from a source 
import org.apache.spark.sql.SparkSession
//In spark you can have multiple spark session and one spark context 

val df1 = spark.read.format("json").load("./dbfs/FileStore/tables/employees.json")
df1.printSchema()
df1.show()

//to execute quries we need to create a temp view first 
df1.registerTempTable("employee") //to create a temp view
sqlContext = new org.apache.spark.SQLContext
sqlContext.sql("select * from employee").show() //to execute a query

//to select particular  and multiple column
df1.select("name").show()
df1.select("name","age").show()