//RDD has 2 functions transformation and action
//no data is executed until action is called
//transformation is lazy
//RDD is immutable

//Data coming into RDD is splited into partions, by default 2 partions
//For eg: In HDFS default bolck size is 128MB, so each RDD partition will have 128mb block 

//When we are coding, along with transformation DAG is also created 
//once action is called, DAG is executed and is sent to DAG scheduler
//DAG scheduler sends the tasks to task scheduler
//Task scheduler sends the tasks to Clustor Manager which then to executors
//Executors execute the tasks and send the result back to the driver

//****Word Count Example***

//importing textfile from the hdfs server
//after importing the file, we are creating a RDD
//RDD is created by using sc.textFile
//sc is the spark context
//textFile is the function which is used to read the file
//textFile is a transformation function
val lines = sc.textFile("hdfs://localhost:9000/user/hduser/wordcount/input")

lines.collect()
//collect is an action function
//collect is used to get the data from the RDD

//flatMap is a transformation function
//flatMap is used to split the words
val words = lines.flatMap(line => line.split(" "))
words.collect

//map is a transformation function
val wordMap = words.map(word => (word,1))
wordMap.collect

//reduceByKey is a transformation function
//reduceByKey is used to reduce the words
//combine the words with the same key and add the values
val wordCount = wordMap.reduceByKey(_+_) // _+_ is a function
wordCount.collect
