//collect function output is always in Array[Int] = Array(, , , )

//parallelize in RDD- creates a RDD from a collection
val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))

//creating new rdd
val map_rdd = rdd1.map(x => x+5)
map_rdd.collect

val filter_rdd = rdd1.filter(x => x%2 == 0)
filter_rdd.collect

val rdd2 = sc.parallelize(List(1,2))
val map_rdd2 = rdd2.map(x => x.to(3)) //List(Lst(1,2,3),List(2,3))
map_rdd2.collect
val flat_rdd2 = rdd2.flatMap( x=> x.to(3)) //List(1,2,3,1,2,3)

val rdd3 = sc.parallelize(List(1,1,2,,3,4,5,5,2,6))
val distinct_rdd3 = rdd3.distinct
distinct_rdd3.collect //List(1,2,3,4,5,6)

val unionrdd = rdd2.union(rdd3)
unionrdd.collect //List(1,2,1,2,3,4,5,5,2,6)
val intersectionrdd = rdd2.intersection(rdd3)
intersectionrdd.collect //List(1,2)
val subtractrdd = rdd2.subtract(rdd3)
subtractrdd.collect //List()

rdd1.reduce((x,y) => x+y) //55
rdd1.take(3) //List(1,2,3)
rdd1.top(3) //List(10,9,8)

