//Arrays-mutable
//data structure which can store  fix sized sequential element of same dattype
val myarray: Array [Int] = new Array[Int](4)
val myarray2 = new Array[Int](5)
val myarray3 = Array()

myarray(0) = 20;

for (i <- myarray){
    println(i)
}
for (i <- 0 to (myarray2.length-1)){
    println(myarray2(i))
}

import Array._
concat(myarray,myarray2)

//LIST - immutable
//can have same datatypes elements 
val lst: List[Int] = List(1,2,3,4) 
val lst1: List[String] = List("Dwija","Panchal","PDEU") 
println(lst)
lst.head //1
lst.tail //List(2,3,4)
lst.isEmpty //false
lst.reverse //List(4,3,2,1)
lst.max //4

lst1(0) //Dwija

List.fill(5)(2) //List(2,2,2,2,2)

lst.foreach(println) //1 2 3 4

//SET - immutable
//collection of unique elements of same datatype 
//not ordered
val myset = Set(1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10)
myset(5) //true

//set can also be made mutable 
import scala.collection.mutable.Set
var myset2 = scala.collection.mutable.Set(1,2,3,4,5,6,7,8,9,10)
println(myset2 + 10) //

myset.head
myset.tail
myset.isEmpty
myset.max
myset ++ myset2 //concatenation
myset.++(myset2)
myset.foreach(println)

//TUPLE - immutable, can have different datatypes
val mytuple = (1,2,3,4,5,"Dwija")
mytuple._1 //1
mytuple._6 //Dwija

//ARRAYBUFFERS
//Valriable size array, mutable
import scala.collection.mutable.ArrayBuffer

val arr = new ArrayBuffer[Int]() //empty arraybuffer
arr += 100 //append
arr += (200,300,400,500) //append multiple
//ArrayBuffer(100,200,300,400,500)

val myarraybuf = ArrayBuffer(1,2,3,4,5,6,7,8,9,10)
//can't reassign variable name to new arraybuffer

//comman functions for array and arraybuffer 
a.trimEnd(5) //remove last 5 elements
a.insert(2,6) //insert 6 at index 2 //ArrayBuffer(1,2,6,3,4,5,6,7,8,9,10)
a.remove(2) //remove element //
a.remove(2,3) //remove 3 elements from index 2 //

//MAPS - immutable
//collection of key-value pairs
import scala.collection.mutable.Map
val mymap: Map[Int,String] = Map(801 -> "Dwija", 802 -> "Panchal", 803 -> "PDEU")
mymap(801) //Dwija
mymap.keys //Set(801,802,803)
mymap.values //MapLike(Dwija,Panchal,PDEU)
mymap.isEmpty //false
mymap.contains(801) //true
mymap.getOrElse(801,"Not Found") //Dwija
mymap ++ Map(804 -> "Dwija2") //concatenation
mymap.get(801) //Some(Dwija)
