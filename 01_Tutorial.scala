// Scala is Scalable Language
// Uses JVM
// Static Typing Language- error caught at compile time not at runtime
// Scala is a functional language, object oriented language and scripting language
// REPL- Read Eval Print Loop. It takes single line of code, evaluates it and prints the result

// Initializing a variable
var a : Int = 10 //mutable 
val b : Int = 20 //immutable

var c = true  //it will recognise datatype automatically from initialization value 

var d = 10.4 //double 
var e = 10.4f //float

// expression
val x ={
  var y1 = 10;
  var y2 = 20;
  y1+y2
}
// lazy loading, on demand loading
val x = 500
lazy val y = 500 // access memory only when it is called

// object is a class is alreay instantiated
object HelloWorld{
  def main(args: Array[String]){
    println("Hello World");
  }
}

// String interpolation: replace the variable with its value
val name = "Dwija"
println(s"Hello $name")
println("Heloo" + name)
println(f"Hello $name%s")

println("Hello \n World") // \n is new line 
println(raw"Hello \n World") // raw interpolation it ignores keywords

// if else
val x =20
if(x<20){
  println("x is less than 20")
}else if(x==20){   
    println("x is equal to 20")
}else{
    println("x is greater than 20")
}

val res = if (x==20) "x is 20" else "x is not 20"
println(res)

// while loop 
var x=0
while (x<4){
    println(x)
    x+=1
} 

// do while loop
var x=0
do{
    println(x)
    x+=1
}while(x<4)

// for loops
// no need to initialize as for loop automatically takes it
for (a <- 1 to 5){ //also use 1.to(5), 1.until(5)
    println(a)
}

for (a <- 1 to 5; b <- 1 to 3){
    println(a + " " + b)
} //output: 
// 1 1
// 1 2
// 1 3
// 2 1
// 2 2

val lst = List(1,2,3,4,5,6,7,8,9,10)
for (a <- lst; if i <6){
    println(a)
} //output: 1 2 3 4 5

// for loop with yield is used to store the result in a variable
val result = for {a <- lst; if a<6} yield {
    a*a
}
println(result) //

// match statement 
val age = 18
age match {
    case 20 => println("age is 20")
    case 18 => println("age is 18")
    case 16 => println("age is 16")
    case _ => println("default")
}
//match with expression
val result = age match {
    case 20 => age
    case 18 => age
    case 16 => age
    case _ => 0
}
println(result)

// function
def Add(x: Int,y: Int ): Int ={
    return x+y  //return is optional
}
println(Add(10,20))

def multiply(x:Int; y:Int): Int = x*y
println(multiply(10,20))

//default value in functions
def Addition(x:Int = 5, y:Int = 10): Int = x+y
Addition() //15
Addition(20) //30
Addition(20,30) //50

// Unit - function does not return anything
def Add(x: Int,y: Int ): Unit ={
    println(x+y)
}

// we can also use operator for function name 
def +(x: Int,y: Int ): Int ={
    return x+y  //return is optional
}
println(+ (10,20))
println(10+20) //here + is also a function

//anaonymous function - function without name,assigned to variable
var add = (x: Int, y:Int) => x+y
//higher order function - function that takes another function as parameter
def math(x: Int, y: Int, f:(Int,Int)=>Int): Int = f(x,y)
//function name : (datatype1, datatype2) => return datatype
val res =  math(10,20, (x,y) => x+y)

// function using curyling method - to take multiple methods 
def add(x: Int) = (y: Int) => x+y
val res = add(10)(20)

//STRINGS-immutable
var str1: String = "Hello"
var str2: String = "World"
str1.length()
str1.concat(str2)
str1+str2
//string foramting 
printf(" %d -- %f -- %s", 10,10.5,"Hello")
println(" %d -- %f -- %s".format(10,10.5,"Hello"))

