//map is a higher order functions
//map access each element onto the list 

var lst = List(1,3,5,2,7,9);

lst.map((x:Int) => x+1) //List(2,4,6,3,8,10)
lst.map((_*2))
lst.map(x => x*2)

//flatmap acts as a map and flatten

lst.flatmap(x => List(x,x+1)) 
//List(1,2,3,4,5,6,2,3,7,8,9,10)

lst.map(x => List(x,x+1)) 
//List(List(1,2),List(3,4),List(5,6),List(2,3),List(7,8),List(9,10))

// So, flatmap is a combination of map and flatten it maps it and then flattens it

//filter
//predict is a function that returns a boolean
// we give a argument to the function and it returns a boolean

// if the expression returns true then the element is included in the list
lst.filter(x => x%2 == 0) 
//List(2)

//reduce 
//reduce takes a function that takes two arguments and returns one
lst.reduce((x,y) => x+y) //27
lst.reduce((x,y) => x*y) //1890