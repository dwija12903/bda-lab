//indegree: number of edges coming into a vertex
//outdegree: number of edges going out of a vertex
//degree: sum of indegree and outdegree
graph.inDegree.collect() //output: Array((1,1), (2,2), (3,1), (4,1), (5,0), (6,1))
graph.outDegree.collect() //output: Array((1,1), (2,2), (3,2), (4,0), (5,2), (6,1))
graph.degrees.collect()// Made up of vertices and edges 
// Types of graphs: 1.Undirected Graph 2.Directed Graph 3.Vertex labelled Graphs
//4.Cyclic 5.Edge labelled 6.weighted 7.Disconnected

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


dbfs:/FileStore/shared_uploads/dwija.pce21@sot.pdpu.ac.in/edges.csv
dbfs:/FileStore/shared_uploads/dwija.pce21@sot.pdpu.ac.in/vertex.csv


df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/dwija.pce21@sot.pdpu.ac.in/edges.csv")
df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/dwija.pce21@sot.pdpu.ac.in/vertex.csv")

//extends Spark rdd BY NEW GRAPH EXTENS,
// helps us in analysing the data. help us find relationsjip in data
// based of mathetaical graph theory

val sc: SparkContext

// to enter data in graphX
val VerArray = Array(
    (1L, ("Alice", 28)),
    (2L, ("Bob", 27)),
    (3L, ("Charlie", 65)),
    (4L, ("David", 42)),
    (5L, ("Ed", 55)),
    (6L, ("Fran", 50))  
)

val EdgeArray = Array(
    Edge(2L, 1L, 7),
    Edge(2L, 4L, 2),
    Edge(3L, 2L, 4),
    Edge(3L, 6L, 3),
    Edge(4L, 1L, 1),
    Edge(5L, 2L, 2),
    Edge(5L, 3L, 8),
    Edge(5L, 6L, 3)
)

// to create RDD apply parallelize method
val verRDD = sc.parallelize(VerArray)
val edgeRDD = sc.parallelize(EdgArray)

// to create graph
val graph = Graph(verRDD, edgeRDD)

// way2 of creating graph
val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),(5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
// Create an RDD for edges
val relationships: RDD[Edge[String]] =
  sc.parallelize(Seq(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
// Define a default user in case there are relationship with missing user
val defaultUser = ("John Doe", "Missing")
// Build the initial Graph
val graph = Graph(users, relationships, defaultUser) //creates a graph

//OTHER FUNCTIONALITIES, STARTS FROM HERE 
graph.numVertices // 6
graph.numEdges // 8

 //output: Array((1,2), (2,4), (3,3), (4,1), (5,2), (6,2))

// triplets: information combine of vertices and edges and there distance
graph.triplets.collect()
tripets.count()

//to get the vertices
graph.vertices.collect().foreach(println)
//output: (1,(Alice,28))    (2,(Bob,27))    (3,(Charlie,65))    (4,(David,42))    (5,(Ed,55))    (6,(Fran,50))

//to get the edges
graph.edges.collect().foreach(println)
//output: Edge(2,1,7)    Edge(2,4,2)    Edge(3,2,4)    Edge(3,6,3)    Edge(4,1,1)    Edge(5,2,2)    Edge(5,3,8)    Edge(5,6,3)

//to get the triplets
graph.triplets.collect().foreach(println)
//output: ((2,(Bob,27)),(1,(Alice,28)),7)    ((2,(Bob,27)),(4,(David,42)),2)    ((3,(Charlie,65)),(2,(Bob,27)),4)    ((3,(Charlie,65)),(6,(Fran,50)),3)    ((4,(David,42)),(1,(Alice,28)),1)    ((5,(Ed,55)),(2,(Bob,27)),2)    ((5,(Ed,55)),(3,(Charlie,65)),8)    ((5,(Ed,55)),(6,(Fran,50)),3)
triplet.attr
//output: 7    2    4    3    1    2    8    3

// FILTER AND CASE CLASS

// Count all users which are postdocs
graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect().foreach(println)
//output: (3,(Charlie,65))    (4,(David,42))    (5,(Ed,55))    (6,(Fran,50))

// Edges have a srcId and a dstId corresponding to the source and destination vertex identifiers. 
// In addition, the Edge class has an attr member which stores the edge property.
// Count all the edges where src > dst
graph.edges.filter(e => e.srcId > e.dstId).count
graph.edges.filter(e => e.srcId > e.dstId).collect().foreach(println)
//output: Edge(5,2,2)    Edge(5,3,8)    Edge(5,6,3)

// case expression to deconstruct the tuple.
graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
graph.edges.filter { case Edge(src, dst, prop) => src > dst }.collect().foreach(println)

//to get the triplets
graph.triplets.filter(t => t.srcAttr._2 > 30 && t.dstAttr._2 < 30).collect().foreach(println)
// output: ((4,(David,42)),(1,(Alice,28)),1)    ((5,(Ed,55)),(2,(Bob,27)),2)
// Use the triplets view to create an RDD of facts.
val facts: RDD[String] =
  graph.triplets.map(triplet =>
    triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
facts.collect.foreach(println(_))
//output: Bob is the colleague of Alice    Bob is the colleague of David    Charlie is the collab of Bob    Charlie is the collab of Fran    David is the colleague of Alice    Ed is the colleague of Bob    Ed is the colleague of Charlie    Ed is the colleague of Fran

val ccGraph = graph.connectedComponents() // No longer contains missing field
// Remove missing vertices as well as the edges to connected to them

// A vertex is part of a triangle when it has two adjacent vertices with an edge between them. 
// GraphX implements a triangle counting algorithm in the TriangleCount object that determines 
// the number of triangles passing through each vertex, providing a measure of clustering.
