// Databricks notebook source
// DBTITLE 1,Importing Modules
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

// COMMAND ----------

sc: SparkContext

// COMMAND ----------

// DBTITLE 1,Accessing CSV Files
val edgeRDD = sc.textFile("dbfs:/FileStore/shared_uploads/dwija.pce21@sot.pdpu.ac.in/edges.csv")
val vertexRDD = sc.textFile("dbfs:/FileStore/shared_uploads/dwija.pce21@sot.pdpu.ac.in/vertex.csv")

// COMMAND ----------

vertexRDD.collect()

// COMMAND ----------

edgeRDD.collect()

// COMMAND ----------

// DBTITLE 1,Creating RDD of Vertices and Edges
val vertices : RDD[(VertexId, (String, String))] = vertexRDD.map{
  lines => val fields = lines.split(",")
  (fields(0).toLong, (fields(1),fields(2)))
}
vertices.collect

// COMMAND ----------

val edges : RDD[Edge[String]] = edgeRDD.map{
  line => val fields = line.split(",")
  Edge(fields(0).toLong, fields(1).toLong, fields(2))
}
edges.collect

// COMMAND ----------

// DBTITLE 1,Final Creation of Graph
val default = ("unknow","missing")
val graph = Graph(vertices,edges,default)

// COMMAND ----------

// DBTITLE 1,Other way to Creating Graph with CSV
val VerArray = Array(
    (1L, ("Alice", 28)),
    (2L, ("Bob", 27)),
    (3L, ("Charlie", 65)),
    (4L, ("David", 42)), 
)

val EdgeArray = Array(
    Edge(2L, 1L, 7),
    Edge(2L, 4L, 2),
    Edge(3L, 2L, 4),
)

val verRDD = sc.parallelize(VerArray)
val edgRDD = sc.parallelize(EdgeArray)

val gph = Graph(verRDD, edgRDD)

// COMMAND ----------

// DBTITLE 1,Joining 2 Graphs using outerJoinVertices
case class MoviesWatched(Movie:String, Genre:String)

val movies:RDD[(VertexId, MoviesWatched)]=sc.parallelize(List(
  (1,MoviesWatched("Toy Story 3", "Kids")), (2, MoviesWatched("Titanic", "Love")), (3, MoviesWatched("The Hangover","Comedy"))))

val movieOuterJoinedGraph = graph.outerJoinVertices(movies)((_,name,movies) => (name,movies))
print("VERTEX \n")
movieOuterJoinedGraph.vertices.collect.foreach(println)
print("EDGES \n")
movieOuterJoinedGraph.edges.collect.foreach(println)

// COMMAND ----------

val newgraph = graph.outerJoinVertices(verRDD)((_,name,age)=>(name,age))
newgraph.vertices.collect.foreach(println)

// so basically, to join to graphs, 
// <graph_in_which_to_be_joined>.outerJoinVertices(name_of_RDD_of_Vertices)((_,col1,col2)=>(col1,col2))

// COMMAND ----------

// DBTITLE 1,triangle count
// a triangle counting algorithm in the TriangleCount object that determines 
// the number of triangles passing through each vertex, providing a measure of clustering.
val tcount = graph.triangleCount().vertices
tcount.collect()

// COMMAND ----------

tcount.collect().mkString("\n")

// COMMAND ----------

// DBTITLE 1,Basic Functionalities
print("Edges= ", graph.numEdges, "\n Vertices=")
print(graph.numVertices)

// COMMAND ----------

//indegree: number of edges coming into a vertex
//outdegree: number of edges going out of a vertex
//degree: sum of indegree and outdegree
graph.inDegrees.collect() 
graph.outDegrees.collect()
graph.degrees.collect()

// COMMAND ----------

// triplets - collection of vertices and edges
graph.triplets.collect.foreach(println)

// COMMAND ----------

graph.triplets.count()

// COMMAND ----------

val connected = graph.connectedComponents.vertices
println(connected)
// No longer contains missing field
// Remove missing vertices as well as the edges to connected to them

// COMMAND ----------

val iterations = 1000
val connected = graph.connectedComponents().vertices
val connectedS = graph.stronglyConnectedComponents(iterations).vertices
val connByPerson = vertices.join(connected).map{ case(id,((person, age), conn)) => (conn,id,person)}
val connByPersonS = vertices.join(connectedS).map{ case(id,((person, age), conn)) => (conn,id,person)}

// COMMAND ----------

connByPerson.collect().foreach{ case (conn,id,person) => println(f"Weak $conn $id $person")}

// COMMAND ----------

// DBTITLE 1,Filter Property
graph.vertices.collect.foreach(println)

// COMMAND ----------

graph.edges.collect.foreach(println)

// COMMAND ----------

graph.vertices.filter{case (id,(name,age)) => age.toLong > 40}.collect.foreach(println)
// to calculate age > 40

// COMMAND ----------

graph.edges.filter{ case Edge(scr, dest, prop) => scr > dest}.collect.foreach(println)

// COMMAND ----------

//  to find relation of daughter
graph.edges.filter{case Edge(scr, dest, prop) => prop == "Daughter"}.collect.foreach(println)

// COMMAND ----------

// to print relation b/w every vertices 
graph.triplets.map(
  triplet => triplet.srcAttr._1 + " is a " + triplet.attr + " of " + triplet.dstAttr._1).collect.foreach(println)

// COMMAND ----------

// DBTITLE 1,To find min and max number of degree
val minDegrees = graph.outDegrees.filter(_._2 <=1)
minDegrees.collect()

def max(a: (VertexId, Int), b: (VertexId, Int)):(VertexId, Int)={
  if (a._2>b._2) a else b
}
val maxInDegree: (VertexId, Int)=graph.inDegrees.reduce(max)
val maxOutDegree: (VertexId, Int)=graph.outDegrees.reduce(max)
val maxDegree: (VertexId, Int)=graph.degrees.reduce(max)

// COMMAND ----------


