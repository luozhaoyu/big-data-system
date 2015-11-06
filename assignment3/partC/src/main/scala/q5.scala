import scala.collection.mutable.Map
import scala.collection.Set

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD


object Question5 {
  def parseContent(content: String): Set[String] = {
    var m = scala.collection.mutable.Map[String, Int]()
    for (line <- content.split('\n')) {
      val two = line.split('\t')
      if (two.length > 1) {
	m(two(1)) = two(0).toInt
      } else {
	println("problematic:" + line + two)
      }
    }
    m.keySet
  }
  
  def parseFile(nameContent: (String, String)): (VertexId, Set[String]) = {
    //val id = java.util.UUID.randomUUID().getMostSignificantBits()
    val id = nameContent._1.split('/').last.toInt
    println("this is:" + nameContent._1 + id)
    (id, parseContent(nameContent._2))
  }
  
  def hasCommon(vertexPair: ((VertexId, Set[String]), (VertexId, Set[String]))): Boolean = {
    if (vertexPair._1._1 != vertexPair._2._1) {
      val common = vertexPair._1._2.intersect(vertexPair._2._2)
      if (common.isEmpty) {
	false
      } else {
	true
      }
    } else {
      false
    }
  } 

  def generateEdgeFromVertexPair(vertexPair: ((VertexId, Set[String]), (VertexId, Set[String]))): Edge[Set[String]] = {
    val common = vertexPair._1._2.intersect(vertexPair._2._2)
    val e = new Edge[Set[String]](vertexPair._1._1, vertexPair._2._1, common)
    e
  }
  
  def createGraph(conf: SparkConf, folderPath: String): Graph[Set[String], Set[String]] = {
    val sc = new SparkContext(conf)

    val nameContents = sc.wholeTextFiles(folderPath)
    val myVertex: RDD[(VertexId, Set[String])] = nameContents.map[(VertexId, Set[String])](parseFile)
    val myEdges: RDD[Edge[Set[String]]] = myVertex.cartesian(myVertex).filter(hasCommon).map(generateEdgeFromVertexPair)

    val graph: Graph[Set[String], Set[String]] = Graph[Set[String], Set[String]](myVertex, myEdges)
    graph
  }
  
  def getVertexIDs(graph: Graph[Set[String], Set[String]]): Array[VertexId] = {
      val vertexIDs = graph.vertices.map[VertexId](x => x._1).collect()
    vertexIDs
  }
  
  def searchLargestSubgraph(graph: Graph[Set[String], Set[String]]): Graph[Set[String], Set[String]] = {
    if (graph == null || graph.numVertices == 0) {
      return null
    }
    val indegree = graph.inDegrees
    if (indegree == null || indegree.isEmpty()) {
      return null
    }
    println(graph.numVertices + "\tChecking:\t" + graph.toString() + "indegree:" + indegree.count())
    val num = graph.numVertices.toInt
    // if exists one vertice's degree is less than all vertice
    if (indegree.filter(x => x._2 < num - 1).count() > 0) {
      val vertexIDs = getVertexIDs(graph)
      var validGraphs: Array[Graph[Set[String], Set[String]]] = new Array(num)
      for ( id <- vertexIDs ) {
	val smallerGraph = graph.subgraph(vpred = (vid, attr) => vid != id)
	val tmpGraph = searchLargestSubgraph(smallerGraph)
	if (tmpGraph != null && tmpGraph.vertices.count() > 0) {
	  validGraphs +:= tmpGraph
	}
      }
      if (validGraphs != null && validGraphs.length > 0) {
	validGraphs.filter(x => x != null).maxBy[Long](x => x.vertices.count())
      } else {
	null
      }
    } else {
      println("found complete subgraph! $graph")
      graph
    }
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("please input tweets folder" + args.mkString(" "))
      println(args(0))
    } else {
      val sparkConf = new SparkConf().setAppName("Q5LargestSubgraph")
      val graph = createGraph(sparkConf, args(0))
      graph.cache()
      
      val subGraph = searchLargestSubgraph(graph)
      
      println("LargestSubgraph size: " + subGraph.vertices.count())
      subGraph.vertices.foreach(x => println("subgraph has vertex:" + x._1))
    }
  }
}
