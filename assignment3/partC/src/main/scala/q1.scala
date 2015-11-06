import scala.collection.mutable.Map
import scala.collection.Set

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

class MyVertex(n: String, m: Map[String, Int]) {
  val name: String = n
  var data: Map[String, Int] = m
}

class MyEdge(s: String, d: String, m: Set[String]) extends Edge {
  val src: String = s
  val dst: String = d
  var data: Set[String] = m
}

object Question1 {
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
    val id = java.util.UUID.randomUUID().hashCode()
    println("this is:" + nameContent._1 + id)
    (id, parseContent(nameContent._2))
   // var m = new MyVertex(nameContent._1, parseContent(nameContent._2))
   // m
  }
  
  def hasCommon(vertexPair: ((VertexId, Set[String]), (VertexId, Set[String]))): Boolean = {
    println(vertexPair)
    if (vertexPair._1._1 != vertexPair._2._1) {
      val common = vertexPair._1._2.intersect(vertexPair._2._2)
      if (common.isEmpty) {
	false
      } else {
	println("hasCommon" + common)
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
  
  def beLarger(tr: EdgeTriplet[Set[String], Set[String]]): Boolean = {
    println("beLarger!" + tr.toString())
    if (tr.srcAttr.size > tr.dstAttr.size) {
      true
    } else {
      false
    }
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("please input tweets folder" + args.mkString(" "))
      println(args(0))
    } else {

	val sparkConf = new SparkConf().setAppName("Q1LargerEdgeNumber")
	val sc = new SparkContext(sparkConf)

	val nameContents = sc.wholeTextFiles(args(0))
	val myVertex: RDD[(VertexId, Set[String])] = nameContents.map[(VertexId, Set[String])](parseFile)
	val myEdges: RDD[Edge[Set[String]]] = myVertex.cartesian(myVertex).filter(hasCommon).map(generateEdgeFromVertexPair)
      
	val graph: Graph[Set[String], Set[String]] = Graph[Set[String], Set[String]](myVertex, myEdges)

	//val num = myEdges.filter(beLarger).count()

	println("vertex: " + myVertex.count())
	println("edges: " + myEdges.count())
        println("graph vertex:" + graph.vertices.count())
        println("graph edges:" + graph.edges.count())
	graph.triplets.foreach(x => println("list" + x.toString()))
        val num = graph.triplets.filter(beLarger).count()
	println("larger:" + num)
      
    }
  }
}
