import scala.collection.mutable.Map
import scala.collection.Set

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD


object Question2 {
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

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("please input tweets folder" + args.mkString(" "))
      println(args(0))
    } else {
      val sparkConf = new SparkConf().setAppName("Q2MostPopularVertex")
      val graph = createGraph(sparkConf, args(0))
      
      // (edge_number, word_number)
      val edgeNumberSum = graph.aggregateMessages[(Int, Int)](
	edgeContext => {
	  edgeContext.sendToDst((1, edgeContext.srcAttr.size))
	},
	(a, b) => (a._1 + b._1, a._2 + b._2)
      )
      
      implicit val morePopular = new Ordering[(Int, Int)] {
	override def compare(a: (Int, Int), b: (Int, Int)) = {
	  if (a._1 > b._1) {
	    1
	  } else if (a._1 < b._1) {
	    -1
	  } else if (a._2 > b._2) {
	    1
	  } else {
	    -1
	  }
	}
      }
      val maxVertex = edgeNumberSum.max()
      println("Most popular vertex is: " + maxVertex._1)
      
    }
  }
}
