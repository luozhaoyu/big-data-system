import scala.collection.mutable.Map
import scala.collection.Set

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD


object Question4 {
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
    println(s"read file $id :" + nameContent._1)
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
  
  def groupByFunc(w: (String, Int)): String = {
    w._1
  }
  
  def tupleSum(t: (String, Iterable[(String, Int)])): (String, Int) = {
    (t._1, t._2.map(x => x._2).sum)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("please input tweets folder" + args.mkString(" "))
      println(args(0))
    } else {
      val sparkConf = new SparkConf().setAppName("Q4MostPopularWord")
      val graph = createGraph(sparkConf, args(0))
      
      val wordTuples = graph.vertices.flatMap[(String, Int)](
	s => s._2.map{ case k => (k, 1)}
      )
      println(wordTuples.collect().mkString(" "))
      val wordCount = wordTuples.groupBy(groupByFunc).map(tupleSum)
      println(wordCount.collect().mkString(" "))
      
      implicit val morePopular = new Ordering[(String, Int)] {
	override def compare(a: (String, Int), b: (String, Int)) = {
	  if (a._2 > b._2) {
	    1
	  } else {
	    -1
	  }
	}
      }
      val mostPopularWord = wordCount.max()
      println("mostPopularWord: " + mostPopularWord.toString())
    }
  }
}
