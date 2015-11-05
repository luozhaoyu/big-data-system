import scala.collection.mutable.Map

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

class MyVertex(n: String, m: Map[String, Int]) {
  val name: String = n
  var data: Map[String, Int] = m
}

object Question1 {
  def parseContent(content: String): Map[String, Int] = {
    var m = scala.collection.mutable.Map[String, Int]()
    for (line <- content.split('\n')) {
      val two = line.split('\t')
      m(two(1)) = two(0).toInt
    }
    m
  }
  
  def parseFile(nameContent: (String, String)): MyVertex = {
    var m = new MyVertex(nameContent._1, parseContent(nameContent._2))
    m
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("please input tweets folder" + args.mkString(" "))
      println(args(0))
    } else {

	val sparkConf = new SparkConf().setAppName("Q1LargerEdgeNumber")
	val sc = new SparkContext(sparkConf)

	val nameContents = sc.wholeTextFiles(args(0))
	val myVertex = nameContents.map[MyVertex](parseFile)
	println("res: " + myVertex + myVertex.count())
    }
  }
}
