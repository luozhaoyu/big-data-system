import scala.collection.mutable.Map

object Test {
  def parseFile(content: String): Map[String, Int] = {
    var m = scala.collection.mutable.Map[String, Int]()
    for (line <- content.split('\n')) {
      val two = line.split('\t')
      m(two(1)) = two(0).toInt
    }
    m
  }

  def main(args: Array[String]) {
    val file = "1\tzzzz\n3\tabcd"
    var a = Array(1)
    for (i <- 1 to 10) {
      a :+= i
    } 
    println(a.deep.mkString("\n"))

  }
}
