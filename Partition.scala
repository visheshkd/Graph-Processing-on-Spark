import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  val depth = 6
  var vt=1
  val neg:Long = -1
  def Vertex(s:Array[String]):(Long,Long,List[Long])={
    var centroid:Long = -1
    if(vt<=10){
      vt=vt+1
      centroid=s(0).toLong
    }
    (s(0).toLong,centroid,s.tail.map(_.toString.toLong).toList)
  }
  //mapper function (1)
  def function1(vertex:(Long,Long,List[Long])):List[(Long,Either[(Long,List[Long]),Long])]={
    var n=List[(Long,Either[(Long,List[Long]),Long])]()
    if(vertex._2>0){
      vertex._3.foreach(x => {n=(x,Right(vertex._2))::n})
    }
    n=(vertex._1,Left(vertex._2,vertex._3))::n
    n
  }
  def function2(vertex1:(Long,Iterable[(Either[(Long,List[Long]),Long])])):(Long,Long,List[Long])={
    var adjacent:List[Long]=List[Long]()
    var cluster:Long = -1
    for(ver <- vertex1._2){
      ver match{
        case Right(c) => {cluster=c}
        case Left((c,ad)) if(c>0) => return (vertex1._1,c,ad)
        case Left((neg,ad)) => adjacent=ad

      }
    }
    return (vertex1._1,cluster,adjacent)
  }

  def main ( args: Array[ String ] ) {
    val cong=new SparkConf().setAppName("Partition")
    val sc=new SparkContext(cong)
    var graph = sc.textFile(args(0)).map(line => Vertex(line.split(",")))
    for(i <- 1 to depth){
      graph=graph.flatMap(vertex => function1(vertex)).groupByKey().map(vertex => function2(vertex))
    }
    var m = graph.map(pt => (pt._2, 1))
    var op = m.reduceByKey(_+_).collect()
    op.foreach(println)


  }
}
