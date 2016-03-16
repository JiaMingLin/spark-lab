package idv.jiaming.scala.algorithms

import org.saddle.io._
import org.saddle._

object DecissionTreePlay extends App {

  //TODO change to the related path to file.
  // Read data to a data frame,
  val file = CsvFile("/root/git/spark-scala/src/main/scala/idv/jiaming/scala/algorithms/tennis_data.csv")
  val df = CsvParser.parse(file).withColIndex(0)
  println(df)
  
  // Obtain the data info
  
}

object DTFunction{
  def apply[T](df: Frame[Int,Int,T]) = new DTFunction(df)
}

class DTFunction[T](df: Frame[Int,Int,T]){
  
  // calculate the entropy of "partition" for a given attribute
  
}