package idv.jiaming.spark.RDDBasic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Transform extends App{
  val conf = new SparkConf().setAppName("TransformPractice").setMaster("local[2]")
  val sc = new SparkContext(conf)
  
  val inputRDD = sc.textFile("file:///log.txt")
  val notfoundRDD = inputRDD.filter( line => line.contains("404"))
  val errorRDD = inputRDD.filter(line => line.contains("500"))
  val badLineRDD = notfoundRDD.union(errorRDD)
  
  println("There are "+badLineRDD.count()+" bad lines")
  
  val searchingFunc_404 = new SearchingFunctions("404")
  val searchResult_404 = searchingFunc_404
  .searchMatchFunctionReference(inputRDD)
  
  val searchResultStaticFunc_404 =  searchingFunc_404.searchMatchStaticFunctionReference(inputRDD)
  println("The count of not-found logs: "+ searchResult_404.count())
  println("The count of not-found logs: "+ searchResultStaticFunc_404.count())
  
}