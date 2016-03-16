package idv.jiaming.spark.pairRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AveragePerKey extends App{
  // initial spark context
  val conf = new SparkConf().setAppName("AveragePerKey").setMaster("local[2]")
  val sc = new SparkContext(conf)
  
  // read data as regular RDD.
  val textRDD = sc.textFile("/root/git/spark-scala/src/main/scala/idv/jiaming/spark/pairRDD/key-value-pair-text")

  // map regular RDD into pair RDD
  val pairRDD = textRDD.map { line => (line.split(" ")(0), line.split(" ")(1).toInt
      ) }
  pairRDD.collect().foreach(println)
  // apply mapValue and reduceByKey
  /*
   * 
panda 0
pink 3
pirate 3
panda 1
pink 4

panda (0, 1)
pink (3, 1)
pirate (3, 1)
panda (1, 1)
pink (4, 1)

panda (0,1) + (1,1) = (1,2)
pink (3,1) + (4,1) = (7,2)
pirate (3,1)
   */
  
  val aggregatedRDD = pairRDD.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  aggregatedRDD.collect().foreach(println)
}