package idv.jiaming.spark.RDDBasic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PiValue {
  def main(arg: Array[String]) {

    val conf = new SparkConf().setAppName("PiValueEstimate").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val NUM_SAMPLES = Integer.parseInt(arg(0))

    val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
      val x = Math.random()
      val y = Math.random()
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)
  }
}