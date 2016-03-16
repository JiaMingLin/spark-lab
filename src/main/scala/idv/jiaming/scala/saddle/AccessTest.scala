package idv.jiaming.scala.saddle

import java.lang.Math
import org.saddle._

object AccessTest extends App {

  val cols = 5
  val rounds = 5000
  val f: Frame[Int, Int, Int] = mat.randpi(40000, cols) % 2

  val x: Series[Int, Int] = vec.randpi(cols) % 2

  val v0 = vec.zeros(cols).map { x => x.toInt }
  
  val queries = for (i <- List.range(0, rounds)) yield (vec.randpi(cols) % 2)
  val totalTimeStart1 = System.currentTimeMillis();
  loopQuery()
  val totalTimeStop1 = System.currentTimeMillis();
  println("The looper: " + (totalTimeStop1 - totalTimeStart1))

  val totalTimeStart2 = System.currentTimeMillis();
  mapQuery()
  val totalTimeStop2 = System.currentTimeMillis();
  println("The mapper: " + (totalTimeStop2 - totalTimeStart2))

  def mapQuery() {

    val result = queries.par.map { q =>
      queryExec(q)
    }
    println(result)
  }

  def loopQuery() {
    val result = for (q <- queries) yield {
      queryExec(q)
    }
    println(result)
  }

  def queryExec(query: Vec[Int]) = {    
    f.rfilter(x => x.toVec == query).col(0).count.toVec.at(0)
  }
}