package idv.jiaming.scala

object AggregateGroupBy extends App{
  // Group by
  val list = Seq(("one", "i"), ("two", "2"), ("two", "ii"), ("one", "1"), ("four", "iv"))
  println(list.groupBy(_._1))
  println(list.groupBy(_._1).mapValues(_.map(_._2)))
  println(list.groupBy(_._1).mapValues(_.map(t => t._2)))
  // Aggregate
  
  val list2 = List("aaaaaa", "vvvvvvv", "ccccccc")
  list2.par.aggregate(0)((x, line) => x + wordCount(line) , _+_)
  
  def wordCount(line:String) = {
    0
  }
}