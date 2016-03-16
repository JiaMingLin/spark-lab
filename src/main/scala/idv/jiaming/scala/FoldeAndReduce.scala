package idv.jiaming.scala

object FoldeAndReduce extends App{
  val numbers = List(1,2,3,4,5,6,7,8,9,0,9,8,7,6,5,4,3,2,1,3,4,6,8,4,5,3,6,7,0,3,2,5,6,7,4)
  println("Sum using fold: "+numbers.foldLeft(0)((r,c)=>r+c))
  println("Sum using reduce: "+numbers.reduce((r,c)=>r+c))
  println()
  var testMap = scala.collection.immutable.Map[String, String]()
  testMap += ("aaa" -> "ccc")
  println(testMap.keySet.contains("aaa"))
  
  if(testMap.keySet.contains("aaa")){
    
  }
  
  val count_map = numbers.foldLeft(scala.collection.mutable.Map[Int, Int]())((result_map, c_num ) => {
    if(result_map.keySet.contains(c_num)){
      result_map(c_num) += 1 
    }else{
      result_map += (c_num -> 1)  
    }
    result_map
  })
}