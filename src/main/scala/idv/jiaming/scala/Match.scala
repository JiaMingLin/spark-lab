package idv.jiaming.scala

object Match extends App{
  
  print("Input the score: ")
  val level = readInt / 10 match {
    case 10 | 9 => "A"
    case 8 => "B"
    case 7 => "C"
    case 6 => "D"
    case _ => "E"
  }
  println("Class "+level)
}