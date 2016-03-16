package idv.jiaming.scala

object TryCatch extends App{
  try{
    val filename = args(0)
  }catch{
    case ex: ArrayIndexOutOfBoundsException => println("Please specify the arguments")
  }
  
  if(args.isEmpty)
    throw new IllegalArgumentException("Please specify the arguments")
  
}