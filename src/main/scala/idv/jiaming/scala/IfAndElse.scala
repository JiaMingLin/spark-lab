package idv.jiaming.scala

object IfAndElse extends App{
  
  var filename = ""
  if(args.isEmpty)
    filename = "default.property"
  else
    filename = args(0)
  
  println(filename)
}