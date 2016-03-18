package idv.jiaming.scala

object TestSpace extends App{
  println("aaaaaa")
  
  def sum(a: Int, b:Int): Int = a+b
  def sum(a: Int, b:Int, c:Int): Int = a+b+c
  def sum(a: Long, b: Long): Long = a+b
  
  println(sum(1,3))
  println(sum(1,2,3))
  println(sum(1L,4L))
  
  
}