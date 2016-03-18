package idv.jiaming.scala.algorithms.basic

/*
 *  If ascend,
 *  1. Initial a pointer at index 0.
 *  2. Find an minimal after the index of pointer.
 *  3. Swap the the minimal number with the pointer number
 *  4. Shift the pointer to the next one position(right one).
 *  5. Repeat step2 to step4 until the pointer at the last position. 
 */

object SelectionSort extends App{
  
  var array = Array(4,1,2,5,7,8,3,1,6,7,3,5,7)
  println(array.deep.mkString(" "))
  
  def selectionSort(array: Array[Int]): Array[Int] = {
     
    def min(m: Int, j: Int):Int ={
       if(j == array.length) m
       else if(array(j) < array(m)) min(j, j+1)
       else min(m,j+1)
    }
    // i is the index pointer.
    // m is the index of minimal.
    for(i <- 0 until array.length - 1){
      // swap the pointer number with the minimal
      val m = min(i, i+1)
      swap(array, i, m)
    }
    array
  }

  def swap(array:Array[Int], pos1: Int, pos2:Int ): Array[Int] = {
    val tmp = array(pos1)
    array(pos1) = array(pos2)
    array(pos2) = tmp
    array
  }
}