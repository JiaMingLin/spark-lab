package idv.jiaming.scala.algorithms.bnlearn

import scala.io.Source
import idv.jiaming.scala.algorithms.bnlearn._

class Domain(dataDomain:String) {
  private var _dim = 0
  private var _isBinary = false
  private var _trans = Map[Int, Translator]()
  private var _cells = Array[Int]()
  private var _convertor = DomainConvertor
  
  // read domain file
  val readerLines = Source.fromFile(dataDomain).getLines()
  
  for ( line <- readerLines){
    // the first line specifies attributes counts.
    if(readerLines.indexOf(line) == 0){
      this._dim = line.toInt
    }else{
      
    }  
  }
  
  def getDim(): Int = {
    _dim
  }
  
  def isBinary(): Boolean ={
    isBinary
  }
  
  def getCell(): Array[Int] = {
    _cells
  }
  
  def getCell(pos: Int): Int = {
    _cells(pos)
  }
  
  def binarization(gData : Data){
    
  }
  
  
}