package idv.jiaming.spark.RDDBasic

import org.apache.spark.rdd.RDD

@SerialVersionUID(100L)
class SearchingFunctions(val query: String) extends Serializable{
  val isMatch: (String => Boolean) = {
    x => x.contains(query)
  }

  def isMatchStatic(x:String): Boolean = {
    x.contains(query)
  }
  
  def searchMatchStaticFunctionReference(rdd: RDD[String]): RDD[String] = {
    val _isMatchStatic = this.isMatchStatic(_)
    rdd.filter(_isMatchStatic)
  }
  
  def searchMatchFunctionReference(rdd: RDD[String]): RDD[String] = {
    val _isMatch = this.isMatch
    rdd.filter(_isMatch)
  }

  def searchMatchFieldReference(rdd: RDD[String]): RDD[String] = {
    val _query = this.query
    rdd.filter { line => line.contains(_query) }
  }
}