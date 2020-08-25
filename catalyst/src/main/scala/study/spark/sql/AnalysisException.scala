package study.spark.sql

class AnalysisException protected[sql] (
     val message: String,
     val line: Option[Int] = None,
     val startPosition: Option[Int] = None)
  extends Exception with Serializable {

}
