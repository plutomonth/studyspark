package study.spark.sql

class ExperimentalMethods protected[sql](sqlContext: SQLContext) {
  var extraStrategies: Seq[Strategy] = Nil
}
