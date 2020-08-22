package study.spark.sql.execution

import study.spark.sql.SQLContext

private[sql] trait Queryable {
  def queryExecution: QueryExecution
  def sqlContext: SQLContext
}
