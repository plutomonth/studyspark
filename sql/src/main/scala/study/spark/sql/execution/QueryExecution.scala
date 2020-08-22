package study.spark.sql.execution

import study.spark.sql.SQLContext
import study.spark.sql.catalyst.plans.logical.LogicalPlan

class QueryExecution(val sqlContext: SQLContext, val logical: LogicalPlan) {

  def assertAnalyzed(): Unit = sqlContext.analyzer.checkAnalysis(analyzed)

  lazy val analyzed: LogicalPlan = sqlContext.analyzer.execute(logical)
}
