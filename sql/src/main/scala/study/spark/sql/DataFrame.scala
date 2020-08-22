package study.spark.sql

import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.execution.{QueryExecution, Queryable}

private[sql] object DataFrame {
  def apply(sqlContext: SQLContext, logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }
}


class DataFrame private[sql](
  @transient override val sqlContext: SQLContext,
  @transient override val queryExecution: QueryExecution)
  extends Queryable with Serializable {

  /**
   * A constructor that automatically analyzes the logical plan.
   *
   * This reports error eagerly as the [[DataFrame]] is constructed, unless
   * [[SQLConf.dataFrameEagerAnalysis]] is turned off.
   */
  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan) = {
    this(sqlContext, {
      val qe = sqlContext.executePlan(logicalPlan)
      if (sqlContext.conf.dataFrameEagerAnalysis) {
        qe.assertAnalyzed()  // This should force analysis and throw errors if there are any
      }
      qe
    })
  }


  /**
    * Filters rows using the given SQL expression.
    * {{{
    *   peopleDf.filter("age > 15")
    * }}}
    * @group dfops
    * @since 1.3.0
    */
  def filter(conditionExpr: String): DataFrame = {
    filter(Column(SqlParser.parseExpression(conditionExpr)))
  }

}
