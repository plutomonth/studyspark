package study.spark.sql

import study.spark.sql.catalyst.SqlParser
import study.spark.sql.catalyst.plans.logical.{Command, Filter, InsertIntoTable, LogicalPlan}
import study.spark.sql.execution.datasources.CreateTableUsingAsSelect
import study.spark.sql.execution.{LogicalRDD, QueryExecution, Queryable}

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

  @transient protected[sql] val logicalPlan: LogicalPlan = queryExecution.logical match {
    // For various commands (like DDL) and queries with side effects, we force query optimization to
    // happen right away to let these side effects take place eagerly.
    case _: Command |
         _: InsertIntoTable |
         _: CreateTableUsingAsSelect =>
      LogicalRDD(queryExecution.analyzed.output, queryExecution.toRdd)(sqlContext)
    case _ =>
      queryExecution.analyzed
  }

  /**
   * Filters rows using the given condition.
   * {{{
   *   // The following are equivalent:
   *   peopleDf.filter($"age" > 15)
   *   peopleDf.where($"age" > 15)
   * }}}
   * @group dfops
   * @since 1.3.0
   */
  def filter(condition: Column): DataFrame = withPlan {
    Filter(condition.expr, logicalPlan)
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

  /** A convenient function to wrap a logical plan and produce a DataFrame. */
  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }

}
