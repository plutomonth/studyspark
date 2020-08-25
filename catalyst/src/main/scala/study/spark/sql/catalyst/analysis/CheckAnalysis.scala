package study.spark.sql.catalyst.analysis

import study.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Throws user facing errors when passed invalid queries that fail to analyze.
  */
trait CheckAnalysis {
  /**
    * Override to provide additional checks for correct analysis.
    * These rules will be evaluated after our built-in check rules.
    */
  val extendedCheckRules: Seq[LogicalPlan => Unit] = Nil

  def checkAnalysis(plan: LogicalPlan): Unit = {

  }
}
