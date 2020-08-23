package study.spark.sql.catalyst.analysis

import study.spark.sql.catalyst.CatalystConf
import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.catalyst.rules.{Rule, RuleExecutor}

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a schema [[Catalog]] and
 * a [[FunctionRegistry]].
 */
class Analyzer(
    catalog: Catalog,
    registry: FunctionRegistry,
    conf: CatalystConf,
    maxIterations: Int = 100)
  extends RuleExecutor[LogicalPlan] with CheckAnalysis {

  /**
    * Override to provide additional rules for the "Resolution" batch.
    */
  val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Nil
}


/**
 * Removes [[Subquery]] operators from the plan. Subqueries are only required to provide
 * scoping information for attributes and can be removed once analysis is complete.
 */
object EliminateSubQueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}
