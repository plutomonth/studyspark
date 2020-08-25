package study.spark.sql.catalyst.planning

import study.spark.Logging
import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.catalyst.trees.TreeNode

/**
 * Given a LogicalPlan, returns a list of `PhysicalPlan`s that can
 * be used for execution. If this strategy does not apply to the give logical operation then an
 * empty list should be returned.
 */
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]] extends Logging {
  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}

abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...
    val iter = strategies.view.flatMap(_(plan)).toIterator
    assert(iter.hasNext, s"No plan for $plan")
    iter
  }
}

