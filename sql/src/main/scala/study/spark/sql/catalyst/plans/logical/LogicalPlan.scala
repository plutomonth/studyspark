package study.spark.sql.catalyst.plans.logical

import study.spark.Logging
import study.spark.sql.catalyst.plans.QueryPlan

abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {
  /**
    * Returns a copy of this node where `rule` has been recursively applied first to all of its
    * children and then itself (post-order). When `rule` does not apply to a given node, it is left
    * unchanged.  This function is similar to `transformUp`, but skips sub-trees that have already
    * been marked as analyzed.
    *
    * @param rule the function use to transform this nodes children
    */
  def resolveOperators(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    this
  }
}
