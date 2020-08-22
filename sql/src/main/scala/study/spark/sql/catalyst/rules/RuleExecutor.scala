package study.spark.sql.catalyst.rules

import study.spark.Logging
import study.spark.sql.catalyst.trees.TreeNode

abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
    * Executes the batches of rules defined by the subclass. The batches are executed serially
    * using the defined execution strategy. Within each batch, rules are also executed serially.
    */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan

    curPlan
  }
}
