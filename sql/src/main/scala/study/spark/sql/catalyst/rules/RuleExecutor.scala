package study.spark.sql.catalyst.rules

import study.spark.Logging
import study.spark.sql.catalyst.trees.TreeNode

abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once. */
  case object Once extends Strategy { val maxIterations = 1 }

  /** A batch of rules. */
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)


  /**
    * Executes the batches of rules defined by the subclass. The batches are executed serially
    * using the defined execution strategy. Within each batch, rules are also executed serially.
    */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan

    curPlan
  }
}
