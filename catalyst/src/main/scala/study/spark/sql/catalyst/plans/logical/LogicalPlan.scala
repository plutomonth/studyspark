package study.spark.sql.catalyst.plans.logical

import study.spark.Logging
import study.spark.sql.catalyst.analysis.EliminateSubQueries
import study.spark.sql.catalyst.expressions.{Alias, BindReferences, ExprId, Expression}
import study.spark.sql.catalyst.plans.QueryPlan
import study.spark.sql.catalyst.trees.TreeNode

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

  /**
   * Computes [[Statistics]] for this plan. The default implementation assumes the output
   * cardinality is the product of of all child plan's cardinality, i.e. applies in the case
   * of cartesian joins.
   *
   * LeafNodes must override this.
   */
  def statistics: Statistics = {
    if (children.size == 0) {
      throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
    }
    Statistics(sizeInBytes = children.map(_.statistics.sizeInBytes).product)
  }

  /** Args that have cleaned such that differences in expression id should not affect equality */
  protected lazy val cleanArgs: Seq[Any] = {
    val input = children.flatMap(_.output)
    def cleanExpression(e: Expression) = e match {
      case a: Alias =>
        // As the root of the expression, Alias will always take an arbitrary exprId, we need
        // to erase that for equality testing.
        val cleanedExprId = Alias(a.child, a.name)(ExprId(-1), a.qualifiers)
        BindReferences.bindReference(cleanedExprId, input, allowFailures = true)
      case other => BindReferences.bindReference(other, input, allowFailures = true)
    }

    productIterator.map {
      // Children are checked using sameResult above.
      case tn: TreeNode[_] if containsChild(tn) => null
      case e: Expression => cleanExpression(e)
      case s: Option[_] => s.map {
        case e: Expression => cleanExpression(e)
        case other => other
      }
      case s: Seq[_] => s.map {
        case e: Expression => cleanExpression(e)
        case other => other
      }
      case other => other
    }.toSeq
  }


  /**
   * Returns true when the given logical plan will return the same results as this logical plan.
   *
   * Since its likely undecidable to generally determine if two given plans will produce the same
   * results, it is okay for this function to return false, even if the results are actually
   * the same.  Such behavior will not affect correctness, only the application of performance
   * enhancements like caching.  However, it is not acceptable to return true if the results could
   * possibly be different.
   *
   * By default this function performs a modified version of equality that is tolerant of cosmetic
   * differences like attribute naming and or expression id differences.  Logical operators that
   * can do better should override this function.
   */
  def sameResult(plan: LogicalPlan): Boolean = {
    val cleanLeft = EliminateSubQueries(this)
    val cleanRight = EliminateSubQueries(plan)

    cleanLeft.getClass == cleanRight.getClass &&
      cleanLeft.children.size == cleanRight.children.size && {
      logDebug(
        s"[${cleanRight.cleanArgs.mkString(", ")}] == [${cleanLeft.cleanArgs.mkString(", ")}]")
      cleanRight.cleanArgs == cleanLeft.cleanArgs
    } &&
      (cleanLeft.children, cleanRight.children).zipped.forall(_ sameResult _)
  }

}

/**
 * A logical plan node with single child.
 */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan

  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * A logical plan node with no children.
 */
abstract class LeafNode extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Nil
}

/**
 * A logical plan node with a left and right child.
 */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override def children: Seq[LogicalPlan] = Seq(left, right)
}
