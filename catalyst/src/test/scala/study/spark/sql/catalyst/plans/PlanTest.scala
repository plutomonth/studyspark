package study.spark.sql.catalyst.plans

import study.spark.SparkFunSuite
import study.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId}
import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.catalyst.util.sideBySide


/**
 * Provides helper methods for comparing plans.
 */
abstract class PlanTest extends SparkFunSuite {


  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan) = {
    plan transformAllExpressions {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
    }
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) {
    val normalized1 = normalizeExprIds(plan1)
    val normalized2 = normalizeExprIds(plan2)
    if (normalized1 != normalized2) {
      fail(
        s"""
           |== FAIL: Plans do not match ===
           |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
         """.stripMargin)
    }
  }

}
