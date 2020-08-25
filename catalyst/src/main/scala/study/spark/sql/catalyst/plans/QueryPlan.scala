package study.spark.sql.catalyst.plans

import study.spark.sql.catalyst.expressions.Attribute
import study.spark.sql.catalyst.trees.TreeNode
import study.spark.sql.types.StructType

abstract class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>

  def output: Seq[Attribute]

  lazy val schema: StructType = StructType.fromAttributes(output)

}
