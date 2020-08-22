package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.trees.TreeNode

abstract class Expression extends TreeNode[Expression] {
  def nullable: Boolean
}

/**
  * An expression that cannot be evaluated. Some expressions don't live past analysis or optimization
  * time (e.g. Star). This trait is used by those expressions.
  */
trait Unevaluable extends Expression {

}