package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.trees.TreeNode

abstract class Expression extends TreeNode[Expression] {
  def nullable: Boolean
}

/**
 * A leaf expression, i.e. one without any child expressions.
 */
abstract class LeafExpression extends Expression {

  def children: Seq[Expression] = Nil
}


/**
  * An expression that cannot be evaluated. Some expressions don't live past analysis or optimization
  * time (e.g. Star). This trait is used by those expressions.
  */
trait Unevaluable extends Expression {

}