package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.rules.{Rule, RuleExecutor}

package object codegen {
  /** Canonicalizes an expression so those that differ only by names can reuse the same code. */
  object ExpressionCanonicalizer extends RuleExecutor[Expression] {
    val batches =
      Batch("CleanExpressions", FixedPoint(20), CleanExpressions) :: Nil

    object CleanExpressions extends Rule[Expression] {
      def apply(e: Expression): Expression = e transform {
        case Alias(c, _) => c
      }
    }
  }
}
