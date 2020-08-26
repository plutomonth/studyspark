package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}

/**
 * An expression that is evaluated to true if the input is null.
 */
case class IsNull(child: Expression) extends UnaryExpression with Predicate {

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    ev.isNull = "false"
    ev.value = eval.isNull
    eval.code
  }


}

/**
 * An expression that is evaluated to true if the input is not null.
 */
case class IsNotNull(child: Expression) extends UnaryExpression with Predicate {
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    ev.isNull = "false"
    ev.value = s"(!(${eval.isNull}))"
    eval.code
  }
}