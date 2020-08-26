package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import study.spark.sql.types.DataType


/**
  * A function that calculates bitwise not(~) of a number.
  */
case class BitwiseNot(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def dataType: DataType = child.dataType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"(${ctx.javaType(dataType)}) ~($c)")
  }
}

/**
 * A function that calculates bitwise and(&) of two numbers.
 *
 * Code generation inherited from BinaryArithmetic.
 */
case class BitwiseAnd(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "&"
}

/**
 * A function that calculates bitwise or(|) of two numbers.
 *
 * Code generation inherited from BinaryArithmetic.
 */
case class BitwiseOr(left: Expression, right: Expression) extends BinaryArithmetic {

  override def symbol: String = "|"
}

/**
 * A function that calculates bitwise xor of two numbers.
 *
 * Code generation inherited from BinaryArithmetic.
 */
case class BitwiseXor(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "^"
}