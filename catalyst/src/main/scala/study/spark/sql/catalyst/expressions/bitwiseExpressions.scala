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