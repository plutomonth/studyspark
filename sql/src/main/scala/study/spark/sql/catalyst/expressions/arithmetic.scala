package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import study.spark.sql.types.DataType


abstract class BinaryArithmetic extends BinaryOperator {
  override def dataType: DataType = left.dataType
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {

  override def symbol: String = "+"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
/*    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$$plus($eval2)")
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case CalendarIntervalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.add($eval2)")*/
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }

}