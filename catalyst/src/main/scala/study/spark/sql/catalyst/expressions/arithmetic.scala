package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import study.spark.sql.types.{CalendarIntervalType, DataType, DecimalType, NumericType}


case class UnaryMinus(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def dataType: DataType = child.dataType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType => defineCodeGen(ctx, ev, c => s"$c.unary_$$minus()")
    case dt: NumericType => nullSafeCodeGen(ctx, ev, eval => {
      val originValue = ctx.freshName("origin")
      // codegen would fail to compile if we just write (-($c))
      // for example, we could not write --9223372036854775808L in code
      s"""
        ${ctx.javaType(dt)} $originValue = (${ctx.javaType(dt)})($eval);
        ${ev.value} = (${ctx.javaType(dt)})(-($originValue));
      """})
    case dt: CalendarIntervalType => defineCodeGen(ctx, ev, c => s"$c.negate()")
  }
}

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