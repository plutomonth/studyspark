package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import study.spark.sql.types.{AbstractDataType, ByteType, CalendarIntervalType, DataType, DecimalType, NumericType, ShortType, TypeCollection}


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
  /** Name of the function for this expression on a [[study.spark.sql.types.Decimal]] type. */
  def decimalMethod: String =
    sys.error("BinaryArithmetics must override either decimalMethod or genCode")

  override def dataType: DataType = left.dataType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$decimalMethod($eval2)")
    // byte and short are casted into int when add, minus, times or divide
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
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

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {

  override def decimalMethod: String = "$div"
  override def symbol: String = "/"
  /**
   * Special case handling due to division by 0 => null.
   */
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.value}.isZero()"
    } else {
      s"${eval2.value} == 0"
    }
    val javaType = ctx.javaType(dataType)
    val divide = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval1.value}.$decimalMethod(${eval2.value})"
    } else {
      s"($javaType)(${eval1.value} $symbol ${eval2.value})"
    }
    s"""
      ${eval2.code}
      boolean ${ev.isNull} = false;
      $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
      if (${eval2.isNull} || $isZero) {
        ${ev.isNull} = true;
      } else {
        ${eval1.code}
        if (${eval1.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $divide;
        }
      }
    """
  }
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "*"
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "%"
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {


  override def symbol: String = "-"

}