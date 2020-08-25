package study.spark.sql.catalyst.expressions

import java.sql.{Date, Timestamp}

import study.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import study.spark.sql.catalyst.expressions.codegen._
import study.spark.sql.catalyst.util.DateTimeUtils._
import study.spark.sql.types._
import study.spark.unsafe.types.{CalendarInterval, UTF8String}

object Literal {
  def apply(v: Any): Literal = v match {
    case i: Int => Literal(i, IntegerType)
    case l: Long => Literal(l, LongType)
    case d: Double => Literal(d, DoubleType)
    case f: Float => Literal(f, FloatType)
    case b: Byte => Literal(b, ByteType)
    case s: Short => Literal(s, ShortType)
    case s: String => Literal(UTF8String.fromString(s), StringType)
    case b: Boolean => Literal(b, BooleanType)
    case d: BigDecimal => Literal(Decimal(d), DecimalType(Math.max(d.precision, d.scale), d.scale))
    case d: java.math.BigDecimal =>
      Literal(Decimal(d), DecimalType(Math.max(d.precision, d.scale), d.scale()))
    case d: Decimal => Literal(d, DecimalType(Math.max(d.precision, d.scale), d.scale))
    case t: Timestamp => Literal(fromJavaTimestamp(t), TimestampType)
    case d: Date => Literal(fromJavaDate(d), DateType)
    case a: Array[Byte] => Literal(a, BinaryType)
    case i: CalendarInterval => Literal(i, CalendarIntervalType)
    case null => Literal(null, NullType)
    case v: Literal => v
    case _ =>
      throw new RuntimeException("Unsupported literal type " + v.getClass + " " + v)
  }

  def create(v: Any, dataType: DataType): Literal = {
    Literal(CatalystTypeConverters.convertToCatalyst(v), dataType)
  }
}

/**
  * In order to do type checking, use Literal.create() instead of constructor
  */
case class Literal protected (value: Any, dataType: DataType)
  extends LeafExpression with CodegenFallback {

  override def nullable: Boolean = value == null

  override def eval(input: InternalRow): Any = value

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    // change the isNull and primitive to consts, to inline them
    if (value == null) {
      ev.isNull = "true"
      s"final ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};"
    } else {
      dataType match {
        case BooleanType =>
          ev.isNull = "false"
          ev.value = value.toString
          ""
        case FloatType =>
          val v = value.asInstanceOf[Float]
          if (v.isNaN || v.isInfinite) {
            super.genCode(ctx, ev)
          } else {
            ev.isNull = "false"
            ev.value = s"${value}f"
            ""
          }
        case DoubleType =>
          val v = value.asInstanceOf[Double]
          if (v.isNaN || v.isInfinite) {
            super.genCode(ctx, ev)
          } else {
            ev.isNull = "false"
            ev.value = s"${value}D"
            ""
          }
        case ByteType | ShortType =>
          ev.isNull = "false"
          ev.value = s"(${ctx.javaType(dataType)})$value"
          ""
        case IntegerType | DateType =>
          ev.isNull = "false"
          ev.value = value.toString
          ""
        case TimestampType | LongType =>
          ev.isNull = "false"
          ev.value = s"${value}L"
          ""
        // eval() version may be faster for non-primitive types
        case other =>
          super.genCode(ctx, ev)
      }
    }
  }
}