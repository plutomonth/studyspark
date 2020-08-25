package study.spark.sql.catalyst.expressions

import study.spark.SparkException
import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, CodegenFallback, GeneratedExpressionCode}
import study.spark.sql.types.{DataType, UserDefinedType}


/** Cast the child expression to the target data type. */
case class Cast(child: Expression, dataType: DataType)
  extends UnaryExpression with CodegenFallback {

  // Since we need to cast child expressions recursively inside ComplexTypes, such as Map's
  // Key and Value, Struct's field, we need to name out all the variable names involved in a cast.
  private[this] def castCode(ctx: CodeGenContext, childPrim: String, childNull: String,
                             resultPrim: String, resultNull: String, resultType: DataType, cast: CastFunction): String = {
    s"""
      boolean $resultNull = $childNull;
      ${ctx.javaType(resultType)} $resultPrim = ${ctx.defaultValue(resultType)};
      if (!${childNull}) {
        ${cast(childPrim, resultPrim, resultNull)}
      }
    """
  }

  // three function arguments are: child.primitive, result.primitive and result.isNull
  // it returns the code snippets to be put in null safe evaluation region
  private[this] type CastFunction = (String, String, String) => String

  private[this] def nullSafeCastFunction(
      from: DataType,
      to: DataType,
      ctx: CodeGenContext): CastFunction = to match {

/*    case _ if from == NullType => (c, evPrim, evNull) => s"$evNull = true;"
    case _ if to == from => (c, evPrim, evNull) => s"$evPrim = $c;"
    case StringType => castToStringCode(from, ctx)
    case BinaryType => castToBinaryCode(from)
    case DateType => castToDateCode(from, ctx)
    case decimal: DecimalType => castToDecimalCode(from, decimal, ctx)
    case TimestampType => castToTimestampCode(from, ctx)
    case CalendarIntervalType => castToIntervalCode(from)
    case BooleanType => castToBooleanCode(from)
    case ByteType => castToByteCode(from)
    case ShortType => castToShortCode(from)
    case IntegerType => castToIntCode(from)
    case FloatType => castToFloatCode(from)
    case LongType => castToLongCode(from)
    case DoubleType => castToDoubleCode(from)

    case array: ArrayType =>
      castArrayCode(from.asInstanceOf[ArrayType].elementType, array.elementType, ctx)
    case map: MapType => castMapCode(from.asInstanceOf[MapType], map, ctx)
    case struct: StructType => castStructCode(from.asInstanceOf[StructType], struct, ctx)
    case udt: UserDefinedType[_]
      if udt.userClass == from.asInstanceOf[UserDefinedType[_]].userClass =>
      (c, evPrim, evNull) => s"$evPrim = $c;"*/
    case _: UserDefinedType[_] =>
      throw new SparkException(s"Cannot cast $from to $to.")
  }


  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    val nullSafeCast = nullSafeCastFunction(child.dataType, dataType, ctx)
    eval.code +
      castCode(ctx, eval.value, eval.isNull, ev.value, ev.isNull, dataType, nullSafeCast)
  }
}
