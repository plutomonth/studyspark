package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import study.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType}


/**
 * An [[Expression]] that returns a boolean value.
 */
trait Predicate extends Expression {
  override def dataType: DataType = BooleanType
}


abstract class BinaryComparison extends BinaryOperator with Predicate {
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (ctx.isPrimitiveType(left.dataType)
      && left.dataType != BooleanType // java boolean doesn't support > or < operator
      && left.dataType != FloatType
      && left.dataType != DoubleType) {
      // faster version
      defineCodeGen(ctx, ev, (c1, c2) => s"$c1 $symbol $c2")
    } else {
      defineCodeGen(ctx, ev, (c1, c2) => s"${ctx.genComp(left.dataType, c1, c2)} $symbol 0")
    }
  }
}

case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "="
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "<"
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "<="
}

case class Not(child: Expression)
  extends UnaryExpression with Predicate with ImplicitCastInputTypes {

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"!($c)")
  }
}

case class EqualNullSafe(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "<=>"
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = ">"
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = ">="
}

case class Or(left: Expression, right: Expression) extends BinaryOperator with Predicate {

  override def symbol: String = "||"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)

    // The result should be `true`, if any of them is `true` whenever the other is null or not.
    s"""
      ${eval1.code}
      boolean ${ev.isNull} = false;
      boolean ${ev.value} = true;

      if (!${eval1.isNull} && ${eval1.value}) {
      } else {
        ${eval2.code}
        if (!${eval2.isNull} && ${eval2.value}) {
        } else if (!${eval1.isNull} && !${eval2.isNull}) {
          ${ev.value} = false;
        } else {
          ${ev.isNull} = true;
        }
      }
     """
  }



}

case class And(left: Expression, right: Expression) extends BinaryOperator with Predicate {
  override def symbol: String = "&&"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)

    // The result should be `false`, if any of them is `false` whenever the other is null or not.
    s"""
      ${eval1.code}
      boolean ${ev.isNull} = false;
      boolean ${ev.value} = false;

      if (!${eval1.isNull} && !${eval1.value}) {
      } else {
        ${eval2.code}
        if (!${eval2.isNull} && !${eval2.value}) {
        } else if (!${eval1.isNull} && !${eval2.isNull}) {
          ${ev.value} = true;
        } else {
          ${ev.isNull} = true;
        }
      }
     """
  }
}

/**
 * Evaluates to `true` if `list` contains `value`.
 */
case class In(value: Expression, list: Seq[Expression]) extends Predicate
  with ImplicitCastInputTypes {

  override def children: Seq[Expression] = value +: list
  override def nullable: Boolean = children.exists(_.nullable)

  override def eval(input: InternalRow): Any = {
    val evaluatedValue = value.eval(input)
    if (evaluatedValue == null) {
      null
    } else {
      var hasNull = false
      list.foreach { e =>
        val v = e.eval(input)
        if (v == evaluatedValue) {
          return true
        } else if (v == null) {
          hasNull = true
        }
      }
      if (hasNull) {
        null
      } else {
        false
      }
    }
  }


  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val valueGen = value.gen(ctx)
    val listGen = list.map(_.gen(ctx))
    val listCode = listGen.map(x =>
      s"""
        if (!${ev.value}) {
          ${x.code}
          if (${x.isNull}) {
            ${ev.isNull} = true;
          } else if (${ctx.genEqual(value.dataType, valueGen.value, x.value)}) {
            ${ev.isNull} = false;
            ${ev.value} = true;
          }
        }
       """).mkString("\n")
    s"""
      ${valueGen.code}
      boolean ${ev.value} = false;
      boolean ${ev.isNull} = ${valueGen.isNull};
      if (!${ev.isNull}) {
        $listCode
      }
    """
  }

}