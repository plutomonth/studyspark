package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, CodegenFallback, GeneratedExpressionCode}
import study.spark.sql.types.DataType



// scalastyle:off
/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseKeyWhen(key: Expression, branches: Seq[Expression])
  extends CaseWhenLike with CodegenFallback {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = key +: branches

  private def evalElse(input: InternalRow): Any = {
    if (branchesArr.length % 2 == 0) {
      null
    } else {
      branchesArr(branchesArr.length - 1).eval(input)
    }
  }

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: InternalRow): Any = {
    val evaluatedKey = key.eval(input)
    // If key is null, we can just return the else part or null if there is no else.
    // If key is not null but doesn't match any when part, we need to return
    // the else part or null if there is no else, according to Hive's semantics.
    if (evaluatedKey != null) {
      val len = branchesArr.length
      var i = 0
      while (i < len - 1) {
        if (evaluatedKey ==  branchesArr(i).eval(input)) {
          return branchesArr(i + 1).eval(input)
        }
        i += 2
      }
    }
    evalElse(input)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (shouldFallback) {
      // Fallback to interpreted mode if there are too many branches, as it may reach the
      // 64K limit (limit on bytecode size for a single function).
      return super[CodegenFallback].genCode(ctx, ev)
    }
    val keyEval = key.gen(ctx)
    val len = branchesArr.length
    val got = ctx.freshName("got")

    val cases = (0 until len/2).map { i =>
      val cond = branchesArr(i * 2).gen(ctx)
      val res = branchesArr(i * 2 + 1).gen(ctx)
      s"""
        if (!$got) {
          ${cond.code}
          if (!${cond.isNull} && ${ctx.genEqual(key.dataType, keyEval.value, cond.value)}) {
            $got = true;
            ${res.code}
            ${ev.isNull} = ${res.isNull};
            ${ev.value} = ${res.value};
          }
        }
      """
    }.mkString("\n")

    val other = if (len % 2 == 1) {
      val res = branchesArr(len - 1).gen(ctx)
      s"""
        if (!$got) {
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.value} = ${res.value};
        }
      """
    } else {
      ""
    }

    s"""
      boolean $got = false;
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      ${keyEval.code}
      if (!${keyEval.isNull}) {
        $cases
      }
      $other
    """
  }


}

// scalastyle:off
/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseWhen(branches: Seq[Expression]) extends CaseWhenLike with CodegenFallback {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = branches


  /** Written in imperative fashion for performance considerations. */
  override def eval(input: InternalRow): Any = {
    val len = branchesArr.length
    var i = 0
    // If all branches fail and an elseVal is not provided, the whole statement
    // defaults to null, according to Hive's semantics.
    while (i < len - 1) {
      if (branchesArr(i).eval(input) == true) {
        return branchesArr(i + 1).eval(input)
      }
      i += 2
    }
    var res: Any = null
    if (i == len - 1) {
      res = branchesArr(i).eval(input)
    }
    return res
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (shouldFallback) {
      // Fallback to interpreted mode if there are too many branches, as it may reach the
      // 64K limit (limit on bytecode size for a single function).
      return super[CodegenFallback].genCode(ctx, ev)
    }
    val len = branchesArr.length
    val got = ctx.freshName("got")

    val cases = (0 until len/2).map { i =>
      val cond = branchesArr(i * 2).gen(ctx)
      val res = branchesArr(i * 2 + 1).gen(ctx)
      s"""
        if (!$got) {
          ${cond.code}
          if (!${cond.isNull} && ${cond.value}) {
            $got = true;
            ${res.code}
            ${ev.isNull} = ${res.isNull};
            ${ev.value} = ${res.value};
          }
        }
      """
    }.mkString("\n")

    val other = if (len % 2 == 1) {
      val res = branchesArr(len - 1).gen(ctx)
      s"""
        if (!$got) {
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.value} = ${res.value};
        }
      """
    } else {
      ""
    }

    s"""
      boolean $got = false;
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      $cases
      $other
    """
  }

}

trait CaseWhenLike extends Expression {

  // Note that `branches` are considered in consecutive pairs (cond, val), and the optional last
  // element is the value for the default catch-all case (if provided).
  // Hence, `branches` consists of at least two elements, and can have an odd or even length.
  def branches: Seq[Expression]

  @transient lazy val thenList =
    branches.sliding(2, 2).collect { case Seq(_, thenExpr) => thenExpr }.toSeq
  val elseValue = if (branches.length % 2 == 0) None else Option(branches.last)

  override def dataType: DataType = thenList.head.dataType

  override def nullable: Boolean = {
    // If no value is nullable and no elseValue is provided, the whole statement defaults to null.
    thenList.exists(_.nullable) || (elseValue.map(_.nullable).getOrElse(true))
  }

  /**
   * Whether should it fallback to interpret mode or not.
   * @return
   */
  protected def shouldFallback: Boolean = {
    branches.length > 20
  }


}