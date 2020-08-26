package study.spark.sql.catalyst.expressions

import java.util.regex.Pattern

import org.apache.commons.lang3.StringEscapeUtils
import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, CodegenFallback, GeneratedExpressionCode}
import study.spark.sql.catalyst.util.StringUtils
import study.spark.sql.types.{BooleanType, DataType}
import study.spark.unsafe.types.UTF8String


trait StringRegexExpression extends ImplicitCastInputTypes {
  self: BinaryExpression =>

  def escape(v: String): String
  override def dataType: DataType = BooleanType

}


case class RLike(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression with CodegenFallback {

  override def escape(v: String): String = v

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val patternClass = classOf[Pattern].getName
    val pattern = ctx.freshName("pattern")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(rVal.asInstanceOf[UTF8String].toString())
        ctx.addMutableState(patternClass, pattern,
          s"""$pattern = ${patternClass}.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.gen(ctx)
        s"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).find(0);
          }
        """
      } else {
        s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        """
      }
    } else {
      val rightStr = ctx.freshName("rightStr")
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String $rightStr = ${eval2}.toString();
          ${patternClass} $pattern = ${patternClass}.compile($rightStr);
          ${ev.value} = $pattern.matcher(${eval1}.toString()).find(0);
        """
      })
    }
  }

}

/**
 * Simple RegEx pattern matching function
 */
case class Like(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression with CodegenFallback {

  override def escape(v: String): String = StringUtils.escapeLikeRegex(v)

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val patternClass = classOf[Pattern].getName
    val escapeFunc = StringUtils.getClass.getName.stripSuffix("$") + ".escapeLikeRegex"
    val pattern = ctx.freshName("pattern")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(escape(rVal.asInstanceOf[UTF8String].toString()))
        ctx.addMutableState(patternClass, pattern,
          s"""$pattern = ${patternClass}.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.gen(ctx)
        s"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).matches();
          }
        """
      } else {
        s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        """
      }
    } else {
      val rightStr = ctx.freshName("rightStr")
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String $rightStr = ${eval2}.toString();
          ${patternClass} $pattern = ${patternClass}.compile($escapeFunc($rightStr));
          ${ev.value} = $pattern.matcher(${eval1}.toString()).matches();
        """
      })
    }
  }

}