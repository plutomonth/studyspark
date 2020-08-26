package study.spark.sql.catalyst.expressions.codegen

import study.spark.sql.catalyst.expressions.{Expression, Nondeterministic}

/**
  * A trait that can be used to provide a fallback mode for expression code generation.
  */
trait CodegenFallback extends Expression {

  protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    foreach {
      case n: Nondeterministic => n.setInitialValues()
      case _ =>
    }

    ctx.references += this
    val objectTerm = ctx.freshName("obj")
    val placeHolder = ctx.registerComment(this.toString)
    s"""
      $placeHolder
      java.lang.Object $objectTerm = expressions[${ctx.references.size - 1}].eval(${ctx.INPUT_ROW});
      boolean ${ev.isNull} = $objectTerm == null;
      ${ctx.javaType(this.dataType)} ${ev.value} = ${ctx.defaultValue(this.dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = (${ctx.boxedType(this.dataType)}) $objectTerm;
      }
    """
  }
}
