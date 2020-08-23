package study.spark.sql.catalyst.expressions.codegen

import study.spark.sql.catalyst.expressions.{Expression, UnsafeProjection}
import study.spark.sql.types.DataType

/**
 * Generates a [[Projection]] that returns an [[UnsafeRow]].
 *
 * It generates the code for all the expressions, compute the total length for all the columns
 * (can be accessed via variables), and then copy the data into a scratch buffer space in the
 * form of UnsafeRow (the scratch buffer will grow as needed).
 *
 * Note: The returned UnsafeRow will be pointed to a scratch buffer inside the projection.
 */
object GenerateUnsafeProjection extends CodeGenerator[Seq[Expression], UnsafeProjection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  def generate(
        expressions: Seq[Expression],
        subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    create(canonicalize(expressions), subexpressionEliminationEnabled)
  }

  protected def create(expressions: Seq[Expression]): UnsafeProjection = {
    create(expressions, subexpressionEliminationEnabled = false)
  }

  private def create(expressions: Seq[Expression],
        subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    val ctx = newCodeGenContext()
    val eval = createCode(ctx, expressions, subexpressionEliminationEnabled)

    val codeBody = s"""
      public java.lang.Object generate($exprType[] exprs) {
        return new SpecificUnsafeProjection(exprs);
      }

      class SpecificUnsafeProjection extends ${classOf[UnsafeProjection].getName} {

        private $exprType[] expressions;

        ${declareMutableStates(ctx)}

        ${declareAddedFunctions(ctx)}

        public SpecificUnsafeProjection($exprType[] expressions) {
          this.expressions = expressions;
          ${initMutableStates(ctx)}
        }

        // Scala.Function1 need this
        public java.lang.Object apply(java.lang.Object row) {
          return apply((InternalRow) row);
        }

        public UnsafeRow apply(InternalRow ${ctx.INPUT_ROW}) {
          ${eval.code.trim}
          return ${eval.value};
        }
      }
      """

    val code = new CodeAndComment(codeBody, ctx.getPlaceHolderToComments())
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[UnsafeProjection]
  }


  def createCode(ctx: CodeGenContext,
        expressions: Seq[Expression],
        useSubexprElimination: Boolean = false): GeneratedExpressionCode = {
    val exprEvals = ctx.generateExpressions(expressions, useSubexprElimination)
    val exprTypes = expressions.map(_.dataType)

    val result = ctx.freshName("result")
    ctx.addMutableState("UnsafeRow", result, s"this.$result = new UnsafeRow();")
    val bufferHolder = ctx.freshName("bufferHolder")
    val holderClass = classOf[BufferHolder].getName
    ctx.addMutableState(holderClass, bufferHolder, s"this.$bufferHolder = new $holderClass();")

    // Reset the subexpression values for each row.
    val subexprReset = ctx.subExprResetVariables.mkString("\n")

    val code =
      s"""
        $bufferHolder.reset();
        $subexprReset
        ${writeExpressionsToBuffer(ctx, ctx.INPUT_ROW, exprEvals, exprTypes, bufferHolder)}

        $result.pointTo($bufferHolder.buffer, ${expressions.length}, $bufferHolder.totalSize());
      """
    GeneratedExpressionCode(code, "false", result)
  }

  private def writeExpressionsToBuffer(
      ctx: CodeGenContext,
      row: String,
      inputs: Seq[GeneratedExpressionCode],
      inputTypes: Seq[DataType],
      bufferHolder: String): String = {

    ""
  }


}
