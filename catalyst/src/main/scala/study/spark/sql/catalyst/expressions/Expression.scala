package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import study.spark.sql.catalyst.trees.TreeNode
import study.spark.sql.types.DataType

/**
 * An expression in Catalyst.
 *
 * If an expression wants to be exposed in the function registry (so users can call it with
 * "name(arguments...)", the concrete implementation must be a case class whose constructor
 * arguments are all Expressions types. See Substring for an example.
 *
 * There are a few important traits:
 *
 * - Nondeterministic: an expression that is not deterministic.
 * - [[Unevaluable]]: an expression that is not supposed to be evaluated.
 * - CodegenFallback: an expression that does not have code gen implemented and falls back to
 *                        interpreted mode.
 *
 * - [[LeafExpression]]: an expression that has no child.
 * - [[UnaryExpression]]: an expression that has one child.
 * - [[BinaryExpression]]: an expression that has two children.
 * - [[BinaryOperator]]: a special case of [[BinaryExpression]] that requires two children to have
 *                       the same output data type.
 *
 */
abstract class Expression extends TreeNode[Expression] {

  /**
   * Returns true when the current expression always return the same result for fixed inputs from
   * children.
   *
   * Note that this means that an expression should be considered as non-deterministic if:
   * - if it relies on some mutable internal state, or
   * - if it relies on some implicit input that is not part of the children expression list.
   * - if it has non-deterministic child or children.
   *
   * An example would be `SparkPartitionID` that relies on the partition id returned by TaskContext.
   * By default leaf expressions are deterministic as Nil.forall(_.deterministic) returns true.
   */
  def deterministic: Boolean = children.forall(_.deterministic)

  def nullable: Boolean

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  def dataType: DataType

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: InternalRow = null): Any

  /**
   * Returns an [[GeneratedExpressionCode]], which contains Java source code that
   * can be used to generate the result of evaluating the expression on an input row.
   *
   * @param ctx a [[CodeGenContext]]
   * @return [[GeneratedExpressionCode]]
   */
  def gen(ctx: CodeGenContext): GeneratedExpressionCode = {
    ctx.subExprEliminationExprs.get(this).map { subExprState =>
      // This expression is repeated meaning the code to evaluated has already been added
      // as a function and called in advance. Just use it.
      GeneratedExpressionCode(
        ctx.registerComment(this.toString),
        subExprState.isNull,
        subExprState.value)
    }.getOrElse {
      val isNull = ctx.freshName("isNull")
      val primitive = ctx.freshName("primitive")
      val ve = GeneratedExpressionCode("", isNull, primitive)
      ve.code = genCode(ctx, ve)
      // Add `this` in the comment.
      ve.copy(code = s"${ctx.registerComment(this.toString)}\n" + ve.code.trim)
    }
  }


  /**
   * Returns Java source code that can be compiled to evaluate this expression.
   * The default behavior is to call the eval method of the expression. Concrete expression
   * implementations should override this to do actual code generation.
   *
   * @param ctx a [[CodeGenContext]]
   * @param ev an [[GeneratedExpressionCode]] with unique terms.
   * @return Java source code
   */
  protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String


  /**
   * Returns true when two expressions will always compute the same result, even if they differ
   * cosmetically (i.e. capitalization of names in attributes may be different).
   */
  def semanticEquals(other: Expression): Boolean = this.getClass == other.getClass && {
    def checkSemantic(elements1: Seq[Any], elements2: Seq[Any]): Boolean = {
      elements1.length == elements2.length && elements1.zip(elements2).forall {
        case (e1: Expression, e2: Expression) => e1 semanticEquals e2
        case (Some(e1: Expression), Some(e2: Expression)) => e1 semanticEquals e2
        case (t1: Traversable[_], t2: Traversable[_]) => checkSemantic(t1.toSeq, t2.toSeq)
        case (i1, i2) => i1 == i2
      }
    }
    // Non-deterministic expressions cannot be semantic equal
    if (!deterministic || !other.deterministic) return false
    val elements1 = this.productIterator.toSeq
    val elements2 = other.asInstanceOf[Product].productIterator.toSeq
    checkSemantic(elements1, elements2)
  }

  /**
   * Returns the hash for this expression. Expressions that compute the same result, even if
   * they differ cosmetically should return the same hash.
   */
  def semanticHash() : Int = {
    def computeHash(e: Seq[Any]): Int = {
      // See http://stackoverflow.com/questions/113511/hash-code-implementation
      var hash: Int = 17
      e.foreach(i => {
        val h: Int = i match {
          case e: Expression => e.semanticHash()
          case Some(e: Expression) => e.semanticHash()
          case t: Traversable[_] => computeHash(t.toSeq)
          case null => 0
          case other => other.hashCode()
        }
        hash = hash * 37 + h
      })
      hash
    }

    computeHash(this.productIterator.toSeq)
  }


}

/**
 * A leaf expression, i.e. one without any child expressions.
 */
abstract class LeafExpression extends Expression {

  def children: Seq[Expression] = Nil
}


/**
  * An expression that cannot be evaluated. Some expressions don't live past analysis or optimization
  * time (e.g. Star). This trait is used by those expressions.
  */
trait Unevaluable extends Expression {

  final override def eval(input: InternalRow = null): Any =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  final override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
}

/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
abstract class UnaryExpression extends Expression {
  def child: Expression

  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = child.nullable

  /**
    * Default behavior of evaluation according to the default nullability of UnaryExpression.
    * If subclass of UnaryExpression override nullable, probably should also override this.
    */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  /**
    * Called by default [[eval]] implementation.  If subclass of UnaryExpression keep the default
    * nullability, they can override this method to save null-check code.  If we need full control
    * of evaluation process, we should override [[eval]].
    */
  protected def nullSafeEval(input: Any): Any =
    sys.error(s"UnaryExpressions must override either eval or nullSafeEval")

  /**
    * Called by unary expressions to generate a code block that returns null if its parent returns
    * null, and if not not null, use `f` to generate the expression.
    *
    * As an example, the following does a boolean inversion (i.e. NOT).
    * {{{
    *   defineCodeGen(ctx, ev, c => s"!($c)")
    * }}}
    *
    * @param f function that accepts a variable name and returns Java code to compute the output.
    */
  protected def defineCodeGen(
                               ctx: CodeGenContext,
                               ev: GeneratedExpressionCode,
                               f: String => String): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = ${f(eval)};"
    })
  }

  /**
    * Called by unary expressions to generate a code block that returns null if its parent returns
    * null, and if not not null, use `f` to generate the expression.
    *
    * @param f function that accepts the non-null evaluation result name of child and returns Java
    *          code to compute the output.
    */
  protected def nullSafeCodeGen(
                                 ctx: CodeGenContext,
                                 ev: GeneratedExpressionCode,
                                 f: String => String): String = {
    val eval = child.gen(ctx)
    val resultCode = f(eval.value)
    eval.code + s"""
      boolean ${ev.isNull} = ${eval.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $resultCode
      }
    """
  }
}


/**
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class BinaryExpression extends Expression {

  def left: Expression
  def right: Expression

  override def children: Seq[Expression] = Seq(left, right)

  override def nullable: Boolean = left.nullable || right.nullable


  /**
   * Default behavior of evaluation according to the default nullability of BinaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }


  /**
   * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")


  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
       ctx: CodeGenContext,
       ev: GeneratedExpressionCode,
       f: (String, String) => String): String = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"${ev.value} = ${f(eval1, eval2)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
     ctx: CodeGenContext,
     ev: GeneratedExpressionCode,
     f: (String, String) => String): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val resultCode = f(eval1.value, eval2.value)
    s"""
      ${eval1.code}
      boolean ${ev.isNull} = ${eval1.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${eval2.code}
        if (!${eval2.isNull}) {
          $resultCode
        } else {
          ${ev.isNull} = true;
        }
      }
    """
  }

}

  /**
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 * 2. Two inputs are expected to the be same type. If the two inputs have different types,
 *    the analyzer will find the tightest common type and do the proper type casting.
 */
abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes {
    def symbol: String
}
