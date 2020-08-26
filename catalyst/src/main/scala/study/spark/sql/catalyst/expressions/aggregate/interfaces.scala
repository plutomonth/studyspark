package study.spark.sql.catalyst.expressions.aggregate

import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import study.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Unevaluable}
import study.spark.sql.types.DataType


/** The mode of an [[AggregateFunction]]. */
private[sql] sealed trait AggregateMode

/**
  * AggregateFunction2 is the superclass of two aggregation function interfaces:
  *
  *  - [[ImperativeAggregate]] is for aggregation functions that are specified in terms of
  *    initialize(), update(), and merge() functions that operate on Row-based aggregation buffers.
  *  - [[DeclarativeAggregate]] is for aggregation functions that are specified using
  *    Catalyst expressions.
  *
  * In both interfaces, aggregates must define the schema ([[aggBufferSchema]]) and attributes
  * ([[aggBufferAttributes]]) of an aggregation buffer which is used to hold partial aggregate
  * results. At runtime, multiple aggregate functions are evaluated by the same operator using a
  * combined aggregation buffer which concatenates the aggregation buffers of the individual
  * aggregate functions.
  *
  * Code which accepts [[AggregateFunction]] instances should be prepared to handle both types of
  * aggregate functions.
  */
sealed abstract class AggregateFunction extends Expression with ImplicitCastInputTypes {
    override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
        throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

    /**
     * Wraps this [[AggregateFunction]] in an [[AggregateExpression]] and set isDistinct
     * field of the [[AggregateExpression]] to the given value because
     * [[AggregateExpression]] is the container of an [[AggregateFunction]], aggregation mode,
     * and the flag indicating if this aggregation is distinct aggregation or not.
     * An [[AggregateFunction]] should not be used without being wrapped in
     * an [[AggregateExpression]].
     */
    def toAggregateExpression(isDistinct: Boolean): AggregateExpression = {
        AggregateExpression(aggregateFunction = this, mode = Complete, isDistinct = isDistinct)
    }
}

/**
  * API for aggregation functions that are expressed in terms of Catalyst expressions.
  *
  * When implementing a new expression-based aggregate function, start by implementing
  * `bufferAttributes`, defining attributes for the fields of the mutable aggregation buffer. You
  * can then use these attributes when defining `updateExpressions`, `mergeExpressions`, and
  * `evaluateExpressions`.
  *
  * Please note that children of an aggregate function can be unresolved (it will happen when
  * we create this function in DataFrame API). So, if there is any fields in
  * the implemented class that need to access fields of its children, please make
  * those fields `lazy val`s.
  */
abstract class DeclarativeAggregate
  extends AggregateFunction
    with Serializable
    with Unevaluable {

    }

/**
 * API for aggregation functions that are expressed in terms of imperative initialize(), update(),
 * and merge() functions which operate on Row-based aggregation buffers.
 *
 * Within these functions, code should access fields of the mutable aggregation buffer by adding the
 * bufferSchema-relative field number to `mutableAggBufferOffset` then using this new field number
 * to access the buffer Row. This is necessary because this aggregation function's buffer is
 * embedded inside of a larger shared aggregation buffer when an aggregation operator evaluates
 * multiple aggregate functions at the same time.
 *
 * We need to perform similar field number arithmetic when merging multiple intermediate
 * aggregate buffers together in `merge()` (in this case, use `inputAggBufferOffset` when accessing
 * the input buffer).
 *
 * Correct ImperativeAggregate evaluation depends on the correctness of `mutableAggBufferOffset` and
 * `inputAggBufferOffset`, but not on the correctness of the attribute ids in `aggBufferAttributes`
 * and `inputAggBufferAttributes`.
 */
abstract class ImperativeAggregate extends AggregateFunction {

}

/**
 * An [[AggregateFunction]] with [[Complete]] mode is used to evaluate this function directly
 * from original input rows without any partial aggregation.
 * This function updates the given aggregation buffer with the original input of this
 * function. When it has processed all input rows, the final result of this function is returned.
 */
private[sql] case object Complete extends AggregateMode

/**
 * A container for an [[AggregateFunction]] with its [[AggregateMode]] and a field
 * (`isDistinct`) indicating if DISTINCT keyword is specified for this function.
 */
private[sql] case class AggregateExpression(
     aggregateFunction: AggregateFunction,
     mode: AggregateMode,
     isDistinct: Boolean)
  extends Expression with Unevaluable {

    override def children: Seq[Expression] = aggregateFunction :: Nil
    override def dataType: DataType = aggregateFunction.dataType
    override def nullable: Boolean = aggregateFunction.nullable


}