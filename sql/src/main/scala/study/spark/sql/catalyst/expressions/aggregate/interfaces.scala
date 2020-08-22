package study.spark.sql.catalyst.expressions.aggregate

import study.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Unevaluable}

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