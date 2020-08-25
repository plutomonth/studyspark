package study.spark.sql.catalyst.expressions

/**
  * An trait that gets mixin to define the expected input types of an expression.
  *
  * This trait is typically used by operator expressions (e.g. [[Add]], [[Subtract]]) to define
  * expected input types without any implicit casting.
  *
  * Most function expressions (e.g. [[Substring]] should extends [[ImplicitCastInputTypes]]) instead.
  */
trait ExpectsInputTypes extends Expression {
}


/**
  * A mixin for the analyzer to perform implicit type casting using [[ImplicitTypeCasts]].
  */
trait ImplicitCastInputTypes extends ExpectsInputTypes {
  // No other methods
}

