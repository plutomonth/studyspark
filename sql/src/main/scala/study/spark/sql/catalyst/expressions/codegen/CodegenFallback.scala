package study.spark.sql.catalyst.expressions.codegen

import study.spark.sql.catalyst.expressions.Expression

/**
  * A trait that can be used to provide a fallback mode for expression code generation.
  */
trait CodegenFallback extends Expression {

}
