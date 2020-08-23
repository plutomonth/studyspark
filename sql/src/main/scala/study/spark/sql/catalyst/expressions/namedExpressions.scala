package study.spark.sql.catalyst.expressions

import study.spark.sql.types.Metadata

/**
 * An [[Expression]] that is named.
 */
trait NamedExpression extends Expression {
  def name: String

  /** Returns the metadata when an expression is a reference to another expression with metadata. */
  def metadata: Metadata = Metadata.empty
}

abstract class Attribute extends LeafExpression with NamedExpression {

}