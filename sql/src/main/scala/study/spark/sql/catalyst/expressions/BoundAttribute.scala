package study.spark.sql.catalyst.expressions

import study.spark.sql.types.DataType

/**
 * A bound reference points to a specific slot in the input tuple, allowing the actual value
 * to be retrieved more efficiently.  However, since operations like column pruning can change
 * the layout of intermediate tuples, BindReferences should be run after all such transformations.
 */
case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends LeafExpression with NamedExpression {

  override def name: String = s"i[$ordinal]"

}