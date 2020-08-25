package study.spark.sql.catalyst.expressions

import study.spark.sql.types.DataType

abstract sealed class SortDirection
case object Ascending extends SortDirection
case object Descending extends SortDirection


/**
 * An expression that can be used to sort a tuple.  This class extends expression primarily so that
 * transformations over expression will descend into its child.
 */
case class SortOrder(child: Expression, direction: SortDirection)
  extends UnaryExpression with Unevaluable {

  override def dataType: DataType = child.dataType
}
