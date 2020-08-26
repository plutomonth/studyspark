package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.InternalRow
import study.spark.sql.types.DataType

/**
 * An extended version of [[InternalRow]] that implements all special getters, toString
 * and equals/hashCode by `genericGet`.
 */
trait BaseGenericInternalRow extends InternalRow {
  protected def genericGet(ordinal: Int): Any

  private def getAs[T](ordinal: Int) = genericGet(ordinal).asInstanceOf[T]
  override def isNullAt(ordinal: Int): Boolean = getAs[AnyRef](ordinal) eq null
  override def get(ordinal: Int, dataType: DataType): AnyRef = getAs(ordinal)
  override def getLong(ordinal: Int): Long = getAs(ordinal)
}

/**
 * An extended interface to [[InternalRow]] that allows the values for each column to be updated.
 * Setting a value through a primitive function implicitly marks that column as not null.
 */
abstract class MutableRow extends InternalRow {

}

/**
 * A internal row implementation that uses an array of objects as the underlying storage.
 * Note that, while the array is not copied, and thus could technically be mutated after creation,
 * this is not allowed.
 */
class GenericInternalRow(private[sql] val values: Array[Any]) extends BaseGenericInternalRow {
  override protected def genericGet(ordinal: Int) = values(ordinal)
}