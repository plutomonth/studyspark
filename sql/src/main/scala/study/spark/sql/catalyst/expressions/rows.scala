package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.InternalRow

/**
 * An extended version of [[InternalRow]] that implements all special getters, toString
 * and equals/hashCode by `genericGet`.
 */
trait BaseGenericInternalRow extends InternalRow {

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

}