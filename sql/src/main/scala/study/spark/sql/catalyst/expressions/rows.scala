package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.InternalRow

/**
 * An extended interface to [[InternalRow]] that allows the values for each column to be updated.
 * Setting a value through a primitive function implicitly marks that column as not null.
 */
abstract class MutableRow extends InternalRow {

}