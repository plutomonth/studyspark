package study.spark.sql.catalyst

import study.spark.sql.catalyst.expressions.SpecializedGetters

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 */
abstract class InternalRow extends SpecializedGetters with Serializable {

}
