package study.spark.sql.execution.columnar

import study.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import study.spark.sql.types.{IntegerType, LongType}

private[columnar] class ColumnStatisticsSchema(a: Attribute) extends Serializable {
  val upperBound = AttributeReference(a.name + ".upperBound", a.dataType, nullable = true)()
  val lowerBound = AttributeReference(a.name + ".lowerBound", a.dataType, nullable = true)()
  val nullCount = AttributeReference(a.name + ".nullCount", IntegerType, nullable = false)()
  val count = AttributeReference(a.name + ".count", IntegerType, nullable = false)()
  val sizeInBytes = AttributeReference(a.name + ".sizeInBytes", LongType, nullable = false)()

  val schema = Seq(lowerBound, upperBound, nullCount, count, sizeInBytes)
}

/**
 * Used to collect statistical information when building in-memory columns.
 *
 * NOTE: we intentionally avoid using `Ordering[T]` to compare values here because `Ordering[T]`
 * brings significant performance penalty.
 */
private[columnar] sealed trait ColumnStats extends Serializable {

}

private[columnar] class PartitionStatistics(tableSchema: Seq[Attribute]) extends Serializable {
  val (forAttribute, schema) = {
    val allStats = tableSchema.map(a => a -> new ColumnStatisticsSchema(a))
    (AttributeMap(allStats), allStats.map(_._2.schema).foldLeft(Seq.empty[Attribute])(_ ++ _))
  }
}