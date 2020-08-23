package study.spark.sql.execution.columnar

import study.spark.Accumulable
import study.spark.rdd.RDD
import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.analysis.MultiInstanceRelation
import study.spark.sql.catalyst.expressions.Attribute
import study.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import study.spark.sql.execution.{ConvertToUnsafe, SparkPlan}
import study.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

private[sql] object InMemoryRelation {
  def apply(
     useCompression: Boolean,
     batchSize: Int,
     storageLevel: StorageLevel,
     child: SparkPlan,
     tableName: Option[String]): InMemoryRelation =
    new InMemoryRelation(child.output, useCompression, batchSize, storageLevel,
      if (child.outputsUnsafeRows) child else ConvertToUnsafe(child),
      tableName)()
}

/**
 * CachedBatch is a cached batch of rows.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
private[columnar]
case class CachedBatch(numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)

private[sql] case class InMemoryRelation(
      output: Seq[Attribute],
      useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      @transient child: SparkPlan,
      tableName: Option[String])(
      @transient private[sql] var _cachedColumnBuffers: RDD[CachedBatch] = null,
      @transient private[sql] var _statistics: Statistics = null,
      private[sql] var _batchStats: Accumulable[ArrayBuffer[InternalRow], InternalRow] = null)
  extends LogicalPlan with MultiInstanceRelation {

  private val batchStats: Accumulable[ArrayBuffer[InternalRow], InternalRow] =
    if (_batchStats == null) {
      child.sqlContext.sparkContext.accumulableCollection(ArrayBuffer.empty[InternalRow])
    } else {
      _batchStats
    }

  override def newInstance(): this.type = {
    new InMemoryRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child,
      tableName)(
      _cachedColumnBuffers,
      statisticsToBePropagated,
      batchStats).asInstanceOf[this.type]
  }

  override def statistics: Statistics = {
    if (_statistics == null) {
      if (batchStats.value.isEmpty) {
        // Underlying columnar RDD hasn't been materialized, no useful statistics information
        // available, return the default statistics.
        Statistics(sizeInBytes = child.sqlContext.conf.defaultSizeInBytes)
      } else {
        // Underlying columnar RDD has been materialized, required information has also been
        // collected via the `batchStats` accumulator, compute the final statistics,
        // and update `_statistics`.
        _statistics = Statistics(sizeInBytes = computeSizeInBytes)
        _statistics
      }
    } else {
      // Pre-computed statistics
      _statistics
    }
  }

  // Statistics propagation contracts:
  // 1. Non-null `_statistics` must reflect the actual statistics of the underlying data
  // 2. Only propagate statistics when `_statistics` is non-null
  private def statisticsToBePropagated = if (_statistics == null) {
    val updatedStats = statistics
    if (_statistics == null) null else updatedStats
  } else {
    _statistics
  }

}