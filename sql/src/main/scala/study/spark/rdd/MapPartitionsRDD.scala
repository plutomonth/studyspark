package study.spark.rdd

import study.spark.TaskContext

import scala.reflect.ClassTag

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
   var prev: RDD[T],
   f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
   preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

}
