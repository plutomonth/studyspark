package study.spark.rdd

import study.spark.SparkContext

import scala.reflect.ClassTag

/**
 * An RDD that recovers checkpointed data from storage.
 */
private[spark] abstract class CheckpointRDD[T: ClassTag](sc: SparkContext)
  extends RDD[T](sc, Nil) {

}
