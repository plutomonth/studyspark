package study.spark.rdd

import study.spark.SparkContext

import scala.collection.Map
import scala.reflect.ClassTag

private[spark] class ParallelCollectionRDD[T: ClassTag](
                                                         sc: SparkContext,
                                                         @transient private val data: Seq[T],
                                                         numSlices: Int,
                                                         locationPrefs: Map[Int, Seq[String]])
  extends RDD[T](sc, Nil){

}
