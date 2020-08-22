package study.spark

import study.spark.rdd.RDD

abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}