package study.spark.broadcast

import java.io.Serializable

import study.spark.Logging

import scala.reflect.ClassTag

/**
  * A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable
  * cached on each machine rather than shipping a copy of it with tasks. They can be used, for
  * example, to give every node a copy of a large input dataset in an efficient manner. Spark also
  * attempts to distribute broadcast variables using efficient broadcast algorithms to reduce
  * communication cost.
  *
  * Broadcast variables are created from a variable `v` by calling
  * [[org.apache.spark.SparkContext#broadcast]].
  * The broadcast variable is a wrapper around `v`, and its value can be accessed by calling the
  * `value` method. The interpreter session below shows this:
  *
  * {{{
  * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
  * broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
  *
  * scala> broadcastVar.value
  * res0: Array[Int] = Array(1, 2, 3)
  * }}}
  *
  * After the broadcast variable is created, it should be used instead of the value `v` in any
  * functions run on the cluster so that `v` is not shipped to the nodes more than once.
  * In addition, the object `v` should not be modified after it is broadcast in order to ensure
  * that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped
  * to a new node later).
  *
  * @param id A unique identifier for the broadcast variable.
  * @tparam T Type of the data contained in the broadcast variable.
  */
abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable with Logging {

}
