package study.spark

import java.io.Serializable

import scala.collection.generic.Growable
import scala.reflect.ClassTag

/**
 * A data type that can be accumulated, ie has an commutative and associative "add" operation,
 * but where the result type, `R`, may be different from the element type being added, `T`.
 *
 * You must define how to add data, and how to merge two of these together.  For some data types,
 * such as a counter, these might be the same operation. In that case, you can use the simpler
 * [[study.spark.Accumulator]]. They won't always be the same, though -- e.g., imagine you are
 * accumulating a set. You will add items to the set, and you will union two sets together.
 *
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `R` and `T`
 * @param name human-readable name for use in Spark's web UI
 * @param internal if this [[Accumulable]] is internal. Internal [[Accumulable]]s will be reported
 *                 to the driver via heartbeats. For internal [[Accumulable]]s, `R` must be
 *                 thread safe so that they can be reported correctly.
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
class Accumulable[R, T] private[spark] (
       initialValue: R,
       param: AccumulableParam[R, T],
       val name: Option[String],
       internal: Boolean)
  extends Serializable {

  private[spark] def this(
        @transient initialValue: R, param: AccumulableParam[R, T], internal: Boolean) = {
    this(initialValue, param, None, internal)
  }

  def this(@transient initialValue: R, param: AccumulableParam[R, T], name: Option[String]) =
    this(initialValue, param, name, false)

  def this(@transient initialValue: R, param: AccumulableParam[R, T]) =
    this(initialValue, param, None)


  val id: Long = Accumulators.newId

  @volatile @transient private var value_ : R = initialValue // Current value on master
  private var deserialized = false

  /**
   * Access the accumulator's current value; only allowed on master.
   */
  def value: R = {
    if (!deserialized) {
      value_
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }
}

/**
 * Helper object defining how to accumulate values of a particular type. An implicit
 * AccumulableParam needs to be available when you create [[Accumulable]]s of a specific type.
 *
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
trait AccumulableParam[R, T] extends Serializable {

}

private[spark] class
GrowableAccumulableParam[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
  extends AccumulableParam[R, T] {

  }

// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private[spark] object Accumulators extends Logging {

  private var lastId: Long = 0

  def newId(): Long = synchronized {
    lastId += 1
    lastId
  }
}