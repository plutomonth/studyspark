package study.spark.sql.types

import study.spark.sql.catalyst.ScalaReflectionLock

import scala.math.{Integral, Numeric, Ordering}
import scala.reflect.runtime.universe.typeTag


/**
 * The data type representing `Short` values. Please use the singleton [[ShortType]].
 */
class ShortType private() extends IntegralType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "ShortType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Short
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val numeric = implicitly[Numeric[Short]]
  private[sql] val integral = implicitly[Integral[Short]]
  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The default size of a value of the ShortType is 2 bytes.
   */
  override def defaultSize: Int = 2

  override def simpleString: String = "smallint"

  private[spark] override def asNullable: ShortType = this
}

case object ShortType extends ShortType
