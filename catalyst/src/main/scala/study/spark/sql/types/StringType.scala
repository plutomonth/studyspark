package study.spark.sql.types

import study.spark.sql.catalyst.ScalaReflectionLock
import study.spark.unsafe.types.UTF8String

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag


/**
 * :: DeveloperApi ::
 * The data type representing `String` values. Please use the singleton [[StringType]].
 */
class StringType private() extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "StringType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = UTF8String
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
   * The default size of a value of the StringType is 4096 bytes.
   */
  override def defaultSize: Int = 4096

  private[spark] override def asNullable: StringType = this
}

case object StringType extends StringType

