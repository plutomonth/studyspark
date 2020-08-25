package study.spark.sql.types

import study.spark.sql.catalyst.ScalaReflectionLock

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag


class DateType private() extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DateType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Int

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }

  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
    * The default size of a value of the DateType is 4 bytes.
    */
  override def defaultSize: Int = 4

  private[spark] override def asNullable: DateType = this
}

case object DateType extends DateType