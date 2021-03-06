package study.spark.sql.types

import study.spark.sql.catalyst.ScalaReflectionLock

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag


class BooleanType private() extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "BooleanType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Boolean
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized {typeTag[InternalType] }
  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
    * The default size of a value of the BooleanType is 1 byte.
    */
  override def defaultSize: Int = 1

  private[spark] override def asNullable: BooleanType = this
}

case object BooleanType extends BooleanType