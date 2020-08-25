package study.spark.sql.types

import study.spark.sql.catalyst.ScalaReflectionLock

import scala.math.{Integral, Numeric, Ordering}
import scala.reflect.runtime.universe.typeTag

class ByteType private() extends IntegralType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "ByteType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Byte
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val numeric = implicitly[Numeric[Byte]]
  private[sql] val integral = implicitly[Integral[Byte]]
  private[sql] val ordering = implicitly[Ordering[InternalType]]

  /**
    * The default size of a value of the ByteType is 1 byte.
    */
  override def defaultSize: Int = 1

  override def simpleString: String = "tinyint"

  private[spark] override def asNullable: ByteType = this
}


case object ByteType extends ByteType