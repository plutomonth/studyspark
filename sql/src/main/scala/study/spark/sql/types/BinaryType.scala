package study.spark.sql.types

import study.spark.sql.catalyst.ScalaReflectionLock

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

class BinaryType private() extends AtomicType {

  private[sql] type InternalType = Array[Byte]

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }

  /**
    * The default size of a value of the BinaryType is 4096 bytes.
    */
  override def defaultSize: Int = 4096

  private[spark] override def asNullable: BinaryType = this

  private[sql] val ordering = new Ordering[InternalType] {
    def compare(x: Array[Byte], y: Array[Byte]): Int = {
      TypeUtils.compareBinary(x, y)
    }
  }

}
case object BinaryType extends BinaryType