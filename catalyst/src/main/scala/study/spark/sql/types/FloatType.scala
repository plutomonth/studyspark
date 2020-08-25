package study.spark.sql.types

import study.spark.sql.catalyst.ScalaReflectionLock
import study.spark.util.Utils

import scala.math.Numeric.FloatAsIfIntegral
import scala.math.{Fractional, Numeric, Ordering}
import scala.reflect.runtime.universe.typeTag


/**
 * :: DeveloperApi ::
 * The data type representing `Float` values. Please use the singleton [[FloatType]].
 */
class FloatType private() extends FractionalType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "FloatType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Float
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val numeric = implicitly[Numeric[Float]]
  private[sql] val fractional = implicitly[Fractional[Float]]
  private[sql] val ordering = new Ordering[Float] {
    override def compare(x: Float, y: Float): Int = Utils.nanSafeCompareFloats(x, y)
  }
  private[sql] val asIntegral = FloatAsIfIntegral

  /**
   * The default size of a value of the FloatType is 4 bytes.
   */
  override def defaultSize: Int = 4

  private[spark] override def asNullable: FloatType = this
}

case object FloatType extends FloatType
