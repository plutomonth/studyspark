package study.spark.sql.types

import study.spark.sql.catalyst.ScalaReflectionLock
import study.spark.util.Utils

import scala.math.Numeric.DoubleAsIfIntegral
import scala.math.{Fractional, Numeric, Ordering}
import scala.reflect.runtime.universe.typeTag


class DoubleType private() extends FractionalType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DoubleType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Double
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val numeric = implicitly[Numeric[Double]]
  private[sql] val fractional = implicitly[Fractional[Double]]
  private[sql] val ordering = new Ordering[Double] {
    override def compare(x: Double, y: Double): Int = Utils.nanSafeCompareDoubles(x, y)
  }
  private[sql] val asIntegral = DoubleAsIfIntegral

  /**
    * The default size of a value of the DoubleType is 8 bytes.
    */
  override def defaultSize: Int = 8

  private[spark] override def asNullable: DoubleType = this
}

case object DoubleType extends DoubleType
