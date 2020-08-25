package study.spark.sql.catalyst

import java.sql.{Date, Timestamp}

import com.sun.rowset.internal.Row
import study.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import study.spark.sql.types.DecimalType

import scala.collection.Map

object CatalystTypeConverters {


  /**
   *  Converts Scala objects to Catalyst rows / types.
   *
   *  Note: This should be called before do evaluation on Row
   *        (It does not support UDT)
   *  This is used to create an RDD or test results with correct types for Catalyst.
   */
  def convertToCatalyst(a: Any): Any = a match {
/*    case s: String => StringConverter.toCatalyst(s)
    case d: Date => DateConverter.toCatalyst(d)
    case t: Timestamp => TimestampConverter.toCatalyst(t)
    case d: BigDecimal => new DecimalConverter(DecimalType(d.precision, d.scale)).toCatalyst(d)
    case d: JavaBigDecimal => new DecimalConverter(DecimalType(d.precision, d.scale)).toCatalyst(d)
    case seq: Seq[Any] => new GenericArrayData(seq.map(convertToCatalyst).toArray)
    case r: Row => InternalRow(r.toSeq.map(convertToCatalyst): _*)*/
    case arr: Array[Any] => new GenericArrayData(arr.map(convertToCatalyst))
    case m: Map[_, _] =>
      val length = m.size
      val convertedKeys = new Array[Any](length)
      val convertedValues = new Array[Any](length)

      var i = 0
      for ((key, value) <- m) {
        convertedKeys(i) = convertToCatalyst(key)
        convertedValues(i) = convertToCatalyst(value)
        i += 1
      }
      ArrayBasedMapData(convertedKeys, convertedValues)
    case other => other
  }

}
