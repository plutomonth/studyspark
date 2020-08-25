package study.spark.sql.catalyst.util

import java.sql.{Date, Timestamp}
import java.util.{Calendar, TimeZone}

object DateTimeUtils {
  // we use Int and Long internally to represent [[DateType]] and [[TimestampType]]
  type SQLDate = Int
  type SQLTimestamp = Long

  final val JULIAN_DAY_OF_EPOCH = 2440588
  final val SECONDS_PER_DAY = 60 * 60 * 24L
  final val MICROS_PER_SECOND = 1000L * 1000L
  final val NANOS_PER_SECOND = MICROS_PER_SECOND * 1000L
  final val MICROS_PER_DAY = MICROS_PER_SECOND * SECONDS_PER_DAY

  final val MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L

  // Java TimeZone has no mention of thread safety. Use thread local instance to be safe.
  private val threadLocalLocalTimeZone = new ThreadLocal[TimeZone] {
    override protected def initialValue: TimeZone = {
      Calendar.getInstance.getTimeZone
    }
  }

  /**
   * Returns the number of micros since epoch from java.sql.Timestamp.
   */
  def fromJavaTimestamp(t: Timestamp): SQLTimestamp = {
    if (t != null) {
      t.getTime() * 1000L + (t.getNanos().toLong / 1000) % 1000L
    } else {
      0L
    }
  }


  /**
   * Returns the number of days since epoch from from java.sql.Date.
   */
  def fromJavaDate(date: Date): SQLDate = {
    millisToDays(date.getTime)
  }

  // we should use the exact day as Int, for example, (year, month, day) -> day
  def millisToDays(millisUtc: Long): SQLDate = {
    // SPARK-6785: use Math.floor so negative number of days (dates before 1970)
    // will correctly work as input for function toJavaDate(Int)
    val millisLocal = millisUtc + threadLocalLocalTimeZone.get().getOffset(millisUtc)
    Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
  }

}
