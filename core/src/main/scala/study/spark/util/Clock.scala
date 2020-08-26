package study.spark.util

/**
 * An interface to represent clocks, so that they can be mocked out in unit tests.
 */
private[spark] trait Clock {
  def getTimeMillis(): Long
  def waitTillTime(targetTime: Long): Long
}