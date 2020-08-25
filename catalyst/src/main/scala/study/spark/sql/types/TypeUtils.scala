package study.spark.sql.types

object TypeUtils {

  def compareBinary(x: Array[Byte], y: Array[Byte]): Int = {
    for (i <- 0 until x.length; if i < y.length) {
      val res = x(i).compareTo(y(i))
      if (res != 0) return res
    }
    x.length - y.length
  }

}
