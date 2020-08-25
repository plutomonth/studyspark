package study.spark.sql.catalyst.util

/**
  * Build a map with String type of key, and it also supports either key case
  * sensitive or insensitive.
  */
object StringKeyHashMap {
  def apply[T](caseSensitive: Boolean): StringKeyHashMap[T] = caseSensitive match {
    case false => new StringKeyHashMap[T](_.toLowerCase)
    case true => new StringKeyHashMap[T](identity)
  }
}


class StringKeyHashMap[T](normalizer: (String) => String) {
  private val base = new collection.mutable.HashMap[String, T]()

  def apply(key: String): T = base(normalizer(key))

  def get(key: String): Option[T] = base.get(normalizer(key))

  def put(key: String, value: T): Option[T] = base.put(normalizer(key), value)

  def remove(key: String): Option[T] = base.remove(normalizer(key))

  def iterator: Iterator[(String, T)] = base.toIterator
}
