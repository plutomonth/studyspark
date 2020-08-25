package study.spark.sql.types

class NullType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "NullType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 1

  private[spark] override def asNullable: NullType = this
}

case object NullType extends NullType
