package study.spark.sql.types

object ArrayType extends AbstractDataType {
  /** Construct a [[ArrayType]] object with the given element type. The `containsNull` is true. */
  def apply(elementType: DataType): ArrayType = ArrayType(elementType, containsNull = true)

  override private[sql] def defaultConcreteType: DataType = ArrayType(NullType, containsNull = true)

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[ArrayType]
  }

  override private[sql] def simpleString: String = "array"
}

case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType {

  /**
    * The default size of a value of the ArrayType is 100 * the default size of the element type.
    * (We assume that there are 100 elements).
    */
  override def defaultSize: Int = 100 * elementType.defaultSize

  override private[spark] def asNullable: ArrayType =
    ArrayType(elementType.asNullable, containsNull = true)
}