package study.spark.sql.types

/**
  * :: DeveloperApi ::
  * The data type for Maps. Keys in a map are not allowed to have `null` values.
  *
  * Please use createMapType() to create a specific instance.
  *
  * @param keyType The data type of map keys.
  * @param valueType The data type of map values.
  * @param valueContainsNull Indicates if map values have `null` values.
  */
case class MapType(
    keyType: DataType,
    valueType: DataType,
    valueContainsNull: Boolean) extends DataType {

  /**
   * The default size of a value of the MapType is
   * 100 * (the default size of the key type + the default size of the value type).
   * (We assume that there are 100 elements).
   */
  override def defaultSize: Int = 100 * (keyType.defaultSize + valueType.defaultSize)

  override private[spark] def asNullable: MapType =
    MapType(keyType.asNullable, valueType.asNullable, valueContainsNull = true)

}

object MapType extends AbstractDataType {

  override private[sql] def defaultConcreteType: DataType = apply(NullType, NullType)

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[MapType]
  }

  override private[sql] def simpleString: String = "map"

  /**
   * Construct a [[MapType]] object with the given key type and value type.
   * The `valueContainsNull` is true.
   */
  def apply(keyType: DataType, valueType: DataType): MapType =
    MapType(keyType: DataType, valueType: DataType, valueContainsNull = true)
}
