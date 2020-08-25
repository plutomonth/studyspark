package study.spark.sql.types

/**
  * :: DeveloperApi ::
  * The data type for Maps. Keys in a map are not allowed to have `null` values.
  *
  * Please use [[DataTypes.createMapType()]] to create a specific instance.
  *
  * @param keyType The data type of map keys.
  * @param valueType The data type of map values.
  * @param valueContainsNull Indicates if map values have `null` values.
  */
case class MapType(
    keyType: DataType,
    valueType: DataType,
    valueContainsNull: Boolean) extends DataType {

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
