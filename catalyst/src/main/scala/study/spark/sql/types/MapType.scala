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
