package study.spark.sql.types

abstract class DataType extends AbstractDataType {

  /**
   * The default size of a value of this data type, used internally for size estimation.
   */
  def defaultSize: Int


  /**
    * Check if `this` and `other` are the same data type when ignoring nullability
    * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
    */
  private[spark] def sameType(other: DataType): Boolean =
    DataType.equalsIgnoreNullability(this, other)


  /** Name of the type used in JSON serialization. */
  def typeName: String = {
    this.getClass.getSimpleName.stripSuffix("$").stripSuffix("Type").stripSuffix("UDT").toLowerCase
  }

  /** Readable string representation for the type. */
  def simpleString: String = typeName


  /**
   * Returns the same data type but set all nullability fields are true
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  private[spark] def asNullable: DataType

  override private[sql] def defaultConcreteType: DataType = this

  override private[sql] def acceptsType(other: DataType): Boolean = sameType(other)
}

object DataType {

  /**
    * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
    */
  private[types] def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
    (left, right) match {
      case (ArrayType(leftElementType, _), ArrayType(rightElementType, _)) =>
        equalsIgnoreNullability(leftElementType, rightElementType)
      case (MapType(leftKeyType, leftValueType, _), MapType(rightKeyType, rightValueType, _)) =>
        equalsIgnoreNullability(leftKeyType, rightKeyType) &&
          equalsIgnoreNullability(leftValueType, rightValueType)
      case (StructType(leftFields), StructType(rightFields)) =>
        leftFields.length == rightFields.length &&
          leftFields.zip(rightFields).forall { case (l, r) =>
            l.name == r.name && equalsIgnoreNullability(l.dataType, r.dataType)
          }
      case (l, r) => l == r
    }
  }


}