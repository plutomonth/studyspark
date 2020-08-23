package study.spark.sql.types

abstract class DataType extends AbstractDataType {

  /**
   * The default size of a value of this data type, used internally for size estimation.
   */
  def defaultSize: Int


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

}
