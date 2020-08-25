package study.spark.sql.types

abstract class UserDefinedType[UserType] extends DataType with Serializable {

  /** Underlying storage type for this UDT */
  def sqlType: DataType
}
