package study.spark.sql.types

private[sql] object ObjectType extends AbstractDataType {
  override private[sql] def defaultConcreteType: DataType =
    throw new UnsupportedOperationException("null literals can't be casted to ObjectType")

  // No casting or comparison is supported.
  override private[sql] def acceptsType(other: DataType): Boolean = false

  override private[sql] def simpleString: String = "Object"
}

/**
 * Represents a JVM object that is passing through Spark SQL expression evaluation.  Note this
 * is only used internally while converting into the internal format and is not intended for use
 * outside of the execution engine.
 */
private[sql] case class ObjectType(cls: Class[_]) extends DataType {
  override def defaultSize: Int =
    throw new UnsupportedOperationException("No size estimation available for objects.")

  def asNullable: DataType = this
}
