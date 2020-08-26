package study.spark.sql.catalyst.expressions.aggregate

import study.spark.sql.catalyst.expressions.Expression
import study.spark.sql.types.{DataType, LongType}

case class Count(children: Seq[Expression]) extends DeclarativeAggregate  {
  override def nullable: Boolean = false

  // Return data type.
  override def dataType: DataType = LongType
}


object Count {
  def apply(child: Expression): Count = Count(child :: Nil)
}
