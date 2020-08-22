package study.spark.sql.catalyst.expressions.aggregate

import study.spark.sql.catalyst.expressions.Expression

case class Count(children: Seq[Expression]) extends DeclarativeAggregate  {
  override def nullable: Boolean = false
}


object Count {
  def apply(child: Expression): Count = Count(child :: Nil)
}
