package study.spark.sql.types

class CalendarIntervalType private() extends DataType {

  override def defaultSize: Int = 16

  private[spark] override def asNullable: CalendarIntervalType = this
}

case object CalendarIntervalType extends CalendarIntervalType