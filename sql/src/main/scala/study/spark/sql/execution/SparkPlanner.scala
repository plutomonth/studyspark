package study.spark.sql.execution

import study.spark.sql.{SQLContext, Strategy}

class SparkPlanner(val sqlContext: SQLContext) extends SparkStrategies {

  def strategies: Seq[Strategy] =
  sqlContext.experimental.extraStrategies ++ (
/*    DataSourceStrategy ::
      DDLStrategy ::
      TakeOrderedAndProject ::
      Aggregation ::
      LeftSemiJoin ::
      EquiJoinSelection ::
      InMemoryScans ::
      BasicOperators ::
      BroadcastNestedLoop ::
      CartesianProduct ::
      DefaultJoin :: */
      Nil)
}
