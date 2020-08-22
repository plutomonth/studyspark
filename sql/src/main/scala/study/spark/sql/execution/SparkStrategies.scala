package study.spark.sql.execution

import study.spark.sql.catalyst.planning.QueryPlanner

private[sql] abstract class SparkStrategies extends QueryPlanner[SparkPlan] {

}
