package study.spark.sql.catalyst.plans.logical

import study.spark.Logging
import study.spark.sql.catalyst.plans.QueryPlan

abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {

}
