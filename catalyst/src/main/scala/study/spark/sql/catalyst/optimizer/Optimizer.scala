package study.spark.sql.catalyst.optimizer

import study.spark.sql.catalyst.CatalystConf
import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.catalyst.rules.RuleExecutor

abstract class Optimizer(conf: CatalystConf) extends RuleExecutor[LogicalPlan] {

}

case class DefaultOptimizer(conf: CatalystConf) extends Optimizer(conf)