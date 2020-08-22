package study.spark.sql.execution


import study.spark.sql.SQLContext
import study.spark.sql.catalyst.expressions.Attribute
import study.spark.sql.catalyst.plans.physical.Partitioning
import study.spark.sql.catalyst.rules.Rule

/**
 * Performs a shuffle that will result in the desired `newPartitioning`.
 */
case class Exchange(
   var newPartitioning: Partitioning,
   child: SparkPlan,
   @transient coordinator: Option[ExchangeCoordinator]) extends UnaryNode {

  override def output: Seq[Attribute] = child.output
}


/**
 * Ensures that the [[study.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[study.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[Exchange]] Operators where required.  Also ensure that the
 * input partition ordering requirements are met.
 */
private[sql] case class EnsureRequirements(sqlContext: SQLContext) extends Rule[SparkPlan] {
}