package study.spark.sql.execution


import study.spark.rdd.RDD
import study.spark.sql.SQLContext
import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.expressions.Attribute
import study.spark.sql.catalyst.plans.physical.Partitioning
import study.spark.sql.catalyst.rules.Rule
import study.spark.sql.catalyst.errors.attachTree

/**
 * Performs a shuffle that will result in the desired `newPartitioning`.
 */
case class Exchange(
   var newPartitioning: Partitioning,
   child: SparkPlan,
   @transient coordinator: Option[ExchangeCoordinator]) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  protected override def doExecute(): RDD[InternalRow] = attachTree(this , "execute") {
    coordinator match {
      case Some(exchangeCoordinator) =>
        val shuffleRDD = exchangeCoordinator.postShuffleRDD(this)
        assert(shuffleRDD.partitions.length == newPartitioning.numPartitions)
        shuffleRDD
      case None =>
        val shuffleDependency = prepareShuffleDependency()
        preparePostShuffleRDD(shuffleDependency)
    }
  }
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