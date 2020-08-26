package study.spark.sql.execution

import study.spark.rdd.RDD
import study.spark.sql.SQLContext
import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.analysis.MultiInstanceRelation
import study.spark.sql.catalyst.expressions.Attribute
import study.spark.sql.catalyst.plans.logical.LogicalPlan

/** Logical plan node for scanning data from an RDD. */
private[sql] case class LogicalRDD(
                                    output: Seq[Attribute],
                                    rdd: RDD[InternalRow])(sqlContext: SQLContext)
  extends LogicalPlan with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override def newInstance(): LogicalRDD.this.type =
    LogicalRDD(output.map(_.newInstance()), rdd)(sqlContext).asInstanceOf[this.type]
}
