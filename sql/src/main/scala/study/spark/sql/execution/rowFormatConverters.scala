package study.spark.sql.execution

import study.spark.rdd.RDD
import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import study.spark.sql.catalyst.rules.Rule


/**
 * Converts Java-object-based rows into [[UnsafeRow]]s.
 */
case class ConvertToUnsafe(child: SparkPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      val convertToUnsafe = UnsafeProjection.create(child.schema)
      iter.map(convertToUnsafe)
    }
  }
}


private[sql] object EnsureRowFormats extends Rule[SparkPlan] {

}