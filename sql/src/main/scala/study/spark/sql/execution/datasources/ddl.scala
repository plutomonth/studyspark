package study.spark.sql.execution.datasources

import study.spark.sql.SaveMode
import study.spark.sql.catalyst.TableIdentifier
import study.spark.sql.catalyst.expressions.Attribute
import study.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

/**
 * A node used to support CTAS statements and saveAsTable for the data source API.
 * This node is a [[UnaryNode]] instead of a [[Command]] because we want the analyzer
 * can analyze the logical plan that will be used to populate the table.
 * So, [[PreWriteCheck]] can detect cases that are not allowed.
 */
case class CreateTableUsingAsSelect(
   tableIdent: TableIdentifier,
   provider: String,
   temporary: Boolean,
   partitionColumns: Array[String],
   mode: SaveMode,
   options: Map[String, String],
   child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = Seq.empty[Attribute]
}