package study.spark.sql.execution.datasources

import study.spark.sql.SQLContext
import study.spark.sql.catalyst.analysis.Catalog
import study.spark.sql.catalyst.plans.logical.LogicalPlan
import study.spark.sql.catalyst.rules.Rule


/**
  * Try to replaces [[UnresolvedRelation]]s with [[ResolvedDataSource]].
  */
private[sql] class ResolveDataSource(sqlContext: SQLContext) extends Rule[LogicalPlan] {

}
/**
  * A rule to do pre-insert data type casting and field renaming. Before we insert into
  * an [[InsertableRelation]], we will use this rule to make sure that
  * the columns to be inserted have the correct data type and fields have the correct names.
  */
private[sql] object PreInsertCastAndRename extends Rule[LogicalPlan] {

}

/**
  * A rule to do various checks before inserting into or writing to a data source table.
  */
private[sql] case class PreWriteCheck(catalog: Catalog) extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {

  }
}