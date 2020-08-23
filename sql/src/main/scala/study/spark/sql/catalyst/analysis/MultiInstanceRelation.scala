package study.spark.sql.catalyst.analysis

import study.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A trait that should be mixed into query operators where an single instance might appear multiple
 * times in a logical query plan.  It is invalid to have multiple copies of the same attribute
 * produced by distinct operators in a query tree as this breaks the guarantee that expression
 * ids, which are used to differentiate attributes, are unique.
 *
 * During analysis, operators that include this trait may be asked to produce a new version
 * of itself with globally unique expression ids.
 */
trait MultiInstanceRelation {
  def newInstance(): LogicalPlan
}