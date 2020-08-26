package study.spark.sql.catalyst.plans.logical

import study.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import study.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, LeftSemi, RightOuter}

case class InsertIntoTable(
                            table: LogicalPlan,
                            partition: Map[String, Option[String]],
                            child: LogicalPlan,
                            overwrite: Boolean,
                            ifNotExists: Boolean)
  extends LogicalPlan {

  override def children: Seq[LogicalPlan] = child :: Nil

  override def output: Seq[Attribute] = Seq.empty
}

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
}

case class Subquery(alias: String, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output.map(_.withQualifiers(alias :: Nil))
}

case class Filter(condition: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class Aggregate(
                      groupingExpressions: Seq[Expression],
                      aggregateExpressions: Seq[NamedExpression],
                      child: LogicalPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

}

/**
 * A relation with one row. This is used in "SELECT ..." without a from clause.
 */
case object OneRowRelation extends LeafNode {
  override def output: Seq[Attribute] = Nil

  /**
   * Computes [[Statistics]] for this plan. The default implementation assumes the output
   * cardinality is the product of of all child plan's cardinality, i.e. applies in the case
   * of cartesian joins.
   *
   * [[LeafNode]]s must override this.
   */
  override def statistics: Statistics = Statistics(sizeInBytes = 1)
}

case class Limit(limitExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override lazy val statistics: Statistics = {
    val limit = limitExpr.eval().asInstanceOf[Int]
    val sizeInBytes = (limit: Long) * output.map(a => a.dataType.defaultSize).sum
    Statistics(sizeInBytes = sizeInBytes)
  }

  override def output: Seq[Attribute] = child.output
}

/**
 * Returns a new logical plan that dedups input rows.
 */
case class Distinct(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}


/**
 * A container for holding named common table expressions (CTEs) and a query plan.
 * This operator will be removed during analysis and the relations will be substituted into child.
 *
 * @param child        The final query of this CTE.
 * @param cteRelations Queries that this CTE defined,
 *                     key is the alias of the CTE definition,
 *                     value is the CTE definition.
 */
case class With(child: LogicalPlan, cteRelations: Map[String, Subquery]) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}


case class Intersect(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right)

case class Except(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right) {
  /** We don't use right.output because those rows get excluded from the set. */
  override def output: Seq[Attribute] = left.output
}

abstract class SetOperation(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
  override def output: Seq[Attribute] =
    left.output.zip(right.output).map { case (leftAttr, rightAttr) =>
      leftAttr.withNullability(leftAttr.nullable || rightAttr.nullable)
    }
}

private[sql] object SetOperation {
  def unapply(p: SetOperation): Option[(LogicalPlan, LogicalPlan)] = Some((p.left, p.right))
}


case class Union(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right) {

  override def statistics: Statistics = {
    val sizeInBytes = left.statistics.sizeInBytes + right.statistics.sizeInBytes
    Statistics(sizeInBytes = sizeInBytes)
  }
}

case class Join(
                 left: LogicalPlan,
                 right: LogicalPlan,
                 joinType: JoinType,
                 condition: Option[Expression]) extends BinaryNode {

  override def output: Seq[Attribute] = {
    joinType match {
      case LeftSemi =>
        left.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case _ =>
        left.output ++ right.output
    }
  }
}


/**
 * @param order  The ordering expressions
 * @param global True means global sorting apply for entire data set,
 *               False means sorting only apply within the partition.
 * @param child  Child logical plan
 */
case class Sort(
                 order: Seq[SortOrder],
                 global: Boolean,
                 child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
