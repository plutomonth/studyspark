package study.spark.sql

import study.spark.Logging
import study.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedStar}
import study.spark.sql.catalyst.expressions.Expression

private[sql] object Column {

  def apply(colName: String): Column = new Column(colName)

  def apply(expr: Expression): Column = new Column(expr)

  def unapply(col: Column): Option[Expression] = Some(col.expr)
}

class Column(protected[sql] val expr: Expression) extends Logging {
  def this(name: String) = this(name match {
    case "*" => UnresolvedStar(None)
    case _ if name.endsWith(".*") => {
      val parts = UnresolvedAttribute.parseAttributeName(name.substring(0, name.length - 2))
      UnresolvedStar(Some(parts))
    }
    case _ => UnresolvedAttribute.quotedString(name)
  })
}
