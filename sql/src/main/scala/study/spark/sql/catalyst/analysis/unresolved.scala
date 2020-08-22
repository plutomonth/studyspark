package study.spark.sql.catalyst.analysis

import study.spark.sql.AnalysisException
import study.spark.sql.catalyst.errors
import study.spark.sql.catalyst.expressions.{Attribute, LeafExpression, NamedExpression, Unevaluable}
import study.spark.sql.catalyst.trees.TreeNode

/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully
 * resolved.
 */
class UnresolvedException[TreeType <: TreeNode[_]](tree: TreeType, function: String) extends
  errors.TreeNodeException(tree, s"Invalid call to $function on unresolved object", null)

/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...". A [[Star]] gets automatically expanded during analysis.
 */
abstract class Star extends LeafExpression with NamedExpression {
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
}

/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...".
 *
 * This is also used to expand structs. For example:
 * "SELECT record.* from (SELECT struct(a,b,c) as record ...)
 *
 * @param target an optional name that should be the target of the expansion.  If omitted all
 *              targets' columns are produced. This can either be a table name or struct name. This
 *              is a list of identifiers that is the path of the expansion.
 */
case class UnresolvedStar(target: Option[Seq[String]]) extends Star with Unevaluable {

}

/**
 * Holds the name of an attribute that has yet to be resolved.
 */
case class UnresolvedAttribute(nameParts: Seq[String])
  extends Attribute with Unevaluable {

  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")

}

object UnresolvedAttribute {
  /**
   * Creates an [[UnresolvedAttribute]] from a string in an embedded language.  In this case
   * we treat it as a quoted identifier, except for '.', which must be further quoted using
   * backticks if it is part of a column name.
   */
  def quotedString(name: String): UnresolvedAttribute =
    new UnresolvedAttribute(parseAttributeName(name))

  /**
   * Used to split attribute name by dot with backticks rule.
   * Backticks must appear in pairs, and the quoted string must be a complete name part,
   * which means `ab..c`e.f is not allowed.
   * Escape character is not supported now, so we can't use backtick inside name part.
   */
  def parseAttributeName(name: String): Seq[String] = {
    def e = new AnalysisException(s"syntax error in attribute name: $name")
    val nameParts = scala.collection.mutable.ArrayBuffer.empty[String]
    val tmp = scala.collection.mutable.ArrayBuffer.empty[Char]
    var inBacktick = false
    var i = 0
    while (i < name.length) {
      val char = name(i)
      if (inBacktick) {
        if (char == '`') {
          inBacktick = false
          if (i + 1 < name.length && name(i + 1) != '.') throw e
        } else {
          tmp += char
        }
      } else {
        if (char == '`') {
          if (tmp.nonEmpty) throw e
          inBacktick = true
        } else if (char == '.') {
          if (name(i - 1) == '.' || i == name.length - 1) throw e
          nameParts += tmp.mkString
          tmp.clear()
        } else {
          tmp += char
        }
      }
      i += 1
    }
    if (inBacktick) throw e
    nameParts += tmp.mkString
    nameParts.toSeq
  }
}