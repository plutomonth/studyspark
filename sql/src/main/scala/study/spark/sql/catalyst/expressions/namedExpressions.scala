package study.spark.sql.catalyst.expressions

import java.util.UUID

import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import study.spark.sql.types.{DataType, Metadata}

object NamedExpression {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  private[expressions] val jvmId = UUID.randomUUID()
  def newExprId: ExprId = ExprId(curId.getAndIncrement(), jvmId)
  def unapply(expr: NamedExpression): Option[(String, DataType)] = Some(expr.name, expr.dataType)
}

/**
 * An [[Expression]] that is named.
 */
trait NamedExpression extends Expression {
  def name: String
  def exprId: ExprId
  /** Returns the metadata when an expression is a reference to another expression with metadata. */
  def metadata: Metadata = Metadata.empty
}

abstract class Attribute extends LeafExpression with NamedExpression {
  def newInstance(): Attribute

  def withQualifiers(newQualifiers: Seq[String]): Attribute
}

/**
 * A globally unique id for a given named expression.
 * Used to identify which attribute output by a relation is being
 * referenced in a subsequent computation.
 *
 * The `id` field is unique within a given JVM, while the `uuid` is used to uniquely identify JVMs.
 */
case class ExprId(id: Long, jvmId: UUID)

/**
 * Used to assign a new name to a computation.
 * For example the SQL expression "1 + 1 AS a" could be represented as follows:
 *  Alias(Add(Literal(1), Literal(1)), "a")()
 *
 * Note that exprId and qualifiers are in a separate parameter list because
 * we only pattern match on child and name.
 *
 * @param child the computation being performed
 * @param name the name to be associated with the result of computing [[child]].
 * @param exprId A globally unique id used to check if an [[AttributeReference]] refers to this
 *               alias. Auto-assigned if left blank.
 * @param explicitMetadata Explicit metadata associated with this alias that overwrites child's.
 */
case class Alias(child: Expression, name: String)(
  val exprId: ExprId = NamedExpression.newExprId,
  val qualifiers: Seq[String] = Nil,
  val explicitMetadata: Option[Metadata] = None)
  extends UnaryExpression with NamedExpression {

  override def eval(input: InternalRow): Any = child.eval(input)

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = ""

}

/**
 * A reference to an attribute produced by another operator in the tree.
 *
 * @param name The name of this attribute, should only be used during analysis or for debugging.
 * @param dataType The [[DataType]] of this attribute.
 * @param nullable True if null is a valid value for this attribute.
 * @param metadata The metadata of this attribute.
 * @param exprId A globally unique id used to check if different AttributeReferences refer to the
 *               same attribute.
 * @param qualifiers a list of strings that can be used to referred to this attribute in a fully
 *                   qualified way. Consider the examples tableName.name, subQueryAlias.name.
 *                   tableName and subQueryAlias are possible qualifiers.
 */
case class AttributeReference(
     name: String,
     dataType: DataType,
     nullable: Boolean = true,
     override val metadata: Metadata = Metadata.empty)(
     val exprId: ExprId = NamedExpression.newExprId,
     val qualifiers: Seq[String] = Nil)
  extends Attribute with Unevaluable {

  }