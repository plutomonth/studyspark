package study.spark.sql.catalyst.expressions

/**
 * An [[Expression]] that is named.
 */
trait NamedExpression extends Expression {

}

abstract class Attribute extends LeafExpression with NamedExpression {

}