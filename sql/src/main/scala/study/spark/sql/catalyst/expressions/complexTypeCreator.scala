package study.spark.sql.catalyst.expressions

import study.spark.sql.types.{Metadata, StructField, StructType}

/**
 * Returns a Row containing the evaluation of all children expressions.
 */
case class CreateStruct(children: Seq[Expression]) extends Expression {
  override def nullable: Boolean = false

  override lazy val dataType: StructType = {
    val fields = children.zipWithIndex.map { case (child, idx) =>
      child match {
        case ne: NamedExpression =>
          StructField(ne.name, ne.dataType, ne.nullable, ne.metadata)
        case _ =>
          StructField(s"col${idx + 1}", child.dataType, child.nullable, Metadata.empty)
      }
    }
    StructType(fields)
  }
}

/**
 * Returns a Row containing the evaluation of all children expressions. This is a variant that
 * returns UnsafeRow directly. The unsafe projection operator replaces [[CreateStruct]] with
 * this expression automatically at runtime.
 */
case class CreateStructUnsafe(children: Seq[Expression]) extends Expression {
  override def nullable: Boolean = false

  override lazy val dataType: StructType = {
    val fields = children.zipWithIndex.map { case (child, idx) =>
      child match {
        case ne: NamedExpression =>
          StructField(ne.name, ne.dataType, ne.nullable, ne.metadata)
        case _ =>
          StructField(s"col${idx + 1}", child.dataType, child.nullable, Metadata.empty)
      }
    }
    StructType(fields)
  }
}

/**
 * Creates a struct with the given field names and values
 *
 * @param children Seq(name1, val1, name2, val2, ...)
 */
case class CreateNamedStruct(children: Seq[Expression]) extends Expression {
  override def nullable: Boolean = false

  private lazy val (nameExprs, valExprs) =
    children.grouped(2).map { case Seq(name, value) => (name, value) }.toList.unzip

  private lazy val names = nameExprs.map(_.eval(EmptyRow))

  override lazy val dataType: StructType = {
    val fields = names.zip(valExprs).map { case (name, valExpr) =>
      StructField(name.asInstanceOf[UTF8String].toString,
        valExpr.dataType, valExpr.nullable, Metadata.empty)
    }
    StructType(fields)
  }


}