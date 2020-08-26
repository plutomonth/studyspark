package study.spark.sql.catalyst.expressions

import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import study.spark.sql.types.{DataType, StructType}

/**
 * A projection that returns UnsafeRow.
 */
abstract class UnsafeProjection extends Projection {
  override def apply(row: InternalRow): UnsafeRow
}


object UnsafeProjection {

  /**
   * Returns an UnsafeProjection for given StructType.
   */
  def create(schema: StructType): UnsafeProjection = create(schema.fields.map(_.dataType))

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
   */
  def create(fields: Array[DataType]): UnsafeProjection = {
    create(fields.zipWithIndex.map(x => new BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns an UnsafeProjection for given sequence of Expressions (bounded).
   */
  def create(exprs: Seq[Expression]): UnsafeProjection = {
    val unsafeExprs = exprs.map(_ transform {
      case CreateStruct(children) => CreateStructUnsafe(children)
      case CreateNamedStruct(children) => CreateNamedStructUnsafe(children)
    })
    GenerateUnsafeProjection.generate(unsafeExprs)
  }
}

