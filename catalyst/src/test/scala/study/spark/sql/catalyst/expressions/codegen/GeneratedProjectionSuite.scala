package study.spark.sql.catalyst.expressions.codegen


import study.spark.SparkFunSuite
import study.spark.sql.catalyst.InternalRow
import study.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import study.spark.sql.catalyst.util.GenericArrayData
import study.spark.sql.types.{ArrayType, BinaryType, DataType}

/**
 * A test suite for generated projections
 */
class GeneratedProjectionSuite extends SparkFunSuite {

  test("generated unsafe projection with array of binary") {
    val row = InternalRow(
      Array[Byte](1, 2),
      new GenericArrayData(Array(Array[Byte](1, 2), null, Array[Byte](3, 4))))
    val fields = (BinaryType :: ArrayType(BinaryType) :: Nil).toArray[DataType]

    val unsafeProj = UnsafeProjection.create(fields)
    val unsafeRow: UnsafeRow = unsafeProj(row)
    assert(java.util.Arrays.equals(unsafeRow.getBinary(0), Array[Byte](1, 2)))
    assert(java.util.Arrays.equals(unsafeRow.getArray(1).getBinary(0), Array[Byte](1, 2)))
    assert(unsafeRow.getArray(1).isNullAt(1))
    assert(unsafeRow.getArray(1).getBinary(1) === null)
    assert(java.util.Arrays.equals(unsafeRow.getArray(1).getBinary(2), Array[Byte](3, 4)))

    val safeProj = FromUnsafeProjection(fields)
    val row2 = safeProj(unsafeRow)
    assert(row2 === row)
  }

  }
