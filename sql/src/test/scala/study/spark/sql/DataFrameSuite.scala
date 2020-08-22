package study.spark.sql

// scalastyle:off
import org.scalatest.FunSuite
import study.spark.sql.test.SQLTestData

class DataFrameSuite extends FunSuite with SQLTestData{



  // scalastyle:on
  test("SPARK-7133: Implement struct, array, and map field accessor") {
    assert(complexData.filter(complexData("a")(0) === 2).count() == 1)
    assert(complexData.filter(complexData("m")("1") === 1).count() == 1)
    assert(complexData.filter(complexData("s")("key") === 1).count() == 1)
    assert(complexData.filter(complexData("m")(complexData("s")("value")) === 1).count() == 1)
    assert(complexData.filter(complexData("a")(complexData("s")("key")) === 1).count() == 1)
  }

}
