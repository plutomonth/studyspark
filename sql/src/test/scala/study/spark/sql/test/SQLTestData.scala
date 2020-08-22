package study.spark.sql.test

import study.spark.sql.SQLContext

private[sql] trait SQLTestData { self =>
  protected def sqlContext: SQLContext
  import SQLTestData._
  protected lazy val complexData: DataFrame = {
    val df = sqlContext.sparkContext.parallelize(
      ComplexData(Map("1" -> 1), TestData(1, "1"), Seq(1, 1, 1), true) ::
        ComplexData(Map("2" -> 2), TestData(2, "2"), Seq(2, 2, 2), false) ::
        Nil).toDF()
    df.registerTempTable("complexData")
    df
  }
  /**
   * Initialize all test data such that all temp tables are properly registered.
   */
  def loadTestData(): Unit = {
    complexData
  }

}

/**
 * Case classes used in test data.
 */
private[sql] object SQLTestData {
  case class TestData(key: Int, value: String)
  case class ComplexData(m: Map[String, Int], s: TestData, a: Seq[Int], b: Boolean)
}