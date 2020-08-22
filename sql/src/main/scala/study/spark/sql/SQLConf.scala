package study.spark.sql

import study.spark.sql.SQLConf.SQLConfEntry.booleanConf
import study.spark.sql.catalyst.CatalystConf


private[spark] object SQLConf {
  private val sqlConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, SQLConfEntry[_]]())

  /**
   * An entry contains all meta information for a configuration.
   *
   * @param key the key for the configuration
   * @param defaultValue the default value for the configuration
   * @param valueConverter how to convert a string to the value. It should throw an exception if the
   *                       string does not have the required format.
   * @param stringConverter how to convert a value to a string that the user can use it as a valid
   *                        string value. It's usually `toString`. But sometimes, a custom converter
   *                        is necessary. E.g., if T is List[String], `a, b, c` is better than
   *                        `List(a, b, c)`.
   * @param doc the document for the configuration
   * @param isPublic if this configuration is public to the user. If it's `false`, this
   *                 configuration is only used internally and we should not expose it to the user.
   * @tparam T the value type
   */
  private[sql] class SQLConfEntry[T] private(
                                              val key: String,
                                              val defaultValue: Option[T],
                                              val valueConverter: String => T,
                                              val stringConverter: T => String,
                                              val doc: String,
                                              val isPublic: Boolean) {

    def defaultValueString: String = defaultValue.map(stringConverter).getOrElse("<undefined>")

    override def toString: String = {
      s"SQLConfEntry(key = $key, defaultValue=$defaultValueString, doc=$doc, isPublic = $isPublic)"
    }
  }
  private[sql] object SQLConfEntry {
    private def apply[T](
                          key: String,
                          defaultValue: Option[T],
                          valueConverter: String => T,
                          stringConverter: T => String,
                          doc: String,
                          isPublic: Boolean): SQLConfEntry[T] =
      sqlConfEntries.synchronized {
        if (sqlConfEntries.containsKey(key)) {
          throw new IllegalArgumentException(s"Duplicate SQLConfEntry. $key has been registered")
        }
        val entry =
          new SQLConfEntry[T](key, defaultValue, valueConverter, stringConverter, doc, isPublic)
        sqlConfEntries.put(key, entry)
        entry
      }


    def booleanConf(
                     key: String,
                     defaultValue: Option[Boolean] = None,
                     doc: String = "",
                     isPublic: Boolean = true): SQLConfEntry[Boolean] =
      SQLConfEntry(key, defaultValue, { v =>
        try {
          v.toBoolean
        } catch {
          case _: IllegalArgumentException =>
            throw new IllegalArgumentException(s"$key should be boolean, but was $v")
        }
      }, _.toString, doc, isPublic)

  }
  val CASE_SENSITIVE = booleanConf("spark.sql.caseSensitive",
    defaultValue = Some(true),
    doc = "Whether the query analyzer should be case sensitive or not.")


  // Whether to perform eager analysis when constructing a dataframe.
  // Set to false when debugging requires the ability to look at invalid query plans.
  val DATAFRAME_EAGER_ANALYSIS = booleanConf(
    "spark.sql.eagerAnalysis",
    defaultValue = Some(true),
    doc = "When true, eagerly applies query analysis on DataFrame operations.",
    isPublic = false)

  val SPECIALIZE_SINGLE_DISTINCT_AGG_PLANNING =
    booleanConf("spark.sql.specializeSingleDistinctAggPlanning",
      defaultValue = Some(false),
      isPublic = false,
      doc = "When true, if a query only has a single distinct column and it has " +
        "grouping expressions, we will use our planner rule to handle this distinct " +
        "column (other cases are handled by DistinctAggregationRewriter). " +
        "When false, we will always use DistinctAggregationRewriter to plan " +
        "aggregation queries with DISTINCT keyword. This is an internal flag that is " +
        "used to benchmark the performance impact of using DistinctAggregationRewriter to " +
        "plan aggregation queries with a single distinct column.")

  val RUN_SQL_ON_FILES = booleanConf("spark.sql.runSQLOnFiles",
    defaultValue = Some(true),
    isPublic = false,
    doc = "When true, we could use `datasource`.`path` as table in SQL query"
  )
}

private[sql] class SQLConf extends Serializable with CatalystConf  {
  import SQLConf._

  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient protected[spark] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())


  private[spark] def dataFrameEagerAnalysis: Boolean = getConf(DATAFRAME_EAGER_ANALYSIS)

  protected[spark] override def specializeSingleDistinctAggPlanning: Boolean =
    getConf(SPECIALIZE_SINGLE_DISTINCT_AGG_PLANNING)

  def caseSensitiveAnalysis: Boolean = getConf(CASE_SENSITIVE)

  private[spark] def runSQLOnFile: Boolean = getConf(RUN_SQL_ON_FILES)

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[SQLConfEntry]].
   */
  def getConf[T](entry: SQLConfEntry[T]): T = {
    require(sqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).orElse(entry.defaultValue).
      getOrElse(throw new NoSuchElementException(entry.key))
  }

}
