package study.spark.sql

import study.spark.sql.SQLConf.SQLConfEntry.{booleanConf, intConf, longConf}
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

    def intConf(
                 key: String,
                 defaultValue: Option[Int] = None,
                 doc: String = "",
                 isPublic: Boolean = true): SQLConfEntry[Int] =
      SQLConfEntry(key, defaultValue, { v =>
        try {
          v.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be int, but was $v")
        }
      }, _.toString, doc, isPublic)


    def longConf(
                  key: String,
                  defaultValue: Option[Long] = None,
                  doc: String = "",
                  isPublic: Boolean = true): SQLConfEntry[Long] =
      SQLConfEntry(key, defaultValue, { v =>
        try {
          v.toLong
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be long, but was $v")
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

  val DEFAULT_SIZE_IN_BYTES = longConf(
    "spark.sql.defaultSizeInBytes",
    doc = "The default table size used in query planning. By default, it is set to a larger " +
      "value than `spark.sql.autoBroadcastJoinThreshold` to be more conservative. That is to say " +
      "by default the optimizer will not choose to broadcast a table unless it knows for sure its" +
      "size is small enough.",
    isPublic = false)

  val AUTO_BROADCASTJOIN_THRESHOLD = intConf("spark.sql.autoBroadcastJoinThreshold",
    defaultValue = Some(10 * 1024 * 1024),
    doc = "Configures the maximum size in bytes for a table that will be broadcast to all worker " +
      "nodes when performing a join.  By setting this value to -1 broadcasting can be disabled. " +
      "Note that currently statistics are only supported for Hive Metastore tables where the " +
      "command<code>ANALYZE TABLE &lt;tableName&gt; COMPUTE STATISTICS noscan</code> has been run.")

}

private[sql] class SQLConf extends Serializable with CatalystConf  {
  import SQLConf._

  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient protected[spark] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())


  private[spark] def dataFrameEagerAnalysis: Boolean = getConf(DATAFRAME_EAGER_ANALYSIS)

  private[spark] def autoBroadcastJoinThreshold: Int = getConf(AUTO_BROADCASTJOIN_THRESHOLD)

  protected[spark] override def specializeSingleDistinctAggPlanning: Boolean =
    getConf(SPECIALIZE_SINGLE_DISTINCT_AGG_PLANNING)

  def caseSensitiveAnalysis: Boolean = getConf(CASE_SENSITIVE)

  private[spark] def runSQLOnFile: Boolean = getConf(RUN_SQL_ON_FILES)

  private[spark] def defaultSizeInBytes: Long =
    getConf(DEFAULT_SIZE_IN_BYTES, autoBroadcastJoinThreshold + 1L)


  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`. This is useful when `defaultValue` in SQLConfEntry is not the
   * desired one.
   */
  def getConf[T](entry: SQLConfEntry[T], defaultValue: T): T = {
    require(sqlConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).getOrElse(defaultValue)
  }

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
